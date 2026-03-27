"""
流水线编排器 - 管理完整的 PDF → JSON 处理管道。

流程:
1. 扫描PDF文件
2. 注册到检查点
3. 分批上传到 MinerU API
4. 轮询处理结果
5. 下载结果包并读取 content_list
6. 转换为目标JSON格式

多API并发支持:
- 多个API同时处理不同的文件批次
- API-1处理文件1-50，API-2处理文件51-100
- 某个API完成批次后自动获取下一批任务
- 达到配额后自动停止，其他API继续处理
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from tqdm import tqdm

from .api_client import AllAPIKeysExhaustedError, MinerUAPIClient
from .checkpoint import Checkpoint
from .config import AppConfig, SingleApiConfig
from .converter import convert_content_blocks, save_paper_json
from .failed_db import FailedDB
from .models import FileRecord, FileState
from .scanner import scan_pdfs

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """上传结果"""
    batch_id: str
    uploaded_recs: list[FileRecord]  # 上传成功的文件列表


class APIWorker:
    """API工作器 - 独立处理分配给它的任务
    
    每个API独立从共享文件列表获取批次：
    - 使用共享索引，每个API获取连续的batch_size个文件
    - API-1 和 API-2 同时处理不同的文件批次
    - 完成后继续获取下一批
    """

    def __init__(
        self,
        worker_id: int,
        api_config: SingleApiConfig,
        config: AppConfig,
        checkpoint: Checkpoint,
        progress_bar: tqdm,
        shutdown_event: asyncio.Event,
        output_dir_selector,
        pending_files: list[FileRecord],
        file_index: list,  # 使用list包装int，实现引用传递
        uploading_flags: list,  # 共享的上传状态标志列表 [bool, bool, ...]
        uploading_lock: asyncio.Lock,  # 上传状态锁
        turn_index: list,  # 当前轮到的API索引，[0]=第一轮顺序索引，[-1]=抢占模式
        num_workers: int,  # API总数
    ):
        self.worker_id = worker_id
        self.api_config = api_config
        self.config = config
        self.checkpoint = checkpoint
        self.progress_bar = progress_bar
        self.shutdown_event = shutdown_event
        self.output_dir_selector = output_dir_selector
        self.pending_files = pending_files
        self.file_index = file_index  # 共享索引 [int]
        self._index_lock = asyncio.Lock()  # 索引锁
        self._uploading_flags = uploading_flags  # 共享的上传状态标志
        self._uploading_lock = uploading_lock  # 上传状态锁
        self._turn_index = turn_index  # 轮次索引
        self._num_workers = num_workers  # API总数

        # 创建独立的API客户端
        self._api_client: Optional[MinerUAPIClient] = None
        self._processed_count = 0
        self._stopped = False

    @property
    def api_client(self) -> MinerUAPIClient:
        """惰性创建 API 客户端"""
        if self._api_client is None:
            # 创建使用该worker的API key的客户端
            self.config.api.api_key = self.api_config.api_key
            self._api_client = MinerUAPIClient(self.config)
        return self._api_client

    @property
    def name(self) -> str:
        return self.api_config.name if self.api_config.name else f"API-{self.worker_id + 1}"

    def _is_other_uploading(self) -> bool:
        """检查是否有其他API正在上传"""
        for i, flag in enumerate(self._uploading_flags):
            if i != self.worker_id and flag:
                return True
        return False

    async def _set_uploading(self, uploading: bool) -> None:
        """设置当前API的上传状态"""
        async with self._uploading_lock:
            self._uploading_flags[self.worker_id] = uploading

    async def _wait_for_other_uploads(self) -> None:
        """等待其他所有API上传完成"""
        while self._is_other_uploading():
            await asyncio.sleep(0.5)

    async def get_remaining_quota(self) -> int:
        """获取剩余配额，-1表示无限制"""
        if self.api_config.daily_limit <= 0:
            return -1  # 无限制
        today_done = await self.checkpoint.get_today_done_count(self.worker_id)
        remaining = self.api_config.daily_limit - today_done
        return max(0, remaining)

    async def run(self) -> int:
        """运行worker，独立获取批次并处理"""
        logger.info(f"{self.name} 开始工作")
        batch_size = self.config.api.batch_size

        while not self.shutdown_event.is_set() and not self._stopped:
            # 检查配额
            remaining = await self.get_remaining_quota()
            if remaining == 0:
                logger.info(f"{self.name} 配额已用完，停止处理")
                break

            # 原子操作：等待轮次、获取批次、设置上传状态
            async with self._uploading_lock:
                # 等待轮次（第一轮：按 turn_index 顺序，后续：等待其他API完成）
                while True:
                    should_wait = False
                    
                    # 第一轮：还没轮到自己
                    if self._turn_index[0] >= 0 and self._turn_index[0] != self.worker_id:
                        should_wait = True
                        logger.debug(f"{self.name} 等待轮次 (turn_index={self._turn_index[0]})")
                    
                    # 有其他API正在上传
                    if self._is_other_uploading():
                        should_wait = True
                        logger.debug(f"{self.name} 等待其他API上传完成")
                    
                    if not should_wait:
                        break
                    
                    self._uploading_lock.release()
                    await asyncio.sleep(0.3)
                    await self._uploading_lock.acquire()
                
                # 获取批次
                batch = await self._get_next_batch(batch_size, remaining)
                
                if not batch:
                    # 没有更多任务
                    logger.info(f"{self.name} 没有更多任务，退出")
                    break
                
                # 设置上传状态为正在上传
                self._uploading_flags[self.worker_id] = True
                
                # 第一轮：轮次+1
                if self._turn_index[0] >= 0:
                    self._turn_index[0] += 1
                    if self._turn_index[0] >= self._num_workers:
                        self._turn_index[0] = -1  # 进入抢占模式
                        logger.info("第一轮完成，进入抢占模式")

            # 上传文件（上传完成后立即释放锁）
            upload_result = None
            try:
                upload_result = await self._upload_batch(batch)
            except AllAPIKeysExhaustedError:
                logger.warning(f"{self.name} API Key 不可用，停止处理")
                self._stopped = True
                async with self._uploading_lock:
                    self._uploading_flags[self.worker_id] = False
                continue
            except Exception as e:
                logger.error(f"{self.name} 上传失败: {e}")
                for rec in batch:
                    await self.checkpoint.update_state(
                        rec.data_id,
                        FileState.FAILED,
                        error_msg=str(e),
                        increment_attempts=True,
                        api_key_index=self.worker_id,
                    )
                self.progress_bar.update(len(batch))
                async with self._uploading_lock:
                    self._uploading_flags[self.worker_id] = False
                continue
            finally:
                # 上传完成后立即清除上传状态
                async with self._uploading_lock:
                    self._uploading_flags[self.worker_id] = False

            # 等待一段时间，让其他API开始上传
            delay = self.config.api.batch_delay_sec
            logger.info(f"{self.name} 上传完成，等待 {delay} 秒...")
            await asyncio.sleep(delay)

            # 轮询和下载（不需要等待其他API）
            if upload_result:
                poll_timeout = False
                try:
                    done_count = await self._poll_and_download(batch, upload_result)
                    self._processed_count += done_count
                    self.progress_bar.update(len(batch))
                except Exception as e:
                    # 检查是否是轮询超时
                    if isinstance(e, TimeoutError) or "超时" in str(e):
                        poll_timeout = True
                        logger.error(f"{self.name} 轮询超时: {e}")
                    else:
                        logger.error(f"{self.name} 轮询/下载失败: {e}")
                    
                    for rec in batch:
                        await self.checkpoint.update_state(
                            rec.data_id,
                            FileState.FAILED,
                            batch_id=upload_result.batch_id,
                            error_msg=str(e),
                            increment_attempts=True,
                            api_key_index=self.worker_id,
                        )
                    self.progress_bar.update(len(batch))

                # 轮询超时后暂停
                if poll_timeout:
                    pause_minutes = self.config.api.poll_timeout_pause_minutes
                    logger.info(
                        f"{self.name} 轮询超时，暂停 {pause_minutes} 分钟后继续..."
                    )
                    await asyncio.sleep(pause_minutes * 60)
                    logger.info(f"{self.name} 暂停结束，继续处理")

        logger.info(f"{self.name} 完成，共处理 {self._processed_count} 个文件")
        return self._processed_count

    async def _get_next_batch(self, batch_size: int, remaining_quota: int) -> list[FileRecord]:
        """从共享索引获取下一批次"""
        async with self._index_lock:
            start = self.file_index[0]
            total = len(self.pending_files)
            
            if start >= total:
                return []
            
            size = min(batch_size, remaining_quota) if remaining_quota > 0 else batch_size
            end = min(start + size, total)
            
            batch = self.pending_files[start:end]
            self.file_index[0] = end  # 更新共享索引
            
            logger.info(f"{self.name} 获取批次 [{start}:{end}] ({len(batch)} 个文件)")
            return batch

    async def _upload_batch(self, batch: list[FileRecord]) -> Optional[UploadResult]:
        """上传一个批次的文件（不包括轮询和下载）"""
        data_ids = [rec.data_id for rec in batch]

        # Step 1: 标记为上传中
        await self.checkpoint.bulk_update_state(data_ids, FileState.UPLOADING, api_key_index=self.worker_id)

        # Step 2: 申请上传URL
        files_payload = [
            {
                "name": Path(rec.pdf_path).name,
                "is_ocr": self.config.extraction.is_ocr,
                "data_id": rec.data_id,
            }
            for rec in batch
        ]

        try:
            upload_resp = await self.api_client.request_upload_urls(files_payload)
        except AllAPIKeysExhaustedError:
            await self.checkpoint.bulk_update_state(data_ids, FileState.PENDING, api_key_index=self.worker_id)
            raise
        except Exception as e:
            logger.error(f"{self.name} 申请上传URL失败: {e}")
            for rec in batch:
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    error_msg=f"申请上传URL失败: {e}",
                    increment_attempts=True,
                    api_key_index=self.worker_id,
                )
            raise

        batch_id = upload_resp.batch_id
        file_urls = upload_resp.file_urls

        if len(file_urls) != len(batch):
            raise RuntimeError(f"上传URL数量不匹配: 期望{len(batch)}, 实际{len(file_urls)}")

        # Step 3: 上传文件
        if self.config.api.enable_concurrent:
            upload_tasks = [self._upload_single(rec, url, batch_id) for rec, url in zip(batch, file_urls)]
            upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)
        else:
            upload_results = []
            for rec, url in zip(batch, file_urls):
                try:
                    await self._upload_single(rec, url, batch_id)
                    upload_results.append(None)
                except Exception as e:
                    upload_results.append(e)

        # 分离上传成功和失败的文件
        uploaded_recs = []
        for rec, result in zip(batch, upload_results):
            if isinstance(result, Exception):
                logger.error(f"{self.name} 上传失败 ({rec.data_id}): {result}")
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg=f"上传失败: {result}",
                    increment_attempts=True,
                    api_key_index=self.worker_id,
                )
            else:
                uploaded_recs.append(rec)

        if not uploaded_recs:
            logger.error(f"{self.name} 批次中所有文件上传失败")
            return None

        uploaded_ids = [rec.data_id for rec in uploaded_recs]

        # Step 4: 标记为轮询中
        await self.checkpoint.bulk_update_state(
            uploaded_ids, FileState.POLLING, batch_id=batch_id, api_key_index=self.worker_id
        )

        logger.info(f"{self.name} 上传完成 {len(uploaded_recs)} 个文件，batch_id={batch_id}")
        return UploadResult(batch_id=batch_id, uploaded_recs=uploaded_recs)

    async def _poll_and_download(self, batch: list[FileRecord], upload_result: UploadResult) -> int:
        """轮询结果并下载

        Raises:
            TimeoutError: 轮询超时时抛出，由调用方处理暂停逻辑
        """
        batch_id = upload_result.batch_id
        uploaded_recs = upload_result.uploaded_recs

        # Step 5: 轮询结果（超时异常向上抛出）
        results = await self.api_client.poll_batch_results(batch_id)

        # Step 6: 处理结果
        done_count = 0
        for rec in uploaded_recs:
            result = None
            for r in results:
                if r.data_id == rec.data_id or r.file_name == Path(rec.pdf_path).name:
                    result = r
                    break

            if result is None:
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg="未在结果中找到对应文件",
                    increment_attempts=True,
                    api_key_index=self.worker_id,
                )
                continue

            if result.state == "failed":
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg=result.err_msg or "MinerU处理失败",
                    increment_attempts=True,
                    api_key_index=self.worker_id,
                )
                continue

            if result.state == "done" and result.full_zip_url:
                try:
                    success = await self._download_and_convert(rec, result.full_zip_url, batch_id)
                    if success:
                        done_count += 1
                except Exception as e:
                    logger.error(f"{self.name} 下载/转换失败 ({rec.data_id}): {e}")
                    await self.checkpoint.update_state(
                        rec.data_id,
                        FileState.FAILED,
                        batch_id=batch_id,
                        error_msg=str(e),
                        increment_attempts=True,
                        api_key_index=self.worker_id,
                    )

        return done_count

    async def _upload_single(self, rec: FileRecord, presigned_url: str, batch_id: str) -> None:
        await self.api_client.upload_file(presigned_url, rec.pdf_path)

    async def _download_and_convert(
        self,
        rec: FileRecord,
        zip_url: str,
        batch_id: str,
    ) -> bool:
        """下载结果并转换"""
        await self.checkpoint.update_state(
            rec.data_id, FileState.CONVERTING, batch_id=batch_id, api_key_index=self.worker_id
        )

        content_blocks = await self.api_client.download_result(zip_url)

        if not content_blocks:
            await self.checkpoint.update_state(
                rec.data_id,
                FileState.FAILED,
                batch_id=batch_id,
                error_msg="未找到有效的 content_list.json",
                increment_attempts=True,
                api_key_index=self.worker_id,
            )
            return False

        doc = convert_content_blocks(
            content_blocks,
            data_id=rec.data_id,
            journal=rec.journal,
        )

        if doc is None:
            await self.checkpoint.update_state(
                rec.data_id,
                FileState.FAILED,
                batch_id=batch_id,
                error_msg="格式转换失败",
                increment_attempts=True,
                api_key_index=self.worker_id,
            )
            return False

        output_dir = self.output_dir_selector()
        output_path = _build_output_path(output_dir, rec, f"{rec.data_id}.json")
        save_paper_json(doc, str(output_path))

        await self.checkpoint.update_state(
            rec.data_id, FileState.DONE, batch_id=batch_id, api_key_index=self.worker_id
        )

        return True

    async def close(self) -> None:
        if self._api_client is not None:
            await self._api_client.close()


class Processor:
    """PDF 处理管道编排器"""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.checkpoint = Checkpoint(config.paths.checkpoint_db)
        self.failed_db = FailedDB(config.paths.failed_db)
        self._api_client: Optional[MinerUAPIClient] = None
        self._shutdown_event = asyncio.Event()
        self._active_batches: list[str] = []
        self._current_output_root: Optional[Path] = None

    @property
    def api_client(self) -> MinerUAPIClient:
        """惰性创建 API 客户端（仅在需要时验证 API Key）"""
        if self._api_client is None:
            self._api_client = MinerUAPIClient(self.config)
        return self._api_client

    async def initialize(self, reset_stale: bool = False) -> None:
        """初始化组件"""
        await self.checkpoint.initialize()
        await self.failed_db.initialize()
        if reset_stale:
            stale_count = await self.checkpoint.reset_stale()
            if stale_count > 0:
                logger.info(f"已重置 {stale_count} 个中间状态文件为待处理")

    async def close(self) -> None:
        if self._api_client is not None:
            await self._api_client.close()
        await self.checkpoint.close()
        await self.failed_db.close()

    def _setup_signal_handlers(self) -> None:
        """设置优雅关闭信号处理"""
        if sys.platform == "win32":
            signal.signal(signal.SIGINT, self._signal_handler)
        else:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: self._signal_handler(sig, None))

    def _signal_handler(self, signum: Any, frame: Any) -> None:
        """信号处理器"""
        logger.info("\n收到停止信号，正在优雅关闭...")
        self._shutdown_event.set()

    async def scan_and_register(self) -> int:
        """扫描新PDF并注册到检查点"""
        logger.info(f"扫描PDF目录: {self.config.paths.pdf_input}")

        existing_records = set()
        if self.checkpoint._db:
            cursor = await self.checkpoint._db.execute("SELECT data_id FROM files")
            rows = await cursor.fetchall()
            existing_records = {row[0] for row in rows}

        new_records = scan_pdfs(
            self.config.paths.pdf_input,
            existing_ids=existing_records,
            exclude_prefixes=self.config.exclude_prefixes,
        )

        if new_records:
            await self.checkpoint.register_files(new_records)
            logger.info(f"新注册 {len(new_records)} 个PDF文件")

        return len(new_records)

    async def run(self, limit: int = 0, journals: Optional[list[str]] = None) -> None:
        """
        运行完整处理管道（多API并发模式）

        Args:
            limit: 最多处理的文件数，0 表示处理所有待处理文件
            journals: 仅处理指定期刊列表中的文件，None 表示不过滤
        """
        self._setup_signal_handlers()

        try:
            # Step 1: 扫描并注册新文件
            new_count = await self.scan_and_register()

            # 显示统计
            stats = await self.checkpoint.get_stats()
            logger.info(
                f"检查点状态: 总计={stats.get('total', 0)}, "
                f"待处理={stats.get('pending', 0)}, "
                f"已完成={stats.get('done', 0)}, "
                f"失败={stats.get('failed', 0)}"
            )

            if journals:
                logger.info(f"期刊过滤: 仅处理 {journals}")

            # Step 2: 获取待处理文件
            pending = await self.checkpoint.get_pending(limit=limit, journals=journals)
            if not pending:
                logger.info("没有待处理的文件")
                return

            total = len(pending)
            logger.info(f"开始处理 {total} 个文件...")

            # Step 3: 检查API配置
            api_configs = self.config.api.api_configs

            if api_configs and len(api_configs) > 1:
                # 多API并发模式
                await self._run_concurrent(api_configs, pending)
            else:
                # 单API模式（兼容旧配置）
                await self._run_single(pending)

            # 最终统计
            final_stats = await self.checkpoint.get_stats()
            logger.info(
                f"\n处理完成! "
                f"总计={final_stats.get('total', 0)}, "
                f"已完成={final_stats.get('done', 0)}, "
                f"失败={final_stats.get('failed', 0)}, "
                f"待处理={final_stats.get('pending', 0)}"
            )

            # 显示各API使用统计
            if api_configs:
                logger.info("\n各API今日处理统计:")
                for idx, api_cfg in enumerate(api_configs):
                    today_done = await self.checkpoint.get_today_done_count(idx)
                    name = api_cfg.name if api_cfg.name else f"API-{idx + 1}"
                    limit_str = f"/ {api_cfg.daily_limit}" if api_cfg.daily_limit > 0 else "(无限制)"
                    logger.info(f"  {name}: {today_done} 个文件 {limit_str}")

        finally:
            pass

    async def _run_concurrent(self, api_configs: list[SingleApiConfig], pending: list[FileRecord]) -> None:
        """多API并发处理模式
        
        每个API独立处理相同的待处理文件列表：
        - API-1 获取前50个待处理文件
        - API-2 也获取前50个待处理文件
        - 通过状态检查避免重复处理
        """
        # 显示所有API状态
        total_quota = 0
        for idx, api_cfg in enumerate(api_configs):
            today_done = await self.checkpoint.get_today_done_count(idx)
            name = api_cfg.name if api_cfg.name else f"API-{idx + 1}"
            if api_cfg.daily_limit > 0:
                remaining = api_cfg.daily_limit - today_done
                logger.info(f"{name}: 今日已处理 {today_done} 个, 每日上限 {api_cfg.daily_limit}, 剩余 {remaining}")
                total_quota += remaining
            else:
                logger.info(f"{name}: 今日已处理 {today_done} 个, 无限制")
                total_quota = -1  # 有一个无限制就足够
                break

        # 根据配额调整处理数量（取所有API配额之和）
        if total_quota > 0 and len(pending) > total_quota:
            pending = pending[:total_quota]
            logger.info(f"根据剩余配额，本次处理 {len(pending)} 个文件")

        batch_size = self.config.api.batch_size
        logger.info(f"共 {len(pending)} 个待处理文件，{len(api_configs)} 个API并发处理（每个API每批 {batch_size} 个文件）")

        # 共享文件索引（使用list包装实现引用传递）
        file_index = [0]
        # 共享的上传状态标志列表（每个API一个）
        uploading_flags = [False for _ in api_configs]
        # 上传状态锁
        uploading_lock = asyncio.Lock()
        # 轮次索引：>=0 表示第一轮顺序，-1 表示抢占模式
        turn_index = [0]
        num_workers = len(api_configs)

        # 创建进度条
        with tqdm(total=len(pending), desc="处理进度", unit="文件") as pbar:
            # 创建workers，每个worker共享pending列表和索引
            workers = [
                APIWorker(
                    worker_id=idx,
                    api_config=cfg,
                    config=self.config,
                    checkpoint=self.checkpoint,
                    progress_bar=pbar,
                    shutdown_event=self._shutdown_event,
                    output_dir_selector=self._select_output_dir,
                    pending_files=pending,
                    file_index=file_index,
                    uploading_flags=uploading_flags,
                    uploading_lock=uploading_lock,
                    turn_index=turn_index,
                    num_workers=num_workers,
                )
                for idx, cfg in enumerate(api_configs)
            ]

            # 并发运行所有workers
            worker_tasks = [worker.run() for worker in workers]
            results = await asyncio.gather(*worker_tasks, return_exceptions=True)

            # 关闭所有workers
            for worker in workers:
                await worker.close()

            # 统计结果
            total_processed = sum(r for r in results if isinstance(r, int))
            logger.info(f"并发处理完成，共处理 {total_processed} 个文件")

    async def _run_single(self, pending: list[FileRecord]) -> None:
        """单API处理模式（兼容旧配置）"""
        api_configs = self.config.api.api_configs

        # 确定API索引
        api_index = 0
        if api_configs:
            api_cfg = api_configs[0]
            daily_limit = api_cfg.daily_limit
        else:
            daily_limit = self.config.api.daily_limit
            api_cfg = None

        # 检查配额
        if daily_limit > 0:
            today_done = await self.checkpoint.get_today_done_count(api_index)
            remaining = daily_limit - today_done
            if remaining <= 0:
                logger.info("今日处理配额已用完，停止处理")
                return
            if len(pending) > remaining:
                pending = pending[:remaining]
                logger.info(f"根据剩余配额，本次处理 {len(pending)} 个文件")

        # 设置API key
        if api_configs:
            self.config.api.api_key = api_configs[api_index].api_key

        batch_size = self.config.api.batch_size
        batches = [pending[i:i + batch_size] for i in range(0, len(pending), batch_size)]

        with tqdm(total=len(pending), desc="处理进度", unit="文件") as pbar:
            for batch_idx, batch in enumerate(batches):
                if self._shutdown_event.is_set():
                    logger.info("收到停止信号，停止处理")
                    break

                logger.info(f"处理批次 {batch_idx + 1}/{len(batches)} ({len(batch)} 个文件)")

                try:
                    done_count = await self._process_batch(batch, api_index)
                    pbar.update(len(batch))
                except Exception as e:
                    logger.error(f"批次 {batch_idx + 1} 处理失败: {e}")
                    for rec in batch:
                        await self.checkpoint.update_state(
                            rec.data_id,
                            FileState.FAILED,
                            error_msg=str(e),
                            increment_attempts=True,
                            api_key_index=api_index,
                        )
                    pbar.update(len(batch))

    async def _process_batch(self, batch: list[FileRecord], api_key_index: int = 0) -> int:
        """处理一个批次的文件（单API模式）"""
        data_ids = [rec.data_id for rec in batch]

        await self.checkpoint.bulk_update_state(data_ids, FileState.UPLOADING, api_key_index=api_key_index)

        files_payload = [
            {
                "name": Path(rec.pdf_path).name,
                "is_ocr": self.config.extraction.is_ocr,
                "data_id": rec.data_id,
            }
            for rec in batch
        ]

        try:
            upload_resp = await self.api_client.request_upload_urls(files_payload)
        except Exception as e:
            logger.error(f"申请上传URL失败: {e}")
            for rec in batch:
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    error_msg=f"申请上传URL失败: {e}",
                    increment_attempts=True,
                    api_key_index=api_key_index,
                )
            raise

        batch_id = upload_resp.batch_id
        file_urls = upload_resp.file_urls

        if len(file_urls) != len(batch):
            raise RuntimeError(f"上传URL数量不匹配: 期望{len(batch)}, 实际{len(file_urls)}")

        if self.config.api.enable_concurrent:
            upload_tasks = [self._upload_single(rec, url, batch_id) for rec, url in zip(batch, file_urls)]
            upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)
        else:
            upload_results = []
            for rec, url in zip(batch, file_urls):
                try:
                    await self._upload_single(rec, url, batch_id)
                    upload_results.append(None)
                except Exception as e:
                    upload_results.append(e)

        uploaded_recs = []
        for rec, result in zip(batch, upload_results):
            if isinstance(result, Exception):
                logger.error(f"上传失败 ({rec.data_id}): {result}")
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg=f"上传失败: {result}",
                    increment_attempts=True,
                    api_key_index=api_key_index,
                )
            else:
                uploaded_recs.append(rec)

        if not uploaded_recs:
            return 0

        uploaded_ids = [rec.data_id for rec in uploaded_recs]
        await self.checkpoint.bulk_update_state(
            uploaded_ids, FileState.POLLING, batch_id=batch_id, api_key_index=api_key_index
        )

        try:
            results = await self.api_client.poll_batch_results(batch_id)
        except TimeoutError as e:
            logger.error(f"轮询超时: {e}")
            for rec in uploaded_recs:
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg=f"轮询超时: {e}",
                    increment_attempts=True,
                    api_key_index=api_key_index,
                )
            return 0

        done_count = 0
        for rec in uploaded_recs:
            result = None
            for r in results:
                if r.data_id == rec.data_id or r.file_name == Path(rec.pdf_path).name:
                    result = r
                    break

            if result is None:
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg="未在结果中找到对应文件",
                    increment_attempts=True,
                    api_key_index=api_key_index,
                )
                continue

            if result.state == "failed":
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg=result.err_msg or "MinerU处理失败",
                    increment_attempts=True,
                    api_key_index=api_key_index,
                )
                continue

            if result.state == "done" and result.full_zip_url:
                try:
                    success = await self._download_and_convert(rec, result.full_zip_url, batch_id, api_key_index)
                    if success:
                        done_count += 1
                except Exception as e:
                    logger.error(f"下载/转换失败 ({rec.data_id}): {e}")
                    await self.checkpoint.update_state(
                        rec.data_id,
                        FileState.FAILED,
                        batch_id=batch_id,
                        error_msg=str(e),
                        increment_attempts=True,
                        api_key_index=api_key_index,
                    )

        return done_count

    async def _upload_single(self, rec: FileRecord, presigned_url: str, batch_id: str) -> None:
        await self.api_client.upload_file(presigned_url, rec.pdf_path)

    async def _download_and_convert(
        self,
        rec: FileRecord,
        zip_url: str,
        batch_id: str,
        api_key_index: int = 0,
    ) -> bool:
        """下载结果并转换"""
        await self.checkpoint.update_state(
            rec.data_id, FileState.CONVERTING, batch_id=batch_id, api_key_index=api_key_index
        )

        content_blocks = await self.api_client.download_result(zip_url)

        if not content_blocks:
            await self.checkpoint.update_state(
                rec.data_id,
                FileState.FAILED,
                batch_id=batch_id,
                error_msg="未找到有效的 content_list.json",
                increment_attempts=True,
                api_key_index=api_key_index,
            )
            return False

        doc = convert_content_blocks(
            content_blocks,
            data_id=rec.data_id,
            journal=rec.journal,
        )

        if doc is None:
            await self.checkpoint.update_state(
                rec.data_id,
                FileState.FAILED,
                batch_id=batch_id,
                error_msg="格式转换失败",
                increment_attempts=True,
                api_key_index=api_key_index,
            )
            return False

        output_dir = self._select_output_dir()
        output_path = _build_output_path(output_dir, rec, f"{rec.data_id}.json")
        save_paper_json(doc, str(output_path))

        await self.checkpoint.update_state(
            rec.data_id, FileState.DONE, batch_id=batch_id, api_key_index=api_key_index
        )

        return True

    async def convert_only(self) -> None:
        """raw 不落盘时，convert-only 不可用。"""
        logger.warning("当前配置不保留 raw 文件，convert-only 不可用")

    async def retry_failed(self) -> int:
        """重置所有失败文件为待处理"""
        count = await self.checkpoint.reset_failed()
        logger.info(f"已重置 {count} 个失败文件为待处理")
        return count

    async def run_retry(self, limit: int = 0) -> None:
        """根据 failed.db 中的记录重新尝试处理"""
        self._setup_signal_handlers()

        try:
            failed_records = await self.failed_db.get_failures_for_retry(limit=limit)
            if not failed_records:
                logger.info("没有失败记录需要重试")
                return

            total = len(failed_records)
            logger.info(f"从失败记录中获取 {total} 个文件准备重试")

            valid_records = []
            for rec in failed_records:
                if Path(rec.pdf_path).exists():
                    valid_records.append(rec)
                else:
                    logger.warning(f"文件不存在，跳过: {rec.pdf_path}")

            if not valid_records:
                logger.warning("所有失败记录的文件都不存在")
                return

            # 使用单API模式重试
            await self._run_single(valid_records)

        finally:
            pass

    async def show_status(self) -> dict[str, int]:
        """显示处理状态"""
        stats = await self.checkpoint.get_stats()
        return stats

    def _iter_output_roots(self) -> list[Path]:
        """按优先级返回输出根目录"""
        candidates = [
            self.config.paths.final_output,
            *self.config.paths.fallback_final_outputs,
        ]

        roots: list[Path] = []
        seen: set[str] = set()
        for item in candidates:
            text = str(item).strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            roots.append(Path(text))

        return roots

    def _has_enough_space(self, path: Path) -> tuple[bool, float]:
        """检查路径所在磁盘空间是否达到阈值"""
        min_free_gb = max(float(self.config.paths.min_free_gb), 0.0)
        usage_target = _find_existing_anchor(path)
        free_bytes = shutil.disk_usage(usage_target).free
        free_gb = free_bytes / (1024**3)
        return free_gb >= min_free_gb, free_gb

    def _select_output_dir(self) -> Path:
        """根据剩余磁盘空间选择可用输出目录"""
        roots = self._iter_output_roots()
        if not roots:
            raise RuntimeError("未配置 final_output 输出目录")

        min_free_gb = max(float(self.config.paths.min_free_gb), 0.0)
        low_space_messages: list[str] = []

        for root in roots:
            try:
                enough, free_gb = self._has_enough_space(root)
            except Exception as exc:
                low_space_messages.append(f"{root} 无法检查磁盘空间: {exc}")
                continue

            if enough:
                if self._current_output_root != root:
                    logger.info(
                        "输出目录切换为: %s (可用空间 %.2f GB, 阈值 %.2f GB)",
                        root,
                        free_gb,
                        min_free_gb,
                    )
                    self._current_output_root = root
                return root

            low_space_messages.append(
                f"{root} 剩余空间不足: {free_gb:.2f} GB < {min_free_gb:.2f} GB"
            )

        detail = "; ".join(low_space_messages)
        raise RuntimeError(f"所有输出目录剩余空间都不足: {detail}")


def _build_output_path(output_dir: Path, rec: FileRecord, filename: str) -> Path:
    """构建输出路径"""
    if rec.journal and rec.year:
        return output_dir / rec.journal / rec.year / filename
    if rec.journal:
        return output_dir / rec.journal / filename
    return output_dir / filename


def _find_existing_anchor(path: Path) -> Path:
    """找到可用于 disk_usage 的已存在路径"""
    current = path
    while not current.exists() and current.parent != current:
        current = current.parent

    if current.exists():
        return current

    if path.anchor:
        anchor = Path(path.anchor)
        if anchor.exists():
            return anchor

    return Path(".")
