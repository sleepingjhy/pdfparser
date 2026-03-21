"""
流水线编排器 - 管理完整的 PDF → JSON 处理管道。

流程:
1. 扫描PDF文件
2. 注册到检查点
3. 分批上传到 MinerU API
4. 轮询处理结果
5. 下载结果包并读取 content_list
6. 转换为目标JSON格式

多API支持:
- 支持多个API并发处理，提高吞吐量
- 支持round_robin（轮询分配）和quota_first（配额优先）策略
- 自动跟踪每个API的每日处理量，达到限额后自动切换
- 检查点机制确保每个文件只被处理一次
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from tqdm import tqdm

from .api_client import AllAPIKeysExhaustedError, MinerUAPIClient
from .checkpoint import Checkpoint
from .config import AppConfig, SingleApiConfig
from .converter import convert_content_blocks, save_paper_json
from .models import FileRecord, FileState
from .scanner import scan_pdfs

logger = logging.getLogger(__name__)


class MultiAPIManager:
    """多API管理器 - 管理多个API的分配和切换"""
    
    def __init__(self, api_configs: list[SingleApiConfig], checkpoint: Checkpoint):
        self.api_configs = api_configs
        self.checkpoint = checkpoint
        self._api_clients: dict[int, MinerUAPIClient] = {}
        self._api_usage_today: dict[int, int] = {}  # 缓存今日使用量
        
    async def get_api_client(self, index: int, config: AppConfig) -> MinerUAPIClient:
        """获取指定索引的API客户端"""
        if index not in self._api_clients:
            # 创建特定API的配置
            api_config = self.api_configs[index]
            config.api.api_key = api_config.api_key
            client = MinerUAPIClient(config)
            self._api_clients[index] = client
        return self._api_clients[index]
    
    async def get_today_usage(self, index: int) -> int:
        """获取指定API今日使用量"""
        if index not in self._api_usage_today:
            self._api_usage_today[index] = await self.checkpoint.get_today_done_count(index)
        return self._api_usage_today[index]
    
    async def increment_usage(self, index: int) -> None:
        """增加API使用计数"""
        if index in self._api_usage_today:
            self._api_usage_today[index] += 1
    
    def get_api_name(self, index: int) -> str:
        """获取API名称"""
        if 0 <= index < len(self.api_configs):
            name = self.api_configs[index].name
            return name if name else f"API-{index + 1}"
        return f"API-{index + 1}"
    
    async def select_api_index(self, strategy: str = "round_robin", last_index: int = -1) -> tuple[int, int]:
        """
        根据策略选择下一个可用的API索引
        
        Args:
            strategy: 选择策略 - round_robin: 轮询 | quota_first: 配额优先
            last_index: 上一次使用的API索引
            
        Returns:
            (api_index, remaining_quota) - API索引和剩余配额（0表示无限制）
        """
        if strategy == "quota_first":
            # 配额优先：选择剩余配额最多的API
            best_index = -1
            best_remaining = -1
            
            for idx, cfg in enumerate(self.api_configs):
                if cfg.daily_limit <= 0:
                    # 无限制，直接返回
                    return idx, 0
                    
                today_usage = await self.get_today_usage(idx)
                remaining = cfg.daily_limit - today_usage
                
                if remaining > 0 and remaining > best_remaining:
                    best_index = idx
                    best_remaining = remaining
            
            if best_index >= 0:
                return best_index, best_remaining
                
        else:  # round_robin
            # 轮询：按顺序选择下一个有配额的API
            start_idx = (last_index + 1) % len(self.api_configs) if last_index >= 0 else 0
            
            for i in range(len(self.api_configs)):
                idx = (start_idx + i) % len(self.api_configs)
                cfg = self.api_configs[idx]
                
                if cfg.daily_limit <= 0:
                    return idx, 0
                    
                today_usage = await self.get_today_usage(idx)
                remaining = cfg.daily_limit - today_usage
                
                if remaining > 0:
                    return idx, remaining
        
        return -1, 0  # 所有API配额用完
    
    async def close_all(self) -> None:
        """关闭所有API客户端"""
        for client in self._api_clients.values():
            await client.close()
        self._api_clients.clear()


class Processor:
    """PDF 处理管道编排器"""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.checkpoint = Checkpoint(config.paths.checkpoint_db)
        self._api_client: Optional[MinerUAPIClient] = None
        self._shutdown_event = asyncio.Event()
        self._active_batches: list[str] = []
        self._current_output_root: Optional[Path] = None
        self._multi_api_manager: Optional[MultiAPIManager] = None
        self._current_api_index: int = 0

    @property
    def api_client(self) -> MinerUAPIClient:
        """惰性创建 API 客户端（仅在需要时验证 API Key）"""
        if self._api_client is None:
            self._api_client = MinerUAPIClient(self.config)
        return self._api_client

    async def initialize(self, reset_stale: bool = False) -> None:
        """初始化组件。

        Args:
            reset_stale: 是否将中间态文件回退到 pending。
                只应在真正开始执行 run 前启用，避免 status 等只读命令产生副作用。
        """
        await self.checkpoint.initialize()
        if reset_stale:
            stale_count = await self.checkpoint.reset_stale()
            if stale_count > 0:
                logger.info(f"已重置 {stale_count} 个中间状态文件为待处理")

    async def close(self) -> None:
        if self._api_client is not None:
            await self._api_client.close()
        if self._multi_api_manager is not None:
            await self._multi_api_manager.close_all()
        await self.checkpoint.close()

    def _setup_signal_handlers(self) -> None:
        """设置优雅关闭信号处理"""
        if sys.platform == "win32":
            # Windows 只支持 SIGINT (Ctrl+C)
            signal.signal(signal.SIGINT, self._signal_handler)
        else:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: self._signal_handler(sig, None))

    def _signal_handler(self, signum: Any, frame: Any) -> None:
        """信号处理器"""
        logger.info("\n收到停止信号，正在优雅关闭（等待当前批次完成）...")
        self._shutdown_event.set()

    async def scan_and_register(self) -> int:
        """扫描新PDF并注册到检查点"""
        logger.info(f"扫描PDF目录: {self.config.paths.pdf_input}")

        existing_records = set()

        # 查询所有已注册的 data_id
        if self.checkpoint._db:
            cursor = await self.checkpoint._db.execute("SELECT data_id FROM files")
            rows = await cursor.fetchall()
            existing_records = {row[0] for row in rows}

        # 扫描新文件
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
        运行完整处理管道。

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

            # Step 1.5: 初始化多API管理器
            api_configs = self.config.api.api_configs
            total_remaining_quota = 0
            
            if api_configs:
                # 多API模式：初始化管理器并显示状态
                self._multi_api_manager = MultiAPIManager(api_configs, self.checkpoint)
                
                # 显示所有API状态
                for idx, api_cfg in enumerate(api_configs):
                    today_done = await self.checkpoint.get_today_done_count(idx)
                    api_name = self._multi_api_manager.get_api_name(idx)
                    api_limit = api_cfg.daily_limit
                    
                    if api_limit <= 0:
                        logger.info(f"{api_name}: 今日已处理 {today_done} 个, 无限制")
                        total_remaining_quota = 0  # 有一个无限制就足够
                    else:
                        api_remaining = api_limit - today_done
                        logger.info(f"{api_name}: 今日已处理 {today_done} 个, 每日上限 {api_limit}, 剩余 {api_remaining}")
                        if total_remaining_quota >= 0:  # 只有在没有无限制API时才累加
                            total_remaining_quota += api_remaining
                
                # 选择初始API
                strategy = self.config.api.multi_api_strategy
                self._current_api_index, remaining = await self._multi_api_manager.select_api_index(strategy, -1)
                
                if self._current_api_index < 0:
                    logger.warning("所有API今日配额已用完，停止处理")
                    return
                
                # 设置当前API
                self._set_current_api_key(self._current_api_index)
                api_name = self._multi_api_manager.get_api_name(self._current_api_index)
                logger.info(f"使用 {api_name} 开始处理（策略: {strategy}）")
                
            else:
                # 单API模式（兼容旧配置）
                daily_limit = self.config.api.daily_limit
                if daily_limit > 0:
                    today_done = await self.checkpoint.get_today_done_count()
                    total_remaining_quota = daily_limit - today_done
                    logger.info(f"今日已处理: {today_done} 个文件, 每日上限: {daily_limit}, 剩余配额: {total_remaining_quota}")
                    
                    if total_remaining_quota <= 0:
                        logger.info("今日处理配额已用完，停止处理")
                        return
                else:
                    total_remaining_quota = 0  # 无限制

            # Step 2: 获取待处理文件
            pending = await self.checkpoint.get_pending(limit=limit, journals=journals)
            if not pending:
                logger.info("没有待处理的文件")
                return

            total = len(pending)
            
            # 根据配额调整处理数量（仅单API模式需要）
            if not api_configs and total_remaining_quota > 0 and total > total_remaining_quota:
                total = total_remaining_quota
                pending = pending[:total]
                logger.info(f"根据剩余配额，本次处理 {total} 个文件")
            
            logger.info(f"开始处理 {total} 个文件...")

            # Step 3: 分批处理
            batch_size = self.config.api.batch_size
            batches = [pending[i : i + batch_size] for i in range(0, total, batch_size)]

            with tqdm(total=total, desc="处理进度", unit="文件") as pbar:
                for batch_idx, batch in enumerate(batches):
                    if self._shutdown_event.is_set():
                        logger.info("收到停止信号，停止处理新批次")
                        break

                    # 多API模式：检查是否需要切换API
                    if api_configs and self._multi_api_manager:
                        strategy = self.config.api.multi_api_strategy
                        new_index, remaining = await self._multi_api_manager.select_api_index(
                            strategy, self._current_api_index
                        )
                        
                        if new_index < 0:
                            logger.warning("所有API今日配额已用完，停止处理")
                            break
                        
                        if new_index != self._current_api_index:
                            self._current_api_index = new_index
                            self._set_current_api_key(new_index)
                            api_name = self._multi_api_manager.get_api_name(new_index)
                            logger.info(f"切换到 {api_name} 继续处理")

                    logger.info(
                        f"处理批次 {batch_idx + 1}/{len(batches)} ({len(batch)} 个文件)"
                    )

                    try:
                        done_count = await self._process_batch(batch, self._current_api_index if api_configs else -1)
                        pbar.update(len(batch))
                        
                        # 更新API使用计数
                        if api_configs and self._multi_api_manager:
                            await self._multi_api_manager.increment_usage(self._current_api_index)
                            
                    except AllAPIKeysExhaustedError as e:
                        logger.warning(f"所有 API Key 都不可继续提交新任务，停止处理: {e}")
                        break
                    except Exception as e:
                        logger.error(f"批次 {batch_idx + 1} 处理失败: {e}")
                        # 标记整个批次为失败
                        for rec in batch:
                            await self.checkpoint.update_state(
                                rec.data_id,
                                FileState.FAILED,
                                error_msg=str(e),
                                increment_attempts=True,
                            )
                        pbar.update(len(batch))

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
            if api_configs and self._multi_api_manager:
                logger.info("\n各API今日处理统计:")
                for idx, api_cfg in enumerate(api_configs):
                    today_done = await self.checkpoint.get_today_done_count(idx)
                    api_name = self._multi_api_manager.get_api_name(idx)
                    logger.info(f"  {api_name}: {today_done} 个文件")

        finally:
            pass

    def _set_current_api_key(self, api_index: int) -> None:
        """设置当前使用的API key"""
        api_configs = self.config.api.api_configs
        if api_configs and 0 <= api_index < len(api_configs):
            self.config.api.api_key = api_configs[api_index].api_key
            # 重新创建API客户端
            if self._api_client is not None:
                asyncio.create_task(self._api_client.close())
                self._api_client = None

    async def _process_batch(self, batch: list[FileRecord], api_key_index: int = -1) -> int:
        """
        处理一个批次的文件。

        流程: 申请上传URL → 上传文件 → 轮询结果 → 下载结果包 → 转换

        Args:
            batch: 要处理的文件列表
            api_key_index: 使用的API索引，-1表示未设置

        Returns:
            成功处理的文件数
        """
        data_ids = [rec.data_id for rec in batch]

        # Step 1: 标记为上传中
        await self.checkpoint.bulk_update_state(data_ids, FileState.UPLOADING, api_key_index=api_key_index)

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
            await self.checkpoint.bulk_update_state(data_ids, FileState.PENDING, api_key_index=api_key_index)
            raise
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
            raise RuntimeError(
                f"上传URL数量不匹配: 期望{len(batch)}, 实际{len(file_urls)}"
            )

        # Step 3: 上传文件（根据配置决定并发或串行）
        if self.config.api.enable_concurrent:
            # 并发上传
            upload_tasks = []
            for rec, url in zip(batch, file_urls):
                upload_tasks.append(self._upload_single(rec, url, batch_id))
            upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)
        else:
            # 串行上传（更稳定但较慢）
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
            logger.error("批次中所有文件上传失败，跳过轮询")
            return 0

        uploaded_ids = [rec.data_id for rec in uploaded_recs]

        # Step 4: 仅对上传成功的文件标记为轮询中
        await self.checkpoint.bulk_update_state(
            uploaded_ids, FileState.POLLING, batch_id=batch_id, api_key_index=api_key_index
        )

        # Step 5: 轮询结果
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

        # Step 6: 处理每个文件的结果
        done_count = 0
        for rec in uploaded_recs:
            # 找到对应的结果
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
                    success = await self._download_and_convert(
                        rec, result.full_zip_url, batch_id, api_key_index
                    )
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

    async def _upload_single(
        self,
        rec: FileRecord,
        presigned_url: str,
        batch_id: str,
    ) -> None:
        await self.api_client.upload_file(presigned_url, rec.pdf_path)

    async def _download_and_convert(
        self,
        rec: FileRecord,
        zip_url: str,
        batch_id: str,
        api_key_index: int = -1,
    ) -> bool:
        """下载结果并直接转换为目标JSON，不保留 raw 文件"""
        # 标记为转换中
        await self.checkpoint.update_state(
            rec.data_id, FileState.CONVERTING, batch_id=batch_id, api_key_index=api_key_index
        )

        # 下载并读取 content_list 内容
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

        # 转换格式
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

        # 保存JSON
        output_dir = self._select_output_dir()
        output_path = _build_output_path(output_dir, rec, f"{rec.data_id}.json")

        save_paper_json(doc, str(output_path))

        # 标记为完成
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

    async def show_status(self) -> dict[str, int]:
        """显示处理状态"""
        stats = await self.checkpoint.get_stats()
        return stats

    def _iter_output_roots(self) -> list[Path]:
        """按优先级返回输出根目录（主目录 + 备用目录）。"""
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
        """检查路径所在磁盘空间是否达到阈值，返回(是否足够, 剩余GB)。"""
        min_free_gb = max(float(self.config.paths.min_free_gb), 0.0)
        usage_target = _find_existing_anchor(path)
        free_bytes = shutil.disk_usage(usage_target).free
        free_gb = free_bytes / (1024**3)
        return free_gb >= min_free_gb, free_gb

    def _select_output_dir(self) -> Path:
        """根据剩余磁盘空间选择可用输出目录。"""
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
        raise RuntimeError(f"所有输出目录剩余空间都不足，无法写入: {detail}")


def _build_output_path(output_dir: Path, rec: FileRecord, filename: str) -> Path:
    """构建输出路径: output/{journal}/{year}/{file} 或退化形式"""
    if rec.journal and rec.year:
        return output_dir / rec.journal / rec.year / filename
    if rec.journal:
        return output_dir / rec.journal / filename
    return output_dir / filename


def _find_existing_anchor(path: Path) -> Path:
    """找到可用于 disk_usage 的已存在路径。"""
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
