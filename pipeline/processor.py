"""
流水线编排器 - 管理完整的 PDF → JSON 处理管道。

流程:
1. 扫描PDF文件
2. 注册到检查点
3. 分批上传到 MinerU API
4. 轮询处理结果
5. 下载结果包并读取 content_list
6. 转换为目标JSON格式
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Any, Optional

from tqdm import tqdm

from .api_client import AllAPIKeysExhaustedError, MinerUAPIClient
from .checkpoint import Checkpoint
from .config import AppConfig
from .converter import convert_content_blocks, save_paper_json
from .models import FileRecord, FileState
from .scanner import scan_pdfs

logger = logging.getLogger(__name__)


class Processor:
    """PDF 处理管道编排器"""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.checkpoint = Checkpoint(config.paths.checkpoint_db)
        self._api_client: Optional[MinerUAPIClient] = None
        self._shutdown_event = asyncio.Event()
        self._active_batches: list[str] = []

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

            # Step 2: 获取待处理文件
            pending = await self.checkpoint.get_pending(limit=limit, journals=journals)
            if not pending:
                logger.info("没有待处理的文件")
                return

            total = len(pending)
            logger.info(f"开始处理 {total} 个文件...")

            # Step 3: 分批处理
            batch_size = self.config.api.batch_size
            batches = [pending[i : i + batch_size] for i in range(0, total, batch_size)]

            with tqdm(total=total, desc="处理进度", unit="文件") as pbar:
                for batch_idx, batch in enumerate(batches):
                    if self._shutdown_event.is_set():
                        logger.info("收到停止信号，停止处理新批次")
                        break

                    logger.info(
                        f"处理批次 {batch_idx + 1}/{len(batches)} ({len(batch)} 个文件)"
                    )

                    try:
                        done_count = await self._process_batch(batch)
                        pbar.update(len(batch))
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

        finally:
            pass

    async def _process_batch(self, batch: list[FileRecord]) -> int:
        """
        处理一个批次的文件。

        流程: 申请上传URL → 上传文件 → 轮询结果 → 下载结果包 → 转换

        Returns:
            成功处理的文件数
        """
        data_ids = [rec.data_id for rec in batch]

        # Step 1: 标记为上传中
        await self.checkpoint.bulk_update_state(data_ids, FileState.UPLOADING)

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
            await self.checkpoint.bulk_update_state(data_ids, FileState.PENDING)
            raise
        except Exception as e:
            logger.error(f"申请上传URL失败: {e}")
            for rec in batch:
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    error_msg=f"申请上传URL失败: {e}",
                    increment_attempts=True,
                )
            raise

        batch_id = upload_resp.batch_id
        file_urls = upload_resp.file_urls

        if len(file_urls) != len(batch):
            raise RuntimeError(
                f"上传URL数量不匹配: 期望{len(batch)}, 实际{len(file_urls)}"
            )

        # Step 3: 并发上传文件，容忍部分失败
        upload_tasks = []
        for rec, url in zip(batch, file_urls):
            upload_tasks.append(self._upload_single(rec, url, batch_id))
        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

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
                )
            else:
                uploaded_recs.append(rec)

        if not uploaded_recs:
            logger.error("批次中所有文件上传失败，跳过轮询")
            return 0

        uploaded_ids = [rec.data_id for rec in uploaded_recs]

        # Step 4: 仅对上传成功的文件标记为轮询中
        await self.checkpoint.bulk_update_state(
            uploaded_ids, FileState.POLLING, batch_id=batch_id
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
                )
                continue

            if result.state == "failed":
                await self.checkpoint.update_state(
                    rec.data_id,
                    FileState.FAILED,
                    batch_id=batch_id,
                    error_msg=result.err_msg or "MinerU处理失败",
                    increment_attempts=True,
                )
                continue

            if result.state == "done" and result.full_zip_url:
                try:
                    success = await self._download_and_convert(
                        rec, result.full_zip_url, batch_id
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
    ) -> bool:
        """下载结果并直接转换为目标JSON，不保留 raw 文件"""
        # 标记为转换中
        await self.checkpoint.update_state(
            rec.data_id, FileState.CONVERTING, batch_id=batch_id
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
            )
            return False

        # 保存JSON
        output_dir = Path(self.config.paths.final_output)
        output_path = _build_output_path(output_dir, rec, f"{rec.data_id}.json")

        save_paper_json(doc, str(output_path))

        # 标记为完成
        await self.checkpoint.update_state(
            rec.data_id, FileState.DONE, batch_id=batch_id
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


def _build_output_path(output_dir: Path, rec: FileRecord, filename: str) -> Path:
    """构建输出路径: output/{journal}/{year}/{file} 或退化形式"""
    if rec.journal and rec.year:
        return output_dir / rec.journal / rec.year / filename
    if rec.journal:
        return output_dir / rec.journal / filename
    return output_dir / filename
