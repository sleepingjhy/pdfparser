"""MinerU API 客户端 - 处理文件上传、轮询和结果下载"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import time
import zipfile
from pathlib import Path
from typing import Any, Optional

import aiohttp

from .config import AppConfig
from .models import BatchUploadResponseData, ExtractResultItem

logger = logging.getLogger(__name__)

# 最大超时重试次数
MAX_TIMEOUT_RETRIES = 3
RETRY_BACKOFF_BASE_SEC = 2.0


class MinerUAPIClient:
    """MinerU 云端 API 客户端"""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.base_url = config.api.base_url.rstrip("/")
        self.api_key = config.api.api_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.api.concurrency)

        if not self.api_key:
            raise ValueError(
                "未配置 MINERU_API_KEY 环境变量。\n"
                "请从 https://mineru.net/apiManage 获取 API Key，\n"
                "然后设置环境变量: set MINERU_API_KEY=你的Key"
            )

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建 HTTP 会话"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=120),
            )
        return self._session

    async def close(self) -> None:
        """关闭 HTTP 会话"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def request_upload_urls(
        self,
        files: list[dict[str, Any]],
    ) -> BatchUploadResponseData:
        """
        请求预签名上传URL。

        Args:
            files: 文件列表，每个元素 {"name": "xxx.pdf", "is_ocr": true, "data_id": "xxx"}

        Returns:
            BatchUploadResponseData 包含 batch_id 和 file_urls
        """
        session = await self._get_session()
        url = f"{self.base_url}/file-urls/batch"
        payload: dict[str, Any] = {
            "files": files,
            "model_version": self.config.extraction.model_version,
            "enable_formula": self.config.extraction.enable_formula,
            "enable_table": self.config.extraction.enable_table,
            "language": self.config.extraction.language,
        }

        for attempt in range(MAX_TIMEOUT_RETRIES):
            try:
                async with session.post(url, json=payload) as resp:
                    resp.raise_for_status()
                    result = await resp.json()

                if result.get("code") != 0:
                    raise RuntimeError(
                        f"申请上传URL失败: {result.get('msg', '未知错误')}"
                    )

                data = result.get("data", {})
                return BatchUploadResponseData(
                    batch_id=data.get("batch_id", ""),
                    file_urls=data.get("file_urls", []),
                )

            except asyncio.TimeoutError:
                if attempt < MAX_TIMEOUT_RETRIES - 1:
                    wait = RETRY_BACKOFF_BASE_SEC * (2**attempt)
                    logger.warning(f"请求超时，{wait:.0f}秒后重试 (第{attempt + 1}次)")
                    await asyncio.sleep(wait)
                else:
                    raise
            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    wait = self.config.api.retry_backoff_sec
                    logger.warning(f"触发速率限制(429)，等待 {wait}秒")
                    await asyncio.sleep(wait)
                elif e.status >= 500:
                    if attempt < MAX_TIMEOUT_RETRIES - 1:
                        wait = RETRY_BACKOFF_BASE_SEC * (2**attempt)
                        logger.warning(f"服务器错误({e.status})，{wait:.0f}秒后重试")
                        await asyncio.sleep(wait)
                    else:
                        raise
                else:
                    raise

        raise RuntimeError("请求上传URL失败：超过最大重试次数")

    async def upload_file(self, presigned_url: str, pdf_path: str) -> None:
        """
        将PDF文件上传到预签名URL。

        注意：预签名URL不需要认证头，但需要 Content-Type 头。
        文件字节先读入内存，避免异步并发时文件句柄问题。
        """
        async with self._semaphore:
            file_path = Path(pdf_path)
            file_size = file_path.stat().st_size

            # 先读入内存，避免并发上传时文件句柄被关闭
            file_bytes = file_path.read_bytes()

            for attempt in range(MAX_TIMEOUT_RETRIES):
                try:
                    # 使用独立的session，不带认证头
                    timeout = aiohttp.ClientTimeout(total=300)
                    headers = {"Content-Type": ""}
                    async with aiohttp.ClientSession(timeout=timeout) as upload_session:
                        async with upload_session.put(
                            presigned_url,
                            data=file_bytes,
                            headers=headers,
                        ) as resp:
                            resp.raise_for_status()

                    logger.debug(
                        f"上传成功: {file_path.name} ({file_size / 1024:.0f}KB)"
                    )
                    return

                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    if attempt < MAX_TIMEOUT_RETRIES - 1:
                        wait = RETRY_BACKOFF_BASE_SEC * (2**attempt)
                        logger.warning(
                            f"上传失败 {file_path.name}: {e}, {wait:.0f}秒后重试"
                        )
                        await asyncio.sleep(wait)
                    else:
                        raise RuntimeError(f"上传文件失败 {file_path.name}: {e}") from e

    async def poll_batch_results(
        self,
        batch_id: str,
    ) -> list[ExtractResultItem]:
        """
        轮询批次处理结果，直到全部完成或失败。

        Returns:
            所有文件的提取结果列表
        """
        session = await self._get_session()
        url = f"{self.base_url}/extract-results/batch/{batch_id}"
        deadline = time.time() + self.config.api.max_poll_minutes * 60
        poll_interval = self.config.api.poll_interval_sec

        while True:
            if time.time() > deadline:
                raise TimeoutError(
                    f"批次 {batch_id} 轮询超时 ({self.config.api.max_poll_minutes}分钟)"
                )

            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    result = await resp.json()
            except aiohttp.ClientError as e:
                logger.warning(f"轮询请求失败: {e}，将继续重试")
                await asyncio.sleep(poll_interval)
                continue

            if result.get("code") != 0:
                logger.warning(f"轮询返回错误: {result.get('msg', '')}")
                await asyncio.sleep(poll_interval)
                continue

            extract_results = (result.get("data") or {}).get("extract_result", [])
            if not extract_results:
                await asyncio.sleep(poll_interval)
                continue

            items = [
                ExtractResultItem(
                    file_name=r.get("file_name", ""),
                    data_id=r.get("data_id", ""),
                    state=r.get("state", ""),
                    full_zip_url=r.get("full_zip_url", ""),
                    err_msg=r.get("err_msg", ""),
                )
                for r in extract_results
                if isinstance(r, dict)
            ]

            # 检查是否所有文件都已完成或失败
            states = [item.state.lower() for item in items]
            done_count = sum(1 for s in states if s == "done")
            failed_count = sum(1 for s in states if s == "failed")
            total = len(states)

            logger.info(
                f"轮询 batch={batch_id}: "
                f"done={done_count}/{total} failed={failed_count}"
            )

            if all(s in ("done", "failed") for s in states):
                return items

            await asyncio.sleep(poll_interval)

    async def download_result(
        self,
        zip_url: str,
        dest_dir: str,
    ) -> Optional[str]:
        """
        下载并解压结果ZIP，提取 content_list.json。

        Returns:
            content_list.json 的路径，如果不存在则返回 None
        """
        dest_path = Path(dest_dir)
        dest_path.mkdir(parents=True, exist_ok=True)

        try:
            timeout = aiohttp.ClientTimeout(total=300)
            async with aiohttp.ClientSession(timeout=timeout) as dl_session:
                async with dl_session.get(zip_url) as resp:
                    resp.raise_for_status()
                    zip_bytes = await resp.read()
        except Exception as e:
            raise RuntimeError(f"下载结果失败: {e}") from e

        # 解压ZIP
        content_list_path = None
        try:
            zip_buffer = io.BytesIO(zip_bytes)
            with zipfile.ZipFile(zip_buffer, "r") as zf:
                for name in zf.namelist():
                    if not name or name.endswith("/"):
                        continue

                    # 安全检查：防止zip slip攻击
                    safe_name = Path(name).name
                    if not safe_name:
                        continue

                    # 提取到目标目录（扁平化）
                    target = dest_path / safe_name
                    target.write_bytes(zf.read(name))

                    if safe_name.endswith("_content_list.json"):
                        content_list_path = str(target)
                    elif safe_name.endswith(".md"):
                        pass  # 保留markdown文件以备后用

        except zipfile.BadZipFile as e:
            raise RuntimeError(f"无效的ZIP文件: {e}") from e

        # 如果没找到 content_list.json，尝试查找任何JSON
        if content_list_path is None:
            for f in dest_path.iterdir():
                if f.name.endswith("_content_list.json"):
                    content_list_path = str(f)
                    break

        return content_list_path
