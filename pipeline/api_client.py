"""MinerU API 客户端 - 处理文件上传、轮询和结果下载"""

from __future__ import annotations

import asyncio
import io
import json
import logging
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


class APIKeyUnavailableError(RuntimeError):
    """当前 API Key 不可继续使用，需要切换到下一个。"""


class AllAPIKeysExhaustedError(RuntimeError):
    """所有 API Key 都不可继续提交新任务。"""


class MinerUAPIClient:
    """MinerU 云端 API 客户端"""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.base_url = config.api.base_url.rstrip("/")
        self._api_keys = self._build_api_keys()
        self.api_key = self._api_keys[0]
        self._current_key_index = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.api.concurrency)

        if not self._api_keys:
            raise ValueError(
                "未配置 API Key。\n"
                "请在 config.local.yaml 中配置 api_key / api_keys，或设置 "
                "MINERU_API_KEY / MINERU_API_KEYS 环境变量。"
            )

        if len(self._api_keys) > 1:
            logger.info(f"已加载 {len(self._api_keys)} 个 API Key，支持自动切换")

    def _build_api_keys(self) -> list[str]:
        """构建去重后的 API Key 池，顺序即优先级。

        当前设置的 api_key 会排在第一位，确保重新创建客户端时使用正确的 key。
        """
        result: list[str] = []

        # 当前设置的 api_key 排在最前面
        current_key = self.config.api.api_key.strip()
        if current_key:
            result.append(current_key)

        # 然后从 api_configs 提取其他 key
        for api_cfg in self.config.api.api_configs:
            key = api_cfg.api_key.strip()
            if key and key not in result:
                result.append(key)

        # 兼容旧配置方式：从 api_keys 提取
        for key in self.config.api.api_keys:
            cleaned = key.strip()
            if cleaned and cleaned not in result:
                result.append(cleaned)

        return result

    def _mask_key(self, key: str) -> str:
        """对日志中的 API Key 做脱敏。"""
        if len(key) <= 8:
            return "*" * len(key)
        return f"{key[:4]}...{key[-4:]}"

    async def _switch_to_next_key(self, reason: str) -> bool:
        """切换到下一个可用 API Key。"""
        if self._current_key_index >= len(self._api_keys) - 1:
            return False

        old_key = self.api_key
        self._current_key_index += 1
        self.api_key = self._api_keys[self._current_key_index]
        await self.close()
        logger.warning(
            "当前 API Key 不可继续提交任务，已切换到下一个 key: %s -> %s，原因: %s",
            self._mask_key(old_key),
            self._mask_key(self.api_key),
            reason,
        )
        return True

    def get_current_key_config_index(self) -> int:
        """获取当前使用的 key 在 api_configs 中的索引。

        Returns:
            api_configs 中的索引，如果找不到返回 0
        """
        current_key = self.api_key
        for idx, api_cfg in enumerate(self.config.api.api_configs):
            if api_cfg.api_key.strip() == current_key:
                return idx
        return 0  # 默认返回第一个

    def _should_switch_key(self, code: Any, message: str) -> bool:
        """判断是否应切换到下一个 API Key。"""
        normalized_code = str(code).strip().lower()
        normalized_msg = message.strip().lower()
        combined = f"{normalized_code} {normalized_msg}"

        auth_markers = (
            "a0202",
            "a0211",
            "token error",
            "token expired",
            "invalid token",
            "unauthorized",
            "forbidden",
        )
        if any(marker in combined for marker in auth_markers):
            return True

        quota_markers = (
            "quota",
            "额度",
            "1万个文件",
            "10000个文件",
            "10000 files",
            "daily upload",
            "daily file",
            "当日",
            "今日",
            "已用完",
            "用完",
            "不足",
        )
        return any(marker in combined for marker in quota_markers)

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
        url = f"{self.base_url}/file-urls/batch"
        payload: dict[str, Any] = {
            "files": files,
            "model_version": self.config.extraction.model_version,
            "enable_formula": self.config.extraction.enable_formula,
            "enable_table": self.config.extraction.enable_table,
            "language": self.config.extraction.language,
        }

        while True:
            try:
                return await self._request_upload_urls_once(url, payload)
            except APIKeyUnavailableError as e:
                if await self._switch_to_next_key(str(e)):
                    continue
                raise AllAPIKeysExhaustedError(
                    f"所有 API Key 都不可继续提交新任务: {e}"
                ) from e

    async def _request_upload_urls_once(
        self,
        url: str,
        payload: dict[str, Any],
    ) -> BatchUploadResponseData:
        """使用当前 API Key 请求上传 URL。"""
        session = await self._get_session()

        for attempt in range(MAX_TIMEOUT_RETRIES):
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status in (401, 403):
                        detail = await resp.text()
                        raise APIKeyUnavailableError(
                            f"HTTP {resp.status}: {detail[:200] or '认证失败'}"
                        )
                    if resp.status == 429:
                        wait = self.config.api.retry_backoff_sec
                        logger.warning(f"触发速率限制(429)，等待 {wait}秒")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500:
                        if attempt < MAX_TIMEOUT_RETRIES - 1:
                            wait = RETRY_BACKOFF_BASE_SEC * (2**attempt)
                            logger.warning(f"服务器错误({resp.status})，{wait:.0f}秒后重试")
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                    resp.raise_for_status()
                    result = await resp.json()

                if result.get("code") != 0:
                    error_code = result.get("code", "")
                    error_msg = result.get("msg", "未知错误")
                    if self._should_switch_key(error_code, error_msg):
                        raise APIKeyUnavailableError(f"{error_code}: {error_msg}")
                    raise RuntimeError(f"申请上传URL失败: {error_msg}")

                data = result.get("data", {})
                return BatchUploadResponseData(
                    batch_id=data.get("batch_id", ""),
                    file_urls=data.get("file_urls", []),
                )

            except APIKeyUnavailableError:
                raise
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
        poll_interval = self.config.api.poll_interval_sec
        max_poll_minutes = self.config.api.max_poll_minutes

        # 记录上一次的 done 数量，用于检测是否卡住
        last_done_count = 0
        # 记录卡住状态的累计等待时间
        stuck_wait_time = 0.0

        while True:
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    result = await resp.json()
                    logger.debug(f"轮询API响应: {result}")
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
                logger.warning(f"轮询返回空结果，batch={batch_id}, data keys: {list((result.get('data') or {}).keys())}")
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
            unique_states = set(states)
            done_count = sum(1 for s in states if s == "done")
            failed_count = sum(1 for s in states if s == "failed")
            total = len(states)

            # 简化 batch_id 显示：只保留第一个 "-" 前的内容
            short_batch_id = batch_id.split("-")[0] if "-" in batch_id else batch_id
            logger.info(
                f"轮询 batch={short_batch_id}: "
                f"done={done_count}/{total} failed={failed_count}, 所有状态: {unique_states}"
            )

            if all(s in ("done", "failed") for s in states):
                return items

            # 检查是否卡住：状态只包含 done、pending、failed，且 done 数量没有增加
            if unique_states <= {"done", "pending", "failed"} and done_count == last_done_count:
                stuck_wait_time += poll_interval
                if stuck_wait_time >= max_poll_minutes * 60:
                    raise TimeoutError(
                        f"批次 {short_batch_id} 轮询超时 ({max_poll_minutes}分钟无进展)"
                    )
            else:
                # 有进展，重置卡住计时
                stuck_wait_time = 0.0

            last_done_count = done_count
            await asyncio.sleep(poll_interval)

    async def download_result(
        self,
        zip_url: str,
    ) -> Optional[list[dict[str, Any]]]:
        """
        下载结果ZIP并直接读取 content_list.json 内容。

        Returns:
            content_list.json 的内容块列表，如果不存在则返回 None
        """
        try:
            timeout = aiohttp.ClientTimeout(total=300)
            async with aiohttp.ClientSession(timeout=timeout) as dl_session:
                async with dl_session.get(zip_url) as resp:
                    resp.raise_for_status()
                    zip_bytes = await resp.read()
        except Exception as e:
            raise RuntimeError(f"下载结果失败: {e}") from e

        try:
            zip_buffer = io.BytesIO(zip_bytes)
            with zipfile.ZipFile(zip_buffer, "r") as zf:
                content_list_name = None
                for name in zf.namelist():
                    if not name or name.endswith("/"):
                        continue

                    safe_name = Path(name).name
                    if not safe_name:
                        continue

                    if safe_name.endswith("_content_list.json"):
                        content_list_name = name
                        break

                if content_list_name is None:
                    return None

                raw_content = zf.read(content_list_name)
                parsed = json.loads(raw_content.decode("utf-8"))
                if not isinstance(parsed, list):
                    raise RuntimeError("content_list.json 顶层不是数组")
                return parsed

        except zipfile.BadZipFile as e:
            raise RuntimeError(f"无效的ZIP文件: {e}") from e
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise RuntimeError(f"解析 content_list.json 失败: {e}") from e
