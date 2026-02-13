"""PDF 文件扫描器 - 发现新PDF并注册到检查点"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from .models import FileRecord

logger = logging.getLogger(__name__)

# 单个文件大小限制 200MB
MAX_FILE_SIZE_BYTES = 200 * 1024 * 1024


def scan_pdfs(
    input_dir: str,
    existing_ids: Optional[set[str]] = None,
    exclude_prefixes: Optional[list[str]] = None,
) -> list[FileRecord]:
    """
    递归扫描输入目录中的 PDF 文件。

    目录结构预期：
      {input_dir}/   (可以是任意层级的嵌套目录)
        *.pdf

    返回尚未在检查点中注册的 FileRecord 列表。
    """
    input_path = Path(input_dir)
    if not input_path.exists():
        logger.error(f"输入目录不存在: {input_dir}")
        return []

    existing = existing_ids or set()
    prefixes = tuple(exclude_prefixes) if exclude_prefixes else ()
    records: list[FileRecord] = []
    skipped_size = 0
    skipped_exists = 0
    skipped_prefix = 0

    for pdf_file in sorted(input_path.rglob("*.pdf")):
        if not pdf_file.is_file():
            continue

        data_id = pdf_file.stem

        if prefixes and data_id.startswith(prefixes):
            skipped_prefix += 1
            continue

        # 跳过已处理的
        if data_id in existing:
            skipped_exists += 1
            continue

        # 检查文件大小
        file_size = pdf_file.stat().st_size
        if file_size > MAX_FILE_SIZE_BYTES:
            logger.warning(
                f"文件过大，跳过: {pdf_file} ({file_size / 1024 / 1024:.1f}MB)"
            )
            skipped_size += 1
            continue

        # 尝试从目录结构中提取期刊和年份
        journal, year = _extract_journal_and_year(pdf_file, input_path)

        records.append(
            FileRecord(
                data_id=data_id,
                pdf_path=str(pdf_file),
                journal=journal,
                year=year,
            )
        )

    logger.info(
        f"扫描完成: 发现 {len(records)} 个新PDF, "
        f"跳过 {skipped_exists} 个已注册, "
        f"跳过 {skipped_size} 个超大文件, "
        f"跳过 {skipped_prefix} 个英文版(排除前缀)"
    )
    return records


def _extract_journal_and_year(pdf_file: Path, base_dir: Path) -> tuple[str, str]:
    """
    从文件路径中推断期刊名称和年份。

    支持多种目录结构：
      {base}/{journal}/{year}/{file}.pdf  → (journal, year)
      {base}/{journal}/{file}.pdf         → (journal, "")
      {base}/{file}.pdf                   → ("", "")

    年份通过匹配4位数字（1900-2099）来识别。
    """
    try:
        rel = pdf_file.relative_to(base_dir)
        parts = rel.parts
        if len(parts) >= 3:
            journal = parts[0]
            year_candidate = parts[1]
            if re.match(r"^(19|20)\d{2}$", year_candidate):
                return journal, year_candidate
            return journal, ""
        if len(parts) >= 2:
            return parts[0], ""
    except ValueError:
        pass
    return "", ""
