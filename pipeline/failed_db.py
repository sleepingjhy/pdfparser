"""失败文件记录数据库 - 记录处理失败的PDF文件信息

用于持久化存储失败文件的信息，支持后续分析和重试。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from .models import FileRecord

logger = logging.getLogger(__name__)

FAILED_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS failed_files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    data_id      TEXT NOT NULL,
    pdf_path     TEXT NOT NULL,
    journal      TEXT DEFAULT '',
    year         TEXT DEFAULT '',
    batch_id     TEXT DEFAULT '',
    error_msg    TEXT DEFAULT '',
    attempts     INTEGER DEFAULT 0,
    api_key_index INTEGER DEFAULT -1,
    created_at   TEXT,
    updated_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_failed_data_id ON failed_files(data_id);
CREATE INDEX IF NOT EXISTS idx_failed_created ON failed_files(created_at);
"""


class FailedDB:
    """失败文件记录数据库"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        """初始化数据库连接和表结构"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(FAILED_SCHEMA_SQL)
        await self._db.commit()
        logger.info(f"失败记录数据库已初始化: {self.db_path}")

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._db:
            await self._db.close()
            self._db = None

    async def record_failure(
        self,
        rec: FileRecord,
        error_msg: str = "",
        batch_id: str = "",
        api_key_index: int = -1,
    ) -> None:
        """记录一个失败的文件"""
        assert self._db is not None
        now = datetime.now().isoformat()

        # 检查是否已存在
        cursor = await self._db.execute(
            "SELECT id, attempts FROM failed_files WHERE data_id = ?",
            (rec.data_id,)
        )
        existing = await cursor.fetchone()

        if existing:
            # 更新现有记录
            await self._db.execute(
                """UPDATE failed_files 
                   SET error_msg = ?, batch_id = ?, attempts = attempts + 1,
                       api_key_index = ?, updated_at = ?
                   WHERE data_id = ?""",
                (error_msg, batch_id, api_key_index, now, rec.data_id)
            )
        else:
            # 插入新记录
            await self._db.execute(
                """INSERT INTO failed_files
                   (data_id, pdf_path, journal, year, batch_id, error_msg,
                    attempts, api_key_index, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (rec.data_id, rec.pdf_path, rec.journal, rec.year,
                 batch_id, error_msg, rec.attempts, api_key_index, now, now)
            )

        await self._db.commit()

    async def get_all_failures(self) -> list[dict[str, Any]]:
        """获取所有失败记录"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT * FROM failed_files ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_failures_for_retry(self, limit: int = 0) -> list[FileRecord]:
        """获取可用于重试的失败记录

        Args:
            limit: 最大返回数量，0表示不限制

        Returns:
            FileRecord 列表
        """
        assert self._db is not None
        query = "SELECT * FROM failed_files ORDER BY created_at"
        if limit > 0:
            query += f" LIMIT {limit}"

        cursor = await self._db.execute(query)
        rows = await cursor.fetchall()

        return [
            FileRecord(
                data_id=row["data_id"],
                pdf_path=row["pdf_path"],
                journal=row["journal"] or "",
                year=row["year"] or "",
                batch_id="",
                state="pending",
                error_msg=row["error_msg"] or "",
                attempts=row["attempts"] or 0,
                api_key_index=row["api_key_index"] if "api_key_index" in row.keys() else -1,
            )
            for row in rows
        ]

    async def remove_failure(self, data_id: str) -> None:
        """从失败记录中移除（重试成功后调用）"""
        assert self._db is not None
        await self._db.execute(
            "DELETE FROM failed_files WHERE data_id = ?",
            (data_id,)
        )
        await self._db.commit()

    async def get_failure_count(self) -> int:
        """获取失败记录总数"""
        assert self._db is not None
        cursor = await self._db.execute("SELECT COUNT(*) FROM failed_files")
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def clear_all(self) -> int:
        """清空所有失败记录"""
        assert self._db is not None
        cursor = await self._db.execute("DELETE FROM failed_files")
        await self._db.commit()
        return cursor.rowcount
