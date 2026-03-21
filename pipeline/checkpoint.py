"""SQLite 检查点系统 - 跟踪每个PDF的处理状态

防重复机制：
1. data_id 是 PRIMARY KEY，确保每个文件只被注册一次
2. 使用 INSERT OR IGNORE 避免重复插入
3. api_key_index 字段跟踪每个文件使用的API
4. 状态机确保文件按正确流程处理：pending -> uploading -> polling -> converting -> done
5. 批量更新使用事务确保原子性
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import aiosqlite

from .models import FileRecord, FileState

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
    data_id      TEXT PRIMARY KEY,
    pdf_path     TEXT NOT NULL,
    journal      TEXT DEFAULT '',
    year         TEXT DEFAULT '',
    batch_id     TEXT DEFAULT '',
    state        TEXT NOT NULL DEFAULT 'pending',
    error_msg    TEXT DEFAULT '',
    attempts     INTEGER DEFAULT 0,
    api_key_index INTEGER DEFAULT -1,
    created_at   TEXT,
    updated_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_state ON files(state);
CREATE INDEX IF NOT EXISTS idx_files_batch ON files(batch_id);
CREATE INDEX IF NOT EXISTS idx_files_date_api ON files(DATE(updated_at), api_key_index);
"""


class Checkpoint:
    """基于 SQLite 的文件处理进度检查点"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        """初始化数据库连接和表结构"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA_SQL)
        await self._migrate_add_year_column()
        await self._migrate_add_api_key_index_column()
        await self._db.commit()
        logger.info(f"检查点数据库已初始化: {self.db_path}")

    async def close(self) -> None:
        """关闭数据库连接"""
        if self._db:
            await self._db.close()
            self._db = None

    async def _migrate_add_year_column(self) -> None:
        """为旧数据库添加 year 列（如果不存在）"""
        assert self._db is not None
        cursor = await self._db.execute("PRAGMA table_info(files)")
        columns = {row[1] for row in await cursor.fetchall()}
        if "year" not in columns:
            await self._db.execute("ALTER TABLE files ADD COLUMN year TEXT DEFAULT ''")
            logger.info("数据库迁移: 添加 year 列")

    async def _migrate_add_api_key_index_column(self) -> None:
        """为旧数据库添加 api_key_index 列（如果不存在）"""
        assert self._db is not None
        cursor = await self._db.execute("PRAGMA table_info(files)")
        columns = {row[1] for row in await cursor.fetchall()}
        if "api_key_index" not in columns:
            await self._db.execute("ALTER TABLE files ADD COLUMN api_key_index INTEGER DEFAULT -1")
            logger.info("数据库迁移: 添加 api_key_index 列")

    async def register_files(self, records: list[FileRecord]) -> int:
        """注册新文件（跳过已存在的）。返回实际新增数量。"""
        assert self._db is not None
        added = 0
        now = datetime.now().isoformat()
        for rec in records:
            try:
                before_changes = self._db.total_changes
                await self._db.execute(
                    """INSERT OR IGNORE INTO files
                       (data_id, pdf_path, journal, year, state, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        rec.data_id,
                        rec.pdf_path,
                        rec.journal,
                        rec.year,
                        FileState.PENDING.value,
                        now,
                        now,
                    ),
                )
                added += self._db.total_changes - before_changes
            except Exception as e:
                logger.warning(f"注册文件失败，已跳过 {rec.data_id}: {e}")
        await self._db.commit()
        logger.info(f"注册 {len(records)} 个文件，新增约 {added} 个")
        return added

    async def get_pending(
        self, limit: int = 0, journals: Optional[list[str]] = None
    ) -> list[FileRecord]:
        """获取待处理的文件列表，可按期刊名过滤"""
        assert self._db is not None
        query = "SELECT * FROM files WHERE state = ?"
        params: list = [FileState.PENDING.value]
        if journals:
            placeholders = ",".join("?" for _ in journals)
            query += f" AND journal IN ({placeholders})"
            params.extend(journals)
        query += " ORDER BY created_at"
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_by_state(self, state: FileState) -> list[FileRecord]:
        """获取指定状态的文件"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT * FROM files WHERE state = ?", (state.value,)
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def get_by_batch(self, batch_id: str) -> list[FileRecord]:
        """获取指定批次的文件"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT * FROM files WHERE batch_id = ?", (batch_id,)
        )
        rows = await cursor.fetchall()
        return [self._row_to_record(row) for row in rows]

    async def update_state(
        self,
        data_id: str,
        state: FileState,
        batch_id: str = "",
        error_msg: str = "",
        increment_attempts: bool = False,
        api_key_index: int = -1,
    ) -> None:
        """更新文件状态"""
        assert self._db is not None
        now = datetime.now().isoformat()
        if increment_attempts:
            await self._db.execute(
                """UPDATE files SET state=?, batch_id=?, error_msg=?,
                   attempts=attempts+1, api_key_index=?, updated_at=? WHERE data_id=?""",
                (state.value, batch_id, error_msg, api_key_index, now, data_id),
            )
        else:
            sql = "UPDATE files SET state=?, updated_at=?"
            params: list = [state.value, now]
            if batch_id:
                sql += ", batch_id=?"
                params.append(batch_id)
            if error_msg:
                sql += ", error_msg=?"
                params.append(error_msg)
            if api_key_index >= 0:
                sql += ", api_key_index=?"
                params.append(api_key_index)
            sql += " WHERE data_id=?"
            params.append(data_id)
            await self._db.execute(sql, params)
        await self._db.commit()

    async def bulk_update_state(
        self,
        data_ids: list[str],
        state: FileState,
        batch_id: str = "",
        api_key_index: int = -1,
    ) -> None:
        """批量更新文件状态"""
        assert self._db is not None
        now = datetime.now().isoformat()
        for data_id in data_ids:
            if api_key_index >= 0:
                await self._db.execute(
                    "UPDATE files SET state=?, batch_id=?, api_key_index=?, updated_at=? WHERE data_id=?",
                    (state.value, batch_id, api_key_index, now, data_id),
                )
            else:
                await self._db.execute(
                    "UPDATE files SET state=?, batch_id=?, updated_at=? WHERE data_id=?",
                    (state.value, batch_id, now, data_id),
                )
        await self._db.commit()

    async def reset_failed(self) -> int:
        """将所有失败的文件重置为待处理"""
        assert self._db is not None
        now = datetime.now().isoformat()
        cursor = await self._db.execute(
            "UPDATE files SET state=?, error_msg='', updated_at=? WHERE state=?",
            (FileState.PENDING.value, now, FileState.FAILED.value),
        )
        await self._db.commit()
        return cursor.rowcount

    async def reset_stale(self) -> int:
        """将卡在中间状态的文件重置为待处理。

        在不保留 raw 文件的模式下，downloaded / converting 也不可恢复，
        因此启动时一并回退到 pending 重新跑整条链路。
        """
        assert self._db is not None
        now = datetime.now().isoformat()
        stale_states = (
            FileState.UPLOADING.value,
            FileState.POLLING.value,
            FileState.DOWNLOADED.value,
            FileState.CONVERTING.value,
        )
        cursor = await self._db.execute(
            f"UPDATE files SET state=?, updated_at=? WHERE state IN (?, ?, ?, ?)",
            (FileState.PENDING.value, now, *stale_states),
        )
        await self._db.commit()
        return cursor.rowcount

    async def get_stats(self) -> dict[str, int]:
        """获取各状态的文件统计"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT state, COUNT(*) FROM files GROUP BY state"
        )
        rows = await cursor.fetchall()
        stats = {row[0]: row[1] for row in rows}
        cursor2 = await self._db.execute("SELECT COUNT(*) FROM files")
        total_row = await cursor2.fetchone()
        stats["total"] = total_row[0] if total_row else 0
        return stats

    async def get_today_done_count(self, api_key_index: int = -1) -> int:
        """获取今天已完成的文件数量
        
        Args:
            api_key_index: API索引，-1表示统计所有API
        """
        assert self._db is not None
        today = datetime.now().date().isoformat()
        if api_key_index >= 0:
            cursor = await self._db.execute(
                """SELECT COUNT(*) FROM files 
                   WHERE state = ? AND DATE(updated_at) = ? AND api_key_index = ?""",
                (FileState.DONE.value, today, api_key_index),
            )
        else:
            cursor = await self._db.execute(
                """SELECT COUNT(*) FROM files 
                   WHERE state = ? AND DATE(updated_at) = ?""",
                (FileState.DONE.value, today),
            )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_all_api_today_stats(self, num_apis: int) -> dict[int, int]:
        """获取所有API今日完成统计
        
        Args:
            num_apis: API数量
            
        Returns:
            {api_index: count} 字典
        """
        assert self._db is not None
        today = datetime.now().date().isoformat()
        cursor = await self._db.execute(
            """SELECT api_key_index, COUNT(*) as cnt FROM files 
               WHERE state = ? AND DATE(updated_at) = ? AND api_key_index >= 0
               GROUP BY api_key_index""",
            (FileState.DONE.value, today),
        )
        rows = await cursor.fetchall()
        result = {idx: 0 for idx in range(num_apis)}
        for row in rows:
            result[row[0]] = row[1]
        return result

    async def is_processing(self, data_id: str) -> bool:
        """检查文件是否正在处理中（非pending和非done状态）"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT state FROM files WHERE data_id=?", (data_id,)
        )
        row = await cursor.fetchone()
        if row is None:
            return False
        state = row[0]
        return state not in (FileState.PENDING.value, FileState.DONE.value, FileState.FAILED.value)

    async def get_record(self, data_id: str) -> Optional[FileRecord]:
        """获取单个文件记录"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT * FROM files WHERE data_id=?", (data_id,)
        )
        row = await cursor.fetchone()
        return self._row_to_record(row) if row else None

    async def is_done(self, data_id: str) -> bool:
        """检查文件是否已完成"""
        assert self._db is not None
        cursor = await self._db.execute(
            "SELECT state FROM files WHERE data_id=?", (data_id,)
        )
        row = await cursor.fetchone()
        return row is not None and row[0] == FileState.DONE.value

    def _row_to_record(self, row: Any) -> FileRecord:
        return FileRecord(
            data_id=row["data_id"],
            pdf_path=row["pdf_path"],
            journal=row["journal"] or "",
            year=row["year"] or "",
            batch_id=row["batch_id"] or "",
            state=FileState(row["state"]),
            error_msg=row["error_msg"] or "",
            attempts=row["attempts"] or 0,
            api_key_index=row["api_key_index"] if "api_key_index" in row.keys() else -1,
            created_at=datetime.fromisoformat(row["created_at"])
            if row["created_at"]
            else None,
            updated_at=datetime.fromisoformat(row["updated_at"])
            if row["updated_at"]
            else None,
        )
