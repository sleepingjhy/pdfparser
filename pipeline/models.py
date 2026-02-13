"""数据模型定义"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ============================================================
# API 请求 / 响应 模型
# ============================================================


class FileUploadItem(BaseModel):
    """单个文件上传请求项"""

    name: str
    is_ocr: bool = True
    data_id: str = ""


class BatchUploadRequest(BaseModel):
    """批量上传请求体"""

    files: list[FileUploadItem]
    model_version: str = "pipeline"
    enable_formula: bool = True
    enable_table: bool = False
    language: str = "ch"


class BatchUploadResponseData(BaseModel):
    """批量上传响应 data 部分"""

    batch_id: str
    file_urls: list[str]


class ExtractResultItem(BaseModel):
    """单个文件提取结果"""

    file_name: str = ""
    data_id: str = ""
    state: str = ""  # pending, waiting-file, running, converting, done, failed
    full_zip_url: str = ""
    err_msg: str = ""


# ============================================================
# 目标 JSON 输出模型
# ============================================================


class Section(BaseModel):
    """论文章节"""

    title: str = ""
    authors: list[str] = Field(default_factory=list)
    abstract: str = ""
    keywords: list[str] = Field(default_factory=list)
    paragraphs: list[str] = Field(default_factory=list)
    section: list[Section] = Field(default_factory=list)


class PaperDocument(BaseModel):
    """论文文档 - 最终输出格式"""

    _id: str = ""
    forum: str = ""  # 期刊中文名
    doi: str = ""
    fulltext: list[Section] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典，保证 _id 在最前面"""
        result: dict[str, Any] = {"_id": self._id}
        result["forum"] = self.forum
        result["doi"] = self.doi
        result["fulltext"] = [self._section_to_dict(s) for s in self.fulltext]
        return result

    def _section_to_dict(self, s: Section) -> dict[str, Any]:
        d: dict[str, Any] = {"title": s.title}
        if s.authors:
            d["authors"] = s.authors
        if s.abstract:
            d["abstract"] = s.abstract
        if s.keywords:
            d["keywords"] = s.keywords
        d["paragraphs"] = s.paragraphs
        d["section"] = [self._section_to_dict(sub) for sub in s.section]
        return d


# ============================================================
# 检查点模型
# ============================================================


class FileState(str, enum.Enum):
    """文件处理状态"""

    PENDING = "pending"
    UPLOADING = "uploading"
    POLLING = "polling"
    DOWNLOADED = "downloaded"
    CONVERTING = "converting"
    DONE = "done"
    FAILED = "failed"


class FileRecord(BaseModel):
    """文件检查点记录"""

    data_id: str  # 唯一标识 (文件名stem)
    pdf_path: str  # PDF 完整路径
    journal: str = ""  # 期刊名称
    year: str = ""  # 发表年份（从目录结构提取）
    batch_id: str = ""  # MinerU 批次ID
    state: FileState = FileState.PENDING
    error_msg: str = ""
    attempts: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# ============================================================
# MinerU content_list.json 内容块模型
# ============================================================


class ContentBlock(BaseModel):
    """content_list.json 中的单个内容块"""

    type: str = ""  # text, equation, image, table, ref_text, header, footer, ...
    text: str = ""
    text_level: Optional[int] = None  # 标题级别: 1=一级, 2=二级, ...
    text_format: str = ""  # latex 等
    img_path: str = ""
    image_caption: list[str] = Field(default_factory=list)
    table_body: str = ""
    table_caption: list[str] = Field(default_factory=list)
    page_idx: int = 0
    bbox: list[float] = Field(default_factory=list)
    sub_type: str = ""
    list_items: list[str] = Field(default_factory=list)
    code_body: str = ""
    code_caption: list[str] = Field(default_factory=list)

    class Config:
        extra = "allow"
