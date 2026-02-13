"""配置加载模块"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class ApiConfig:
    base_url: str = "https://mineru.net/api/v4"
    api_key: str = ""
    concurrency: int = 5
    batch_size: int = 10
    poll_interval_sec: int = 30
    max_poll_minutes: int = 60
    retry_max: int = 3
    retry_backoff_sec: int = 60


@dataclass
class PathsConfig:
    pdf_input: str = r"E:\Crawler\data\pdf"
    raw_output: str = r"E:\MinerU\data\raw"
    final_output: str = r"E:\MinerU\data\output"
    checkpoint_db: str = r"E:\MinerU\data\checkpoint.db"
    log_file: str = r"E:\MinerU\pipeline.log"


@dataclass
class ExtractionConfig:
    is_ocr: bool = True
    enable_formula: bool = True
    enable_table: bool = False
    language: str = "ch"
    model_version: str = "pipeline"


@dataclass
class AppConfig:
    api: ApiConfig = field(default_factory=ApiConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    exclude_prefixes: list[str] = field(default_factory=list)


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """加载配置文件，合并环境变量"""
    if config_path is None:
        config_path = str(Path(__file__).parent.parent / "config.yaml")

    cfg = AppConfig()

    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        # API 配置
        api_raw = raw.get("api", {})
        cfg.api.base_url = api_raw.get("base_url", cfg.api.base_url)
        cfg.api.api_key = api_raw.get("api_key", cfg.api.api_key)
        cfg.api.concurrency = api_raw.get("concurrency", cfg.api.concurrency)
        cfg.api.batch_size = api_raw.get("batch_size", cfg.api.batch_size)
        cfg.api.poll_interval_sec = api_raw.get(
            "poll_interval_sec", cfg.api.poll_interval_sec
        )
        cfg.api.max_poll_minutes = api_raw.get(
            "max_poll_minutes", cfg.api.max_poll_minutes
        )
        cfg.api.retry_max = api_raw.get("retry_max", cfg.api.retry_max)
        cfg.api.retry_backoff_sec = api_raw.get(
            "retry_backoff_sec", cfg.api.retry_backoff_sec
        )

        # 路径配置
        paths_raw = raw.get("paths", {})
        cfg.paths.pdf_input = paths_raw.get("pdf_input", cfg.paths.pdf_input)
        cfg.paths.raw_output = paths_raw.get("raw_output", cfg.paths.raw_output)
        cfg.paths.final_output = paths_raw.get("final_output", cfg.paths.final_output)
        cfg.paths.checkpoint_db = paths_raw.get(
            "checkpoint_db", cfg.paths.checkpoint_db
        )
        cfg.paths.log_file = paths_raw.get("log_file", cfg.paths.log_file)

        # 提取配置
        ext_raw = raw.get("extraction", {})
        cfg.extraction.is_ocr = ext_raw.get("is_ocr", cfg.extraction.is_ocr)
        cfg.extraction.enable_formula = ext_raw.get(
            "enable_formula", cfg.extraction.enable_formula
        )
        cfg.extraction.enable_table = ext_raw.get(
            "enable_table", cfg.extraction.enable_table
        )
        cfg.extraction.language = ext_raw.get("language", cfg.extraction.language)
        cfg.extraction.model_version = ext_raw.get(
            "model_version", cfg.extraction.model_version
        )

        cfg.exclude_prefixes = raw.get("exclude_prefixes", [])

    # API Key: 环境变量优先，否则从配置文件读取
    env_key = os.environ.get("MINERU_API_KEY", "").strip()
    if env_key:
        cfg.api.api_key = env_key

    return cfg
