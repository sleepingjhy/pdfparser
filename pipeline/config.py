"""配置加载模块"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class SingleApiConfig:
    """单个API的配置"""
    api_key: str = ""
    daily_limit: int = 0  # 该API每日处理文件数上限，0表示不限制
    name: str = ""  # API名称标识，便于日志识别


@dataclass
class ApiConfig:
    base_url: str = "https://mineru.net/api/v4"
    api_key: str = ""
    api_keys: list[str] = field(default_factory=list)
    api_configs: list[SingleApiConfig] = field(default_factory=list)  # 多API配置
    enable_concurrent: bool = True  # 是否启用并发处理
    concurrency: int = 5
    batch_size: int = 10
    poll_interval_sec: int = 30
    max_poll_minutes: int = 60
    retry_max: int = 3
    retry_backoff_sec: int = 60
    daily_limit: int = 0  # 兼容旧配置：每日处理文件数上限，0表示不限制
    first_upload_delay_sec: int = 10  # 首次上传时，多个API之间的间隔时间（秒）


@dataclass
class PathsConfig:
    pdf_input: str = r"E:\Files\pdf"
    # 兼容旧配置保留；当前流程不再落地 raw 文件。
    raw_output: str = r"E:\MinerU\data\raw"
    final_output: str = r"E:\MinerU\data\output"
    # 备用输出目录列表（主目录空间不足时按顺序切换）
    fallback_final_outputs: list[str] = field(default_factory=list)
    # 输出目录所在磁盘的最小剩余空间阈值（GB）
    min_free_gb: float = 5.0
    checkpoint_db: str = r"E:\MinerU\data\checkpoint.db"
    # 失败文件记录数据库路径
    failed_db: str = r"E:\MinerU\data\failed.db"
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


def _merge_config(cfg: AppConfig, raw: dict) -> None:
    """将单个 YAML 配置块合并到当前配置对象。"""
    api_raw = raw.get("api", {})
    cfg.api.base_url = api_raw.get("base_url", cfg.api.base_url)
    cfg.api.api_key = api_raw.get("api_key", cfg.api.api_key)
    if "api_keys" in api_raw:
        cfg.api.api_keys = [
            key.strip()
            for key in api_raw.get("api_keys", [])
            if isinstance(key, str) and key.strip()
        ]
    
    # 读取多API配置
    if "api_configs" in api_raw:
        cfg.api.api_configs = []
        for api_cfg in api_raw.get("api_configs", []):
            if isinstance(api_cfg, dict) and api_cfg.get("api_key"):
                cfg.api.api_configs.append(SingleApiConfig(
                    api_key=api_cfg["api_key"].strip(),
                    daily_limit=api_cfg.get("daily_limit", 0),
                    name=api_cfg.get("name", "")
                ))
    
    cfg.api.enable_concurrent = api_raw.get("enable_concurrent", cfg.api.enable_concurrent)
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
    cfg.api.daily_limit = api_raw.get("daily_limit", cfg.api.daily_limit)
    cfg.api.first_upload_delay_sec = api_raw.get(
        "first_upload_delay_sec", cfg.api.first_upload_delay_sec
    )

    paths_raw = raw.get("paths", {})
    cfg.paths.pdf_input = paths_raw.get("pdf_input", cfg.paths.pdf_input)
    cfg.paths.raw_output = paths_raw.get("raw_output", cfg.paths.raw_output)
    cfg.paths.final_output = paths_raw.get("final_output", cfg.paths.final_output)
    if "fallback_final_outputs" in paths_raw:
        cfg.paths.fallback_final_outputs = [
            p.strip()
            for p in paths_raw.get("fallback_final_outputs", [])
            if isinstance(p, str) and p.strip()
        ]
    cfg.paths.min_free_gb = float(paths_raw.get("min_free_gb", cfg.paths.min_free_gb))
    cfg.paths.checkpoint_db = paths_raw.get("checkpoint_db", cfg.paths.checkpoint_db)
    cfg.paths.failed_db = paths_raw.get("failed_db", cfg.paths.failed_db)
    cfg.paths.log_file = paths_raw.get("log_file", cfg.paths.log_file)

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

    if "exclude_prefixes" in raw:
        cfg.exclude_prefixes = raw.get("exclude_prefixes", [])


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """加载配置文件，自动叠加 config.local.yaml，并合并环境变量。"""
    if config_path is None:
        config_path = str(Path(__file__).parent.parent / "config.yaml")

    cfg = AppConfig()
    config_file = Path(config_path)

    if config_file.exists():
        with open(config_file, "r", encoding="utf-8") as f:
            _merge_config(cfg, yaml.safe_load(f) or {})

    local_config_path = config_file.with_name("config.local.yaml")
    if local_config_path.exists():
        with open(local_config_path, "r", encoding="utf-8") as f:
            _merge_config(cfg, yaml.safe_load(f) or {})

    # API Key: 环境变量优先，否则从配置文件读取
    env_keys_raw = os.environ.get("MINERU_API_KEYS", "").strip()
    if env_keys_raw:
        parsed_keys = [
            key.strip()
            for key in env_keys_raw.replace(";", ",").split(",")
            if key.strip()
        ]
        if parsed_keys:
            cfg.api.api_keys = parsed_keys
            cfg.api.api_key = parsed_keys[0]
    else:
        env_key = os.environ.get("MINERU_API_KEY", "").strip()
        if env_key:
            cfg.api.api_key = env_key
            cfg.api.api_keys = [env_key]

    return cfg
