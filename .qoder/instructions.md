# MinerU PDF 提取管道 - 项目结构说明

## 项目概述

这是一个 PDF 批量处理工具，用于将学术论文 PDF 自动转换成结构化的 JSON 文件。通过调用 MinerU 云端 API 识别 PDF 内容，自动提取标题、摘要、正文等核心内容。

---

## 目录结构

```
minerU/
├── config.yaml          # 主配置文件（API、路径、并发等参数）
├── config.local.yaml    # 本地配置覆盖（不提交到仓库，存放 API Key）
├── requirements.txt     # Python 依赖包列表
├── run.py               # CLI 入口（命令行接口）
├── checkpoint.db        # SQLite 数据库，记录所有文件处理状态
├── failed.db            # SQLite 数据库，记录失败文件详情
├── pipeline.log         # 运行日志
│
├── pipeline/            # 核心处理模块
│   ├── __init__.py      # 模块初始化
│   ├── config.py        # 配置加载和数据类定义
│   ├── models.py        # 数据模型（FileRecord, FileState 等）
│   ├── checkpoint.py    # 检查点管理（SQLite 状态跟踪）
│   ├── failed_db.py     # 失败记录数据库管理
│   ├── scanner.py       # PDF 文件扫描器
│   ├── api_client.py    # MinerU API 客户端
│   ├── converter.py     # JSON 格式转换器
│   └── processor.py     # 主处理器（编排整个流程）
│
└── .qoder/              # Qoder AI 助手配置目录
    └── instructions.md  # 本文件
```

---

## 核心文件功能说明

### run.py
- **功能**: CLI 命令入口
- **命令**:
  - `python run.py run` - 运行完整管道
  - `python run.py run --limit N` - 限制处理数量
  - `python run.py run --retry 1` - 从 failed.db 重试失败文件
  - `python run.py scan` - 仅扫描注册新 PDF
  - `python run.py status` - 显示处理状态
  - `python run.py retry-failed` - 重置失败文件为待处理

### config.yaml
- **功能**: 主配置文件
- **关键配置**:
  - `api.api_configs` - 多 API 密钥配置（每个有独立 daily_limit）
  - `api.enable_concurrent` - 是否启用并发处理
  - `api.concurrency` - 并发数量
  - `api.multi_api_strategy` - 多 API 策略（round_robin / quota_first）
  - `paths.final_output` - 主输出目录
  - `paths.fallback_final_outputs` - 备用输出目录列表
  - `paths.min_free_gb` - 磁盘空间阈值
  - `paths.checkpoint_db` - 检查点数据库路径
  - `paths.failed_db` - 失败记录数据库路径

### pipeline/config.py
- **功能**: 配置加载和验证
- **关键类**:
  - `AppConfig` - 应用配置根类
  - `ApiConfig` - API 配置
  - `SingleApiConfig` - 单个 API 配置（key, daily_limit, name）
  - `PathsConfig` - 路径配置
  - `ExtractionConfig` - 提取参数配置

### pipeline/models.py
- **功能**: 数据模型定义
- **关键类**:
  - `FileRecord` - 文件记录（data_id, pdf_path, journal, year, state, error_msg 等）
  - `FileState` - 文件状态枚举（pending, uploading, polling, converting, done, failed）

### pipeline/checkpoint.py
- **功能**: 检查点管理，跟踪每个文件的处理状态
- **关键类**: `Checkpoint`
- **关键方法**:
  - `register_files()` - 注册新文件
  - `get_pending()` - 获取待处理文件
  - `update_state()` - 更新文件状态
  - `get_today_done_count()` - 获取今日完成数量（支持按 API 索引过滤）
  - `get_all_api_today_stats()` - 获取所有 API 今日统计
  - `reset_failed()` - 重置失败文件
  - `reset_stale()` - 重置中间状态文件

### pipeline/failed_db.py
- **功能**: 失败文件记录管理
- **关键类**: `FailedDB`
- **关键方法**:
  - `record_failure()` - 记录失败文件
  - `get_failures_for_retry()` - 获取可重试的失败记录
  - `remove_failure()` - 移除失败记录（重试成功后）

### pipeline/processor.py
- **功能**: 主处理器，编排整个处理流程
- **关键类**:
  - `MultiAPIManager` - 多 API 管理器（分配、切换、配额跟踪）
  - `Processor` - 主处理器
- **关键方法**:
  - `run()` - 运行完整管道
  - `run_retry()` - 从 failed.db 重试
  - `_process_batch()` - 处理单个批次
  - `_download_and_convert()` - 下载结果并转换

### pipeline/api_client.py
- **功能**: MinerU API 客户端
- **关键类**: `MinerUAPIClient`
- **关键方法**:
  - `request_upload_urls()` - 申请上传 URL
  - `upload_file()` - 上传文件
  - `poll_batch_results()` - 轮询处理结果
  - `download_result()` - 下载结果包

### pipeline/scanner.py
- **功能**: PDF 文件扫描器
- **关键函数**: `scan_pdfs()` - 递归扫描目录，提取期刊名和年份

### pipeline/converter.py
- **功能**: JSON 格式转换器
- **关键函数**:
  - `convert_content_blocks()` - 将 API 返回的内容块转换为目标格式
  - `save_paper_json()` - 保存 JSON 文件

---

## 处理流程

```
1. scan_and_register()     扫描 PDF 目录，注册新文件到 checkpoint.db
         ↓
2. get_pending()           获取待处理文件列表
         ↓
3. _process_batch()        分批处理
   ├── request_upload_urls()  申请上传 URL
   ├── upload_file()          上传 PDF
   ├── poll_batch_results()   轮询处理结果
   └── _download_and_convert() 下载结果并转换 JSON
         ↓
4. update_state()          更新文件状态为 done/failed
```

---

## 多 API 机制

1. **配置**: 在 `config.yaml` 中配置多个 API 密钥
2. **策略**:
   - `round_robin`: 轮询分配，按顺序使用各 API
   - `quota_first`: 配额优先，优先使用剩余配额最多的 API
3. **切换**: 当一个 API 达到每日限额时，自动切换到下一个
4. **跟踪**: 通过 `api_key_index` 字段跟踪每个文件使用的 API

---

## 防重复机制

1. `data_id` 是 PRIMARY KEY，确保每个文件只注册一次
2. 使用 `INSERT OR IGNORE` 避免重复插入
3. 状态机确保文件按正确流程处理
4. 批量更新使用事务确保原子性

---

## Git 推送注意事项

**重要安全规则**：每次提交推送时都要先遮掩 config.yaml 中的 api_key 配置，推送到 GitHub 上后恢复原 api_key。

操作步骤：
1. 推送前：将 `config.yaml` 中的 `api_key` 值替换为占位符（如 `YOUR_API_KEY_HERE`）
2. 提交并推送到 GitHub
3. 推送后：恢复原有的真实 api_key 值

---

## 输出格式

每篇论文生成一个 JSON 文件：

```
data/output/{journal}/{year}/{data_id}.json
```

JSON 结构：
```json
{
  "_id": "论文ID",
  "forum": "期刊名",
  "doi": "",
  "fulltext": [
    {
      "title": "章节标题",
      "paragraphs": ["段落1", "段落2"],
      "section": [子章节列表]
    }
  ]
}
```
