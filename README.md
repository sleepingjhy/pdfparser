# MinerU PDF 提取管道

这个工具可以把大量学术论文 PDF **自动转换成结构化的 JSON 文件**，方便用来训练 AI 模型或构建语料库。

它通过调用 [MinerU 云端 API](https://mineru.net) 来识别 PDF 中的文字和公式，自动去掉表格、图片、参考文献和附录，只保留论文的核心内容（标题、摘要、正文），并按章节层级整理好。

---

## 项目结构

```
E:\MinerU\
├── config.yaml          # 配置文件（路径、API参数等）
├── requirements.txt     # Python 依赖包列表
├── run.py               # 程序入口（在这里运行命令）
├── pipeline/
│   ├── __init__.py
│   ├── models.py        # 数据模型定义
│   ├── config.py        # 读取配置文件
│   ├── checkpoint.py    # 断点续传（记录处理进度）
│   ├── scanner.py       # 扫描发现 PDF 文件
│   ├── api_client.py    # 和 MinerU API 通信
│   ├── converter.py     # 把 API 返回的结果转成目标 JSON
│   └── processor.py     # 把上面的模块串起来，编排整个流程
└── data/
    ├── checkpoint.db    # 进度数据库（自动生成，不用管）
    ├── raw/             # API 返回的原始结果（自动生成）
    └── output/          # 最终输出的 JSON 文件
```

---

## 环境准备

你需要先装好以下软件：

- **Python 3.10 或更高版本** — 在命令行输入 `python --version` 可以查看版本
- **pip** — Python 自带的包管理工具，一般装了 Python 就有

---

## 安装步骤

打开命令提示符（按 `Win + R`，输入 `cmd`，回车），进入项目目录，安装依赖：

```bash
cd E:\MinerU
pip install -r requirements.txt
```

看到类似 `Successfully installed ...` 或 `Requirement already satisfied` 就说明装好了。

---

## 获取 API Key

这个工具需要调用 MinerU 的云端服务来识别 PDF，所以你需要一个 API Key（相当于通行证）。

1. 打开 [MinerU API 管理页面](https://mineru.net/apiManage)
2. 注册 / 登录账号
3. 在页面上找到你的 API Key，复制下来

⚠️ MinerU API 是按页数计费的，请先用少量文件测试，确认效果满意后再大批量处理。

---

## 配置 API Key

每次打开新的命令行窗口，都需要先设置 API Key：

**Windows（命令提示符）：**
```bash
set MINERU_API_KEY=你的Key粘贴在这里
```

**Windows（PowerShell）：**
```powershell
$env:MINERU_API_KEY="你的Key粘贴在这里"
```

**Linux / Mac：**
```bash
export MINERU_API_KEY=你的Key粘贴在这里
```

设置完后可以在同一个窗口里运行程序。关掉窗口后需要重新设置。

---

## 配置文件说明

项目根目录有一个 `config.yaml`，里面可以调整各种参数。大部分情况下不需要改，但有几个你可能会用到：

```yaml
paths:
  pdf_input: "E:\\Crawler\\data\\pdf"    # PDF文件放在哪里
  final_output: "E:\\MinerU\\data\\output" # JSON输出到哪里

api:
  concurrency: 5      # 同时上传几个文件（默认5，比较保守）
  batch_size: 10       # 每批处理几个文件

extraction:
  is_ocr: true         # 是否启用OCR（扫描版PDF需要开启）
```

### PDF 文件怎么放？

把 PDF 文件放到 `pdf_input` 指定的目录下就行。支持嵌套子目录，程序会自动递归扫描所有 `.pdf` 文件。

**第一级子目录的名称会自动作为期刊名**，写入 JSON 的 `forum` 字段。例如：

```
E:\Crawler\data\pdf\
├── 计算机学报\
│   ├── 论文A.pdf      → forum = "计算机学报"
│   └── 论文B.pdf      → forum = "计算机学报"
├── 软件学报\
│   └── 2023\
│       └── 论文C.pdf  → forum = "软件学报"
└── 论文D.pdf          → forum = ""（没有子目录，期刊名为空）
```

---

## 快速开始

打开命令行，进入项目目录，按顺序执行：

### 第一步：设置 API Key

```bash
cd E:\MinerU
set MINERU_API_KEY=你的Key
```

### 第二步：扫描 PDF 文件

```bash
python run.py scan
```

程序会扫描 PDF 目录，告诉你发现了多少个文件。这一步不消耗 API 额度。

### 第三步：先用少量文件测试

```bash
python run.py run --limit 10
```

这会只处理 10 个文件，让你看看效果。处理过程中会显示进度条。

### 第四步：检查输出

去 `data\output\` 目录下看看生成的 JSON 文件，确认格式和内容是否符合预期。

### 第五步：处理全部文件

确认没问题后，运行（不加 `--limit` 就是处理所有）：

```bash
python run.py run
```

处理 10 万篇论文需要较长时间，可以随时按 `Ctrl + C` 中断，下次继续。

---

## 所有命令一览

| 命令 | 说明 |
|------|------|
| `python run.py run` | 运行完整管道（扫描 → 上传 → 等待结果 → 下载 → 转换） |
| `python run.py run --limit 10` | 只处理 10 个文件（数字可以改） |
| `python run.py scan` | 仅扫描并注册新 PDF（不上传，不消耗额度） |
| `python run.py status` | 查看当前处理进度（多少完成、多少失败等） |
| `python run.py retry-failed` | 把所有失败的文件重置为待处理，下次 run 时会重新处理 |
| `python run.py convert-only` | 仅重新转换已下载的数据（不重新上传，不消耗额度） |

额外选项：

| 选项 | 说明 |
|------|------|
| `-v` 或 `--verbose` | 输出更详细的日志，排查问题时有用 |
| `-c 路径` 或 `--config 路径` | 使用自定义配置文件（默认用 config.yaml） |

例如：`python run.py -v run --limit 5`

---

## 输出 JSON 格式说明

每篇论文生成一个 JSON 文件，格式如下：

```json
{
  "_id": "论文A",
  "forum": "计算机学报",
  "doi": "",
  "fulltext": [
    {
      "title": "",
      "paragraphs": ["论文标题文本...", "作者: 张三, 李四", "摘要: 本文提出了..."],
      "section": []
    },
    {
      "title": "1 引言",
      "paragraphs": ["随着深度学习的发展...", "本文的主要贡献包括 $$E=mc^2$$ 等公式..."],
      "section": [
        {
          "title": "1.1 研究背景",
          "paragraphs": ["近年来..."],
          "section": []
        }
      ]
    },
    {
      "title": "2 相关工作",
      "paragraphs": ["..."],
      "section": []
    }
  ]
}
```

各字段含义：

| 字段 | 说明 |
|------|------|
| `_id` | PDF 文件名（不含 `.pdf` 后缀） |
| `forum` | 期刊中文名（从目录结构自动提取） |
| `doi` | DOI 号（暂时为空，保留字段） |
| `fulltext` | 论文正文，按章节层级组织 |
| `title` | 章节标题（空字符串表示论文开头的前言部分） |
| `paragraphs` | 该章节下的段落列表 |
| `section` | 子章节列表（结构相同，可以嵌套） |

✅ 数学公式会以 LaTeX 格式内联在段落文本中（如 `$$E=mc^2$$`）

✅ 参考文献、附录、致谢等部分会被自动过滤掉

---

## 断点续传

程序会把每个文件的处理进度记录在 `data\checkpoint.db`（一个小型数据库文件）里。

这意味着：

- **随时可以中断**：按 `Ctrl + C` 停止程序，已完成的文件不会重复处理
- **随时可以继续**：再次运行 `python run.py run`，会自动跳过已完成的文件
- **查看进度**：运行 `python run.py status` 可以看到总数、已完成、失败等统计

---

## 常见问题

### Q: 提示"未配置 MINERU_API_KEY"怎么办？

在命令行里设置环境变量：

```bash
set MINERU_API_KEY=你的Key
```

每次打开新窗口都要重新设置一次。

### Q: 某些 PDF 处理失败了怎么办？

先看看失败了多少个：

```bash
python run.py status
```

然后重试：

```bash
python run.py retry-failed
python run.py run
```

这会把失败的文件重新放回队列，再处理一次。

### Q: 我只想先处理一部分文件试试？

用 `--limit` 参数：

```bash
python run.py run --limit 20
```

### Q: API 额度用完了怎么办？

按 `Ctrl + C` 停止程序。去 MinerU 充值后，直接运行 `python run.py run` 继续，已处理的不会重复扣费。

### Q: PDF 目录里有很多子文件夹，会不会出问题？

不会。程序会递归扫描所有子目录中的 `.pdf` 文件。第一级子目录的名称会自动作为期刊名。

### Q: 不同文件夹里有同名的 PDF 怎么办？

⚠️ 程序用文件名（不含后缀）作为唯一标识。如果两个不同目录下有同名的 PDF（比如都叫 `paper.pdf`），只会处理第一个，后面的会被跳过。建议确保 PDF 文件名不重复。

---

## 注意事项

- ⚠️ MinerU API **按页数计费**，处理 10 万篇论文会产生费用，请务必先用 `--limit 10` 小批量测试
- 处理速度取决于 MinerU 服务端，通常每批 10 个文件需要 1-5 分钟
- 日志文件保存在 `pipeline.log`，遇到问题可以查看详细信息
- 加 `-v` 参数可以看到更详细的日志输出
