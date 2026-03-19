"""
格式转换器 - 将 MinerU content_list.json 转换为目标 JSON 格式。

目标格式:
{
    "_id": "unique_id",
    "forum": "期刊中文名",
    "doi": "",
    "fulltext": [
        {
            "title": "1 引言",
            "paragraphs": ["段落1...", "段落2..."],
            "section": [
                {"title": "1.1 背景", "paragraphs": [...], "section": []}
            ]
        }
    ]
}

过滤规则:
- 保留: text (正文+标题), equation (公式)
- 去掉: table, image, ref_text, header, footer, page_number, page_footnote, aside_text
- 去掉标题匹配: 参考文献, References, Bibliography, 附录, Appendix
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Optional

from .models import ContentBlock, PaperDocument, Section

logger = logging.getLogger(__name__)

# 需要过滤掉的内容类型
FILTERED_TYPES = {
    "table",
    "image",
    "header",
    "footer",
    "page_number",
    "page_footnote",
    "aside_text",
}

# 需要过滤掉的章节标题模式（匹配后该节及后续内容截断）
REFERENCE_PATTERNS = [
    re.compile(r"^(参\s*考\s*文\s*献|references?|bibliography)$", re.IGNORECASE),
]

# 需要过滤掉的章节标题模式（匹配后该节被跳过）
APPENDIX_PATTERNS = [
    re.compile(r"^(附\s*录|append(ix|ices))[\s\.:：]?", re.IGNORECASE),
]

# 致谢等不太有用的章节也过滤
ACKNOWLEDGMENT_PATTERNS = [
    re.compile(r"^(致\s*谢|acknowledgm?ents?|funding)$", re.IGNORECASE),
]

# 从标题编号推断层级
_HEADING_LEVEL_RE = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?\s")


def _infer_heading_level(text: str) -> int:
    """
    从标题文本中的编号推断层级:
      "1 xxx"     → 1
      "1.1 xxx"   → 2
      "1.1.1 xxx" → 3
      "一、xxx"    → 1
      无编号       → 0 (无法推断)
    """
    stripped = text.strip()
    m = _HEADING_LEVEL_RE.match(stripped)
    if m:
        if m.group(3):
            return 3
        if m.group(2):
            return 2
        return 1
    # 中文数字编号: "一、", "二、" etc. → level 1
    if re.match(r"^[一二三四五六七八九十]+[、．.\s]", stripped):
        return 1
    # "(一)", "(二)" etc. → level 2
    if re.match(r"^[（(][一二三四五六七八九十]+[）)]", stripped):
        return 2
    return 0


def convert_content_blocks(
    raw_blocks: list[dict[str, Any]],
    data_id: str,
    journal: str = "",
) -> Optional[PaperDocument]:
    """
    将 content_list 内容块转换为目标 PaperDocument。

    Args:
        raw_blocks: content_list.json 顶层数组
        data_id: 文件唯一标识
        journal: 期刊名称

    Returns:
        PaperDocument 或 None（如果解析失败）
    """
    if not isinstance(raw_blocks, list):
        logger.error(f"content_list 内容无效 ({data_id}): 顶层不是数组")
        return None

    # 解析内容块
    blocks = _parse_blocks(raw_blocks)

    # 过滤内容
    filtered = _filter_blocks(blocks)

    if not filtered:
        logger.warning(f"过滤后无内容 ({data_id})")
        return None

    # 构建章节结构
    sections = _build_sections(filtered)

    # 从前言Section中提取作者、摘要、关键词
    _extract_preamble_metadata(sections)

    doc = PaperDocument()
    doc._id = data_id
    doc.forum = journal
    doc.doi = ""
    doc.fulltext = sections

    return doc


def convert_content_list(
    content_list_path: str,
    data_id: str,
    journal: str = "",
) -> Optional[PaperDocument]:
    """
    从 content_list.json 文件路径读取并转换为目标 PaperDocument。
    """
    try:
        with open(content_list_path, "r", encoding="utf-8") as f:
            raw_blocks = json.load(f)
    except Exception as e:
        logger.error(f"读取 content_list.json 失败 ({data_id}): {e}")
        return None

    return convert_content_blocks(raw_blocks, data_id=data_id, journal=journal)


def _parse_blocks(raw_blocks: list[dict[str, Any]]) -> list[ContentBlock]:
    """解析原始 JSON 为 ContentBlock 列表"""
    blocks: list[ContentBlock] = []
    for raw in raw_blocks:
        if not isinstance(raw, dict):
            continue
        try:
            block = ContentBlock(**raw)
            blocks.append(block)
        except Exception as e:
            logger.debug(f"解析内容块失败: {e}")
    return blocks


def _filter_blocks(blocks: list[ContentBlock]) -> list[ContentBlock]:
    """
    过滤内容块:
    1. 移除不需要的类型（table, image, header等）
    2. 移除 ref_text 类型（参考文献条目）
    3. 在遇到"参考文献"/"References"标题后截断
    4. 跳过附录、致谢章节
    """
    filtered: list[ContentBlock] = []
    skip_until_same_or_higher_level = -1  # 用于跳过附录等章节
    hit_references = False

    for block in blocks:
        # 跳过被过滤的类型
        if block.type in FILTERED_TYPES:
            continue

        # 跳过 ref_text
        if block.type == "ref_text":
            continue

        # 跳过 list 类型中的 ref_text
        if block.type == "list" and block.sub_type == "ref_text":
            continue

        # 如果已到参考文献，截断后续所有内容
        if hit_references:
            continue

        # 检查标题是否是参考文献
        if _is_heading(block):
            title_text = block.text.strip()

            # 检查是否是参考文献标题
            if _matches_patterns(title_text, REFERENCE_PATTERNS):
                hit_references = True
                continue

            # 检查是否是附录或致谢
            if _matches_patterns(title_text, APPENDIX_PATTERNS):
                skip_until_same_or_higher_level = block.text_level or 1
                continue

            if _matches_patterns(title_text, ACKNOWLEDGMENT_PATTERNS):
                skip_until_same_or_higher_level = block.text_level or 1
                continue

            # 如果当前在跳过模式，检查是否遇到同级或更高级标题
            if skip_until_same_or_higher_level > 0:
                current_level = block.text_level or 1
                if current_level <= skip_until_same_or_higher_level:
                    # 遇到同级或更高级标题，结束跳过
                    skip_until_same_or_higher_level = -1
                else:
                    continue

        # 如果在跳过模式中，跳过非标题内容
        if skip_until_same_or_higher_level > 0:
            continue

        # 跳过空文本
        if block.type in ("text", "equation") and not block.text.strip():
            continue

        filtered.append(block)

    return filtered


def _is_heading(block: ContentBlock) -> bool:
    """判断是否是标题"""
    return (
        block.type == "text" and block.text_level is not None and block.text_level > 0
    )


def _matches_patterns(text: str, patterns: list[re.Pattern]) -> bool:  # type: ignore[type-arg]
    """检查文本是否匹配任一模式"""
    clean = text.strip()
    # 去掉章节编号前缀 如 "1. ", "2.3 ", "A. " 等
    clean_no_num = re.sub(r"^[\d\.]+\s*", "", clean)
    clean_no_num = re.sub(r"^[A-Z][\.\s]+", "", clean_no_num)
    for pattern in patterns:
        if pattern.search(clean) or pattern.search(clean_no_num):
            return True
    return False


def _build_sections(blocks: list[ContentBlock]) -> list[Section]:
    """
    将过滤后的内容块构建为层级章节结构。

    算法:
    - 先检测 MinerU 是否返回了有意义的层级（不全是 level 1）
    - 如果全是 level 1，则从标题编号推断层级（如 "1" → 1, "1.1" → 2）
    - text_level == 1 的标题 → fulltext 数组中的顶级 Section
    - text_level == 2 的标题 → 嵌套在上一个 level 1 Section 的 section[] 中
    - text_level >= 3 的标题 → 嵌套在上一个 level 2 Section 的 section[] 中
    - 普通文本/公式 → 追加到当前最内层 Section 的 paragraphs[]
    - 首个标题之前的内容 → 放入一个标题为空的前言 Section
    """
    all_flat = all(
        (block.text_level or 1) == 1 for block in blocks if _is_heading(block)
    )

    result: list[Section] = []
    stack: list[Section] = []
    para_buffer: list[str] = []

    def flush_paragraphs() -> None:
        """将缓冲区的段落写入当前最内层 Section"""
        nonlocal para_buffer
        if not para_buffer:
            return
        target = _current_section(stack, result)
        for p in para_buffer:
            target.paragraphs.append(p)
        para_buffer = []

    for block in blocks:
        if _is_heading(block):
            flush_paragraphs()

            level = block.text_level or 1
            if all_flat:
                inferred = _infer_heading_level(block.text)
                if inferred > 0:
                    level = inferred
            new_section = Section(title=block.text.strip())

            if level == 1:
                # 顶级章节
                stack = [new_section]
                result.append(new_section)
            elif level == 2:
                if not stack:
                    # 没有 level 1 父节点，直接作为顶级
                    stack = [new_section]
                    result.append(new_section)
                else:
                    # 截断到 level 1，添加为子节点
                    stack = stack[:1]
                    stack[0].section.append(new_section)
                    stack.append(new_section)
            else:
                # level 3+
                if len(stack) < 2:
                    # 没有足够的父节点，作为 level 2 处理
                    if stack:
                        stack = stack[:1]
                        stack[0].section.append(new_section)
                        stack.append(new_section)
                    else:
                        stack = [new_section]
                        result.append(new_section)
                else:
                    # 截断到 level 2，添加为子节点
                    stack = stack[:2]
                    stack[1].section.append(new_section)
                    stack.append(new_section)

        elif block.type == "equation":
            # 公式：内联到当前段落，或作为独立段落
            formula_text = block.text.strip()
            if formula_text:
                if para_buffer:
                    # 尝试内联到前一个段落
                    para_buffer[-1] = para_buffer[-1].rstrip() + " " + formula_text
                else:
                    para_buffer.append(formula_text)

        elif block.type == "text":
            # 普通文本段落
            text = block.text.strip()
            if text:
                para_buffer.append(text)

        elif block.type == "list":
            # 列表项合并为一个段落
            items = block.list_items
            if items:
                combined = "\n".join(f"• {item}" for item in items)
                para_buffer.append(combined)

        elif block.type == "code":
            # 代码块（不太常见于学术论文，但保留）
            code_text = block.code_body.strip()
            if code_text:
                para_buffer.append(code_text)

    # 刷新最后的段落
    flush_paragraphs()

    # 如果第一个 section 没有标题，标记为前言
    # （一般包含论文标题、作者、摘要等）

    return result


def _current_section(stack: list[Section], result: list[Section]) -> Section:
    """获取当前最内层的 Section，如果没有则创建前言 Section"""
    if stack:
        return stack[-1]

    # 没有任何章节，创建一个前言 Section
    preamble = Section(title="")
    result.append(preamble)
    stack.append(preamble)
    return preamble


def save_paper_json(doc: PaperDocument, output_path: str) -> None:
    """将 PaperDocument 保存为 JSON 文件"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = doc.to_dict()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.debug(f"保存JSON: {output_path}")


# ============================================================
# 前言元数据提取（作者、摘要、关键词）
# ============================================================

# 摘要匹配: "摘要" / "摘 要" / "Abstract" 开头
_ABSTRACT_RE = re.compile(
    r"^(摘\s*要|abstract)\s*[:：\s]?\s*",
    re.IGNORECASE,
)

# 关键词匹配: "关键词" / "关键字" / "关 键 词" / "Keywords" / "Key words" 开头
_KEYWORDS_RE = re.compile(
    r"^(关\s*键\s*[词字]|key\s*words?)\s*[:：\s]\s*",
    re.IGNORECASE,
)

# 关键词分隔符: 中文分号、英文分号、逗号
_KW_SPLIT_RE = re.compile(r"[;；,，]\s*")

# 作者行特征: 短文本（< 100字符），在第0-1页，不含摘要/关键词标记
_AUTHOR_MAX_LEN = 100

# 引用行: 含 "．" 且含期刊名/年份/页码
_CITATION_RE = re.compile(
    r"[．\.].*(?:\d{4}|\d+[-–]\d+|学报|journal|chin\b)",
    re.IGNORECASE,
)

# 机构行: 以编号开头（"1 大学..."）或含邮编模式
_AFFILIATION_RE = re.compile(
    r"(?:^\d+\s+.{0,10}(?:大学|研究所|学院|实验室|university|institute|college))"
    r"|(?:\d{6})",
    re.IGNORECASE,
)


# 栏目标记: 如 "·综述·"、"·研究报告·" 等
_SECTION_MARKER_RE = re.compile(r"^·.+·$")


def _is_section_marker(section: Section) -> bool:
    title = section.title.strip()
    if _SECTION_MARKER_RE.match(title):
        return True
    if not title and section.paragraphs:
        first_para = section.paragraphs[0].strip()
        if _SECTION_MARKER_RE.match(first_para):
            return True
    return False


def _extract_preamble_metadata(sections: list[Section]) -> None:
    if not sections:
        return

    target = sections[0]
    if len(sections) > 1 and _is_section_marker(target):
        target = sections[1]

    if not target.paragraphs:
        return

    _extract_metadata_from_section(target)


def _extract_metadata_from_section(target: Section) -> None:
    remaining: list[str] = []
    abstract_parts: list[str] = []
    found_abstract = False

    for para in target.paragraphs:
        stripped = para.strip()

        kw_match = _KEYWORDS_RE.match(stripped)
        if kw_match:
            found_abstract = False
            kw_text = stripped[kw_match.end() :]
            keywords = [k.strip() for k in _KW_SPLIT_RE.split(kw_text) if k.strip()]
            if keywords:
                target.keywords = keywords
            continue

        abs_match = _ABSTRACT_RE.match(stripped)
        if abs_match:
            found_abstract = True
            abstract_body = stripped[abs_match.end() :].strip()
            if abstract_body:
                abstract_parts.append(abstract_body)
            continue

        if found_abstract and len(stripped) > _AUTHOR_MAX_LEN:
            abstract_parts.append(stripped)
            continue

        if found_abstract:
            found_abstract = False

        remaining.append(para)

    if abstract_parts:
        target.abstract = " ".join(abstract_parts)

    _extract_authors_from_remaining(target, remaining)


def _is_citation_line(text: str) -> bool:
    """判断是否为引用行（如 '张三，李四．论文标题．生物工程学报，2020...'）。"""
    return bool(_CITATION_RE.search(text))


def _is_sentence_like(text: str) -> bool:
    """判断文本是否像正文句子（含句号等终止符）。"""
    return "。" in text or "．" in text or text.endswith(".")


def _extract_authors_from_remaining(preamble: Section, remaining: list[str]) -> None:
    """从前言剩余段落中识别作者行。

    启发式规则:
    - 若 preamble.title 非空（MinerU 已检测标题），第一段即为作者行
    - 若 preamble.title 为空，跳过第一段（通常是论文标题）
    - 短文本（<100字符）且不含句号/非引用行的段落视为作者行
    - 引用行（含 '．' 和期刊/年份特征）直接丢弃
    - 机构行（长文本，含大学/研究所等）保留在 paragraphs
    """
    if not remaining:
        preamble.paragraphs = []
        return

    title_detected = bool(preamble.title)

    # 确定从哪里开始扫描作者
    if title_detected:
        # MinerU 已将论文标题放入 title 字段，第一段是作者行
        scan_start = 0
        final_paragraphs: list[str] = []
    else:
        # 第一段是论文标题，保留在 paragraphs
        if len(remaining) < 2:
            preamble.paragraphs = remaining
            return
        scan_start = 1
        final_paragraphs = [remaining[0]]

    authors: list[str] = []
    collecting_authors = True

    for para in remaining[scan_start:]:
        stripped = para.strip()
        is_short = len(stripped) < _AUTHOR_MAX_LEN

        if _is_citation_line(stripped):
            continue

        if _AFFILIATION_RE.search(stripped):
            collecting_authors = False
            continue

        if collecting_authors and is_short and not _is_sentence_like(stripped):
            authors.append(stripped)
        else:
            collecting_authors = False
            final_paragraphs.append(para)

    if authors:
        preamble.authors = authors

    preamble.paragraphs = final_paragraphs
