import json
import re
import sys
from datetime import datetime, timedelta


def build_text_block(block_type, text, mentions=None):
    return {
        "type": block_type,
        "text": (text or "").strip(),
        "mentions": mentions or []
    }


def build_table_block(table_headers, table_rows):
    return {
        "type": "table",
        "headers": table_headers or [],
        "rows": table_rows or []
    }


def extract_text_from_block_container(block_container):
    """
    从一个 blockContainer 中提取可读文本。
    主要用于 tableCell / tableHeader 内部内容提取。
    """
    if not block_container or block_container.get("type") != "blockContainer":
        return ""

    text_parts = []

    for item in block_container.get("content", []):
        item_type = item.get("type")

        if item_type in ("paragraph", "heading", "fheading", "bulletListItem", "numberedListItem"):
            inline_content = item.get("content", [])
            text = ""

            for inline_item in inline_content:
                inline_type = inline_item.get("type")

                if inline_type == "text":
                    text += inline_item.get("text", "")
                elif inline_type == "mention":
                    attrs = inline_item.get("attrs", {})
                    label = attrs.get("label", "?")
                    uid = attrs.get("uid", "")
                    user_id = attrs.get("id", "")
                    text += f"[@{label}](mention:{uid}:{user_id})"
                elif inline_type == "mentionUrl":
                    attrs = inline_item.get("attrs", {})
                    content = attrs.get("content", "")
                    original_url = attrs.get("originalUrl", "")
                    uid = attrs.get("uid", "")
                    data_type = attrs.get("dataType", 1)
                    text += f"[{content}](mentionUrl:{uid}:{data_type}:{original_url})"

            if text.strip():
                text_parts.append(text.strip())

        elif item_type == "codeBlock":
            code_parts = []
            for code_item in item.get("content", []):
                if code_item.get("type") == "text":
                    code_parts.append(code_item.get("text", ""))
            code_text = "\n".join(code_parts).strip()
            if code_text:
                text_parts.append(code_text)

        elif item_type == "blockContainer":
            nested_text = extract_text_from_block_container(item)
            if nested_text.strip():
                text_parts.append(nested_text.strip())

    return " ".join(text_parts).strip()


class DailyReportParser:
    """
    输出结构：
    {
        "meta": {
            "project_name": "...",
            "date": "...",
            "week": "..."
        },
        "members": [
            {
                "person_info": {...},
                "projects": [
                    {
                        "project_name": "...",
                        "sections": {
                            "progress": [],
                            "issue_help": [],
                            "next_focus": []
                        }
                    }
                ]
            }
        ]
    }
    """

    CONTAINER_BLOCK_TYPES = {"blockContainer", "blockGroup"}
    META_BLOCK_TYPES = {"heading", "fheading", "title"}
    MEMBER_HEADER_BLOCK_TYPES = {"heading", "fheading"}
    CONTENT_BLOCK_TYPES = {"bulletListItem", "numberedListItem", "paragraph", "codeBlock"}

    def __init__(self, project_name="Unknown", generate_weekend=False):
        self.project_name = project_name
        self.generate_weekend = generate_weekend

        self.date_patterns = [
            re.compile(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})"),
            re.compile(r"(\d{4}年\d{1,2}月\d{1,2}日)")
        ]
        self.week_patterns = [
            re.compile(r"第\s*([0-9]+)\s*周", re.I),
            re.compile(r"Week\s*([0-9]+)", re.I),
            re.compile(r"W([0-9]+)", re.I)
        ]

    def _normalize_text(self, text):
        return (text or "").replace("\u200b", "").replace("\xa0", " ").strip()

    def extract_text_and_mentions(self, inline_content):
        """
        从 inline content 中提取：
        - 可读文本
        - mention 信息
        """
        if not inline_content:
            return "", []

        text_parts = []
        mentions = []

        for item in inline_content:
            item_type = item.get("type")

            if item_type == "text":
                text_parts.append(item.get("text", ""))

            elif item_type == "mention":
                attrs = item.get("attrs", {})
                mentions.append(dict(attrs))

                uid = attrs.get("uid", "")
                user_id = attrs.get("id", "")
                label = attrs.get("label", "?")
                text_parts.append(f"[@{label}](mention:{uid}:{user_id})")

            elif item_type == "mentionUrl":
                attrs = item.get("attrs", {})
                content = attrs.get("content", "")
                original_url = attrs.get("originalUrl", "")
                uid = attrs.get("uid", "")
                data_type = attrs.get("dataType", 1)
                text_parts.append(f"[{content}](mentionUrl:{uid}:{data_type}:{original_url})")

        return "".join(text_parts).strip(), mentions

    def _extract_codeblock_text(self, block):
        code_parts = []
        for item in block.get("content", []):
            if item.get("type") == "text":
                code_parts.append(item.get("text", ""))
        return self._normalize_text("\n".join(code_parts))

    def _extract_project_name(self, text):
        """
        识别项目标题：
        - 【项目A】
        - [Project A]
        """
        text = self._normalize_text(text)
        m = re.match(r"^[\[\【](.*?)[\]\】]\s*$", text)
        if m:
            return m.group(1).strip()
        return None

    def _normalize_section_name(self, text):
        """
        统一映射 section：
        - progress
        - issue_help
        - next_focus

        规则：
        - “今日主要进展” 严格匹配
        - “困难及所需支援” 严格匹配
        - “下一步计划” / “Next Key Focus” 可宽松一点
        """
        text = self._normalize_text(text)
        text_no_colon = text.replace("：", "").replace(":", "").strip()

        # 严格匹配
        if text_no_colon == "今日主要进展":
            return "progress"

        if text_no_colon == "困难及所需支援":
            return "issue_help"

        # 相对宽松
        if "下一步计划" in text_no_colon or "Next Key Focus" in text_no_colon:
            return "next_focus"

        return None

    def _create_empty_project(self, project_name):
        return {
            "project_name": project_name,
            "sections": {
                "progress": [],
                "issue_help": [],
                "next_focus": []
            }
        }

    def _find_or_create_project(self, member_obj, project_name):
        for proj in member_obj["projects"]:
            if proj["project_name"] == project_name:
                return proj

        new_proj = self._create_empty_project(project_name)
        member_obj["projects"].append(new_proj)
        return new_proj

    def parse(self, raw_json_data):
        root_blocks = (
            raw_json_data.get("data", {}).get("content", [])
            or raw_json_data.get("content", [])
        )

        meta_info = {
            "project_name": self.project_name,
            "date": None,
            "week": None,
        }

        members = []
        current_member = None
        current_project = None
        current_section = None

        def parse_table(table_block):
            headers = []
            rows = []

            for row_index, row in enumerate(table_block.get("content", [])):
                if row.get("type") != "tableRow":
                    continue

                row_cells = []

                for cell in row.get("content", []):
                    cell_text = ""

                    if cell.get("type") in ("tableHeader", "tableCell"):
                        cell_blocks = cell.get("content", [])
                        extracted_parts = []

                        for sub_block in cell_blocks:
                            if sub_block.get("type") == "blockContainer":
                                part = extract_text_from_block_container(sub_block)
                                if part.strip():
                                    extracted_parts.append(part.strip())

                        cell_text = " ".join(extracted_parts).strip()

                    elif cell.get("type") == "blockContainer":
                        cell_text = extract_text_from_block_container(cell)

                    row_cells.append(cell_text)

                if row_index == 0:
                    headers = row_cells
                else:
                    rows.append(row_cells)

            if not headers and rows:
                headers = rows[0]
                rows = rows[1:]

            return build_table_block(headers, rows)

        def append_block_to_section(block_obj):
            nonlocal current_member, current_project, current_section

            if not current_member or not current_project or not current_section:
                return

            current_project["sections"][current_section].append(block_obj)

        def ensure_context_defaults():
            nonlocal current_member, current_project, current_section

            if not current_member:
                return False

            if not current_project:
                current_project = self._find_or_create_project(current_member, "未分类项目")

            if not current_section:
                current_section = "progress"

            return True

        def traverse(blocks):
            nonlocal current_member, current_project, current_section

            for block in blocks:
                block_type = block.get("type")

                # 1) 容器递归
                if block_type in self.CONTAINER_BLOCK_TYPES:
                    if "content" in block:
                        traverse(block["content"])
                    continue

                # 2) table 单独处理
                if block_type == "table":
                    table_block = parse_table(block)
                    if ensure_context_defaults():
                        append_block_to_section(table_block)
                    continue

                # 3) codeBlock 单独处理
                if block_type == "codeBlock":
                    code_text = self._extract_codeblock_text(block)

                    if code_text and ensure_context_defaults():
                        append_block_to_section({
                            "type": "code",
                            "text": code_text,
                            "mentions": []
                        })
                    continue

                inline_content = block.get("content", [])
                text, mentions = self.extract_text_and_mentions(inline_content)
                text = self._normalize_text(text)

                # 4) meta 提取
                if block_type in self.META_BLOCK_TYPES:
                    if not meta_info["date"]:
                        for pattern in self.date_patterns:
                            match = pattern.search(text)
                            if match:
                                meta_info["date"] = match.group(1)
                                break

                    if not meta_info["week"]:
                        for pattern in self.week_patterns:
                            match = pattern.search(text)
                            if match:
                                meta_info["week"] = f"第{match.group(1)}周"
                                break

                # 5) 成员识别：heading/fheading + mention
                if block_type in self.MEMBER_HEADER_BLOCK_TYPES and mentions:
                    person_info = mentions[0]
                    current_member = {
                        "person_info": person_info,
                        "projects": []
                    }
                    members.append(current_member)

                    current_project = None
                    current_section = None
                    continue

                # 没进入成员上下文，不收正文
                if not current_member:
                    if "content" in block and isinstance(block["content"], list):
                        traverse(block["content"])
                    continue

                # 6) 项目识别
                project_name = self._extract_project_name(text)
                if (
                    project_name
                    and current_member
                    and current_section is None
                    and block_type in ("bulletListItem", "paragraph")
                ):
                    current_project = self._find_or_create_project(current_member, project_name)
                    current_section = None
                    continue

                # 7) section 识别
                section_name = self._normalize_section_name(text)
                if (
                    section_name
                    and block_type in ("bulletListItem", "paragraph")
                ):
                    if not current_project:
                        current_project = self._find_or_create_project(current_member, "未分类项目")
                    current_section = section_name
                    continue

                # 8) 正文内容
                if block_type in ("bulletListItem", "numberedListItem", "paragraph"):
                    clean_text = re.sub(r"^[\d]+\.[\s]*|^[*-]\s*", "", text).strip()
                    if not clean_text:
                        if "content" in block and isinstance(block["content"], list):
                            traverse(block["content"])
                        continue

                    if block_type == "bulletListItem":
                        normalized_block_type = "bullet"
                    elif block_type == "numberedListItem":
                        normalized_block_type = "numbered"
                    else:
                        normalized_block_type = "paragraph"

                    if ensure_context_defaults():
                        text_block = build_text_block(
                            block_type=normalized_block_type,
                            text=clean_text,
                            mentions=mentions
                        )
                        append_block_to_section(text_block)

                # 9) 继续递归
                if "content" in block and isinstance(block["content"], list):
                    traverse(block["content"])

        traverse(root_blocks)

        # fallback 日期
        if not meta_info["date"]:
            if self.generate_weekend:
                fallback_days_ago = 1
            else:
                fallback_days_ago = 3 if datetime.now().weekday() == 0 else 1

            fallback_date = datetime.now() - timedelta(days=fallback_days_ago)
            meta_info["date"] = fallback_date.strftime("%Y-%m-%d")

        return {
            "meta": meta_info,
            "members": members
        }


def main():
    """
    用法：
    python parser_debug.py input.json output.json

    例子：
    python parser_debug.py raw_daily.json parsed_daily.json
    """
    if len(sys.argv) < 3:
        print("用法: python parser_debug.py <input_json_path> <output_json_path>")
        sys.exit(1)

    input_json_path = sys.argv[1]
    output_json_path = sys.argv[2]

    with open(input_json_path, "r", encoding="utf-8") as f:
        raw_json = json.load(f)

    parser = DailyReportParser(project_name="Local Debug Project", generate_weekend=False)
    parsed_result = parser.parse(raw_json)

    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(parsed_result, f, ensure_ascii=False, indent=2)

    print(f"✅ 解析完成，输出文件: {output_json_path}")
    print(f"成员数: {len(parsed_result.get('members', []))}")

    for idx, member in enumerate(parsed_result.get("members", []), 1):
        name = member.get("person_info", {}).get("label", "未知成员")
        project_count = len(member.get("projects", []))
        print(f"  - 成员{idx}: {name}, 项目数: {project_count}")

        for proj in member.get("projects", []):
            sections = proj.get("sections", {})
            print(
                f"      项目: {proj.get('project_name', '')} | "
                f"progress={len(sections.get('progress', []))}, "
                f"issue_help={len(sections.get('issue_help', []))}, "
                f"next_focus={len(sections.get('next_focus', []))}"
            )


if __name__ == "__main__":
    main()