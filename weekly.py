# =============================================================================
# Weekly Report Workflow Agent
# =============================================================================
# 目标：
# 1. 不强依赖固定日报模板
# 2. 不维护复杂 parser
# 3. 通过“文档展平 + LLM 事实抽取 + 聚类 + 撰写”生成周报
#
# 核心流程：
# Step 1: 收集上周日报，并把平台 JSON 展平成成员级文本
# Step 2: 三阶段 Agent
#         - Indexer Agent：抽取任务骨架 / Fact Skeleton
#         - Planner Agent：语义聚类
#         - Writer Agent：生成周报正文
# Step 3: 生成团队关键进展摘要，并拼接最终 Markdown
# Step 4: 创建正式周报笔记
# Step 5: 推送飞书卡片 / 个人消息
# =============================================================================


# =============================================================================
# 0. 基础依赖与 print 刷新设置
# =============================================================================
import builtins
import sys
import os
import requests
import json
import time
import re
import uuid
import tempfile
import traceback

from datetime import datetime, timedelta
from zenv import get_zdkit_env
from zdbase import ZFile


# 确保 print 立即刷新，方便平台日志实时查看
if not getattr(builtins.print, "_patched_flush", False):
    _original_print = builtins.print

    def print(*args, **kwargs):
        kwargs.setdefault("flush", True)
        _original_print(*args, **kwargs)

    print._patched_flush = True
    builtins.print = print


# =============================================================================
# 1. 全局配置加载
# =============================================================================

zenv_obj = get_zdkit_env()

# 兼容不同环境下 zenv_obj.zdkit / zenv_obj.zkit 的命名差异
try:
    BASE_URL = zenv_obj.zdkit._http_client.config.get("url")
except AttributeError:
    BASE_URL = zenv_obj.zkit._http_client.config.get("url")

try:
    with open(config_file.path, "r", encoding="utf-8") as config_fp:
        config = json.load(config_fp)
except Exception as e:
    print(f"配置文件读取失败: {e}")
    raise


# 平台鉴权配置
AK = config.get("ak")
SK = config.get("sk")
ORG_GUID = config.get("org_guid")
USER_GUID = config.get("user_guid")

# 项目配置列表
projects = config.get("projects", [])

# 批量处理上限，暂时保留，后续如果做 chunk batch 可使用
BATCH_NUMBER = min(int(config.get("batch_number", 50)), 50)

# 当前生成类型
generate_type = "weekly"


# =============================================================================
# 2. API 路由与默认业务参数
# =============================================================================

# 鉴权与文档读取
ACCESS_TOKEN_ROUTE = "/api/user/platform/getAccessToken"
NOTE_JSON_ROUTE = "/platform/ws/noteInfo/getDocJson"
DOC_TREE_ROUTE = "/platform/api/main/doc/treeList"
SIGNED_URL_ROUTE = "/platform/api/main/storage/getSignedUrl"

# 文档创建与写入
WORKSPACE_SAVE_ROUTE = "/middle/server/api/workspace/save"
MD_INSERT_ROUTE = "/middle/server/api/file/md/insert"

# 消息推送
MESSAGE_SEND_ROUTE = "/middle/server/api/msg/send"

# LLM 默认参数
DEFAULT_LLM_PARAMS = {
    "temperature": 0.5,
    "max_tokens": 4096
}

# 消息模板
MESSAGE_TEMPLATE_ID = "80"
PLATFORM_TYPE = "all"


# =============================================================================
# 3. 通用工具函数
# =============================================================================

def get_headers_with_ak(user_guid="", doc_id=""):
    """
    获取带 AccessToken 的请求头。

    参数：
    - user_guid: 当前操作用户 GUID
    - doc_id: 文档 ID，部分接口需要带 docId

    返回：
    - headers: 平台 API 请求头
    """
    response = requests.post(
        url=BASE_URL + ACCESS_TOKEN_ROUTE,
        json={"ak": AK, "sk": SK}
    )

    response_json = response.json()

    if not response_json.get("data"):
        raise Exception(f"获取 AccessToken 失败: {response_json}")

    access_token = response_json["data"].get("accessToken")

    headers = {
        "Access-Token": access_token,
        "ak": AK,
        "X-User-GUID": user_guid or USER_GUID,
    }

    if doc_id:
        headers["docId"] = doc_id

    return headers


def get_note_json_content(user_guid="", doc_id=""):
    """
    获取指定文档的 JSON 内容。
    """
    headers = get_headers_with_ak(user_guid=user_guid, doc_id=doc_id)

    response = requests.get(
        url=BASE_URL + NOTE_JSON_ROUTE,
        headers=headers,
        params={"docId": doc_id}
    )

    return response.json()


def strip_markdown_wrapper(content):
    """
    去除 LLM 输出中可能出现的 ```json / ```markdown 包裹。
    """
    content = (content or "").strip()

    if content.startswith("```json"):
        content = content[len("```json"):].lstrip("\n")
    elif content.startswith("```markdown"):
        content = content[len("```markdown"):].lstrip("\n")
    elif content.startswith("```"):
        content = content[3:].lstrip("\n")

    if content.endswith("```"):
        content = content[:-3].rstrip("\n")

    return content.strip()


def safe_json_loads(raw, expected_type=None):
    """
    尽量从 LLM 输出中解析 JSON。

    支持：
    1. 纯 JSON
    2. ```json 包裹
    3. 前后混入少量解释文字时，尝试截取 [] 或 {}

    参数：
    - raw: LLM 原始输出
    - expected_type: list / dict / None

    返回：
    - 解析成功则返回 Python 对象
    - 失败则返回 None
    """
    raw = strip_markdown_wrapper(raw).strip()

    try:
        result = json.loads(raw)
        if expected_type is None or isinstance(result, expected_type):
            return result
    except Exception:
        pass

    # 尝试截取 JSON 数组
    if expected_type in (None, list):
        start = raw.find("[")
        end = raw.rfind("]")
        if start != -1 and end != -1 and end > start:
            try:
                result = json.loads(raw[start:end + 1])
                if expected_type is None or isinstance(result, expected_type):
                    return result
            except Exception:
                pass

    # 尝试截取 JSON 对象
    if expected_type in (None, dict):
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                result = json.loads(raw[start:end + 1])
                if expected_type is None or isinstance(result, expected_type):
                    return result
            except Exception:
                pass

    return None


def _convert_special_nodes(content):
    """
    将中间 Markdown 中的平台特殊节点转换为写入接口可识别的 HTML 节点。

    支持：
    1. mention:
       [@张三](mention:uid:id)
       -> <span data-node-type="mention" data-guid="id"></span>

    2. mentionUrl:
       [原笔记](mentionUrl:uid:type:url)
       -> <a data-node-type="mentionUrl" data-url="url">原笔记</a>

    3. 普通链接:
       [文本](https://xxx)
       -> <a href="https://xxx">文本</a>

    4. highlight block
    """
    content = re.sub(
        r"$begin:math:display$\@\(\[\^$end:math:display$]*)\]$begin:math:text$mention\:\[\^\:\]\+\:\(\[\^\)\]\+\)$end:math:text$",
        lambda m: f'<span data-node-type="mention" data-guid="{m.group(2)}"></span>',
        content
    )

    content = re.sub(
        r"$begin:math:display$\(\[\^$end:math:display$]+)\]$begin:math:text$mentionUrl\:\[\^\:\]\+\:\[\^\:\]\+\:\(\[\^\)\]\+\)$end:math:text$",
        lambda m: f'<a data-node-type="mentionUrl" data-url="{m.group(2)}">{m.group(1)}</a>',
        content
    )

    content = re.sub(
        r"$begin:math:display$\(\[\^$end:math:display$]+)\]$begin:math:text$\(https\?\:\/\/\[\^\)\\s\]\+\)$end:math:text$",
        lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>',
        content
    )

    content = re.sub(
        r":::highlight$begin:math:display$\[\^$end:math:display$]*\]\n(.*?):::",
        lambda m: f'<div data-node-type="highlightBlock" data-content-markdown>\n{m.group(1).rstrip()}\n</div>',
        content,
        flags=re.DOTALL
    )

    return content


def normalize_receiver_guids(receiver_guids_raw):
    """
    统一接收人 GUID 配置格式。
    """
    if isinstance(receiver_guids_raw, str):
        return [receiver_guids_raw]

    return receiver_guids_raw or []


def build_message_text(note_title, note_url):
    """
    构建个人消息文本。
    """
    return f"[{note_title}] 已生成，请点击查看。\n<a href='{note_url}'>点击查看详情</a>"


def load_prompt_text(prompt_file_guid, default_prompt):
    """
    从平台文件读取 prompt。
    如果未配置或读取失败，则使用 default_prompt。
    """
    if not prompt_file_guid:
        return default_prompt

    try:
        signed_url_response = requests.get(
            BASE_URL + SIGNED_URL_ROUTE,
            headers=get_headers_with_ak(),
            params={"categoryGuid": prompt_file_guid}
        )

        signed_url = (signed_url_response.json().get("data") or {}).get("signedUrl")
        if not signed_url:
            return default_prompt

        return requests.get(signed_url, timeout=10).text

    except Exception:
        return default_prompt


def get_last_week_info():
    """
    获取上周周一到周日的信息。
    """
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    week_dates = [last_monday + timedelta(days=i) for i in range(7)]

    return {
        "start_date": week_dates[0].strftime("%Y-%m-%d"),
        "end_date": week_dates[-1].strftime("%Y-%m-%d"),
        "start_title": week_dates[0].strftime("%Y/%m/%d"),
        "end_title": week_dates[-1].strftime("%Y/%m/%d"),
        "date_list": [d.strftime("%Y-%m-%d") for d in week_dates],
        "week_number": week_dates[0].isocalendar()[1],
    }


def build_intermediate_markdown_file(project_guid, target_date_str, markdown_content):
    """
    写出中间 Markdown 文件，便于调试。
    """
    tmp_dir = tempfile.gettempdir()
    unique_suffix = uuid.uuid4().hex[:8]

    file_name = f"weekly_{project_guid}_{target_date_str.replace('-', '')}_{unique_suffix}.md"
    file_path = os.path.join(tmp_dir, file_name)

    with open(file_path, "w", encoding="utf-8") as output_fp:
        output_fp.write(markdown_content)

    return file_path


def build_intermediate_json_file(project_guid, target_date_str, json_content, suffix=""):
    """
    写出中间 JSON 文件，便于调试。
    """
    tmp_dir = tempfile.gettempdir()
    unique_suffix = uuid.uuid4().hex[:8]
    name_suffix = f"_{suffix}" if suffix else ""

    file_name = f"weekly_{project_guid}_{target_date_str.replace('-', '')}{name_suffix}_{unique_suffix}.json"
    file_path = os.path.join(tmp_dir, file_name)

    with open(file_path, "w", encoding="utf-8") as output_fp:
        json.dump(json_content, output_fp, ensure_ascii=False, indent=2)

    return file_path


def cleanup_temp_files(file_paths, project_name=""):
    """
    清理中间临时文件。
    """
    if not file_paths:
        return

    for file_path in file_paths:
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

                if project_name:
                    print(f"[Cleanup][{project_name}] 已删除临时文件: {file_path}")
                else:
                    print(f"[Cleanup] 已删除临时文件: {file_path}")

        except Exception as e:
            if project_name:
                print(f"[Cleanup][{project_name}] 删除临时文件失败: {file_path}, error={e}")
            else:
                print(f"[Cleanup] 删除临时文件失败: {file_path}, error={e}")


def build_weekly_note_title(week_info, project_name):
    """
    构建周报标题。
    """
    year = week_info["start_date"][:4]
    week_number = week_info["week_number"]

    return f"{year}#W{week_number:02d} {project_name}周报"


def get_mention_attrs(mention_obj):
    """
    获取 mention 对象 attrs。
    支持：
    - {"type": "mention", "attrs": {...}}
    - {...}
    """
    if not mention_obj:
        return {}

    return mention_obj.get("attrs", mention_obj)


def get_mention_label(mention_obj):
    """
    获取 mention 的展示名。
    """
    attrs = get_mention_attrs(mention_obj)
    return attrs.get("label", "未知")


def get_mention_id(mention_obj):
    """
    获取 mention 的用户 ID。
    """
    attrs = get_mention_attrs(mention_obj)
    return attrs.get("id", "")


def mention_to_markdown(mention_obj):
    """
    将 mention 对象转成中间 Markdown 表达。

    输出示例：
    [@张三](mention:uid:id)
    """
    if not mention_obj:
        return "[@未知](mention::)"

    attrs = get_mention_attrs(mention_obj)

    uid = attrs.get("uid", "")
    user_id = attrs.get("id", "")
    label = attrs.get("label", "未知")

    return f"[@{label}](mention:{uid}:{user_id})"


# =============================================================================
# 4. 文档展平器 Document Flattener
# =============================================================================
# 说明：
# 这里不做业务 parser，不识别项目名、不识别“进展/风险/计划”。
# 它只负责把平台 JSON 中的文本、mention、链接、表格内容展平成可读文本。
#
# 这层的目标是：
# - 尽量减少对日报模板的依赖
# - 保留人名 mention
# - 保留原始文本信息
# - 为后续 LLM 事实抽取提供相对干净的输入
# =============================================================================

class DocumentFlattener:
    """
    平台文档 JSON 展平器。

    当前策略：
    1. 如果 heading/fheading 中出现 mention，则认为它可能是成员标题。
    2. 成员标题后面的段落、列表、表格归到该成员下。
    3. 不识别项目/section，不做业务模板 parser。
    """

    CONTAINER_BLOCK_TYPES = {"blockContainer", "blockGroup"}
    MEMBER_HEADER_BLOCK_TYPES = {"heading", "fheading"}
    CONTENT_BLOCK_TYPES = {"bulletListItem", "numberedListItem", "paragraph"}

    def __init__(self, project_config):
        self.project_name = project_config.get("project_name", "Unknown")

    def extract_text_and_mentions(self, inline_content):
        """
        从 inline content 中提取可读文本和 mention 信息。
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

    def extract_text_from_block_container(self, block_container):
        """
        从 blockContainer 递归抽取文本。
        主要用于表格单元格或嵌套结构。
        """
        if not block_container or block_container.get("type") != "blockContainer":
            return ""

        text_parts = []

        for item in block_container.get("content", []):
            item_type = item.get("type")

            if item_type in ("paragraph", "heading", "fheading", "bulletListItem", "numberedListItem"):
                text, _mentions = self.extract_text_and_mentions(item.get("content", []))

                if text.strip():
                    text_parts.append(text.strip())

            elif item_type == "blockContainer":
                nested_text = self.extract_text_from_block_container(item)

                if nested_text.strip():
                    text_parts.append(nested_text.strip())

        return " ".join(text_parts).strip()

    def parse_table(self, table_block):
        """
        将表格展平成 headers + rows。
        """
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
                            part = self.extract_text_from_block_container(sub_block)

                            if part.strip():
                                extracted_parts.append(part.strip())

                    cell_text = " ".join(extracted_parts).strip()

                elif cell.get("type") == "blockContainer":
                    cell_text = self.extract_text_from_block_container(cell)

                row_cells.append(cell_text)

            if row_index == 0:
                headers = row_cells
            else:
                rows.append(row_cells)

        if not headers and rows:
            headers = rows[0]
            rows = rows[1:]

        return {
            "type": "table",
            "headers": headers or [],
            "rows": rows or []
        }

    def table_to_text(self, table_block):
        """
        将表格转为可读文本。
        """
        headers = table_block.get("headers", [])
        rows = table_block.get("rows", [])

        if not headers and not rows:
            return ""

        lines = []

        if headers:
            lines.append(" | ".join(headers))

        for row in rows:
            lines.append(" | ".join(row))

        return "\n".join(lines).strip()

    def flatten(self, raw_json_data):
        """
        主入口：将平台文档 JSON 展平成成员级文本。

        返回：
        {
            "members": [
                {
                    "person_info": {...},
                    "raw_blocks": [...],
                    "raw_content": [...],
                    "full_text": "..."
                }
            ]
        }
        """
        root_blocks = (
            raw_json_data.get("data", {}).get("content", [])
            or raw_json_data.get("content", [])
        )

        members = []
        current_member = None

        def traverse(blocks):
            nonlocal current_member

            for block in blocks:
                block_type = block.get("type")

                # 容器节点：递归进入
                if block_type in self.CONTAINER_BLOCK_TYPES:
                    if "content" in block:
                        traverse(block["content"])
                    continue

                # 表格节点：如果当前已经识别到成员，则归入该成员
                if block_type == "table":
                    if current_member:
                        table_block = self.parse_table(block)
                        table_text = self.table_to_text(table_block)

                        current_member["raw_blocks"].append(table_block)

                        if table_text:
                            current_member["raw_content"].append(table_text)

                    continue

                inline_content = block.get("content", [])
                text, mentions = self.extract_text_and_mentions(inline_content)

                # 成员标题识别：
                # 这里仍然是轻量规则，不识别业务结构，只识别“这个 heading 里有 mention”
                if block_type in self.MEMBER_HEADER_BLOCK_TYPES:
                    if mentions:
                        person_info = mentions[0]

                        current_member = {
                            "person_info": person_info,
                            "raw_blocks": [],
                            "raw_content": [],
                            "full_text": ""
                        }

                        members.append(current_member)
                        continue

                # 普通内容块
                if block_type in self.CONTENT_BLOCK_TYPES:
                    if not current_member:
                        continue

                    clean_text = re.sub(r"^[\d]+\.[\s]*|^[*-]\s*", "", text).strip()

                    if not clean_text:
                        continue

                    if block_type == "bulletListItem":
                        normalized_block_type = "bullet"
                    elif block_type == "numberedListItem":
                        normalized_block_type = "numbered"
                    else:
                        normalized_block_type = "paragraph"

                    text_block = {
                        "type": normalized_block_type,
                        "text": clean_text,
                        "mentions": mentions or []
                    }

                    current_member["raw_blocks"].append(text_block)
                    current_member["raw_content"].append(clean_text)

                # 某些节点可能既是内容节点，也有子节点，继续递归
                if "content" in block and isinstance(block["content"], list):
                    traverse(block["content"])

        traverse(root_blocks)

        for member in members:
            text_parts = []

            for block in member.get("raw_blocks", []):
                if block.get("type") in ("paragraph", "bullet", "numbered") and block.get("text"):
                    text_parts.append(block["text"])

                elif block.get("type") == "table":
                    table_text = self.table_to_text(block)
                    if table_text:
                        text_parts.append(table_text)

            member["full_text"] = "\n".join(text_parts).strip()

        return {"members": members}


# =============================================================================
# 5. Step 1 - 日报收集与周维度聚合
# =============================================================================

def find_weekly_notes(user_guid, project_guid, folder_guid, date_list):
    """
    在指定日报文件夹中，查找上周日期范围内的日报文档。
    """
    response = requests.post(
        url=BASE_URL + DOC_TREE_ROUTE,
        headers=get_headers_with_ak(user_guid=user_guid),
        json={
            "projectGuid": project_guid,
            "parentGuid": folder_guid
        }
    )

    response_json = response.json()
    note_list = response_json.get("data") or []

    matched_notes = []

    # 支持多种日期标题格式
    date_variants_map = {}

    for date_str in date_list:
        date_variants_map[date_str] = [
            date_str,
            date_str.replace("-", "/"),
            date_str.replace("-", "."),
        ]

    for note in note_list:
        note_title = note.get("dataTitle", "")
        note_guid = note.get("categoryGuid")

        if not note_guid:
            continue

        for date_str, variants in date_variants_map.items():
            if any(v in note_title for v in variants):
                matched_notes.append({
                    "date": date_str,
                    "categoryGuid": note_guid,
                    "dataTitle": note_title
                })
                break

    return matched_notes


def aggregate_weekly_json(parsed_note_entries):
    """
    将多篇日报的成员内容聚合为周维度 JSON。

    输出结构：
    {
        "metadata": {
            "generated_at": "...",
            "range_dates": [...],
            "source_urls": {
                "2026-04-20": ["url1", "url2"]
            },
            "week_number": 17
        },
        "users": [
            {
                "user_name": {"type": "mention", "attrs": {...}},
                "reports": [
                    {"date": "2026-04-20", "content": "..."}
                ]
            }
        ]
    }
    """
    user_map = {}
    actual_dates = set()
    source_urls = {}

    for entry in parsed_note_entries:
        report_date = entry["date"]
        note_guid = entry["note_guid"]
        parsed_result = entry["parsed_result"]

        actual_dates.add(report_date)

        note_url = f"{BASE_URL}/workspace/{note_guid}"
        source_urls.setdefault(report_date, [])

        if note_url not in source_urls[report_date]:
            source_urls[report_date].append(note_url)

        for member in parsed_result.get("members", []):
            person_info = member.get("person_info", {})
            user_id = person_info.get("id")
            user_label = person_info.get("label", "未知用户")

            if not user_id or not user_label:
                continue

            mention_obj = {
                "type": "mention",
                "attrs": person_info
            }

            key = (user_id, user_label)

            content = member.get("full_text", "").strip()
            if not content:
                content = "（当日无正文内容）"

            if key not in user_map:
                user_map[key] = {
                    "user_name": mention_obj,
                    "reports_dict": {}
                }

            if report_date in user_map[key]["reports_dict"]:
                user_map[key]["reports_dict"][report_date] += "\n" + content
            else:
                user_map[key]["reports_dict"][report_date] = content

    final_users = []

    for key in sorted(user_map.keys(), key=lambda x: x[1]):
        user_data = user_map[key]
        reports = []

        for date in sorted(user_data["reports_dict"].keys()):
            reports.append({
                "date": date,
                "content": user_data["reports_dict"][date]
            })

        final_users.append({
            "user_name": user_data["user_name"],
            "reports": reports
        })

    week_number = None

    if actual_dates:
        first_date = datetime.strptime(sorted(actual_dates)[0], "%Y-%m-%d")
        week_number = first_date.isocalendar()[1]

    return {
        "metadata": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "range_dates": sorted(list(actual_dates)),
            "source_urls": source_urls,
            "week_number": week_number
        },
        "users": final_users
    }


def step1_collect_and_flatten_weekly_reports(project):
    """
    Step 1:
    收集上周所有日报，并将平台 JSON 展平成周维度 raw JSON。
    """
    generated_files = []

    try:
        project_name = project["project_name"]
        project_guid = project["project_guid"]
        work_log_folder_guid = project["work_log_folder_guid"]

        project_user_guids = project.get(
            "user_guid_list",
            [project.get("user_guid") or project.get("leader_guid")]
        )

        week_info = get_last_week_info()

        print(f"[Step 1][{project_name}] 目标周期: {week_info['start_date']} ~ {week_info['end_date']}")

        matched_notes = []
        seen_note_guids = set()

        # 支持多个用户视角查找日报目录
        for user_guid in project_user_guids:
            if not user_guid:
                continue

            user_notes = find_weekly_notes(
                user_guid=user_guid,
                project_guid=project_guid,
                folder_guid=work_log_folder_guid,
                date_list=week_info["date_list"]
            )

            for note in user_notes:
                note_guid = note["categoryGuid"]

                if note_guid in seen_note_guids:
                    continue

                seen_note_guids.add(note_guid)

                matched_notes.append({
                    "date": note["date"],
                    "user_guid": user_guid,
                    "note_guid": note_guid,
                    "note_title": note["dataTitle"]
                })

        if not matched_notes:
            print(f"[Step 1][{project_name}] 未找到上周日报笔记")
            return {}, False, []

        matched_notes.sort(key=lambda x: (x["date"], x["note_title"]))

        print(f"[Step 1][{project_name}] 找到 {len(matched_notes)} 份笔记，开始展平...")

        flattener = DocumentFlattener(project)
        parsed_note_entries = []

        for matched_note in matched_notes:
            raw_json = get_note_json_content(
                user_guid=matched_note["user_guid"],
                doc_id=matched_note["note_guid"]
            )

            parsed_result = flattener.flatten(raw_json)

            parsed_note_entries.append({
                "date": matched_note["date"],
                "note_guid": matched_note["note_guid"],
                "note_title": matched_note["note_title"],
                "parsed_result": parsed_result
            })

        weekly_json = aggregate_weekly_json(parsed_note_entries)

        json_file_path = build_intermediate_json_file(
            project_guid=project_guid,
            target_date_str=f"{week_info['start_date']}_to_{week_info['end_date']}",
            json_content=weekly_json,
            suffix="raw"
        )

        generated_files.append(json_file_path)

        print(f"[Step 1][{project_name}] 周维度 raw JSON 已生成: {json_file_path}")

        return weekly_json, True, generated_files

    except Exception as e:
        print(f"[Step 1] 发生异常: {e}")
        traceback.print_exc()
        return {}, False, []


# =============================================================================
# 6. Step 2 - 多阶段 Agent：Indexer / Planner / Writer
# =============================================================================

def build_user_daily_text(users_data):
    """
    将 users_data 拼接成 LLM 可读文本。

    注意：
    这里必须保留：
    - 日期
    - 用户姓名
    - 用户 mention
    因为后续 skeleton 要带这些字段，否则聚类和撰写阶段会丢人名/日期。
    """
    parts = []

    for user in users_data:
        user_mention_obj = user.get("user_name", {})
        user_label = get_mention_label(user_mention_obj)
        user_md = mention_to_markdown(user_mention_obj)

        parts.append(f"=== 用户：{user_md} ===")

        for report in user.get("reports", []):
            parts.append(
                f"## 日期：{report['date']}\n"
                f"用户姓名：{user_label}\n"
                f"用户Mention：{user_md}\n"
                f"内容：\n{report['content']}\n"
            )

    return "\n".join(parts)


def batch_extract_skeletons(users_data, project):
    """
    Indexer Agent:
    从自由格式日报文本中抽取任务骨架。

    这是整个 Agent 的事实抽取阶段。
    不负责写文章，只负责提取结构化事实。
    """
    if not users_data:
        return []

    prompt_file_guid = project.get("weekly_summary_prompt_file_guid")

    default_prompt = """你是一周任务骨架提取助手。

任务：阅读下方按人、按日期分组的日报文本，提取每个独立任务，输出 JSON 列表。

每个对象必须包含以下字段：
- date: 日期，格式 YYYY-MM-DD，必须来自输入标题
- user_label: 成员姓名，必须来自输入中的“用户姓名”
- user_mention: 成员 mention 原文，必须来自输入中的“用户Mention”，如 [@张三](mention:uid:id)，没有则填空字符串
- task_theme: 项目/模块名/事项主题
- progress_status: 当前阶段，枚举：开发中/联调/已上线/规划中/待评审/排查中/修复中/测试中/已完成/其他
- key_output: 具体完成物、结果、数量或里程碑
- risk_blocker: 困难、风险、阻塞、需协助内容；无则填空字符串
- next_plan: 下一步计划；无则填空字符串

提取规则：
1. 每个独立任务一个 JSON 对象。
2. 不要合并不同日期、不同成员的内容。
3. key_output 必须具体，优先保留数量、对象名称、case 名称、算法名称、问题名称。
4. 如果输入中出现“完成了多个算法/多个 case/若干问题”，必须尽量保留具体名称和数量。
5. 风险、阻塞、需支援内容必须进入 risk_blocker。
6. 下一步、明日计划、后续计划必须进入 next_plan。
7. 不允许编造输入中没有的信息。
8. 只输出 JSON 数组，不要输出任何解释。

输入日报：
{{daily_content}}
"""

    daily_text = build_user_daily_text(users_data)
    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)
    user_content = prompt_text.replace("{{daily_content}}", daily_text)

    context_messages = [
        {
            "role": "system",
            "content": "你是任务骨架提取助手，请只输出 JSON 数组，不要输出解释文字。",
            "variables": []
        },
        {
            "role": "user",
            "content": user_content,
            "variables": []
        }
    ]

    llm_result = _call_llm_with_retry(
        llm_name=model.llm_name,
        llm_params=model.llm_params,
        context_messages=context_messages,
        max_retries=5
    )

    skeletons = safe_json_loads(llm_result, expected_type=list)

    if isinstance(skeletons, list):
        normalized = []

        for item in skeletons:
            if not isinstance(item, dict):
                continue

            normalized.append({
                "date": item.get("date", ""),
                "user_label": item.get("user_label", ""),
                "user_mention": item.get("user_mention", ""),
                "task_theme": item.get("task_theme", ""),
                "progress_status": item.get("progress_status", "其他"),
                "key_output": item.get("key_output", ""),
                "risk_blocker": item.get("risk_blocker", ""),
                "next_plan": item.get("next_plan", "")
            })

        return normalized

    print("  ⚠️ 索引员解析骨架 JSON 失败，回退到空列表")
    return []


def semantic_cluster_skeletons(skeletons, users_data, project):
    """
    Planner Agent:
    对任务骨架进行语义聚类。

    目标：
    - 将同一项目/模块/事项合并
    - 保留成员、日期、mention、风险和下一步
    """
    if not skeletons:
        return {"clusters": []}

    skeleton_lines = []

    for idx, s in enumerate(skeletons, 1):
        skeleton_lines.append(
            f"{idx}. "
            f"date={s.get('date', '')}; "
            f"user_label={s.get('user_label', '')}; "
            f"user_mention={s.get('user_mention', '')}; "
            f"theme={s.get('task_theme', '')}; "
            f"status={s.get('progress_status', '')}; "
            f"output={s.get('key_output', '')}; "
            f"risk={s.get('risk_blocker', '')}; "
            f"next_plan={s.get('next_plan', '')}"
        )

    skeleton_text = "\n".join(skeleton_lines)

    prompt_file_guid = project.get("weekly_summary_prompt_file_guid")

    default_prompt = """你是任务聚类助手。

任务：将下方任务骨架按语义聚类，将同一项目/模块/事项主题的跨天任务合并为一组。

输入骨架：
{{skeleton_text}}

聚类规则：
1. 同一主题合并为一组。
2. 不要丢失 date、user_label、user_mention、status、output、risk、next_plan。
3. 若 risk 非空，该任务应进入 risk 类聚类。
4. 若 next_plan 非空，可进入 next_plan 类聚类。
5. 每个 cluster 的 type 只能是：progress / risk / help_needed / next_plan。
6. 同一个任务如果既有 output 又有 next_plan，可以根据主要含义放入 progress 或 next_plan，但字段不能丢。
7. 不允许编造输入中没有的信息。
8. 输出 JSON，格式如下：

{
  "clusters": [
    {
      "cluster_id": "C001",
      "theme": "主题名称",
      "type": "progress",
      "tasks": [
        {
          "date": "2026-04-21",
          "user_label": "张三",
          "user_mention": "[@张三](mention:uid:id)",
          "status": "开发中",
          "output": "完成登录接口开发",
          "risk": "",
          "next_plan": ""
        }
      ]
    }
  ]
}

只输出 JSON，不要输出其他文字。
"""

    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)
    user_content = prompt_text.replace("{{skeleton_text}}", skeleton_text)

    context_messages = [
        {
            "role": "system",
            "content": "你是任务聚类助手，请只输出 JSON 对象，不要输出解释文字。",
            "variables": []
        },
        {
            "role": "user",
            "content": user_content,
            "variables": []
        }
    ]

    llm_result = _call_llm_with_retry(
        llm_name=model.llm_name,
        llm_params=model.llm_params,
        context_messages=context_messages,
        max_retries=5
    )

    result = safe_json_loads(llm_result, expected_type=dict)

    if isinstance(result, dict) and "clusters" in result:
        return result

    print("  ⚠️ 规划师解析聚类 JSON 失败，回退到空列表")
    return {"clusters": []}


def write_weekly_from_clusters(clustered, weekly_json, project):
    """
    Writer Agent:
    根据聚类结果生成周报正文。

    注意：
    撰写人只基于 clustered 结果写，不直接读取原文，避免重新丢失结构。
    """
    clusters = clustered.get("clusters", [])

    if not clusters:
        return (
            "### 本周核心进展\n"
            "暂无核心产出\n\n"
            "### 困难风险及所需支持\n"
            "本周无阻塞性困难。\n\n"
            "### Next Key Focus\n"
            "按既定路线图推进中。"
        )

    progress_clusters = [c for c in clusters if c.get("type") == "progress"]
    risk_clusters = [c for c in clusters if c.get("type") in ("risk", "help_needed")]
    plan_clusters = [c for c in clusters if c.get("type") == "next_plan"]

    def cluster_to_text(c):
        lines = []

        for t in c.get("tasks", []):
            name = t.get("user_mention") or t.get("user_label", "未知")

            desc_parts = []

            if t.get("output"):
                desc_parts.append(t.get("output"))

            if t.get("risk"):
                desc_parts.append(f"风险/阻塞：{t.get('risk')}")

            if t.get("next_plan"):
                desc_parts.append(f"下一步：{t.get('next_plan')}")

            desc = "；".join(desc_parts) if desc_parts else "未提取到具体描述"

            lines.append(
                f"  - {name}: [{t.get('date', '')}] {t.get('status', '')} → {desc}"
            )

        tasks_text = "\n".join(lines)

        return f"- **{c.get('theme', '')}**\n{tasks_text}"

    progress_text = "\n".join(cluster_to_text(c) for c in progress_clusters) if progress_clusters else ""
    risk_text = "\n".join(cluster_to_text(c) for c in risk_clusters) if risk_clusters else ""
    plan_text = "\n".join(cluster_to_text(c) for c in plan_clusters) if plan_clusters else ""

    prompt_file_guid = project.get("weekly_summary_prompt_file_guid")

    default_prompt = """请基于以下聚类结果，按指定格式输出周报正文（Markdown）。

# 本周核心进展
{progress_text}

# 困难风险及所需支持
{risk_text}

# Next Key Focus
{plan_text}

# 输出要求
1. 将上述内容按项目/模块重新组织，相同项目的不同事项放在同一分类下。
2. 每个事项以成员 mention 或人名开头，例如：`[@姓名](mention:uid:id)：完成xxx`。
3. 必须保留具体对象、数量、算法名、case 名、问题名。
4. 不要只写“完成多个算法”“推进相关工作”这种空泛表达。
5. 格式如下：

### 本周核心进展
- **{项目名}**
  * [@姓名](mention:uid:id)：进展描述

### 困难风险及所需支持
- **{项目名}**
  * [@姓名](mention:uid:id)：风险描述
（若无则输出：本周无阻塞性困难。）

### Next Key Focus
- **{项目名}**
  * [@姓名](mention:uid:id)：计划描述
（若无则输出：按既定路线图推进中。）

6. 仅输出正文 Markdown，不要输出“日期范围”等头部元信息。
7. 语言精炼、专业、适合团队同步。
8. 不允许编造输入中没有的信息。
"""

    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)

    user_content = (
        prompt_text
        .replace("{progress_text}", progress_text if progress_text else "暂无核心产出")
        .replace("{risk_text}", risk_text if risk_text else "")
        .replace("{plan_text}", plan_text if plan_text else "")
    )

    context_messages = [
        {
            "role": "system",
            "content": "你是专业的项目周报撰写助手，请按指定格式输出 Markdown 周报正文。",
            "variables": []
        },
        {
            "role": "user",
            "content": user_content,
            "variables": []
        }
    ]

    llm_result = _call_llm_with_retry(
        llm_name=model.llm_name,
        llm_params=model.llm_params,
        context_messages=context_messages,
        max_retries=5
    )

    return strip_markdown_wrapper(llm_result).strip()


# =============================================================================
# 7. Step 3 - 摘要生成与最终 Markdown 拼接
# =============================================================================

def extract_progress_section(weekly_body_md):
    """
    从完整周报正文中提取“本周核心进展”部分，用于生成团队关键进展摘要。
    避免风险和下一步计划污染摘要。
    """
    if not weekly_body_md:
        return ""

    pattern = r"###\s*本周核心进展\s*(.*?)(?=###\s*困难风险及所需支持|###\s*Next Key Focus|$)"
    match = re.search(pattern, weekly_body_md, flags=re.DOTALL)

    if match:
        return match.group(1).strip()

    return weekly_body_md


def generate_key_summary(progress_content, project):
    """
    生成“团队关键进展”宏观摘要。
    """
    prompt_file_guid = project.get("weekly_key_summary_prompt_file_guid")

    default_prompt = """请基于以下“本周核心进展”内容，用一段 80~120 字的客观、平实的文字宏观总结本周整体进度。

要求：
1. 仅陈述里程碑达成情况、整体健康度。
2. 严禁使用“表现优异”、“进展神速”等主观评价。
3. 这一段必须来自输入中已有的“核心进展”归纳，不允许额外创造事实。
4. 不要写空泛评价，不要写“整体进展顺利/符合预期/状态良好”等未在输入中明确出现的判断。
5. 若全员无实质进展，写“本周暂无核心产出”。
6. 只输出一段文字，不要加标题，不要加 Markdown 格式。

输入内容：
{{progress_content}}
"""

    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)
    user_content = prompt_text.replace("{{progress_content}}", progress_content)

    context_messages = [
        {
            "role": "system",
            "content": "你是周报摘要生成助手，请输出一段客观精炼的总结文字，80~150字。",
            "variables": []
        },
        {
            "role": "user",
            "content": user_content,
            "variables": []
        }
    ]

    llm_result = _call_llm_with_retry(
        llm_name=model.llm_name,
        llm_params=model.llm_params,
        context_messages=context_messages,
        max_retries=10
    )

    return strip_markdown_wrapper(llm_result)


def build_final_markdown(weekly_json, weekly_body_md, key_summary):
    """
    拼接最终写入工作区的 Markdown。
    """
    metadata = weekly_json.get("metadata", {})

    range_dates = metadata.get("range_dates", [])
    source_urls = metadata.get("source_urls", {})
    week_number = metadata.get("week_number", "")

    start_date = range_dates[0] if range_dates else ""
    end_date = range_dates[-1] if range_dates else ""

    header_parts = [
        f"**日期范围：** {start_date} 至 {end_date} | **周数：** 第 {week_number} 周",
        "",
        "**源日报链接：**"
    ]

    for report_date in sorted(source_urls.keys()):
        urls = source_urls.get(report_date, [])

        if isinstance(urls, str):
            urls = [urls]

        for idx, source_url in enumerate(urls, 1):
            header_parts.append(
                f"- {report_date} 原日报{idx}: [{source_url}]({source_url})"
            )

    header_parts.append("")
    header_parts.append("---")
    header_parts.append("")

    parts = list(header_parts)

    parts.append("### 团队关键进展")
    parts.append(key_summary)
    parts.append("")
    parts.append(weekly_body_md)

    return "\n".join(parts)


# =============================================================================
# 8. 文档创建与写入
# =============================================================================

def _request_with_retry(method, url, max_retries=3, **kwargs):
    """
    通用请求重试函数。
    """
    kwargs.setdefault("timeout", 30)

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            if method == "post":
                response = requests.post(url, **kwargs)
            else:
                response = requests.get(url, **kwargs)

            return response

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e

            if attempt < max_retries:
                wait = min(2 ** attempt, 10)
                print(f"    {url.split('/')[-1]} 第 {attempt} 次请求失败: {e}, {wait}s 后重试...")
                time.sleep(wait)
            else:
                raise last_error


def insert_markdown_to_note(user_guid, note_guid, markdown_content, max_retries=3):
    """
    将 Markdown 写入已有笔记。
    """
    clean_content = strip_markdown_wrapper(markdown_content)
    html_content = _convert_special_nodes(clean_content)

    response = _request_with_retry(
        "post",
        BASE_URL + MD_INSERT_ROUTE,
        max_retries=max_retries,
        headers=get_headers_with_ak(user_guid=user_guid),
        json={
            "note_guid": note_guid,
            "markdown_content": html_content,
            "mode": "w",
            "location": 1
        },
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(f"写入笔记失败: {response.text}")

    return response.json()


def create_note_api(content, title, project_guid, parent_guid, tags, creator_guid=None):
    """
    创建工作区笔记，并写入内容。
    """
    creator_guid = creator_guid or USER_GUID

    headers = get_headers_with_ak()
    headers["X-User-GUID"] = creator_guid

    if not project_guid:
        raise ValueError("target_project_guid 不能为空！")

    response = _request_with_retry(
        "post",
        BASE_URL + WORKSPACE_SAVE_ROUTE,
        max_retries=3,
        headers=headers,
        json={
            "project_guid": project_guid,
            "parent_guid": parent_guid,
            "target": {
                "name": title,
                "type": 1,
                "tags": tags
            },
            "creator_guid": creator_guid
        }
    )

    response_json = response.json()

    if response.status_code != 200 or not response_json.get("data"):
        raise Exception(f"创建笔记 API 返回错误: {response_json}")

    doc_id = response_json.get("data", {}).get("guid")

    if doc_id and content:
        try:
            insert_markdown_to_note(creator_guid, doc_id, content, max_retries=5)

        except Exception as e:
            print(f"    笔记已创建(doc_id={doc_id})但内容写入失败: {e}")
            print("    -> 将在 5s 后单独重试写入...")

            time.sleep(5)

            try:
                insert_markdown_to_note(creator_guid, doc_id, content, max_retries=5)
                print("    重试写入成功")

            except Exception as e2:
                print(f"    重试写入仍失败: {e2}，笔记已创建但内容为空，doc_id={doc_id}")

    return doc_id


def write_debug_note_to_worklog_folder(project, title, markdown_content, extra_tags=None):
    """
    调试用：把中间结果写回日报目录。
    """
    project_name = project.get("project_name", "")
    project_guid = project.get("project_guid")
    work_log_folder_guid = project.get("work_log_folder_guid")
    creator_guid = project.get("weekly_target_user_guid") or USER_GUID

    tags = ["周报", "调试"]

    if extra_tags:
        tags.extend(extra_tags)

    doc_id = create_note_api(
        content=markdown_content,
        title=title,
        project_guid=project_guid,
        parent_guid=work_log_folder_guid,
        tags=tags,
        creator_guid=creator_guid
    )

    if doc_id:
        debug_url = f"{BASE_URL}/workspace/{doc_id}"
        print(f"[Debug][{project_name}] 调试笔记已写回 work log folder: {debug_url}")
        return debug_url

    return ""


# =============================================================================
# 9. Step 4 - 创建正式周报笔记
# =============================================================================

def create_final_weekly_note(content, project, week_info):
    """
    创建正式周报笔记。
    """
    try:
        project_name = project.get("project_name", "")

        target_project_guid = project.get("weekly_target_project_guid")
        target_parent_guid = project.get("weekly_target_parent_guid", "0")
        target_user_guid = project.get("weekly_target_user_guid")

        if not target_project_guid:
            raise ValueError(f"配置错误: project '{project_name}' 的 weekly_target_project_guid 为空！")

        print(f"[Step 4][{project_name}] 正在创建正式周报笔记...")

        title = build_weekly_note_title(week_info, project_name)

        doc_id = create_note_api(
            content=content,
            title=title,
            project_guid=target_project_guid,
            parent_guid=target_parent_guid,
            tags=["周报", "AI"],
            creator_guid=target_user_guid
        )

        if not doc_id:
            return [], []

        note_url = f"{BASE_URL}/workspace/{doc_id}"

        print(f"[Step 4][{project_name}] 正式周报笔记创建完成")

        return [note_url], [title]

    except Exception as e:
        print(f"[Step 4] 发生异常: {e}")
        traceback.print_exc()
        return [], []


# =============================================================================
# 10. Step 5 - 卡片生成与消息发送
# =============================================================================

def generate_card_content(project, long_markdown, week_info=None):
    """
    生成飞书卡片摘要正文。
    """
    project_name = project.get("project_name", "")
    card_prompt_file_guid = project.get(f"{generate_type}_card_prompt_guid")

    default_prompt = config.get(
        "card_prompt_default",
        "请将以下内容 {{markdown_content}} 整理为简洁的飞书消息卡片正文。"
        "格式要求：禁止使用任何标题语法（#、##），全部使用正文；仅必要时用加粗（**关键词**）强调；"
        "使用项目符号组织内容；重点突出、不超过 300 字。"
    )

    prompt_text = load_prompt_text(card_prompt_file_guid, default_prompt)

    def fallback_format_content(content, max_len=20000):
        content = re.sub(
            r"^###\s+(.+?)\s*$",
            lambda m: f"**{m.group(1).strip()}**",
            content,
            flags=re.MULTILINE
        )

        if len(content) > max_len:
            truncated = content[:max_len]
            suffix = "\n\n......\n[系统提示：AI 生成失败，此为自动截断的格式化预览]"
            return truncated + suffix

        return content

    if week_info is None:
        week_info = get_last_week_info()

    start_date = week_info["start_date"]
    end_date = week_info["end_date"]

    summary_prefix = f"**本周摘要 | {start_date} 至 {end_date}**\n\n"

    meta_header = (
        f"时间范围：{start_date} 至 {end_date} | "
        f"第{week_info['week_number']}周"
    )

    card_input_markdown = f"{meta_header}\n\n{long_markdown[:8000]}"
    user_content = prompt_text.replace("{{markdown_content}}", card_input_markdown)

    context_messages = [
        {
            "role": "system",
            "content": "你是内容整理助手，请输出纯文本摘要，不要 Markdown 代码块标记。",
            "variables": []
        },
        {
            "role": "user",
            "content": user_content,
            "variables": []
        }
    ]

    try:
        llm_name = getattr(model, "llm_name", None)
        llm_params = getattr(model, "llm_params", None) or DEFAULT_LLM_PARAMS

        if not llm_name:
            raise ValueError("model.llm_name 不能为空")

        llm_result = _call_llm_with_retry(
            llm_name=llm_name,
            llm_params=llm_params,
            context_messages=context_messages,
            max_retries=10
        )

        return summary_prefix + strip_markdown_wrapper(llm_result)

    except Exception as e:
        print(f"[Card][{project_name}] AI 生成在 10 次重试后仍失败 (Error: {e})")
        return summary_prefix + fallback_format_content(card_input_markdown, max_len=20000)


def build_feishu_card(title, card_content, note_url, source_note_entries=None):
    """
    构建飞书交互卡片。
    """
    elements = [
        {
            "tag": "markdown",
            "content": card_content,
            "margin": "0px",
            "text_size": "normal"
        }
    ]

    # 源日报入口按钮
    if source_note_entries:
        elements.append({"tag": "hr"})

        total_count = len(source_note_entries)
        display_entries = source_note_entries[:5]
        has_more = total_count > 5

        elements.append({
            "tag": "markdown",
            "content": f"**源日报入口**（共 {total_count} 篇）",
            "margin": "0px",
            "text_size": "normal"
        })

        button_items = []

        for item in display_entries:
            date_text = item.get("date", "")
            short_date = date_text[5:] if len(date_text) >= 10 else date_text

            btn_text = f"{short_date} 日报"

            if item.get("index"):
                btn_text = f"{short_date} 日报{item.get('index')}"

            button_items.append({
                "text": btn_text,
                "url": item.get("url", ""),
                "type": "default"
            })

        if has_more:
            button_items.append({
                "text": "更多日报",
                "url": note_url,
                "type": "default"
            })

        for i in range(0, len(button_items), 2):
            pair = button_items[i:i + 2]
            columns = []

            for item in pair:
                columns.append({
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "elements": [
                        {
                            "tag": "button",
                            "type": item.get("type", "default"),
                            "width": "fill",
                            "margin": "4px 0px 4px 0px",
                            "text": {
                                "tag": "plain_text",
                                "content": item.get("text", "查看")
                            },
                            "behaviors": [
                                {
                                    "type": "open_url",
                                    "default_url": item.get("url", "")
                                }
                            ]
                        }
                    ]
                })

            if len(columns) == 1:
                columns.append({
                    "tag": "column",
                    "width": "weighted",
                    "weight": 1,
                    "elements": []
                })

            elements.append({
                "tag": "column_set",
                "flex_mode": "stretch",
                "horizontal_spacing": "8px",
                "margin": "0px",
                "columns": columns
            })

    # 完整周报按钮
    elements.append({
        "tag": "column_set",
        "flex_mode": "stretch",
        "horizontal_spacing": "8px",
        "margin": "8px 0px 0px 0px",
        "columns": [
            {
                "tag": "column",
                "width": "auto",
                "elements": [
                    {
                        "tag": "button",
                        "type": "primary_filled",
                        "width": "fill",
                        "margin": "4px 0px 4px 0px",
                        "text": {
                            "tag": "plain_text",
                            "content": "查看完整周报"
                        },
                        "behaviors": [
                            {
                                "type": "open_url",
                                "default_url": note_url
                            }
                        ]
                    }
                ]
            }
        ]
    })

    return {
        "schema": "2.0",
        "header": {
            "padding": "12px 8px 12px 8px",
            "template": "orange",
            "title": {
                "content": title,
                "tag": "plain_text"
            }
        },
        "body": {
            "vertical_spacing": "12px",
            "elements": elements
        }
    }


def send_webhook(webhook_url, card):
    """
    通过 Webhook 发送群消息。
    """
    response = requests.post(
        url=webhook_url,
        headers={"Content-Type": "application/json"},
        json={
            "msg_type": "interactive",
            "card": card
        }
    )

    return response.json()


def send_message_api(receiver_guids, title, content, sender_guid="", interactive_content=None):
    """
    通过平台消息 API 发送个人消息。
    """
    payload = {
        "template_id": MESSAGE_TEMPLATE_ID,
        "receiver_guid": receiver_guids,
        "content": content,
        "org_guid": ORG_GUID,
        "title": title,
        "platform_type": PLATFORM_TYPE
    }

    if interactive_content is not None:
        payload["interactive_content"] = json.dumps(interactive_content, ensure_ascii=False)

    return requests.post(
        url=BASE_URL + MESSAGE_SEND_ROUTE,
        headers=get_headers_with_ak(user_guid=sender_guid),
        json=payload
    )


def step5_send_messages(note_url_list, note_title_list, project, content_list, week_info=None, source_note_entries=None):
    """
    Step 5:
    发送群 Webhook 和个人消息。
    """
    try:
        project_name = project.get("project_name", "")

        raw_webhook_config = project.get(f"{generate_type}_webhook_url", [])

        if isinstance(raw_webhook_config, str):
            webhook_urls = [raw_webhook_config] if raw_webhook_config else []
        elif isinstance(raw_webhook_config, list):
            webhook_urls = raw_webhook_config
        else:
            webhook_urls = []

        receiver_guids = normalize_receiver_guids(
            project.get(f"{generate_type}_sender_guid", [])
        )

        sender_guid = project.get(f"{generate_type}_target_user_guid", "") or USER_GUID

        if not note_url_list:
            print(f"[Step 5][{project_name}] 没有 URL 可发送")
            return

        for note_title, note_url, full_content in zip(note_title_list, note_url_list, content_list):
            card_summary = generate_card_content(project, full_content, week_info=week_info)

            card = build_feishu_card(
                note_title,
                card_summary,
                note_url,
                source_note_entries=source_note_entries
            )

            has_sent_any = False

            # 群 Webhook
            if webhook_urls:
                for idx, url in enumerate(webhook_urls, 1):
                    try:
                        print(f"[Step 5][{project_name}] 正在发送群消息 (Webhook {idx}/{len(webhook_urls)})...")

                        webhook_result = send_webhook(url, card)

                        if webhook_result.get("code") == 0 or webhook_result.get("StatusCode") == 0:
                            print(f"  -> 群消息发送成功: {url[:30]}...")
                            has_sent_any = True
                        else:
                            print(f"  -> 群消息发送失败 ({url}): {webhook_result}")

                    except Exception as e:
                        print(f"  -> 群消息发送异常 ({url}): {e}")

            else:
                print(f"[Step 5][{project_name}] 未配置 Webhook 地址，跳过群消息发送")

            # 个人消息
            if receiver_guids:
                try:
                    print(f"[Step 5][{project_name}] 正在发送个人消息给 {len(receiver_guids)} 人...")

                    text_content = build_message_text(note_title, note_url)

                    response = send_message_api(
                        receiver_guids=receiver_guids,
                        title=note_title,
                        content=text_content,
                        sender_guid=sender_guid,
                        interactive_content=card
                    )

                    if response.status_code == 200 and response.json().get("data"):
                        print("  -> 个人消息发送成功")
                        has_sent_any = True
                    else:
                        print(f"  -> 个人消息发送失败: {response.text}")

                except Exception as e:
                    print(f"  -> 个人消息发送异常: {e}")

            if not has_sent_any and not webhook_urls and not receiver_guids:
                print(f"[Step 5][{project_name}] 未配置 Webhook 且未配置接收人，跳过发送步骤")

        print(f"[Step 5][{project_name}] 消息分发流程结束")

    except Exception as e:
        print(f"[Step 5] 发生异常: {e}")
        traceback.print_exc()


# =============================================================================
# 11. LLM 工作流调用封装
# =============================================================================

def _create_chat_id(conversation_id="", id_type="conversation"):
    """
    创建 conversation/message ID。
    """
    response = requests.post(
        BASE_URL + "/platform/peerup_chatbot/conversation/id",
        headers=get_headers_with_ak(),
        json={
            "conversation_id": conversation_id,
            "type": id_type
        }
    )

    response_json = response.json()

    return response_json.get("data", {}).get("id")


def _call_llm_with_retry(llm_name, llm_params, context_messages, max_retries=20):
    """
    调用 AI 工作流模型，并轮询结果。

    支持：
    - 自动重试
    - 指数退避
    - 工作流结果轮询
    """
    attempt = 0
    last_error = None

    while attempt < max_retries:
        try:
            print(f"  [尝试 {attempt + 1}/{max_retries}] 调用 AI 工作流...")

            conversation_id = _create_chat_id("", "conversation")
            message_id = _create_chat_id(conversation_id, "message")

            response = requests.post(
                BASE_URL + "/platform/peerup_chatbot/workflow/model",
                headers=get_headers_with_ak(),
                json={
                    "message_id": message_id,
                    "llm_config": {
                        "llm_name": llm_name,
                        "llm_params": llm_params
                    },
                    "context_messages": context_messages
                },
                timeout=60
            )

            response_json = response.json()
            task_message_id = response_json.get("data", {}).get("message_id")

            if not task_message_id:
                raise Exception(f"No task ID, response={response_json}")

            # 轮询结果
            for _ in range(240):
                poll_response = requests.post(
                    BASE_URL + "/platform/peerup_chatbot/workflow/model/result",
                    headers=get_headers_with_ak(),
                    json={"message_id": task_message_id},
                    timeout=30
                )

                poll_data = poll_response.json().get("data", {})
                status = poll_data.get("status")

                if status == "completed":
                    return poll_data.get("content")

                if status == "failed":
                    raise Exception(f"AI Failed: {poll_data.get('error_message')}")

                time.sleep(3)

            raise Exception("AI Timeout")

        except Exception as e:
            last_error = e
            attempt += 1

            if attempt < max_retries:
                wait_time = min(2 ** (attempt - 1), 30)
                print(f"  AI 调用失败: {e}. {wait_time}秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"  AI 调用连续 {max_retries} 次失败，放弃重试。错误: {e}")
                raise last_error


# =============================================================================
# 12. 主执行流程 Orchestrator
# =============================================================================

print("=" * 60)
print(f"开始执行周报 Workflow Agent v1.1 | 项目数: {len(projects)}")
print("=" * 60)

for project in projects:
    project_name = project.get("project_name", "Unknown")
    enable_ai = project.get("enable_weekly_summary", True)

    if not enable_ai:
        print(f"\n跳过项目: {project_name} (enable_weekly_summary=False)")
        continue

    print(f"\n处理项目: {project_name}")

    temp_files = []
    week_info = get_last_week_info()

    try:
        # ---------------------------------------------------------------------
        # Step 1: Collector + Flattener
        # 收集日报，并展平成周维度 raw JSON
        # ---------------------------------------------------------------------
        weekly_json, found, step1_temp_files = step1_collect_and_flatten_weekly_reports(project)
        temp_files.extend(step1_temp_files)

        if not found:
            print(f"  跳过 {project_name}")
            cleanup_temp_files(temp_files, project_name=project_name)
            continue

        users = weekly_json.get("users", [])

        if not users:
            raise Exception("没有找到任何用户数据")

        # ---------------------------------------------------------------------
        # Step 2.1: Indexer Agent
        # 从自由文本日报中抽取任务骨架
        # ---------------------------------------------------------------------
        print(f"[Step 2.1][{project_name}] Indexer Agent 开始提取任务骨架...")

        skeletons = batch_extract_skeletons(users, project)

        print(f"[Step 2.1][{project_name}] Indexer 完成，提取到 {len(skeletons)} 个任务骨架")

        temp_files.append(
            build_intermediate_json_file(
                project_guid=project["project_guid"],
                target_date_str=f"{week_info['start_date']}_to_{week_info['end_date']}",
                json_content=skeletons,
                suffix="skeletons"
            )
        )

        # ---------------------------------------------------------------------
        # Step 2.2: Planner Agent
        # 对任务骨架进行语义聚类
        # ---------------------------------------------------------------------
        print(f"[Step 2.2][{project_name}] Planner Agent 开始语义聚类...")

        clustered = semantic_cluster_skeletons(skeletons, users, project)
        cluster_count = len(clustered.get("clusters", []))

        print(f"[Step 2.2][{project_name}] Planner 完成，聚成 {cluster_count} 个主题")

        temp_files.append(
            build_intermediate_json_file(
                project_guid=project["project_guid"],
                target_date_str=f"{week_info['start_date']}_to_{week_info['end_date']}",
                json_content=clustered,
                suffix="clusters"
            )
        )

        # ---------------------------------------------------------------------
        # Step 2.3: Writer Agent
        # 基于聚类结果生成周报正文
        # ---------------------------------------------------------------------
        print(f"[Step 2.3][{project_name}] Writer Agent 开始生成周报正文...")

        weekly_body_md = write_weekly_from_clusters(clustered, weekly_json, project)

        print(f"[Step 2.3][{project_name}] 周报正文生成完成")

        # ---------------------------------------------------------------------
        # Step 3: 生成团队关键进展摘要 + 拼接最终 Markdown
        # ---------------------------------------------------------------------
        progress_content = extract_progress_section(weekly_body_md)

        key_summary = generate_key_summary(progress_content, project)

        print(f"[Step 3][{project_name}] 团队关键进展摘要已生成")

        final_weekly_markdown = build_final_markdown(
            weekly_json=weekly_json,
            weekly_body_md=weekly_body_md,
            key_summary=key_summary
        )

        if not final_weekly_markdown:
            raise Exception("AI 生成内容为空")

        # ---------------------------------------------------------------------
        # Step 4: 创建正式周报笔记
        # ---------------------------------------------------------------------
        note_urls, note_titles = create_final_weekly_note(
            content=final_weekly_markdown,
            project=project,
            week_info=week_info
        )

        # ---------------------------------------------------------------------
        # Step 5: 构建源日报入口，并发送消息
        # ---------------------------------------------------------------------
        raw_source_urls_map = weekly_json.get("metadata", {}).get("source_urls", {})
        source_note_entries = []

        for report_date, urls in sorted(raw_source_urls_map.items()):
            if isinstance(urls, str):
                urls = [urls]

            for idx, url in enumerate(urls, 1):
                source_note_entries.append({
                    "date": report_date,
                    "url": url,
                    "index": idx
                })

        # 卡片最多展示 5 个源日报入口
        source_note_entries = source_note_entries[:5]

        step5_send_messages(
            note_url_list=note_urls,
            note_title_list=note_titles,
            project=project,
            content_list=[final_weekly_markdown],
            week_info=week_info,
            source_note_entries=source_note_entries
        )

        cleanup_temp_files(temp_files, project_name=project_name)

        print(f"{project_name} 周报流程结束")

    except Exception as e:
        cleanup_temp_files(temp_files, project_name=project_name)
        print(f"{project_name} 周报流程中断: {e}")
        traceback.print_exc()

print("\n" + "=" * 60)
print("全部周报任务执行完毕")
print("=" * 60)