# =============================================================================
# Weekly Report Workflow Agent v2.1
# =============================================================================
# 设计目标：
# 1. 通用性：不依赖固定日报模板，不维护复杂业务 parser
# 2. 适配弱模型：大输入先拆 unit，再自动打包 chunk，避免一次读完整周
# 3. 降低错分：每个文本单元带 unit_id，模型只引用 unit_id，程序侧强制回填人/日期/mention
# 4. 模型调用：使用 OpenAI SDK 兼容接口，例如 doubao-seed-2.0-pro
#
# 核心流程：
# Step 1: 收集日报 + 通用展平
# Step 2.1: 构建 member-date unit，并自动打包为 chunk
# Step 2.2: Indexer Agent 分 chunk 抽取 skeleton
# Step 2.3: 程序侧用 unit_id 强制回填来源，降低错分
# Step 2.4: Planner Agent 聚类
# Step 2.5: Writer Agent 写周报正文
# Step 3: 生成团队摘要 + 拼接最终 Markdown
# Step 4: 创建正式周报笔记
# Step 5: 推送消息
# =============================================================================


# =============================================================================
# 0. 基础依赖与 print 刷新设置
# =============================================================================
import builtins
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
from openai import OpenAI


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

AK = config.get("ak")
SK = config.get("sk")
ORG_GUID = config.get("org_guid")
USER_GUID = config.get("user_guid")
projects = config.get("projects", [])

generate_type = "weekly"

DEFAULT_LLM_PARAMS = {
    "temperature": 0.3,
    "max_tokens": 4096
}

MESSAGE_TEMPLATE_ID = "80"
PLATFORM_TYPE = "all"


# =============================================================================
# 2. OpenAI-Compatible LLM Client 配置
# =============================================================================
# 不建议把 api_key 写死在代码里。
# 请在 config.json 顶层配置：
# {
#   "llm_base_url": "http://agi-gateway.cxmt.com/cloud/v1",
#   "llm_api_key": "xxx",
#   "llm_name": "doubao-seed-2.0-pro",
#   "llm_params": {
#     "temperature": 0.3,
#     "max_tokens": 4096
#   }
# }
# =============================================================================

LLM_BASE_URL = config.get("llm_base_url", "http://agi-gateway.cxmt.com/cloud/v1")
LLM_API_KEY = config.get("llm_api_key")
LLM_MODEL_NAME = config.get("llm_name", "doubao-seed-2.0-pro")

if not LLM_API_KEY:
    raise ValueError("llm_api_key 不能为空，请在 config.json 顶层配置 llm_api_key，不建议写死在代码中。")

llm_client = OpenAI(
    base_url=LLM_BASE_URL,
    api_key=LLM_API_KEY
)


def get_llm_config(project=None):
    """
    获取 LLM 配置。

    优先级：
    1. project.llm_name / project.llm_params
    2. config.llm_name / config.llm_params
    3. 默认 LLM_MODEL_NAME / DEFAULT_LLM_PARAMS
    """
    project = project or {}

    llm_name = (
        project.get("llm_name")
        or config.get("llm_name")
        or LLM_MODEL_NAME
    )

    llm_params = (
        project.get("llm_params")
        or config.get("llm_params")
        or DEFAULT_LLM_PARAMS
    )

    llm_params = dict(llm_params)
    llm_params.setdefault("temperature", 0.3)
    llm_params.setdefault("max_tokens", 4096)

    return llm_name, llm_params


# =============================================================================
# 3. API 路由
# =============================================================================
ACCESS_TOKEN_ROUTE = "/api/user/platform/getAccessToken"
NOTE_JSON_ROUTE = "/platform/ws/noteInfo/getDocJson"
DOC_TREE_ROUTE = "/platform/api/main/doc/treeList"
SIGNED_URL_ROUTE = "/platform/api/main/storage/getSignedUrl"

WORKSPACE_SAVE_ROUTE = "/middle/server/api/workspace/save"
MD_INSERT_ROUTE = "/middle/server/api/file/md/insert"
MESSAGE_SEND_ROUTE = "/middle/server/api/msg/send"


# =============================================================================
# 4. 通用工具函数
# =============================================================================
def get_headers_with_ak(user_guid="", doc_id=""):
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
    headers = get_headers_with_ak(user_guid=user_guid, doc_id=doc_id)

    response = requests.get(
        url=BASE_URL + NOTE_JSON_ROUTE,
        headers=headers,
        params={"docId": doc_id}
    )

    return response.json()


def strip_markdown_wrapper(content):
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
    """
    raw = strip_markdown_wrapper(raw).strip()

    try:
        result = json.loads(raw)
        if expected_type is None or isinstance(result, expected_type):
            return result
    except Exception:
        pass

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
    """
    content = re.sub(
        r"\[@([^\]]*)\]\(mention:[^:]+:([^)]+)\)",
        lambda m: f'<span data-node-type="mention" data-guid="{m.group(2)}"></span>',
        content
    )

    content = re.sub(
        r"\[([^\]]+)\]\(mentionUrl:[^:]+:[^:]+:([^)]+)\)",
        lambda m: f'<a data-node-type="mentionUrl" data-url="{m.group(2)}">{m.group(1)}</a>',
        content
    )

    content = re.sub(
        r"\[([^\]]+)\]\((https?://[^)\s]+)\)",
        lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>',
        content
    )

    content = re.sub(
        r":::highlight\[[^\]]*\]\n(.*?):::",
        lambda m: f'<div data-node-type="highlightBlock" data-content-markdown>\n{m.group(1).rstrip()}\n</div>',
        content,
        flags=re.DOTALL
    )

    return content


def normalize_receiver_guids(receiver_guids_raw):
    if isinstance(receiver_guids_raw, str):
        return [receiver_guids_raw]
    return receiver_guids_raw or []


def build_message_text(note_title, note_url):
    return f"[{note_title}] 已生成，请点击查看。\n<a href='{note_url}'>点击查看详情</a>"


def load_prompt_text(prompt_file_guid, default_prompt):
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


def build_intermediate_json_file(project_guid, target_date_str, json_content, suffix=""):
    tmp_dir = tempfile.gettempdir()
    unique_suffix = uuid.uuid4().hex[:8]
    name_suffix = f"_{suffix}" if suffix else ""

    file_name = f"weekly_{project_guid}_{target_date_str.replace('-', '')}{name_suffix}_{unique_suffix}.json"
    file_path = os.path.join(tmp_dir, file_name)

    with open(file_path, "w", encoding="utf-8") as output_fp:
        json.dump(json_content, output_fp, ensure_ascii=False, indent=2)

    return file_path


def build_intermediate_markdown_file(project_guid, target_date_str, markdown_content):
    tmp_dir = tempfile.gettempdir()
    unique_suffix = uuid.uuid4().hex[:8]

    file_name = f"weekly_{project_guid}_{target_date_str.replace('-', '')}_{unique_suffix}.md"
    file_path = os.path.join(tmp_dir, file_name)

    with open(file_path, "w", encoding="utf-8") as output_fp:
        output_fp.write(markdown_content)

    return file_path


def cleanup_temp_files(file_paths, project_name=""):
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
    year = week_info["start_date"][:4]
    week_number = week_info["week_number"]

    return f"{year}#W{week_number:02d} {project_name}周报"


def get_mention_attrs(mention_obj):
    if not mention_obj:
        return {}
    return mention_obj.get("attrs", mention_obj)


def get_mention_label(mention_obj):
    attrs = get_mention_attrs(mention_obj)
    return attrs.get("label", "未知")


def mention_to_markdown(mention_obj):
    if not mention_obj:
        return "[@未知](mention::)"

    attrs = get_mention_attrs(mention_obj)
    uid = attrs.get("uid", "")
    user_id = attrs.get("id", "")
    label = attrs.get("label", "未知")

    return f"[@{label}](mention:{uid}:{user_id})"


def make_safe_id_text(text):
    text = str(text or "")
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^\w\u4e00-\u9fff\-_.]", "_", text)
    return text[:40]


# =============================================================================
# 5. 通用文档展平器 DocumentFlattener
# =============================================================================
class DocumentFlattener:
    """
    通用文档展平器。

    不是业务 parser，不识别项目、不识别进展/风险/计划。
    只做：
    - 提取文本
    - 提取 mention
    - 提取 mentionUrl
    - 提取表格文本
    - 尽量按成员 heading 组织内容
    """

    CONTAINER_BLOCK_TYPES = {"blockContainer", "blockGroup"}
    MEMBER_HEADER_BLOCK_TYPES = {"heading", "fheading"}
    CONTENT_BLOCK_TYPES = {"bulletListItem", "numberedListItem", "paragraph"}

    def __init__(self, project_config):
        self.project_name = project_config.get("project_name", "Unknown")

    def extract_text_and_mentions(self, inline_content):
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

                if block_type in self.CONTAINER_BLOCK_TYPES:
                    if "content" in block:
                        traverse(block["content"])
                    continue

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

                # 轻量成员识别：heading/fheading 中有 mention，就认为是成员标题
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
# 6. Step 1 - 收集并展平日报
# =============================================================================
def find_weekly_notes(user_guid, project_guid, folder_guid, date_list):
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
# 7. Step 2.1 - 构建 unit 与自适应 chunk
# =============================================================================
def build_indexer_units(users_data):
    """
    构建最小语义单元：member + date。
    """
    units = []

    for user in users_data:
        user_mention_obj = user.get("user_name", {})
        user_label = get_mention_label(user_mention_obj)
        user_md = mention_to_markdown(user_mention_obj)
        safe_user = make_safe_id_text(user_label)

        for report in user.get("reports", []):
            report_date = report.get("date", "")
            content = report.get("content", "") or ""

            if not content.strip():
                continue

            unit_index = len(units) + 1
            unit_id = f"U{unit_index:05d}_{report_date}_{safe_user}"

            unit_text = (
                f"\n--- UNIT START ---\n"
                f"unit_id: {unit_id}\n"
                f"date: {report_date}\n"
                f"user_label: {user_label}\n"
                f"user_mention: {user_md}\n"
                f"content:\n{content.strip()}\n"
                f"--- UNIT END ---\n"
            )

            units.append({
                "unit_id": unit_id,
                "date": report_date,
                "user_label": user_label,
                "user_mention": user_md,
                "text": unit_text,
                "char_count": len(unit_text)
            })

    return units


def split_oversized_unit(unit, max_chars_per_chunk):
    """
    如果单个 unit 本身超长，则按段落切分。
    """
    if unit["char_count"] <= max_chars_per_chunk:
        return [unit]

    header = (
        f"\n--- UNIT START ---\n"
        f"unit_id: {{unit_id}}\n"
        f"date: {unit['date']}\n"
        f"user_label: {unit['user_label']}\n"
        f"user_mention: {unit['user_mention']}\n"
        f"content:\n"
    )

    footer = "\n--- UNIT END ---\n"

    raw_text = unit["text"]
    content_start = raw_text.find("content:")

    if content_start != -1:
        content = raw_text[content_start + len("content:"):].replace("--- UNIT END ---", "").strip()
    else:
        content = raw_text

    parts = []
    buffer = ""
    part_index = 1

    for para in content.split("\n"):
        para = para.strip()

        if not para:
            continue

        candidate = buffer + "\n" + para if buffer else para

        part_unit_id = f"{unit['unit_id']}_part{part_index}"
        part_header = header.replace("{unit_id}", part_unit_id)

        if len(part_header) + len(candidate) + len(footer) > max_chars_per_chunk:
            if buffer.strip():
                part_text = part_header + buffer.strip() + footer

                parts.append({
                    "unit_id": part_unit_id,
                    "date": unit["date"],
                    "user_label": unit["user_label"],
                    "user_mention": unit["user_mention"],
                    "text": part_text,
                    "char_count": len(part_text)
                })

                part_index += 1

            buffer = para
        else:
            buffer = candidate

    if buffer.strip():
        part_unit_id = f"{unit['unit_id']}_part{part_index}"
        part_header = header.replace("{unit_id}", part_unit_id)
        part_text = part_header + buffer.strip() + footer

        parts.append({
            "unit_id": part_unit_id,
            "date": unit["date"],
            "user_label": unit["user_label"],
            "user_mention": unit["user_mention"],
            "text": part_text,
            "char_count": len(part_text)
        })

    return parts


def build_indexer_chunks(users_data, max_chars_per_chunk=10000):
    """
    自适应 chunk 构建。
    """
    raw_units = build_indexer_units(users_data)

    normalized_units = []

    for unit in raw_units:
        normalized_units.extend(
            split_oversized_unit(unit, max_chars_per_chunk=max_chars_per_chunk)
        )

    chunks = []
    current_units = []
    current_text_parts = []
    current_size = 0

    for unit in normalized_units:
        unit_text = unit["text"]
        unit_size = len(unit_text)

        if current_units and current_size + unit_size > max_chars_per_chunk:
            chunk_index = len(chunks) + 1

            chunks.append({
                "chunk_id": f"C{chunk_index:04d}",
                "text": "\n".join(current_text_parts),
                "units": current_units,
                "char_count": current_size
            })

            current_units = []
            current_text_parts = []
            current_size = 0

        current_units.append({
            "unit_id": unit["unit_id"],
            "date": unit["date"],
            "user_label": unit["user_label"],
            "user_mention": unit["user_mention"]
        })

        current_text_parts.append(unit_text)
        current_size += unit_size

    if current_units:
        chunk_index = len(chunks) + 1

        chunks.append({
            "chunk_id": f"C{chunk_index:04d}",
            "text": "\n".join(current_text_parts),
            "units": current_units,
            "char_count": current_size
        })

    return chunks


def build_unit_map_from_chunks(chunks):
    """
    构建 unit_id -> metadata 映射。
    """
    unit_map = {}

    for chunk in chunks:
        for unit in chunk.get("units", []):
            unit_id = unit.get("unit_id")
            if unit_id:
                unit_map[unit_id] = {
                    "date": unit.get("date", ""),
                    "user_label": unit.get("user_label", ""),
                    "user_mention": unit.get("user_mention", ""),
                    "source_chunk_id": chunk.get("chunk_id", "")
                }

    return unit_map


# =============================================================================
# 8. Step 2.2 - Indexer Agent 分块抽取 skeleton
# =============================================================================
def extract_skeletons_from_chunk(chunk, project):
    """
    对单个 chunk 调用 Indexer Agent。
    """
    prompt_file_guid = project.get("weekly_indexer_prompt_file_guid") or project.get("weekly_summary_prompt_file_guid")

    default_prompt = """你是一周任务骨架提取助手。

你会收到若干个 UNIT，每个 UNIT 都有唯一 unit_id，并且包含 date、user_label、user_mention 和 content。

你的任务：
从每个 UNIT 的 content 中提取独立任务，输出 JSON 数组。

每个输出对象必须包含以下字段：
- unit_id: 必须严格复制对应 UNIT 的 unit_id，不能编造，不能留空
- task_theme: 项目/模块名/事项主题
- progress_status: 当前阶段，枚举：开发中/联调/已上线/规划中/待评审/排查中/修复中/测试中/已完成/其他
- key_output: 具体完成物、结果、数量或里程碑
- risk_blocker: 困难、风险、阻塞、需协助内容；无则填空字符串
- next_plan: 下一步计划；无则填空字符串

重要规则：
1. 一个独立任务输出一个 JSON 对象。
2. 必须使用任务所在 UNIT 的 unit_id。
3. 不要把 A UNIT 的内容归到 B UNIT。
4. 不要合并不同 UNIT 的内容。
5. key_output 必须具体，优先保留数量、对象名称、case 名称、算法名称、问题名称。
6. 风险、阻塞、需支援内容必须进入 risk_blocker。
7. 下一步、明日计划、后续计划必须进入 next_plan。
8. 不允许编造输入中没有的信息。
9. 如果某个 UNIT 没有可提取内容，不需要输出。
10. 如果所有 UNIT 都没有可提取内容，输出空数组 []。
11. 只输出 JSON 数组，不要输出解释文字，不要 Markdown 代码块。

输入：
{{daily_content}}
"""

    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)
    user_content = prompt_text.replace("{{daily_content}}", chunk.get("text", ""))

    context_messages = [
        {
            "role": "system",
            "content": "你是任务骨架提取助手。你只能输出 JSON 数组，不要输出解释文字。",
        },
        {
            "role": "user",
            "content": user_content,
        }
    ]

    llm_name, llm_params = get_llm_config(project)

    llm_result = _call_llm_with_retry(
        llm_name=llm_name,
        llm_params=llm_params,
        context_messages=context_messages,
        max_retries=5
    )

    skeletons = safe_json_loads(llm_result, expected_type=list)

    if not isinstance(skeletons, list):
        print(f"  ⚠️ chunk 解析失败: {chunk.get('chunk_id')}")
        return []

    normalized = []

    valid_unit_ids = {
        unit.get("unit_id")
        for unit in chunk.get("units", [])
        if unit.get("unit_id")
    }

    for item in skeletons:
        if not isinstance(item, dict):
            continue

        unit_id = item.get("unit_id", "")

        normalized.append({
            "unit_id": unit_id,
            "unit_id_valid": unit_id in valid_unit_ids,
            "task_theme": item.get("task_theme", ""),
            "progress_status": item.get("progress_status", "其他"),
            "key_output": item.get("key_output", ""),
            "risk_blocker": item.get("risk_blocker", ""),
            "next_plan": item.get("next_plan", ""),
            "source_chunk_id": chunk.get("chunk_id", "")
        })

    return normalized


def normalize_skeletons_with_unit_map(raw_skeletons, unit_map):
    """
    程序侧强制回填来源信息。
    """
    normalized = []
    dropped = []

    allowed_status = {
        "开发中",
        "联调",
        "已上线",
        "规划中",
        "待评审",
        "排查中",
        "修复中",
        "测试中",
        "已完成",
        "其他"
    }

    for item in raw_skeletons:
        unit_id = item.get("unit_id", "")

        if unit_id not in unit_map:
            dropped.append({
                "reason": "invalid_unit_id",
                "item": item
            })
            continue

        metadata = unit_map[unit_id]

        progress_status = item.get("progress_status", "其他")
        if progress_status not in allowed_status:
            progress_status = "其他"

        key_output = (item.get("key_output") or "").strip()
        task_theme = (item.get("task_theme") or "").strip()

        if not key_output:
            dropped.append({
                "reason": "empty_key_output",
                "item": item
            })
            continue

        normalized.append({
            "unit_id": unit_id,
            "date": metadata["date"],
            "user_label": metadata["user_label"],
            "user_mention": metadata["user_mention"],
            "source_chunk_id": metadata["source_chunk_id"],
            "task_theme": task_theme or "未命名事项",
            "progress_status": progress_status,
            "key_output": key_output,
            "risk_blocker": item.get("risk_blocker", "") or "",
            "next_plan": item.get("next_plan", "") or ""
        })

    return normalized, dropped


def batch_extract_skeletons_by_chunks(users_data, project):
    """
    分块版 Indexer 主入口。
    """
    if not users_data:
        return [], [], []

    max_chars_per_chunk = int(project.get("weekly_indexer_chunk_chars", 10000))

    chunks = build_indexer_chunks(
        users_data=users_data,
        max_chars_per_chunk=max_chars_per_chunk
    )

    unit_map = build_unit_map_from_chunks(chunks)

    print(f"  [Indexer] unit 数量: {len(unit_map)}")
    print(f"  [Indexer] 已自动打包为 {len(chunks)} 个 chunk，每块上限 {max_chars_per_chunk} 字符")

    all_raw_skeletons = []

    for idx, chunk in enumerate(chunks, 1):
        unit_count = len(chunk.get("units", []))

        print(
            f"  [Indexer][{idx}/{len(chunks)}] "
            f"chunk={chunk.get('chunk_id')} "
            f"units={unit_count} "
            f"chars={chunk.get('char_count')}"
        )

        try:
            chunk_skeletons = extract_skeletons_from_chunk(chunk, project)
            print(f"    -> 原始提取 {len(chunk_skeletons)} 条")
            all_raw_skeletons.extend(chunk_skeletons)

        except Exception as e:
            print(f"    -> chunk 提取失败: {chunk.get('chunk_id')}, error={e}")
            traceback.print_exc()

    skeletons, dropped = normalize_skeletons_with_unit_map(
        raw_skeletons=all_raw_skeletons,
        unit_map=unit_map
    )

    print(f"  [Indexer] 原始 skeleton: {len(all_raw_skeletons)} 条")
    print(f"  [Indexer] 有效 skeleton: {len(skeletons)} 条")
    print(f"  [Indexer] 丢弃 skeleton: {len(dropped)} 条")

    return skeletons, chunks, dropped


# =============================================================================
# 9. Step 2.3 - Planner Agent 语义聚类
# =============================================================================
def semantic_cluster_skeletons(skeletons, project):
    """
    Planner Agent:
    对 skeleton 做语义聚类。
    """
    if not skeletons:
        return {"clusters": []}

    skeleton_lines = []

    for idx, s in enumerate(skeletons, 1):
        skeleton_lines.append(
            f"{idx}. "
            f"unit_id={s.get('unit_id', '')}; "
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

    prompt_file_guid = project.get("weekly_planner_prompt_file_guid") or project.get("weekly_summary_prompt_file_guid")

    default_prompt = """你是任务聚类助手。

任务：
将下方任务骨架按语义聚类，将同一项目/模块/事项主题的任务合并为一组。

输入骨架：
{{skeleton_text}}

重要规则：
1. 只能基于输入骨架聚类，不允许编造新任务。
2. 不允许修改 unit_id、date、user_label、user_mention。
3. 不要丢失任何任务。
4. 不要把明显不同项目/模块的任务强行合并。
5. 每个 cluster 的 type 只能是：progress / risk / help_needed / next_plan。
6. 若 risk 非空，该任务所在 cluster 可以标为 risk。
7. 若 next_plan 非空，但没有明显产出，可以标为 next_plan。
8. 输出 JSON 对象，不要输出解释文字。

输出格式：
{
  "clusters": [
    {
      "cluster_id": "C001",
      "theme": "主题名称",
      "type": "progress",
      "tasks": [
        {
          "unit_id": "U00001_2026-04-20_张三",
          "date": "2026-04-20",
          "user_label": "张三",
          "user_mention": "[@张三](mention:uid:id)",
          "status": "已完成",
          "output": "完成 A case 验证",
          "risk": "",
          "next_plan": ""
        }
      ]
    }
  ]
}
"""

    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)
    user_content = prompt_text.replace("{{skeleton_text}}", skeleton_text)

    context_messages = [
        {
            "role": "system",
            "content": "你是任务聚类助手。你只能输出 JSON 对象，不要输出解释文字。",
        },
        {
            "role": "user",
            "content": user_content,
        }
    ]

    llm_name, llm_params = get_llm_config(project)

    llm_result = _call_llm_with_retry(
        llm_name=llm_name,
        llm_params=llm_params,
        context_messages=context_messages,
        max_retries=5
    )

    result = safe_json_loads(llm_result, expected_type=dict)

    if isinstance(result, dict) and "clusters" in result:
        return result

    print("  ⚠️ Planner 解析聚类 JSON 失败，回退到空列表")
    return {"clusters": []}


def repair_clustered_with_skeletons(clustered, skeletons):
    """
    轻量程序兜底：
    如果 Planner 丢任务，则把丢失的 skeleton 追加到“未归类事项”中。
    """
    skeleton_by_unit = {
        s.get("unit_id"): s
        for s in skeletons
        if s.get("unit_id")
    }

    appeared_unit_ids = set()

    for cluster in clustered.get("clusters", []):
        repaired_tasks = []

        for task in cluster.get("tasks", []):
            unit_id = task.get("unit_id", "")

            if unit_id in skeleton_by_unit:
                s = skeleton_by_unit[unit_id]

                repaired_tasks.append({
                    "unit_id": s["unit_id"],
                    "date": s["date"],
                    "user_label": s["user_label"],
                    "user_mention": s["user_mention"],
                    "status": s["progress_status"],
                    "output": s["key_output"],
                    "risk": s["risk_blocker"],
                    "next_plan": s["next_plan"]
                })

                appeared_unit_ids.add(unit_id)

        cluster["tasks"] = repaired_tasks

    missing_unit_ids = [
        uid for uid in skeleton_by_unit.keys()
        if uid not in appeared_unit_ids
    ]

    if missing_unit_ids:
        fallback_tasks = []

        for uid in missing_unit_ids:
            s = skeleton_by_unit[uid]

            fallback_tasks.append({
                "unit_id": s["unit_id"],
                "date": s["date"],
                "user_label": s["user_label"],
                "user_mention": s["user_mention"],
                "status": s["progress_status"],
                "output": s["key_output"],
                "risk": s["risk_blocker"],
                "next_plan": s["next_plan"]
            })

        clustered.setdefault("clusters", []).append({
            "cluster_id": "C_UNCLASSIFIED",
            "theme": "未归类事项",
            "type": "progress",
            "tasks": fallback_tasks
        })

        print(f"  [Planner Repair] 发现 {len(missing_unit_ids)} 条任务未进入聚类，已追加到未归类事项")

    return clustered


# =============================================================================
# 10. Step 2.4 - Writer Agent 生成周报正文
# =============================================================================
def write_weekly_from_clusters(clustered, project):
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

        return f"- **{c.get('theme', '')}**\n" + "\n".join(lines)

    progress_text = "\n".join(cluster_to_text(c) for c in progress_clusters) if progress_clusters else ""
    risk_text = "\n".join(cluster_to_text(c) for c in risk_clusters) if risk_clusters else ""
    plan_text = "\n".join(cluster_to_text(c) for c in plan_clusters) if plan_clusters else ""

    prompt_file_guid = project.get("weekly_writer_prompt_file_guid") or project.get("weekly_summary_prompt_file_guid")

    default_prompt = """请基于以下聚类结果，输出正式周报正文 Markdown。

# 本周核心进展
{progress_text}

# 困难风险及所需支持
{risk_text}

# Next Key Focus
{plan_text}

输出要求：
1. 只基于输入内容写，不允许编造。
2. 每个事项以成员 mention 或人名开头。
3. 必须保留具体对象、数量、算法名、case 名、问题名。
4. 不要写“推进相关工作”“完成多个事项”等空泛描述。
5. 不要输出日期范围、源日报链接等头部信息。
6. 输出格式必须是：

### 本周核心进展
- **项目/模块名**
  * [@姓名](mention:uid:id)：进展描述

### 困难风险及所需支持
- **项目/模块名**
  * [@姓名](mention:uid:id)：风险描述
若无风险，则输出：本周无阻塞性困难。

### Next Key Focus
- **项目/模块名**
  * [@姓名](mention:uid:id)：下一步计划
若无计划，则输出：按既定路线图推进中。

只输出 Markdown 正文，不要输出解释。
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
            "content": "你是专业的项目周报撰写助手。请只输出 Markdown 正文。",
        },
        {
            "role": "user",
            "content": user_content,
        }
    ]

    llm_name, llm_params = get_llm_config(project)

    llm_result = _call_llm_with_retry(
        llm_name=llm_name,
        llm_params=llm_params,
        context_messages=context_messages,
        max_retries=5
    )

    return strip_markdown_wrapper(llm_result).strip()


# =============================================================================
# 11. Step 3 - 摘要生成与最终 Markdown
# =============================================================================
def extract_progress_section(weekly_body_md):
    if not weekly_body_md:
        return ""

    pattern = r"###\s*本周核心进展\s*(.*?)(?=###\s*困难风险及所需支持|###\s*Next Key Focus|$)"
    match = re.search(pattern, weekly_body_md, flags=re.DOTALL)

    if match:
        return match.group(1).strip()

    return weekly_body_md


def generate_key_summary(progress_content, project):
    prompt_file_guid = project.get("weekly_key_summary_prompt_file_guid")

    default_prompt = """请基于以下“本周核心进展”内容，用一段 80~120 字的客观、平实文字总结本周整体进度。

要求：
1. 仅陈述输入中已有的里程碑和事实。
2. 不允许编造。
3. 不要写“整体进展顺利/符合预期/表现优异”等主观判断。
4. 若没有实质进展，写“本周暂无核心产出”。
5. 只输出一段话，不要标题，不要 Markdown。

输入内容：
{{progress_content}}
"""

    prompt_text = load_prompt_text(prompt_file_guid, default_prompt)
    user_content = prompt_text.replace("{{progress_content}}", progress_content)

    context_messages = [
        {
            "role": "system",
            "content": "你是周报摘要生成助手。只输出一段客观总结文字。",
        },
        {
            "role": "user",
            "content": user_content,
        }
    ]

    llm_name, llm_params = get_llm_config(project)

    llm_result = _call_llm_with_retry(
        llm_name=llm_name,
        llm_params=llm_params,
        context_messages=context_messages,
        max_retries=10
    )

    return strip_markdown_wrapper(llm_result)


def build_final_markdown(weekly_json, weekly_body_md, key_summary):
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
# 12. 文档创建与写入
# =============================================================================
def _request_with_retry(method, url, max_retries=3, **kwargs):
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


def create_final_weekly_note(content, project, week_info):
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
# 13. Step 5 - 卡片与消息发送
# =============================================================================
def generate_card_content(project, long_markdown, week_info=None):
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
        },
        {
            "role": "user",
            "content": user_content,
        }
    ]

    try:
        llm_name, llm_params = get_llm_config(project)

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
    elements = [
        {
            "tag": "markdown",
            "content": card_content,
            "margin": "0px",
            "text_size": "normal"
        }
    ]

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
# 14. LLM 调用封装：OpenAI-Compatible SDK
# =============================================================================
def _call_llm_with_retry(llm_name, llm_params, context_messages, max_retries=5):
    """
    使用 OpenAI SDK 兼容接口调用大模型。

    支持 stream=True，并将流式输出拼接为完整字符串返回。
    """
    attempt = 0
    last_error = None

    temperature = llm_params.get("temperature", 0.3)
    max_tokens = llm_params.get("max_tokens", 4096)

    while attempt < max_retries:
        try:
            print(f"  [尝试 {attempt + 1}/{max_retries}] 调用 LLM: {llm_name}")

            response = llm_client.chat.completions.create(
                model=llm_name,
                messages=context_messages,
                max_tokens=max_tokens,
                temperature=temperature,
                stream=True
            )

            output_parts = []

            for chunk in response:
                if not chunk.choices:
                    continue

                delta = chunk.choices[0].delta

                if delta.content is not None:
                    output_parts.append(delta.content)

            result = "".join(output_parts).strip()

            if not result:
                raise Exception("LLM 返回内容为空")

            return result

        except Exception as e:
            last_error = e
            attempt += 1

            if attempt < max_retries:
                wait_time = min(2 ** (attempt - 1), 30)
                print(f"  LLM 调用失败: {e}. {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"  LLM 连续 {max_retries} 次失败，放弃重试。错误: {e}")
                raise last_error


# =============================================================================
# 15. 主执行流程 Orchestrator
# =============================================================================
print("=" * 60)
print(f"开始执行周报 Workflow Agent v2.1 | 项目数: {len(projects)}")
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
        # Step 2.1: Indexer Agent 分块抽取
        # ---------------------------------------------------------------------
        print(f"[Step 2.1][{project_name}] Indexer Agent 开始分块提取任务骨架...")

        skeletons, chunks, dropped_skeletons = batch_extract_skeletons_by_chunks(users, project)

        print(f"[Step 2.1][{project_name}] Indexer 完成，有效 skeleton {len(skeletons)} 条")

        temp_files.append(
            build_intermediate_json_file(
                project_guid=project["project_guid"],
                target_date_str=f"{week_info['start_date']}_to_{week_info['end_date']}",
                json_content=chunks,
                suffix="chunks"
            )
        )

        temp_files.append(
            build_intermediate_json_file(
                project_guid=project["project_guid"],
                target_date_str=f"{week_info['start_date']}_to_{week_info['end_date']}",
                json_content=skeletons,
                suffix="skeletons"
            )
        )

        if dropped_skeletons:
            temp_files.append(
                build_intermediate_json_file(
                    project_guid=project["project_guid"],
                    target_date_str=f"{week_info['start_date']}_to_{week_info['end_date']}",
                    json_content=dropped_skeletons,
                    suffix="dropped_skeletons"
                )
            )

        if not skeletons:
            print(f"[Step 2.1][{project_name}] 未提取到有效 skeleton，将生成空周报")

        # ---------------------------------------------------------------------
        # Step 2.2: Planner Agent 聚类
        # ---------------------------------------------------------------------
        print(f"[Step 2.2][{project_name}] Planner Agent 开始语义聚类...")

        clustered = semantic_cluster_skeletons(skeletons, project)
        clustered = repair_clustered_with_skeletons(clustered, skeletons)

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
        # Step 2.3: Writer Agent 写周报正文
        # ---------------------------------------------------------------------
        print(f"[Step 2.3][{project_name}] Writer Agent 开始生成周报正文...")

        weekly_body_md = write_weekly_from_clusters(clustered, project)

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
        # Step 5: 源日报入口 + 消息发送
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