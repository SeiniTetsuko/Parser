import json
import sys
from collections import OrderedDict


def mention_to_text(person_info):
    """
    把 person_info 转成 markdown mention 结构：
    [@姓名](mention:uid:user_id)
    """
    if not person_info:
        return "@未知成员"

    label = person_info.get("label") or "未知成员"
    uid = person_info.get("uid", "")
    user_id = person_info.get("id", "")

    if uid and user_id:
        return f"[@{label}](mention:{uid}:{user_id})"

    return f"@{label}"


def normalize_item_text(text):
    return (text or "").strip()


def aggregate_parsed_json(parsed_json):
    aggregated = {
        "progress": OrderedDict(),
        "issue_help": OrderedDict(),
        "next_focus": OrderedDict()
    }

    members = parsed_json.get("members", [])

    for member in members:
        person_info = member.get("person_info", {})
        member_name = mention_to_text(person_info)

        for project in member.get("projects", []):
            project_name = (project.get("project_name") or "未分类项目").strip()
            sections = project.get("sections", {})

            for section_key in ("progress", "issue_help", "next_focus"):
                items = sections.get(section_key, [])

                if project_name not in aggregated[section_key]:
                    aggregated[section_key][project_name] = []

                for item in items:
                    item_text = normalize_item_text(item.get("text", ""))
                    if not item_text:
                        continue

                    aggregated[section_key][project_name].append({
                        "member": member_name,
                        "text": item_text,
                        "type": item.get("type", "paragraph")
                    })

    return aggregated


def render_section_markdown(title, project_map):
    """
    同一个 project 下，按 member 合并输出：
    - @成员：
      - 事项1
      - 事项2
    """
    lines = [f"# {title}", ""]

    has_any_content = False

    for project_name, items in project_map.items():
        if not items:
            continue

        has_any_content = True
        lines.append(f"## {project_name}")

        # 先按 member 分组，保持原顺序
        member_grouped = OrderedDict()

        for item in items:
            member = item.get("member", "@未知成员")
            text = item.get("text", "").strip()
            block_type = item.get("type", "paragraph")

            if not text:
                continue

            if member not in member_grouped:
                member_grouped[member] = []

            member_grouped[member].append({
                "text": text,
                "type": block_type
            })

        # 再按 member 输出
        for member, member_items in member_grouped.items():
            lines.append(f"- {member}：")

            for sub_item in member_items:
                sub_text = sub_item["text"]
                sub_type = sub_item["type"]

                if sub_type == "code":
                    lines.append("  - 代码块：")
                    lines.append("```")
                    lines.append(sub_text)
                    lines.append("```")
                elif sub_type == "table":
                    lines.append("  - [表格内容]")
                else:
                    sub_text_single_line = sub_text.replace("\n", " / ").strip()
                    lines.append(f"  - {sub_text_single_line}")

            lines.append("")

        lines.append("")

    if not has_any_content:
        lines.append("- 暂无")
        lines.append("")

    return "\n".join(lines).rstrip()


def build_llm_input_markdown(parsed_json):
    meta = parsed_json.get("meta", {})
    project_name = meta.get("project_name", "Unknown Project")
    date_str = meta.get("date", "")
    week_str = meta.get("week", "")

    aggregated = aggregate_parsed_json(parsed_json)

    parts = []
    parts.append(f"# {project_name} 日报汇总")

    if date_str or week_str:
        meta_line_parts = []
        if date_str:
            meta_line_parts.append(f"日期：{date_str}")
        if week_str:
            meta_line_parts.append(f"周次：{week_str}")
        parts.append("")
        parts.append(" | ".join(meta_line_parts))

    parts.append("")
    parts.append("---")
    parts.append("")

    parts.append(render_section_markdown("今日核心进展", aggregated["progress"]))
    parts.append("")
    parts.append(render_section_markdown("困难及所需协助", aggregated["issue_help"]))
    parts.append("")
    parts.append(render_section_markdown("下一步计划", aggregated["next_focus"]))
    parts.append("")

    return "\n".join(parts).strip() + "\n"


def main():
    """
    用法：
    python parsed_to_md.py parsed_daily.json llm_input.md
    """
    if len(sys.argv) < 3:
        print("用法: python parsed_to_md.py <parsed_json_path> <output_md_path>")
        sys.exit(1)

    parsed_json_path = sys.argv[1]
    output_md_path = sys.argv[2]

    with open(parsed_json_path, "r", encoding="utf-8") as f:
        parsed_json = json.load(f)

    markdown_content = build_llm_input_markdown(parsed_json)

    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"✅ Markdown 已生成: {output_md_path}")


if __name__ == "__main__":
    main()