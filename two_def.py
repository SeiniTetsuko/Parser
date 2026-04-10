def _extract_project_name(self, text):
    """
    只识别带 emoji 前缀的项目标题：
    - 📌【Project A】
    - 📌[Project A]

    只有这种格式才会被识别为项目名。
    """
    text = self._normalize_text(text)
    m = re.match(r"^📌\s*[\[\【](.*?)[\]\】]\s*$", text)
    if m:
        return m.group(1).strip()
    return None


def _normalize_section_name(self, text):
    """
    统一映射 section：
    - 📍今日主要进展      -> progress
    - ⚠️困难及所需支援    -> issue_help
    - 📝下一步计划        -> next_focus
    - 📝Next Key Focus    -> next_focus
    """
    text = self._normalize_text(text)
    text_no_colon = text.replace("：", "").replace(":", "").strip()

    if text_no_colon == "📍今日主要进展":
        return "progress"

    if text_no_colon == "⚠️困难及所需支援":
        return "issue_help"

    if text_no_colon == "📝下一步计划":
        return "next_focus"

    if text_no_colon == "📝Next Key Focus":
        return "next_focus"

    return None
    
    
    def build_step3_note_header_line(step1_meta):
    """
    构造 Step3 笔记正文开头的一行元信息：
    **日期**：2026-04-09 ｜ **部门负责人**：mention ｜ **原笔记链接**：link1；link2
    """
    target_date_str = step1_meta.get("target_date_str", "")
    pm_person_info = step1_meta.get("pm_person_info")
    note_entries = step1_meta.get("note_entries", [])

    pm_markdown = build_mention_markdown(pm_person_info, fallback_text="部门负责人未识别")

    note_links = []
    seen = set()
    for note_entry in note_entries:
        note_guid = note_entry.get("note_guid")
        if note_guid and note_guid not in seen:
            seen.add(note_guid)
            note_links.append(build_note_link_markdown(note_guid, BASE_URL))

    note_links_text = "；".join(note_links) if note_links else "无"

    return (
        f"**日期**：{target_date_str} ｜ "
        f"**部门负责人**：{pm_markdown} ｜ "
        f"**原笔记链接**：{note_links_text}"
    )


def prepend_step3_note_header(ai_contents, step1_meta):
    """
    给 Step2 输出的长总结统一加上 Step3 的头部行
    """
    header_line = build_step3_note_header_line(step1_meta)
    wrapped_contents = []

    for content in ai_contents:
        wrapped_contents.append(f"{header_line}\n\n{content}")

    return wrapped_contents