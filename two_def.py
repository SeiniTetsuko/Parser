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