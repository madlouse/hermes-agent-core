from gateway.response_filters import (
    extract_explicit_final_response,
    is_autonomous_silence_response,
    is_intentional_silence_agent_result,
    is_intentional_silence_response,
)


def test_explicit_final_response_ignores_fenced_examples():
    report = (
        "Report includes this documentation example:\n"
        "```markdown\n## Response\n[SILENT]\n## End Response\n```\n"
        "Three real changes were found."
    )
    assert extract_explicit_final_response(report) == report


def test_explicit_final_response_selects_closed_top_level_frame():
    response = (
        "```markdown\n## Response\nnot trusted\n## End Response\n```\n"
        "## Response\n[SILENT]\n## End Response"
    )
    assert extract_explicit_final_response(response) == "[SILENT]"


def test_explicit_final_response_rejects_multiple_top_level_frames():
    response = (
        "## Response\nfirst\n## End Response\n"
        "## Response\n[SILENT]\n## End Response"
    )

    assert extract_explicit_final_response(response) == response


def test_exact_silence_tokens_are_intentional_silence():
    for token in ("[SILENT]", " SILENT ", "NO_REPLY", "no reply"):
        assert is_intentional_silence_response(token)


def test_autonomous_silence_accepts_marker_with_own_line_note():
    """The loose rule for cron/webhook lanes: marker + explanation suppresses."""
    assert is_autonomous_silence_response("[SILENT]")
    assert is_autonomous_silence_response("[SILENT]\n\nNothing new this tick.")
    assert is_autonomous_silence_response("2 deals filtered\n\n[SILENT]")
    assert is_autonomous_silence_response("no_reply\nduplicate inbound, already handled")
    assert is_autonomous_silence_response("[SILENT] No changes detected")
