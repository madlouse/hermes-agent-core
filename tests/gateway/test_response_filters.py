from gateway.response_filters import (
    extract_explicit_final_response,
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


def test_exact_silence_tokens_are_intentional_silence():
    for token in ("[SILENT]", " SILENT ", "NO_REPLY", "no reply"):
        assert is_intentional_silence_response(token)


def test_edge_punctuation_silence_tokens_are_intentional_silence():
    for token in (".NO_REPLY", "*NO_REPLY*", " .NO_REPLY ", "*[SILENT]*", "NO_REPLY."):
        assert is_intentional_silence_response(token)


def test_blank_and_prose_mentions_are_not_silence():
    assert not is_intentional_silence_response("")
    assert not is_intentional_silence_response("Use NO_REPLY when no answer is needed.")
    assert not is_intentional_silence_response("The reply was [SILENT], intentionally.")
    assert not is_intentional_silence_response("😄 NO_REPLY")
    assert not is_intentional_silence_response("[SILENT")


def test_failed_agent_result_never_counts_as_intentional_silence():
    assert is_intentional_silence_agent_result({"failed": False}, "NO_REPLY")
    assert not is_intentional_silence_agent_result({"failed": True}, "NO_REPLY")
