import sys
from types import ModuleType, SimpleNamespace

from gateway.hooks import HookRegistry
from gateway import run as gateway_run


def write_policy(profile, body):
    (profile / "channel_policy.toml").write_text(body, encoding="utf-8")


def source(platform="feishu", chat_id="grp", thread_id=None):
    return SimpleNamespace(platform=platform, chat_id=chat_id, thread_id=thread_id)


def test_operator_enforce_streaming_boundary_matches_channel_id_and_platform_alias(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'channel_id = "grp"',
                'platform = "lark"',
                "operator_enforce_enabled = true",
                "",
            ]
        ),
    )

    assert gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="feishu", chat_id="grp")
    )
    assert not gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="qihu360teams", chat_id="grp")
    )


def test_operator_enforce_streaming_boundary_matches_dict_channel_identity(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[channels.safety_gate]",
                'platform = "qihu-360teams"',
                'operator_enforce_enabled = "yes"',
                "",
            ]
        ),
    )

    assert gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="qihu360teams", chat_id="safety_gate")
    )
    assert not gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="qihu360teams", chat_id="other")
    )


def test_operator_enforce_streaming_boundary_matches_thread_id(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'thread_id = "thread-a"',
                "operator_enforce_enabled = 1",
                "",
            ]
        ),
    )

    assert gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="mattermost", chat_id="root", thread_id="thread-a")
    )
    assert not gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="mattermost", chat_id="root", thread_id="thread-b")
    )


def test_operator_enforce_streaming_boundary_ambiguous_armed_channel_buffers_platform(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'platform = "feishu"',
                "operator_enforce_enabled = true",
                "",
            ]
        ),
    )

    assert gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="feishu", chat_id="any")
    )
    assert not gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source(platform="qihu360teams", chat_id="any")
    )


def test_operator_enforce_streaming_boundary_missing_or_bad_policy_is_not_armed(tmp_path):
    assert not gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source()
    )

    write_policy(tmp_path, "not = [valid")
    assert not gateway_run._operator_enforce_streaming_boundary_source_armed(
        tmp_path, source()
    )


def test_armed_channel_disables_delta_and_interim_consumers_before_creation(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'channel_id = "grp"',
                'platform = "feishu"',
                "operator_enforce_enabled = true",
                "",
            ]
        ),
    )

    assert gateway_run._resolve_output_streaming_modes(
        tmp_path,
        source(platform="feishu", chat_id="grp"),
        streaming_enabled=True,
        interim_enabled=True,
    ) == (True, False, False)


def test_unarmed_channel_preserves_configured_streaming_modes(tmp_path):
    assert gateway_run._resolve_output_streaming_modes(
        tmp_path,
        source(platform="feishu", chat_id="grp"),
        streaming_enabled=True,
        interim_enabled=True,
    ) == (False, True, True)


def test_operator_enforce_outbound_boundary_rewrites_armed_complete_reply(tmp_path, monkeypatch):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'channel_id = "grp"',
                'platform = "feishu"',
                "operator_enforce_enabled = true",
                "",
            ]
        ),
    )
    fake_boundary = ModuleType("gateway.outbound_boundary")

    def build_outbound_context(**kwargs):
        return dict(kwargs)

    def outbound_before_send_sync(hooks, context):
        assert hooks == "hooks"
        assert context["source_kind"] == "streaming_final_reply"
        assert context["enforced_channel"] is True
        assert context["output_screening_required"] is True
        return SimpleNamespace(
            transmit=True,
            decision="rewrite",
            content="rewritten",
            reason="ok",
            raw={"decision": "rewrite"},
        )

    fake_boundary.build_outbound_context = build_outbound_context
    fake_boundary.outbound_before_send_sync = outbound_before_send_sync
    monkeypatch.setitem(sys.modules, "gateway.outbound_boundary", fake_boundary)

    allowed, content, context = gateway_run._operator_enforce_outbound_boundary_for_source(
        tmp_path,
        source(platform="feishu", chat_id="grp"),
        "raw",
        hooks="hooks",
        producer_id="queued_followup_first_response",
    )

    assert allowed is True
    assert content == "rewritten"
    assert context["before_send_decision"] == {"decision": "rewrite"}


def test_operator_enforce_streaming_uses_only_capable_screening_result(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'channel_id = "grp"',
                'platform = "feishu"',
                "operator_enforce_enabled = true",
                "",
            ]
        ),
    )

    def unrelated(_event_type, _context):
        return {"decision": "rewrite", "content": "unsafe raw output"}

    def screening(_event_type, _context):
        return {"decision": "rewrite", "content": "safe business result"}

    hooks = HookRegistry()
    hooks._handlers["outbound:before_send"] = [unrelated, screening]
    hooks._handler_owners[id(unrelated)] = "metrics"
    hooks._handler_owners[id(screening)] = "policy-screen"
    hooks._handler_capabilities[id(screening)] = frozenset({"output-screening"})

    allowed, content, _context = gateway_run._operator_enforce_outbound_boundary_for_source(
        tmp_path,
        source(platform="feishu", chat_id="grp"),
        "raw",
        hooks=hooks,
    )

    assert allowed is True
    assert content == "safe business result"


def test_operator_enforce_outbound_boundary_holds_armed_reply_without_allow(tmp_path):
    write_policy(
        tmp_path,
        "\n".join(
            [
                "[[channels]]",
                'channel_id = "grp"',
                'platform = "feishu"',
                "operator_enforce_enabled = true",
                "",
            ]
        ),
    )

    allowed, content, _context = gateway_run._operator_enforce_outbound_boundary_for_source(
        tmp_path,
        source(platform="feishu", chat_id="grp"),
        "raw",
        hooks=None,
    )

    assert allowed is False
    assert content == ""
