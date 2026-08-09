import contextvars
from types import SimpleNamespace
from unittest.mock import patch

from cron.scheduler import _CronRunControl
from gateway.session_context import (
    get_cron_runtime_context,
    reset_cron_authorization,
    set_cron_authorization,
)


def test_timeout_revokes_authority_in_already_copied_provider_context():
    control = _CronRunControl({"id": "proof-0039", "run_timeout_seconds": 1})
    tokens = set_cron_authorization(
        {
            "HERMES_CRON_JOB_ID": "proof-0039",
            "HERMES_CRON_AUTHORIZED_BEHAVIOR_REF": "behavior.proof-0039",
        },
        lease=control.lease,
    )
    try:
        copied = contextvars.copy_context()
        assert copied.run(get_cron_runtime_context).job_id == "proof-0039"
        control.interrupt("deadline")
        assert copied.run(get_cron_runtime_context) is None
    finally:
        reset_cron_authorization(tokens)


def test_timeout_waits_for_the_single_owned_script_cleanup_fence():
    process = SimpleNamespace()
    control = _CronRunControl({"id": "proof-0039"})
    assert control.begin_script_spawn() is True
    cleanup = control.attach_script_process(process)

    with patch("cron.scheduler._kill_cron_process_group") as kill_group:
        control.interrupt("deadline")

    kill_group.assert_called_once_with(process)
    assert cleanup.done.is_set() is True
    assert control._script_process is None
    assert control._script_cleanup is None
