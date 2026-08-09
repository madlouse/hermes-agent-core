import inspect

import pytest

from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.signal import SignalAdapter
from plugins.platforms.discord.adapter import DiscordAdapter
from plugins.platforms.email.adapter import EmailAdapter
from plugins.platforms.matrix.adapter import MatrixAdapter
from plugins.platforms.mattermost.adapter import MattermostAdapter
from plugins.platforms.slack.adapter import SlackAdapter
from plugins.platforms.telegram.adapter import TelegramAdapter


MULTI_IMAGE_OVERRIDES = [
    SignalAdapter,
    SlackAdapter,
    DiscordAdapter,
    TelegramAdapter,
    MatrixAdapter,
    MattermostAdapter,
    EmailAdapter,
]


@pytest.mark.parametrize("adapter_type", MULTI_IMAGE_OVERRIDES)
def test_multi_image_override_without_receipt_is_normalized_fail_closed(adapter_type):
    assert inspect.getattr_static(
        adapter_type, "send_multiple_images"
    ) is not inspect.getattr_static(BasePlatformAdapter, "send_multiple_images")
    assert inspect.getattr_static(
        adapter_type, "_process_message_background"
    ) is inspect.getattr_static(BasePlatformAdapter, "_process_message_background")

    result = BasePlatformAdapter.normalize_delivery_result(
        None,
        operation=f"{adapter_type.__name__}.send_multiple_images",
    )

    assert result == SendResult(
        success=False,
        error=(
            f"{adapter_type.__name__}.send_multiple_images returned no "
            "SendResult delivery evidence"
        ),
    )
