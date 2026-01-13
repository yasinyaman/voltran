import json

import pytest

from voltran.core.adapters.nats_adapter import NatsMessagingAdapter
from voltran.core.ports.outbound.messaging import Message


class _FakeMsg:
    def __init__(self, data: bytes, reply: str | None = None) -> None:
        self.data = data
        self.reply = reply


class _FakeNatsClient:
    def __init__(self) -> None:
        self.subscriptions: list[tuple[str, object]] = []
        self.published: list[tuple[str, bytes]] = []

    async def subscribe(self, topic: str, cb: object) -> object:
        self.subscriptions.append((topic, cb))
        return object()

    async def publish(self, subject: str, data: bytes) -> None:
        self.published.append((subject, data))


@pytest.mark.asyncio
async def test_nats_subscribe_sets_reply_to_from_msg_reply() -> None:
    adapter = NatsMessagingAdapter()
    fake_nc = _FakeNatsClient()
    adapter._nc = fake_nc
    adapter._connected = True

    captured: dict[str, str] = {}

    async def handler(message: Message) -> None:
        captured["reply_to"] = message.reply_to or ""

    await adapter.subscribe("voltran.test.request", handler)

    topic, cb = fake_nc.subscriptions[0]
    assert topic == "voltran.test.request"

    msg = Message(
        topic="voltran.test.request",
        payload={"action": "ping"},
        source_voltran_id="peer",
    )
    fake_msg = _FakeMsg(adapter._serialize(msg), reply="INBOX.123")

    await cb(fake_msg)

    assert captured["reply_to"] == "INBOX.123"


@pytest.mark.asyncio
async def test_nats_respond_uses_reply_to_subject() -> None:
    adapter = NatsMessagingAdapter()
    fake_nc = _FakeNatsClient()
    adapter._nc = fake_nc
    adapter._connected = True

    original = Message(
        topic="voltran.test.request",
        payload={"action": "ping"},
        source_voltran_id="peer",
        reply_to="INBOX.999",
        correlation_id="corr-123",
    )

    await adapter.respond(original, {"ok": True})

    assert fake_nc.published
    subject, data = fake_nc.published[0]
    assert subject == "INBOX.999"
    decoded = json.loads(data.decode("utf-8"))
    assert decoded["payload"]["ok"] is True
    assert decoded["payload"]["correlation_id"] == "corr-123"
