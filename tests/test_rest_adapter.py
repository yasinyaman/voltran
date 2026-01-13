import json

import pytest

from voltran.core.adapters.rest_adapter import RestMessagingAdapter
from voltran.core.ports.outbound.messaging import Message


class _DummyRequest:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    async def json(self) -> dict:
        return self._payload


@pytest.mark.asyncio
async def test_rest_request_returns_handler_result() -> None:
    adapter = RestMessagingAdapter()

    async def handler(message: Message) -> dict:
        return {"result": "ok"}

    await adapter.subscribe("voltran.test.request", handler)

    message = Message(
        topic="voltran.test.request",
        payload={"action": "custom"},
        source_voltran_id="peer",
    )
    request = _DummyRequest(adapter._serialize_message(message))

    response = await adapter._handle_request(request)
    payload = json.loads(response.text)

    assert payload["payload"] == {"result": "ok"}


@pytest.mark.asyncio
async def test_rest_request_uses_respond_payload() -> None:
    adapter = RestMessagingAdapter()

    async def handler(message: Message) -> None:
        await adapter.respond(message, {"result": "responded"})

    await adapter.subscribe("voltran.test.respond", handler)

    message = Message(
        topic="voltran.test.respond",
        payload={"action": "custom"},
        source_voltran_id="peer",
    )
    request = _DummyRequest(adapter._serialize_message(message))

    response = await adapter._handle_request(request)
    payload = json.loads(response.text)

    assert payload["payload"] == {"result": "responded"}
