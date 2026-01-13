import asyncio

import pytest

from voltran.core.adapters.grpc_adapter import GrpcMessagingAdapter


@pytest.mark.asyncio
async def test_grpc_topic_filtering() -> None:
    adapter = GrpcMessagingAdapter()
    await adapter.connect()

    event_a = asyncio.Event()
    event_b = asyncio.Event()

    async def handler_a(message) -> None:
        event_a.set()

    async def handler_b(message) -> None:
        event_b.set()

    await adapter.subscribe("topic.a", handler_a)
    await adapter.subscribe("topic.b", handler_b)

    await adapter.publish("topic.a", {"value": 1})
    await asyncio.wait_for(event_a.wait(), timeout=1)
    await asyncio.sleep(0)

    assert event_a.is_set()
    assert not event_b.is_set()

    await adapter.disconnect()


@pytest.mark.asyncio
async def test_grpc_wildcard_subscription() -> None:
    adapter = GrpcMessagingAdapter()
    await adapter.connect()

    event = asyncio.Event()

    async def handler(message) -> None:
        event.set()

    await adapter.subscribe("foo.*", handler)
    await adapter.publish("foo.bar", {"value": 1})
    await asyncio.wait_for(event.wait(), timeout=1)

    assert event.is_set()

    await adapter.disconnect()
