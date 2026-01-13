import json

import pytest

from voltran.core.adapters.rest_adapter import RestMessagingAdapter
from voltran.core.domain.models import Cluster, HealthStatus, ModuleDescriptor
from voltran.core.ports.outbound.messaging import Message


class _DummyRequest:
    def __init__(self, payload: dict | None = None, match_info: dict | None = None) -> None:
        self._payload = payload or {}
        self.match_info = match_info or {}

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
        correlation_id="corr-1",
    )
    request = _DummyRequest(adapter._serialize_message(message))

    response = await adapter._handle_request(request)
    payload = json.loads(response.text)

    assert payload["payload"]["result"] == "ok"
    assert payload["payload"]["correlation_id"] == "corr-1"


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
        correlation_id="corr-2",
    )
    request = _DummyRequest(adapter._serialize_message(message))

    response = await adapter._handle_request(request)
    payload = json.loads(response.text)

    assert payload["payload"]["result"] == "responded"
    assert payload["payload"]["correlation_id"] == "corr-2"


@pytest.mark.asyncio
async def test_rest_request_handler_exception_returns_500() -> None:
    adapter = RestMessagingAdapter()

    async def handler(message: Message) -> dict:
        raise RuntimeError("boom")

    await adapter.subscribe("voltran.test.error", handler)

    message = Message(
        topic="voltran.test.error",
        payload={"action": "custom"},
        source_voltran_id="peer",
        correlation_id="corr-3",
    )
    request = _DummyRequest(adapter._serialize_message(message))

    response = await adapter._handle_request(request)
    payload = json.loads(response.text)

    assert response.status == 500
    assert payload["success"] is False
    assert payload["correlation_id"] == "corr-3"


class _FakeRegistry:
    def __init__(self, modules: list[ModuleDescriptor]) -> None:
        self._modules = {m.id: m for m in modules}

    async def get_all(self) -> list[ModuleDescriptor]:
        return list(self._modules.values())

    async def get(self, module_id: str) -> ModuleDescriptor | None:
        return self._modules.get(module_id)


class _FakeClusterPort:
    def __init__(self) -> None:
        self._clusters: dict[str, Cluster] = {}

    async def create_cluster(self, name: str) -> Cluster:
        cluster = Cluster(name=name)
        self._clusters[cluster.id] = cluster
        return cluster

    async def fuse_cluster(self, cluster_id: str, virtual_name: str) -> ModuleDescriptor:
        if cluster_id not in self._clusters:
            raise ValueError("Cluster not found")
        return ModuleDescriptor(
            name=virtual_name,
            health=HealthStatus.HEALTHY,
            metadata={"is_virtual": True},
        )


@pytest.mark.asyncio
async def test_rest_modules_endpoint_uses_registry() -> None:
    adapter = RestMessagingAdapter()
    adapter.set_registry(
        _FakeRegistry(
            [
                ModuleDescriptor(
                    id="m-1",
                    name="module-one",
                    version="1.0.0",
                    health=HealthStatus.HEALTHY,
                )
            ]
        )
    )

    response = await adapter._handle_list_modules(_DummyRequest())
    payload = json.loads(response.text)

    assert payload["modules"][0]["id"] == "m-1"
    assert payload["modules"][0]["health"] == HealthStatus.HEALTHY.value


@pytest.mark.asyncio
async def test_rest_module_detail_not_found() -> None:
    adapter = RestMessagingAdapter()
    adapter.set_registry(_FakeRegistry([]))

    response = await adapter._handle_get_module(_DummyRequest(match_info={"module_id": "x"}))
    payload = json.loads(response.text)

    assert response.status == 404
    assert payload["success"] is False


@pytest.mark.asyncio
async def test_rest_cluster_create_and_fuse() -> None:
    adapter = RestMessagingAdapter()
    adapter.set_cluster_port(_FakeClusterPort())

    create_response = await adapter._handle_create_cluster(
        _DummyRequest(payload={"name": "cluster-a"})
    )
    create_payload = json.loads(create_response.text)

    assert create_response.status == 200
    cluster_id = create_payload["id"]

    fuse_response = await adapter._handle_fuse_cluster(
        _DummyRequest(payload={"name": "virtual-a"}, match_info={"cluster_id": cluster_id})
    )
    fuse_payload = json.loads(fuse_response.text)

    assert fuse_response.status == 200
    assert fuse_payload["name"] == "virtual-a"
