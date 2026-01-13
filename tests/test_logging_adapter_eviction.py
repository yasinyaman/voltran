import pytest

from voltran.core.adapters.logging_adapter import InMemoryLoggingAdapter
from voltran.core.ports.outbound.logging import LogLevel


@pytest.mark.asyncio
async def test_logging_adapter_evicts_oldest_entries() -> None:
    adapter = InMemoryLoggingAdapter(max_entries=2, min_level=LogLevel.INFO)

    await adapter.info("first")
    await adapter.info("second")
    await adapter.info("third")

    assert len(adapter._logs) == 2
    assert len(adapter._logs_by_id) == 2
    assert adapter._counts_by_level[LogLevel.INFO.value] == 2

    ids = {entry.id for entry in adapter._logs}
    assert ids == set(adapter._logs_by_id.keys())
