import pytest

from voltran import HealthStatus, Voltran, voltran_module
from voltran.sdk.module import BaseModule


@voltran_module(name="health-module", version="1.0.0")
class _HealthModule(BaseModule):
    async def health_check(self) -> dict:
        return {"status": "ok"}


@pytest.mark.asyncio
async def test_health_check_falls_back_to_instance_health() -> None:
    voltran = Voltran(
        enable_monitoring=False,
        enable_logging=False,
        enable_authorization=False,
        use_nats=False,
        use_rest=False,
    )

    voltran.register_module(_HealthModule())
    result = await voltran.health_check()

    assert result["modules"][0]["health"] == HealthStatus.UNKNOWN.value
