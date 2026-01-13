import pytest

from voltran.sdk.decorators import require_permission, require_role


class _Context:
    def __init__(self, authorization=None) -> None:
        self.authorization = authorization


class _Module:
    def __init__(self) -> None:
        self._voltran_context = _Context(authorization=None)

    @require_permission("module:test:read")
    async def read(self, subject_id: str = "user-1") -> str:
        return "ok"

    @require_role("admin")
    async def admin(self, subject_id: str = "user-1") -> str:
        return "ok"


@pytest.mark.asyncio
async def test_require_permission_allows_when_auth_disabled() -> None:
    module = _Module()
    assert await module.read() == "ok"


@pytest.mark.asyncio
async def test_require_role_allows_when_auth_disabled() -> None:
    module = _Module()
    assert await module.admin() == "ok"
