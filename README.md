# voltran

Hexagonal Modular Federation System for modular services and federated nodes.

## Features

- Ports and adapters style modules (inbound/outbound ports)
- Local discovery and federation-wide discovery
- Messaging via gRPC, NATS, or REST
- Cluster fusion and virtual nodes
- Built-in monitoring, logging, and authorization

## Requirements

- Python 3.11+

## Install

```bash
pip install voltran
```

For development:

```bash
pip install -e ".[dev]"
```

## Quickstart

```python
from voltran import Voltran, inbound_port, voltran_module
from voltran.sdk.module import BaseModule


@voltran_module(name="hello-service", version="1.0.0")
class HelloModule(BaseModule):
    @inbound_port(contract="hello.v1")
    async def hello(self, name: str = "World") -> dict:
        return {"message": f"Hello, {name}!"}


async def main() -> None:
    voltran = Voltran(name="hello-node", host="localhost", port=50051)
    voltran.register_module(HelloModule())
    await voltran.start()
    await voltran.run_forever()
```

## REST decorators

FastAPI is included as a core dependency so you can expose class methods as REST
endpoints with minimal boilerplate:

```python
from voltran import RestService, get, post
from fastapi import FastAPI


@RestService("/api/users")
class UserService:
    @get("/")
    async def list_users(self) -> list[dict]:
        return [{"id": 1, "name": "Ada"}]

    @post("/")
    async def create_user(self, name: str) -> dict:
        return {"id": 2, "name": name}


app = FastAPI()
UserService().mount(app)
```

## Federation

```python
# Leader
await voltran.create_federation("my-federation")

# Member
await voltran.join_federation(leader_id)
```

## CLI

```bash
voltran init my-project
voltran start --name my-node --host 0.0.0.0 --port 50051
```

## Tests

```bash
pytest
```
