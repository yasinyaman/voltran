"""Voltran CLI - command line interface for managing Voltran nodes."""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax

console = Console()


# === Project Templates ===

PYPROJECT_TEMPLATE = '''[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "{project_name}"
version = "0.1.0"
description = "{description}"
requires-python = ">=3.11"

dependencies = [
    "voltran>=0.1.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
]
'''

MAIN_MODULE_TEMPLATE = '''"""
{project_name} - Voltran Module

Ana modul dosyasi.
"""

from voltran import voltran_module, inbound_port, outbound_port
from voltran.sdk.module import BaseModule


@voltran_module(name="{module_name}", version="1.0.0")
class {class_name}(BaseModule):
    """
    {description}
    
    Bu modul hexagonal mimari prensiplerini takip eder.
    Inbound port'lar disaridan gelen istekleri karsilar,
    Outbound port'lar dis servislere baglanir.
    """

    async def start(self) -> None:
        """Modul baslatildiginda calisir."""
        await super().start()
        print(f"{{self.__class__.__name__}} basladi!")

    async def stop(self) -> None:
        """Modul durduruldugunda calisir."""
        await super().stop()
        print(f"{{self.__class__.__name__}} durdu!")

    @inbound_port(contract="{module_name}.hello.v1")
    async def hello(self, name: str = "World") -> dict:
        """
        Ornek inbound port - disaridan cagrilabilir.
        
        Args:
            name: Selamlanacak isim
            
        Returns:
            Selamlama mesaji
        """
        return {{
            "message": f"Hello, {{name}}!",
            "from": "{module_name}",
        }}

    @inbound_port(contract="{module_name}.health.v1")
    async def health_check(self) -> dict:
        """Saglik kontrolu endpoint'i."""
        return {{
            "status": "healthy",
            "module": "{module_name}",
        }}

    # Ornek outbound port (baska modulleri cagirmak icin)
    # @outbound_port(contract="other.service.v1")
    # async def call_other_service(self, data: dict) -> dict:
    #     """Baska bir servisi cagir."""
    #     pass
'''

SERVER_TEMPLATE = '''"""
{project_name} - Voltran Server

Uygulamayi baslatmak icin bu dosyayi calistirin.
"""

import asyncio
from voltran import Voltran

from {package_name}.modules.main import {class_name}


async def main() -> None:
    """Ana uygulama fonksiyonu."""
    
    # Voltran node olustur
    voltran = Voltran(
        name="{project_name}",
        host="localhost",
        port=50051,
        # REST API icin:
        # use_rest=True,
        # rest_port=8080,
    )
    
    # Modulleri kaydet
    voltran.register_module({class_name}())
    
    # Baslat
    await voltran.start()
    
    print(f"Voltran baslatildi: {{voltran.name}}")
    print(f"Node ID: {{voltran.id}}")
    print(f"Moduller: {{await voltran.list_modules()}}")
    
    # Sonsuza kadar calis
    await voltran.run_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\\nKapatiliyor...")
'''

README_TEMPLATE = '''# {project_name}

{description}

## Kurulum

```bash
pip install -e ".[dev]"
```

## Calistirma

```bash
# Sunucuyu baslat
python -m {package_name}.server

# VEYA
python {package_name}/server.py
```

## Proje Yapisi

```
{package_name}/
├── modules/
│   ├── __init__.py
│   └── main.py          # Ana modul
├── server.py             # Sunucu baslangic noktasi
└── __init__.py
```

## Modul Ekleme

Yeni modul eklemek icin `modules/` klasorune yeni bir dosya ekleyin:

```python
from voltran import voltran_module, inbound_port
from voltran.sdk.module import BaseModule


@voltran_module(name="my-new-module", version="1.0.0")
class MyNewModule(BaseModule):
    
    @inbound_port(contract="my.contract.v1")
    async def my_method(self, data: dict) -> dict:
        return {{"status": "ok"}}
```

Sonra `server.py`'de kaydedin:

```python
from {package_name}.modules.my_module import MyNewModule

voltran.register_module(MyNewModule())
```

## Federation

Birden fazla Voltran'i birlestirmek icin:

```python
# Lider node
await voltran.create_federation("my-federation")

# Uye node (baska makinede)
await voltran.join_federation(leader_id)
```

## Lisans

MIT
'''

INIT_TEMPLATE = '''"""
{project_name} - Voltran Projesi
"""

__version__ = "0.1.0"
'''

MODULES_INIT_TEMPLATE = '''"""Voltran modulleri."""

from {package_name}.modules.main import {class_name}

__all__ = ["{class_name}"]
'''


@click.group()
@click.version_option(version="0.1.0", prog_name="Voltran")
def main() -> None:
    """Voltran - Hexagonal Modular Federation System."""
    pass


@main.command()
@click.argument("project_name")
@click.option("--description", "-d", default="Voltran projesi", help="Proje aciklamasi")
@click.option("--template", "-t", type=click.Choice(["basic", "rest", "federation"]), default="basic", help="Proje sablonu")
@click.option("--output", "-o", default=".", help="Cikti dizini")
def init(project_name: str, description: str, template: str, output: str) -> None:
    """Yeni bir Voltran projesi olustur.
    
    Ornek:
        voltran init my-project
        voltran init my-api --template rest
        voltran init my-cluster --template federation
    """
    # Proje adini normalize et
    package_name = project_name.lower().replace("-", "_").replace(" ", "_")
    class_name = "".join(word.capitalize() for word in project_name.replace("-", " ").replace("_", " ").split()) + "Module"
    module_name = project_name.lower().replace("_", "-").replace(" ", "-")
    
    # Cikti dizini
    output_path = Path(output)
    project_path = output_path / project_name
    
    # Dizin kontrolu
    if project_path.exists():
        console.print(f"[red]Hata:[/red] '{project_path}' dizini zaten mevcut!")
        return
    
    console.print(Panel.fit(
        f"[bold green]Voltran Projesi Olusturuluyor[/bold green]\n\n"
        f"Proje: [cyan]{project_name}[/cyan]\n"
        f"Paket: [cyan]{package_name}[/cyan]\n"
        f"Sablon: [cyan]{template}[/cyan]\n"
        f"Dizin: [cyan]{project_path}[/cyan]",
        title="Voltran Init"
    ))
    
    # Dizinleri olustur
    (project_path / package_name / "modules").mkdir(parents=True, exist_ok=True)
    
    # pyproject.toml
    pyproject_content = PYPROJECT_TEMPLATE.format(
        project_name=project_name,
        description=description,
    )
    (project_path / "pyproject.toml").write_text(pyproject_content)
    console.print("  [green]✓[/green] pyproject.toml")
    
    # README.md
    readme_content = README_TEMPLATE.format(
        project_name=project_name,
        package_name=package_name,
        description=description,
    )
    (project_path / "README.md").write_text(readme_content)
    console.print("  [green]✓[/green] README.md")
    
    # __init__.py
    init_content = INIT_TEMPLATE.format(project_name=project_name)
    (project_path / package_name / "__init__.py").write_text(init_content)
    console.print(f"  [green]✓[/green] {package_name}/__init__.py")
    
    # modules/__init__.py
    modules_init_content = MODULES_INIT_TEMPLATE.format(
        package_name=package_name,
        class_name=class_name,
    )
    (project_path / package_name / "modules" / "__init__.py").write_text(modules_init_content)
    console.print(f"  [green]✓[/green] {package_name}/modules/__init__.py")
    
    # modules/main.py
    main_module_content = MAIN_MODULE_TEMPLATE.format(
        project_name=project_name,
        module_name=module_name,
        class_name=class_name,
        description=description,
    )
    (project_path / package_name / "modules" / "main.py").write_text(main_module_content)
    console.print(f"  [green]✓[/green] {package_name}/modules/main.py")
    
    # server.py
    server_content = SERVER_TEMPLATE.format(
        project_name=project_name,
        package_name=package_name,
        class_name=class_name,
    )
    (project_path / package_name / "server.py").write_text(server_content)
    console.print(f"  [green]✓[/green] {package_name}/server.py")
    
    # Template'e gore ek dosyalar
    if template == "rest":
        _create_rest_template(project_path, package_name, class_name, module_name)
    elif template == "federation":
        _create_federation_template(project_path, package_name, class_name, module_name)
    
    # Sonuc
    console.print("\n[bold green]Proje basariyla olusturuldu![/bold green]\n")
    console.print("Baslamak icin:")
    console.print(f"  [cyan]cd {project_name}[/cyan]")
    console.print(f"  [cyan]pip install -e .[/cyan]")
    console.print(f"  [cyan]python {package_name}/server.py[/cyan]")


def _create_rest_template(project_path: Path, package_name: str, class_name: str, module_name: str) -> None:
    """REST API sablonu icin ek dosyalar."""
    rest_server = f'''"""
REST API Server

REST API ile Voltran calistirmak icin.
"""

import asyncio
from voltran import Voltran

from {package_name}.modules.main import {class_name}


async def main() -> None:
    """REST API sunucusu."""
    
    voltran = Voltran(
        name="{package_name}-rest",
        host="0.0.0.0",
        use_rest=True,
        rest_port=8080,
    )
    
    voltran.register_module({class_name}())
    
    await voltran.start()
    
    print("REST API baslatildi: http://localhost:8080/api/v1")
    print("Endpoints:")
    print("  GET  /api/v1/health")
    print("  GET  /api/v1/info")
    print("  GET  /api/v1/modules")
    
    await voltran.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
'''
    (project_path / package_name / "rest_server.py").write_text(rest_server)
    console.print(f"  [green]✓[/green] {package_name}/rest_server.py")


def _create_federation_template(project_path: Path, package_name: str, class_name: str, module_name: str) -> None:
    """Federation sablonu icin ek dosyalar."""
    federation_server = f'''"""
Federation Server

Birden fazla Voltran'i birlestirmek icin.
"""

import asyncio
import sys
from voltran import Voltran

from {package_name}.modules.main import {class_name}


async def run_leader(nats_url: str) -> None:
    """Federation lideri olarak calis."""
    voltran = Voltran(
        name="{package_name}-leader",
        host="0.0.0.0",
        port=50051,
        nats_servers=[nats_url],
        use_nats=True,
    )
    
    voltran.register_module({class_name}())
    await voltran.start()
    await voltran.create_federation("{package_name}-federation")
    
    print(f"Federation olusturuldu!")
    print(f"Leader ID: {{voltran.id}}")
    print(f"Katilmak icin: python federation_server.py join {{voltran.id}}")
    
    await voltran.run_forever()


async def run_member(leader_id: str, nats_url: str) -> None:
    """Federation uyesi olarak calis."""
    voltran = Voltran(
        name="{package_name}-member",
        host="0.0.0.0",
        port=50052,
        nats_servers=[nats_url],
        use_nats=True,
    )
    
    voltran.register_module({class_name}())
    await voltran.start()
    
    if await voltran.join_federation(leader_id):
        print("Federation'a katilindi!")
        await voltran.run_forever()
    else:
        print("Katilim basarisiz!")


def main() -> None:
    nats_url = "nats://localhost:4222"
    
    if len(sys.argv) < 2:
        print("Kullanim:")
        print("  python federation_server.py leader")
        print("  python federation_server.py join <leader_id>")
        return
    
    cmd = sys.argv[1]
    
    if cmd == "leader":
        asyncio.run(run_leader(nats_url))
    elif cmd == "join" and len(sys.argv) > 2:
        asyncio.run(run_member(sys.argv[2], nats_url))
    else:
        print("Gecersiz komut!")


if __name__ == "__main__":
    main()
'''
    (project_path / package_name / "federation_server.py").write_text(federation_server)
    console.print(f"  [green]✓[/green] {package_name}/federation_server.py")


@main.command()
@click.option("--name", "-n", default="voltran-node", help="Node name")
@click.option("--host", "-h", default="0.0.0.0", help="Host to bind to")
@click.option("--port", "-p", default=50051, type=int, help="gRPC port")
@click.option("--nats", default=None, help="NATS server URL")
@click.option("--rest", is_flag=True, help="Use REST API instead of gRPC")
@click.option("--rest-port", default=8080, type=int, help="REST API port")
@click.option("--auto-discovery", is_flag=True, help="Enable Bonjour/mDNS auto-discovery")
def start(name: str, host: str, port: int, nats: Optional[str], rest: bool, rest_port: int, auto_discovery: bool) -> None:
    """Start a Voltran node."""
    from voltran.server import Voltran
    
    console.print(f"[bold green]Starting Voltran node:[/bold green] {name}")
    console.print(f"  Host: {host}")
    
    if rest:
        console.print(f"  REST API: {host}:{rest_port}")
    else:
        console.print(f"  gRPC: {host}:{port}")
    
    if nats:
        console.print(f"  NATS: {nats}")
    
    if auto_discovery:
        console.print(f"  [cyan]Auto-Discovery:[/cyan] Enabled (Bonjour/mDNS)")
    
    nats_servers = [nats] if nats else None
    
    voltran = Voltran(
        name=name,
        host=host,
        port=port,
        nats_servers=nats_servers,
        use_nats=nats is not None and not rest,
        use_rest=rest,
        rest_port=rest_port,
        auto_discovery=auto_discovery,
    )
    
    try:
        asyncio.run(voltran.run_forever())
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")


@main.command()
@click.option("--name", "-n", required=True, help="Federation name")
@click.option("--host", "-h", default="0.0.0.0", help="Host to bind to")
@click.option("--port", "-p", default=50051, type=int, help="gRPC port")
@click.option("--nats", required=True, help="NATS server URL")
def create_federation(name: str, host: str, port: int, nats: str) -> None:
    """Create a new federation as leader."""
    from voltran.server import Voltran
    
    console.print(f"[bold blue]Creating federation:[/bold blue] {name}")
    
    async def run() -> None:
        voltran = Voltran(
            name=f"{name}-leader",
            host=host,
            port=port,
            nats_servers=[nats],
            use_nats=True,
        )
        
        await voltran.start()
        await voltran.create_federation(name)
        
        console.print(f"[green]Federation created![/green]")
        console.print(f"  Federation: {name}")
        console.print(f"  Leader ID: {voltran.id}")
        console.print(f"  Join with: voltran join --leader {voltran.id} --nats {nats}")
        
        await voltran.run_forever()
    
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")


@main.command()
@click.option("--leader", "-l", required=True, help="Leader node ID")
@click.option("--name", "-n", default="voltran-node", help="Node name")
@click.option("--host", "-h", default="0.0.0.0", help="Host to bind to")
@click.option("--port", "-p", default=50052, type=int, help="gRPC port")
@click.option("--nats", required=True, help="NATS server URL")
def join(leader: str, name: str, host: str, port: int, nats: str) -> None:
    """Join an existing federation."""
    console.print(f"[bold blue]Joining federation...[/bold blue]")
    
    async def run() -> None:
        from voltran.server import Voltran
        
        voltran = Voltran(
            name=name,
            host=host,
            port=port,
            nats_servers=[nats],
            use_nats=True,
        )
        
        await voltran.start()
        
        success = await voltran.join_federation(leader)
        
        if success:
            console.print(f"[green]Successfully joined federation![/green]")
            console.print(f"  Node ID: {voltran.id}")
            await voltran.run_forever()
        else:
            console.print(f"[red]Failed to join federation[/red]")
            await voltran.stop()
    
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")


@main.command()
@click.option("--endpoint", "-e", default="localhost:50051", help="Node endpoint")
def status(endpoint: str) -> None:
    """Check status of a Voltran node."""
    console.print(f"[bold]Checking status of:[/bold] {endpoint}")
    
    # For now, just show example output
    table = Table(title="Node Status")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Endpoint", endpoint)
    table.add_row("Status", "Unknown (remote check not implemented)")
    table.add_row("Note", "Use health_check() on a running instance")
    
    console.print(table)


@main.command()
def info() -> None:
    """Show Voltran system information."""
    table = Table(title="Voltran System Info")
    table.add_column("Component", style="cyan")
    table.add_column("Description", style="white")
    
    table.add_row("Version", "0.1.0")
    table.add_row("Python", sys.version.split()[0])
    table.add_row("Architecture", "Hexagonal (Ports & Adapters)")
    table.add_row("Messaging", "gRPC + NATS + REST API")
    table.add_row("Discovery", "Local + Global (Gossip)")
    
    console.print(table)
    
    console.print("\n[bold]Features:[/bold]")
    console.print("  - Module registration with @voltran_module decorator")
    console.print("  - Inbound/Outbound ports with @inbound_port, @outbound_port")
    console.print("  - Module clustering and fusion")
    console.print("  - Federation with gossip-based peer discovery")
    console.print("  - Multi-Voltran fusion into virtual nodes")


@main.group()
def module() -> None:
    """Module management commands."""
    pass


@module.command("list")
@click.option("--format", "-f", type=click.Choice(["table", "json"]), default="table")
def list_modules(format: str) -> None:
    """List registered modules."""
    console.print("[yellow]Note: This command requires a running Voltran instance.[/yellow]")
    console.print("Use Voltran.list_modules() programmatically.")


@module.command("info")
@click.argument("module_id")
def module_info(module_id: str) -> None:
    """Show module information."""
    console.print(f"[yellow]Looking up module:[/yellow] {module_id}")
    console.print("[yellow]Note: This command requires a running Voltran instance.[/yellow]")


@main.group()
def cluster() -> None:
    """Cluster management commands."""
    pass


@cluster.command("create")
@click.argument("name")
def create_cluster(name: str) -> None:
    """Create a new cluster."""
    console.print(f"[yellow]Creating cluster:[/yellow] {name}")
    console.print("[yellow]Note: This command requires a running Voltran instance.[/yellow]")


@cluster.command("fuse")
@click.argument("cluster_id")
@click.option("--name", "-n", required=True, help="Virtual module name")
def fuse_cluster(cluster_id: str, name: str) -> None:
    """Fuse a cluster into a virtual module."""
    console.print(f"[yellow]Fusing cluster:[/yellow] {cluster_id} -> {name}")
    console.print("[yellow]Note: This command requires a running Voltran instance.[/yellow]")


if __name__ == "__main__":
    main()

