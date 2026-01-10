"""Federation mesh - manages Voltran node federation."""

import asyncio
from datetime import datetime
from typing import Optional

import structlog

from voltran.core.domain.models import (
    Federation,
    HealthStatus,
    ModuleDescriptor,
    NodeRole,
    VoltranNode,
)
from voltran.core.domain.services.fusion import FusionService
from voltran.core.ports.inbound.federation import IFederationPort
from voltran.core.ports.outbound.messaging import IMessagingPort, Message
from voltran.discovery.global_ import GlobalDiscoveryService
from voltran.discovery.local import LocalDiscoveryService
from voltran.federation.gossip import GossipProtocol

logger = structlog.get_logger(__name__)


class FederationMesh(IFederationPort):
    """
    Federation mesh for connecting multiple Voltran nodes.
    
    Manages:
    - Federation creation and joining
    - Peer discovery via gossip
    - Registry synchronization
    - Virtual Voltran fusion
    """

    def __init__(
        self,
        local_node: VoltranNode,
        local_discovery: LocalDiscoveryService,
        messaging: IMessagingPort,
    ):
        """
        Initialize federation mesh.
        
        Args:
            local_node: This Voltran node
            local_discovery: Local discovery service
            messaging: Messaging port for communication
        """
        self._local_node = local_node
        self._local_discovery = local_discovery
        self._messaging = messaging
        
        # Global discovery
        self._global_discovery = GlobalDiscoveryService(
            local_discovery=local_discovery,
            messaging=messaging,
        )
        
        # Gossip protocol
        self._gossip = GossipProtocol(
            local_node=local_node,
            messaging=messaging,
        )
        
        # Fusion service
        self._fusion = FusionService(local_discovery)
        
        # Federation state
        self._federation: Optional[Federation] = None
        
        # Running state
        self._running = False

    @property
    def is_federated(self) -> bool:
        """Check if node is part of a federation."""
        return self._federation is not None

    @property
    def is_leader(self) -> bool:
        """Check if node is federation leader."""
        return (
            self._federation is not None
            and self._federation.leader_id == self._local_node.id
        )

    async def start(self) -> None:
        """Start federation mesh."""
        if self._running:
            return

        self._running = True
        
        # Connect messaging
        await self._messaging.connect()
        
        # Start local discovery
        await self._local_discovery.start()
        
        # Setup message handlers
        await self._setup_message_handlers()

        logger.info(
            "federation_mesh_started",
            node_id=self._local_node.id,
            node_name=self._local_node.name,
        )

    async def stop(self) -> None:
        """Stop federation mesh."""
        self._running = False
        
        # Leave federation if member
        if self._federation:
            await self.leave_federation()
        
        # Stop components
        await self._gossip.stop()
        await self._global_discovery.stop()
        await self._local_discovery.stop()
        await self._messaging.disconnect()

        logger.info("federation_mesh_stopped")

    async def create_federation(self, name: str) -> Federation:
        """Create a new federation with this node as leader."""
        if self._federation:
            raise ValueError("Already in a federation")

        # Create federation
        self._federation = Federation(
            name=name,
            leader_id=self._local_node.id,
        )
        
        # Update local node
        self._local_node.role = NodeRole.FEDERATION_LEADER
        self._local_node.federation_id = self._federation.id
        self._local_node.health = HealthStatus.HEALTHY
        
        # Add self as member
        self._federation.members[self._local_node.id] = self._local_node
        
        # Copy local modules to global registry
        for module in await self._local_discovery.get_all():
            self._federation.global_registry[module.id] = module
        
        # Start gossip and global discovery
        await self._gossip.start()
        await self._global_discovery.start()
        
        # Announce federation creation
        await self._messaging.publish(
            "voltran.federation.created",
            {
                "federation_id": self._federation.id,
                "federation_name": name,
                "leader_id": self._local_node.id,
                "leader_name": self._local_node.name,
            },
        )

        logger.info(
            "federation_created",
            federation_id=self._federation.id,
            federation_name=name,
        )

        return self._federation

    async def join_federation(self, federation_endpoint: str) -> bool:
        """Join an existing federation."""
        if self._federation:
            raise ValueError("Already in a federation")

        try:
            # Parse endpoint (format: voltran_id or nats://host:port)
            target_id = federation_endpoint
            
            # Send join request
            response = await self._messaging.request(
                target_voltran_id=target_id,
                payload={
                    "action": "join_request",
                    "node_id": self._local_node.id,
                    "node_name": self._local_node.name,
                    "module_count": await self._local_discovery.count(),
                },
                timeout=30.0,
            )

            if not response.get("accepted"):
                logger.warning(
                    "federation_join_rejected",
                    reason=response.get("reason", "unknown"),
                )
                return False

            # Create federation from response
            self._federation = Federation(
                id=response["federation_id"],
                name=response["federation_name"],
                leader_id=response["leader_id"],
            )
            
            # Update local node
            self._local_node.role = NodeRole.FEDERATION_MEMBER
            self._local_node.federation_id = self._federation.id
            self._local_node.health = HealthStatus.HEALTHY
            
            # Add known peers
            for peer_id in response.get("peer_ids", []):
                self._local_node.known_peers.add(peer_id)
                await self._gossip.add_seed_peer(peer_id)
            
            # Start gossip and global discovery
            await self._gossip.start()
            await self._global_discovery.start()
            
            # Sync registry
            await self._global_discovery.sync_registry()

            logger.info(
                "federation_joined",
                federation_id=self._federation.id,
                federation_name=self._federation.name,
            )

            return True

        except TimeoutError:
            logger.error("federation_join_timeout", endpoint=federation_endpoint)
            return False
        except Exception as e:
            logger.error("federation_join_error", error=str(e))
            return False

    async def leave_federation(self) -> bool:
        """Leave the current federation."""
        if not self._federation:
            return False

        # Announce leaving
        await self._messaging.publish(
            "voltran.federation.node.leaving",
            {
                "node_id": self._local_node.id,
                "federation_id": self._federation.id,
            },
        )
        
        # Stop gossip
        await self._gossip.stop()
        await self._global_discovery.stop()
        
        # Clear state
        old_federation = self._federation
        self._federation = None
        self._local_node.role = NodeRole.STANDALONE
        self._local_node.federation_id = None
        self._local_node.known_peers.clear()

        logger.info(
            "federation_left",
            federation_id=old_federation.id,
        )

        return True

    async def discover_peers(self) -> list[VoltranNode]:
        """Discover all peer nodes in federation."""
        if not self._federation:
            return []

        # Get peers from gossip
        peer_states = await self._gossip.get_all_peers()
        
        # Convert to VoltranNodes
        peers: list[VoltranNode] = []
        for state in peer_states.values():
            node = VoltranNode(
                id=state.node_id,
                name=state.node_name,
                health=state.health,
                federation_id=self._federation.id,
            )
            peers.append(node)
        
        return peers

    async def sync_registry(self) -> dict[str, ModuleDescriptor]:
        """Synchronize module registry across federation."""
        if not self._federation:
            return {}

        registry = await self._global_discovery.sync_registry()
        self._federation.global_registry = registry
        
        return registry

    async def get_federation_state(self) -> Optional[Federation]:
        """Get current federation state."""
        return self._federation

    async def find_in_federation(self, contract_id: str) -> list[ModuleDescriptor]:
        """Find modules by contract ID across federation."""
        if not self._federation:
            return []

        return await self._global_discovery.find_by_contract(contract_id)

    async def fuse_voltrans(
        self,
        voltran_ids: list[str],
        virtual_name: str,
    ) -> VoltranNode:
        """Fuse multiple Voltran nodes into a virtual node."""
        if not self._federation:
            raise ValueError("Not in a federation")

        # Collect Voltrans to fuse
        voltrans: list[VoltranNode] = []
        
        for vid in voltran_ids:
            if vid == self._local_node.id:
                voltrans.append(self._local_node)
            elif vid in self._federation.members:
                voltrans.append(self._federation.members[vid])
            else:
                # Try to get from gossip
                state = await self._gossip.get_peer_state(vid)
                if state:
                    node = VoltranNode(
                        id=state.node_id,
                        name=state.node_name,
                        health=state.health,
                    )
                    voltrans.append(node)

        if len(voltrans) != len(voltran_ids):
            missing = set(voltran_ids) - {v.id for v in voltrans}
            raise ValueError(f"Voltran nodes not found: {missing}")

        # Fuse using fusion service
        virtual_voltran = await self._fusion.fuse_voltrans(voltrans, virtual_name)
        
        # Store in federation
        self._federation.virtual_voltran = virtual_voltran
        
        # Announce fusion
        await self._messaging.publish(
            "voltran.federation.voltrans.fused",
            {
                "virtual_id": virtual_voltran.id,
                "virtual_name": virtual_name,
                "fused_ids": voltran_ids,
            },
        )

        logger.info(
            "voltrans_fused",
            virtual_id=virtual_voltran.id,
            fused_count=len(voltrans),
        )

        return virtual_voltran

    # === Private Methods ===

    async def _setup_message_handlers(self) -> None:
        """Setup message handlers."""
        # Handle join requests (for leader)
        await self._messaging.subscribe(
            f"voltran.{self._local_node.id}.request",
            self._handle_request,
        )

    async def _handle_request(self, message: Message) -> None:
        """Handle incoming requests."""
        action = message.payload.get("action", "")
        
        if action == "join_request":
            await self._handle_join_request(message)
        elif action == "get_modules":
            await self._handle_get_modules(message)

    async def _handle_join_request(self, message: Message) -> None:
        """Handle federation join request."""
        if not self.is_leader:
            await self._messaging.respond(
                message,
                {"accepted": False, "reason": "Not a leader"},
            )
            return

        # Accept the join
        node_id = message.payload.get("node_id", "")
        node_name = message.payload.get("node_name", "")
        
        # Create node entry
        new_node = VoltranNode(
            id=node_id,
            name=node_name,
            role=NodeRole.FEDERATION_MEMBER,
            federation_id=self._federation.id if self._federation else None,
            health=HealthStatus.HEALTHY,
        )
        
        if self._federation:
            self._federation.members[node_id] = new_node
        
        # Add to gossip
        await self._gossip.add_seed_peer(node_id)
        
        # Send response
        await self._messaging.respond(
            message,
            {
                "accepted": True,
                "federation_id": self._federation.id if self._federation else "",
                "federation_name": self._federation.name if self._federation else "",
                "leader_id": self._local_node.id,
                "peer_ids": list(self._local_node.known_peers),
            },
        )

        logger.info(
            "node_joined_federation",
            node_id=node_id,
            node_name=node_name,
        )

    async def _handle_get_modules(self, message: Message) -> None:
        """Handle get modules request."""
        modules = await self._local_discovery.get_all()
        
        await self._messaging.respond(
            message,
            {
                "modules": [
                    {
                        "id": m.id,
                        "name": m.name,
                        "version": m.version,
                        "voltran_id": m.voltran_id,
                        "health": m.health.value,
                    }
                    for m in modules
                ],
            },
        )

