"""Gossip protocol for peer discovery and state synchronization."""

import asyncio
import random
from datetime import datetime
from typing import Optional

import structlog

from voltran.core.domain.models import HealthStatus, VoltranNode
from voltran.core.ports.outbound.messaging import IMessagingPort, Message

logger = structlog.get_logger(__name__)


class GossipState:
    """State information shared via gossip."""

    def __init__(
        self,
        node_id: str,
        node_name: str,
        module_count: int,
        health: HealthStatus,
        known_peers: set[str],
        timestamp: datetime,
    ):
        self.node_id = node_id
        self.node_name = node_name
        self.module_count = module_count
        self.health = health
        self.known_peers = known_peers
        self.timestamp = timestamp

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "node_id": self.node_id,
            "node_name": self.node_name,
            "module_count": self.module_count,
            "health": self.health.value,
            "known_peers": list(self.known_peers),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GossipState":
        """Create from dictionary."""
        return cls(
            node_id=data["node_id"],
            node_name=data["node_name"],
            module_count=data["module_count"],
            health=HealthStatus(data["health"]),
            known_peers=set(data.get("known_peers", [])),
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )


class GossipProtocol:
    """
    Gossip protocol implementation for peer discovery.
    
    Uses epidemic/gossip-based communication to:
    - Discover new peers
    - Share node state
    - Detect failed nodes
    """

    def __init__(
        self,
        local_node: VoltranNode,
        messaging: IMessagingPort,
        gossip_interval: float = 5.0,
        fanout: int = 3,
        failure_threshold: int = 3,
    ):
        """
        Initialize gossip protocol.
        
        Args:
            local_node: This Voltran node
            messaging: Messaging port for communication
            gossip_interval: Seconds between gossip rounds
            fanout: Number of peers to gossip with each round
            failure_threshold: Missed heartbeats before marking dead
        """
        self._local_node = local_node
        self._messaging = messaging
        self._gossip_interval = gossip_interval
        self._fanout = fanout
        self._failure_threshold = failure_threshold
        
        # Peer states (node_id -> GossipState)
        self._peer_states: dict[str, GossipState] = {}
        
        # Failure counters (node_id -> missed_count)
        self._failure_counts: dict[str, int] = {}
        
        # Background task
        self._gossip_task: Optional[asyncio.Task[None]] = None
        self._running = False

    @property
    def known_peers(self) -> set[str]:
        """Get set of known peer IDs."""
        return set(self._peer_states.keys())

    async def start(self) -> None:
        """Start gossip protocol."""
        if self._running:
            return

        self._running = True
        
        # Subscribe to gossip messages
        await self._messaging.subscribe(
            "voltran.federation.gossip",
            self._handle_gossip,
        )
        
        # Subscribe to direct gossip
        await self._messaging.subscribe(
            f"voltran.{self._local_node.id}.gossip",
            self._handle_direct_gossip,
        )
        
        # Start gossip loop
        self._gossip_task = asyncio.create_task(self._gossip_loop())

        logger.info(
            "gossip_started",
            node_id=self._local_node.id,
            interval=self._gossip_interval,
        )

    async def stop(self) -> None:
        """Stop gossip protocol."""
        self._running = False
        
        if self._gossip_task:
            self._gossip_task.cancel()
            try:
                await self._gossip_task
            except asyncio.CancelledError:
                pass

        logger.info("gossip_stopped")

    async def add_seed_peer(self, node_id: str) -> None:
        """
        Add a seed peer to bootstrap discovery.
        
        Args:
            node_id: ID of seed peer
        """
        if node_id != self._local_node.id and node_id not in self._peer_states:
            # Create placeholder state
            self._peer_states[node_id] = GossipState(
                node_id=node_id,
                node_name="",
                module_count=0,
                health=HealthStatus.UNKNOWN,
                known_peers=set(),
                timestamp=datetime.now(),
            )
            self._local_node.known_peers.add(node_id)

            logger.info("seed_peer_added", peer_id=node_id)

    async def get_peer_state(self, node_id: str) -> Optional[GossipState]:
        """Get state of a peer."""
        return self._peer_states.get(node_id)

    async def get_healthy_peers(self) -> list[str]:
        """Get list of healthy peer IDs."""
        healthy = []
        for node_id, state in self._peer_states.items():
            if state.health in (HealthStatus.HEALTHY, HealthStatus.DEGRADED):
                healthy.append(node_id)
        return healthy

    async def get_all_peers(self) -> dict[str, GossipState]:
        """Get all peer states."""
        return self._peer_states.copy()

    # === Private Methods ===

    async def _gossip_loop(self) -> None:
        """Main gossip loop."""
        while self._running:
            try:
                await asyncio.sleep(self._gossip_interval)
                await self._do_gossip_round()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("gossip_loop_error", error=str(e))

    async def _do_gossip_round(self) -> None:
        """Perform one gossip round."""
        # Build local state
        local_state = GossipState(
            node_id=self._local_node.id,
            node_name=self._local_node.name,
            module_count=self._local_node.module_count(),
            health=self._local_node.health,
            known_peers=self._local_node.known_peers,
            timestamp=datetime.now(),
        )

        # Broadcast to gossip topic
        await self._messaging.publish(
            "voltran.federation.gossip",
            local_state.to_dict(),
        )

        # Also do direct gossip to random peers
        peers = list(self._peer_states.keys())
        if peers:
            # Select random subset
            selected = random.sample(peers, min(self._fanout, len(peers)))
            
            for peer_id in selected:
                await self._messaging.publish(
                    f"voltran.{peer_id}.gossip",
                    local_state.to_dict(),
                )

        # Check for failed peers
        await self._check_failures()

        logger.debug(
            "gossip_round_complete",
            peer_count=len(peers),
            gossip_targets=len(selected) if peers else 0,
        )

    async def _handle_gossip(self, message: Message) -> None:
        """Handle gossip message from broadcast topic."""
        await self._process_gossip(message.payload)

    async def _handle_direct_gossip(self, message: Message) -> None:
        """Handle direct gossip message."""
        await self._process_gossip(message.payload)

    async def _process_gossip(self, payload: dict) -> None:
        """Process gossip payload."""
        try:
            state = GossipState.from_dict(payload)
            
            # Ignore self
            if state.node_id == self._local_node.id:
                return

            # Update peer state
            old_state = self._peer_states.get(state.node_id)
            self._peer_states[state.node_id] = state
            
            # Reset failure count
            self._failure_counts[state.node_id] = 0
            
            # Add to known peers
            self._local_node.known_peers.add(state.node_id)
            
            # Learn about new peers from gossip
            for peer_id in state.known_peers:
                if peer_id not in self._peer_states and peer_id != self._local_node.id:
                    await self.add_seed_peer(peer_id)

            # Log new peer discovery
            if old_state is None:
                logger.info(
                    "peer_discovered",
                    peer_id=state.node_id,
                    peer_name=state.node_name,
                    module_count=state.module_count,
                )

        except Exception as e:
            logger.error("gossip_process_error", error=str(e))

    async def _check_failures(self) -> None:
        """Check for and handle failed peers."""
        now = datetime.now()
        failed_peers: list[str] = []
        
        for node_id, state in self._peer_states.items():
            # Check if we haven't heard from peer recently
            elapsed = (now - state.timestamp).total_seconds()
            expected_interval = self._gossip_interval * 2
            
            if elapsed > expected_interval:
                self._failure_counts[node_id] = self._failure_counts.get(node_id, 0) + 1
                
                if self._failure_counts[node_id] >= self._failure_threshold:
                    failed_peers.append(node_id)
                    state.health = HealthStatus.UNHEALTHY

        # Remove dead peers
        for peer_id in failed_peers:
            logger.warning(
                "peer_failed",
                peer_id=peer_id,
                missed_heartbeats=self._failure_counts[peer_id],
            )
            # Don't remove, just mark unhealthy - they might come back

