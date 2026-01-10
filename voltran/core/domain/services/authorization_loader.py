"""Authorization rules loader from configuration files."""

import json
from pathlib import Path
from typing import Any, Optional

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

from voltran.core.ports.outbound.authorization import (
    Permission,
    PermissionEffect,
    Policy,
    ResourceType,
    Role,
)


class AuthorizationRulesLoader:
    """
    Load authorization rules from YAML or JSON files.
    
    Usage:
        loader = AuthorizationRulesLoader()
        rules = loader.load_from_file("auth_rules.yaml")
        
        # Apply to authorization service
        await auth.apply_rules(rules)
    """

    def load_from_file(self, file_path: str | Path) -> dict[str, Any]:
        """
        Load authorization rules from a file.
        
        Supports YAML (.yaml, .yml) and JSON (.json) formats.
        
        Args:
            file_path: Path to rules file
            
        Returns:
            Dictionary with permissions, roles, and policies
            
        Raises:
            ValueError: If file format is not supported
            FileNotFoundError: If file does not exist
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Rules file not found: {file_path}")
        
        if path.suffix in (".yaml", ".yml"):
            return self._load_yaml(path)
        elif path.suffix == ".json":
            return self._load_json(path)
        else:
            raise ValueError(
                f"Unsupported file format: {path.suffix}. "
                "Supported formats: .yaml, .yml, .json"
            )

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        """Load rules from YAML file."""
        if yaml is None:
            raise ImportError(
                "PyYAML is required for YAML support. Install with: pip install pyyaml"
            )
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _load_json(self, path: Path) -> dict[str, Any]:
        """Load rules from JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def parse_permissions(
        self,
        permissions_data: list[dict[str, Any]],
    ) -> list[Permission]:
        """
        Parse permissions from configuration data.
        
        Args:
            permissions_data: List of permission dictionaries
            
        Returns:
            List of Permission objects
        """
        permissions = []
        
        for perm_data in permissions_data:
            permission = Permission(
                name=perm_data.get("name", ""),
                description=perm_data.get("description", ""),
                resource_type=ResourceType(perm_data.get("resource_type", "system")),
                resource_id=perm_data.get("resource_id"),
                action=perm_data.get("action", ""),
                effect=PermissionEffect(perm_data.get("effect", "allow")),
                conditions=perm_data.get("conditions", {}),
            )
            permissions.append(permission)
        
        return permissions

    def parse_roles(
        self,
        roles_data: list[dict[str, Any]],
    ) -> list[Role]:
        """
        Parse roles from configuration data.
        
        Args:
            roles_data: List of role dictionaries
            
        Returns:
            List of Role objects
        """
        roles = []
        
        for role_data in roles_data:
            role = Role(
                name=role_data.get("name", ""),
                description=role_data.get("description", ""),
                permissions=role_data.get("permissions", []),
                inherited_roles=role_data.get("inherited_roles", []),
                is_system=role_data.get("is_system", False),
            )
            roles.append(role)
        
        return roles

    def parse_policies(
        self,
        policies_data: list[dict[str, Any]],
    ) -> list[Policy]:
        """
        Parse policies from configuration data.
        
        Args:
            policies_data: List of policy dictionaries
            
        Returns:
            List of Policy objects
        """
        policies = []
        
        for policy_data in policies_data:
            policy = Policy(
                name=policy_data.get("name", ""),
                description=policy_data.get("description", ""),
                subjects=policy_data.get("subjects", []),
                resources=policy_data.get("resources", []),
                actions=policy_data.get("actions", []),
                effect=PermissionEffect(policy_data.get("effect", "allow")),
                conditions=policy_data.get("conditions", {}),
                priority=policy_data.get("priority", 0),
                enabled=policy_data.get("enabled", True),
            )
            policies.append(policy)
        
        return policies

    def parse_rules(self, rules_data: dict[str, Any]) -> dict[str, Any]:
        """
        Parse complete rules configuration.
        
        Args:
            rules_data: Rules dictionary from file
            
        Returns:
            Dictionary with parsed permissions, roles, and policies
        """
        return {
            "permissions": self.parse_permissions(
                rules_data.get("permissions", [])
            ),
            "roles": self.parse_roles(rules_data.get("roles", [])),
            "policies": self.parse_policies(rules_data.get("policies", [])),
            "assignments": rules_data.get("assignments", []),  # subject_id -> role_name
        }

