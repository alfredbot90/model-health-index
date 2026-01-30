"""
Unified Authentication Module for Power BI / Fabric APIs

Supports multiple authentication methods:
1. Azure CLI (recommended for development)
2. Environment variable (for CI/CD)
3. Keyring (secure local storage)
"""

import os
import subprocess
import json
import platform
from typing import Optional
from dataclasses import dataclass


@dataclass
class AuthConfig:
    """Authentication configuration"""
    token: str
    method: str  # 'azure_cli', 'env_var', 'keyring', 'manual'
    expires_on: Optional[str] = None


class AuthenticationManager:
    """Manages authentication for Power BI and Fabric APIs"""

    def __init__(self, resource: str = "https://analysis.windows.net/powerbi/api"):
        """
        Initialize authentication manager

        Args:
            resource: OAuth resource URL (default: Power BI API)
        """
        self.resource = resource
        self._cached_token: Optional[AuthConfig] = None

    def get_token(self, force_refresh: bool = False) -> str:
        """
        Get access token using available authentication method

        Args:
            force_refresh: Force token refresh even if cached

        Returns:
            Access token string

        Raises:
            RuntimeError: If no authentication method is available
        """
        # Return cached token if available and not forcing refresh
        if self._cached_token and not force_refresh:
            return self._cached_token.token

        # Try authentication methods in order of preference
        token = (
            self._try_azure_cli() or
            self._try_env_var() or
            self._try_keyring()
        )

        if not token:
            raise RuntimeError(
                "No authentication token available. Please authenticate using:\n"
                "1. Azure CLI: Run 'az login'\n"
                "2. Environment variable: Set POWERBI_TOKEN\n"
                "3. Keyring: Run 'keyring set powerbi token'"
            )

        return token

    def _try_azure_cli(self) -> Optional[str]:
        """Attempt to get token from Azure CLI"""
        try:
            use_shell = platform.system() == "Windows"

            result = subprocess.run(
                [
                    "az", "account", "get-access-token",
                    "--resource", self.resource
                ],
                capture_output=True,
                text=True,
                check=True,
                shell=use_shell
            )

            data = json.loads(result.stdout)
            token = data.get("accessToken", "")

            if token:
                self._cached_token = AuthConfig(
                    token=token,
                    method="azure_cli",
                    expires_on=data.get("expiresOn")
                )
                return token

        except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError):
            pass

        return None

    def _try_env_var(self) -> Optional[str]:
        """Attempt to get token from environment variable"""
        token = os.environ.get("POWERBI_TOKEN", "")

        if token:
            self._cached_token = AuthConfig(
                token=token,
                method="env_var"
            )
            return token

        return None

    def _try_keyring(self) -> Optional[str]:
        """Attempt to get token from keyring"""
        try:
            import keyring
            token = keyring.get_password("powerbi", "token")

            if token:
                self._cached_token = AuthConfig(
                    token=token,
                    method="keyring"
                )
                return token

        except ImportError:
            pass  # keyring not installed
        except Exception:
            pass  # keyring access failed

        return None

    def set_token_manual(self, token: str) -> None:
        """
        Manually set authentication token

        Args:
            token: Bearer token string
        """
        self._cached_token = AuthConfig(
            token=token,
            method="manual"
        )

    def clear_cache(self) -> None:
        """Clear cached token"""
        self._cached_token = None

    def get_auth_info(self) -> Optional[AuthConfig]:
        """Get current authentication configuration"""
        return self._cached_token


# Singleton instance for easy import
_default_auth = AuthenticationManager()

def get_token(force_refresh: bool = False) -> str:
    """
    Get authentication token using default manager

    Args:
        force_refresh: Force token refresh

    Returns:
        Access token string
    """
    return _default_auth.get_token(force_refresh)


def get_auth_manager() -> AuthenticationManager:
    """Get default authentication manager instance"""
    return _default_auth
