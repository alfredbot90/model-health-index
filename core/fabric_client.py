"""
Fabric API Client

Provides interface to Microsoft Fabric REST APIs
"""

import requests
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from .auth import get_token


@dataclass
class WorkspaceInfo:
    """Fabric workspace information"""
    id: str
    name: str
    type: str
    capacity_id: Optional[str] = None


@dataclass
class SemanticModelInfo:
    """Semantic model information"""
    id: str
    name: str
    workspace_id: str
    description: Optional[str] = None


class FabricClient:
    """Client for Microsoft Fabric REST API"""

    API_BASE = "https://api.fabric.microsoft.com/v1"

    def __init__(self, auth_token: Optional[str] = None):
        """
        Initialize Fabric API client

        Args:
            auth_token: Optional bearer token (will auto-acquire if not provided)
        """
        self.token = auth_token or get_token()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make API request with error handling

        Args:
            method: HTTP method
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            data: Request body
            **kwargs: Additional requests arguments

        Returns:
            Response JSON data

        Raises:
            RuntimeError: If request fails
        """
        url = f"{self.API_BASE}/{endpoint.lstrip('/')}"

        try:
            response = self.session.request(
                method,
                url,
                params=params,
                json=data,
                **kwargs
            )

            if response.ok:
                return response.json() if response.text else {}

            error_msg = f"HTTP {response.status_code}"
            try:
                error_detail = response.json()
                error_msg = f"{error_msg}: {error_detail}"
            except:
                error_msg = f"{error_msg}: {response.text[:200]}"

            raise RuntimeError(error_msg)

        except requests.RequestException as e:
            raise RuntimeError(f"Request failed: {str(e)}")

    def get_workspaces(self) -> List[WorkspaceInfo]:
        """
        Get list of workspaces

        Returns:
            List of workspace information
        """
        response = self._request("GET", "/workspaces")
        workspaces = response.get("value", [])

        return [
            WorkspaceInfo(
                id=ws.get("id", ""),
                name=ws.get("displayName", ""),
                type=ws.get("type", ""),
                capacity_id=ws.get("capacityId")
            )
            for ws in workspaces
        ]

    def get_workspace(self, workspace_id: str) -> WorkspaceInfo:
        """
        Get workspace by ID

        Args:
            workspace_id: Workspace GUID

        Returns:
            Workspace information
        """
        response = self._request("GET", f"/workspaces/{workspace_id}")

        return WorkspaceInfo(
            id=response.get("id", ""),
            name=response.get("displayName", ""),
            type=response.get("type", ""),
            capacity_id=response.get("capacityId")
        )

    def get_semantic_models(self, workspace_id: str) -> List[SemanticModelInfo]:
        """
        Get semantic models in workspace

        Args:
            workspace_id: Workspace GUID

        Returns:
            List of semantic model information
        """
        response = self._request(
            "GET",
            f"/workspaces/{workspace_id}/items",
            params={"type": "SemanticModel"}
        )

        models = response.get("value", [])

        return [
            SemanticModelInfo(
                id=model.get("id", ""),
                name=model.get("displayName", ""),
                workspace_id=workspace_id,
                description=model.get("description")
            )
            for model in models
        ]

    def get_semantic_model(
        self,
        workspace_id: str,
        model_id: str
    ) -> SemanticModelInfo:
        """
        Get semantic model by ID

        Args:
            workspace_id: Workspace GUID
            model_id: Semantic model GUID

        Returns:
            Semantic model information
        """
        response = self._request(
            "GET",
            f"/workspaces/{workspace_id}/semanticModels/{model_id}"
        )

        return SemanticModelInfo(
            id=response.get("id", ""),
            name=response.get("displayName", ""),
            workspace_id=workspace_id,
            description=response.get("description")
        )

    def get_model_definition(
        self,
        workspace_id: str,
        model_id: str
    ) -> Dict[str, Any]:
        """
        Get semantic model definition (TMDL format)

        Args:
            workspace_id: Workspace GUID
            model_id: Semantic model GUID

        Returns:
            Model definition as dictionary
        """
        # Initiate export
        response = self._request(
            "POST",
            f"/workspaces/{workspace_id}/items/{model_id}/getDefinition",
            data={"format": "TMSL"}  # or TMDL when available
        )

        # Handle long-running operation
        if response.get("status") == "InProgress":
            operation_id = response.get("id")
            return self._wait_for_operation(workspace_id, operation_id)

        return response

    def _wait_for_operation(
        self,
        workspace_id: str,
        operation_id: str,
        max_wait: int = 300,
        poll_interval: int = 5
    ) -> Dict[str, Any]:
        """
        Wait for long-running operation to complete

        Args:
            workspace_id: Workspace GUID
            operation_id: Operation ID
            max_wait: Maximum wait time in seconds
            poll_interval: Polling interval in seconds

        Returns:
            Operation result

        Raises:
            RuntimeError: If operation fails or times out
        """
        start_time = time.time()

        while time.time() - start_time < max_wait:
            response = self._request(
                "GET",
                f"/workspaces/{workspace_id}/operations/{operation_id}"
            )

            status = response.get("status", "")

            if status == "Succeeded":
                return response.get("result", {})
            elif status == "Failed":
                error = response.get("error", {})
                raise RuntimeError(f"Operation failed: {error}")

            time.sleep(poll_interval)

        raise RuntimeError(f"Operation timed out after {max_wait} seconds")

    def refresh_semantic_model(
        self,
        workspace_id: str,
        model_id: str,
        wait_for_completion: bool = False
    ) -> Dict[str, Any]:
        """
        Trigger semantic model refresh

        Args:
            workspace_id: Workspace GUID
            model_id: Semantic model GUID
            wait_for_completion: Wait for refresh to complete

        Returns:
            Refresh response
        """
        response = self._request(
            "POST",
            f"/workspaces/{workspace_id}/semanticModels/{model_id}/refresh"
        )

        if wait_for_completion and response.get("id"):
            return self._wait_for_operation(workspace_id, response["id"])

        return response
