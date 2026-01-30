"""
Power BI REST API Client

Provides interface to Power BI Service REST APIs
"""

import requests
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from .auth import get_token


@dataclass
class ReportInfo:
    """Power BI report information"""
    id: str
    name: str
    workspace_id: str
    dataset_id: Optional[str] = None
    web_url: Optional[str] = None


class PowerBIClient:
    """Client for Power BI REST API"""

    API_BASE = "https://api.powerbi.com/v1.0/myorg"

    def __init__(self, auth_token: Optional[str] = None):
        """
        Initialize Power BI API client

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
        stream: bool = False,
        **kwargs
    ) -> requests.Response:
        """
        Make API request with retry logic for rate limiting

        Args:
            method: HTTP method
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            data: Request body
            stream: Enable streaming response
            **kwargs: Additional requests arguments

        Returns:
            Response object

        Raises:
            RuntimeError: If request fails after retries
        """
        url = f"{self.API_BASE}/{endpoint.lstrip('/')}"
        max_retries = 5
        backoff = 1.0

        for attempt in range(max_retries):
            response = self.session.request(
                method,
                url,
                params=params,
                json=data,
                stream=stream,
                **kwargs
            )

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff))
                time.sleep(max(retry_after, backoff))
                backoff = min(backoff * 2, 30)
                continue

            return response

        raise RuntimeError(f"Request failed after {max_retries} retries")

    def get_reports(self, workspace_id: str) -> List[ReportInfo]:
        """
        Get reports in workspace

        Args:
            workspace_id: Workspace GUID

        Returns:
            List of report information
        """
        response = self._request(
            "GET",
            f"/groups/{workspace_id}/reports"
        )

        if not response.ok:
            raise RuntimeError(f"Failed to get reports: HTTP {response.status_code}")

        data = response.json()
        reports = data.get("value", [])

        return [
            ReportInfo(
                id=report.get("id", ""),
                name=report.get("name", ""),
                workspace_id=workspace_id,
                dataset_id=report.get("datasetId"),
                web_url=report.get("webUrl")
            )
            for report in reports
        ]

    def get_report(self, workspace_id: str, report_id: str) -> ReportInfo:
        """
        Get report by ID

        Args:
            workspace_id: Workspace GUID
            report_id: Report GUID

        Returns:
            Report information
        """
        response = self._request(
            "GET",
            f"/groups/{workspace_id}/reports/{report_id}"
        )

        if not response.ok:
            raise RuntimeError(f"Failed to get report: HTTP {response.status_code}")

        report = response.json()

        return ReportInfo(
            id=report.get("id", ""),
            name=report.get("name", ""),
            workspace_id=workspace_id,
            dataset_id=report.get("datasetId"),
            web_url=report.get("webUrl")
        )

    def get_report_pages(
        self,
        workspace_id: str,
        report_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get report pages

        Args:
            workspace_id: Workspace GUID
            report_id: Report GUID

        Returns:
            List of page information
        """
        response = self._request(
            "GET",
            f"/groups/{workspace_id}/reports/{report_id}/pages"
        )

        if not response.ok:
            raise RuntimeError(f"Failed to get pages: HTTP {response.status_code}")

        data = response.json()
        return data.get("value", [])

    def export_report_pbix(
        self,
        workspace_id: str,
        report_id: str,
        output_path: str
    ) -> str:
        """
        Export report as PBIX file

        Args:
            workspace_id: Workspace GUID
            report_id: Report GUID
            output_path: Local file path to save PBIX

        Returns:
            Output file path

        Raises:
            RuntimeError: If export fails or is disabled
        """
        response = self._request(
            "GET",
            f"/groups/{workspace_id}/reports/{report_id}/Export",
            stream=True
        )

        if response.status_code == 403:
            raise RuntimeError(
                "PBIX export failed: 403 Forbidden. "
                "PBIX export may be disabled by admin."
            )

        if not response.ok:
            raise RuntimeError(f"Export failed: HTTP {response.status_code}")

        # Write to file
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        return output_path

    def execute_dax_query(
        self,
        workspace_id: str,
        dataset_id: str,
        dax_query: str
    ) -> List[Dict[str, Any]]:
        """
        Execute DAX query against dataset

        Args:
            workspace_id: Workspace GUID
            dataset_id: Dataset GUID
            dax_query: DAX query string

        Returns:
            Query results as list of dictionaries

        Note:
            Requires XMLA endpoint access (Premium/PPU capacity)
        """
        response = self._request(
            "POST",
            f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
            data={
                "queries": [{"query": dax_query}],
                "serializerSettings": {"includeNulls": False}
            }
        )

        if not response.ok:
            raise RuntimeError(
                f"DAX query failed: HTTP {response.status_code} - {response.text[:200]}"
            )

        result = response.json()

        # Extract rows from response
        if result.get("results"):
            tables = result["results"][0].get("tables", [])
            if tables:
                return tables[0].get("rows", [])

        return []

    def get_dataset_refresh_history(
        self,
        workspace_id: str,
        dataset_id: str,
        top: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get dataset refresh history

        Args:
            workspace_id: Workspace GUID
            dataset_id: Dataset GUID
            top: Number of refresh records to return

        Returns:
            List of refresh records
        """
        response = self._request(
            "GET",
            f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
            params={"$top": top}
        )

        if not response.ok:
            raise RuntimeError(
                f"Failed to get refresh history: HTTP {response.status_code}"
            )

        data = response.json()
        return data.get("value", [])
