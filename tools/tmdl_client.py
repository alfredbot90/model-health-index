import subprocess
import tempfile
from pathlib import Path


class TmdlClient:
    def __init__(self, exe_path: str | None = None):
        # Default to the published CLI location relative to this repo
        if exe_path is None:
            repo_root = Path(__file__).resolve().parent
            # Use the V7 TOM interop location (dotnet publish output) matching V5 layout
            # Prefer win-x64 published path for stability
            exe_path = (
                repo_root
                / "tom_interop"
                / "bin"
                / "Release"
                / "net8.0"
                / "win-x64"
                / "publish"
                / "TmdlTools.exe"
            )
        self.exe_path = str(exe_path)

    def download_tmdl(self, workspace_id: str, semantic_model_id: str, out_dir: str | None = None) -> str:
        out_dir = out_dir or tempfile.mkdtemp(prefix="tmdl_")
        cmd = [
            self.exe_path,
            "fabric-download",
            "--workspace-id",
            workspace_id,
            "--semantic-model-id",
            semantic_model_id,
            "--out",
            out_dir,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr or proc.stdout or f"Download failed with exit code {proc.returncode}")
        return proc.stdout.strip()

    def export_workspace_models(self, workspace_id: str, out_root: str) -> dict:
        """Export all semantic models in a workspace to TMDL folders under out_root.

        Returns a summary with successes and failures like V4's TMDL_Definitions population.
        """
        from core.fabric_client import FabricClient  # local import to avoid cycles

        out_root_path = Path(out_root)
        out_root_path.mkdir(parents=True, exist_ok=True)

        client = FabricClient()
        models = client.get_semantic_models(workspace_id)

        summary: dict = {"workspace_id": workspace_id, "total": len(models), "exported": 0, "failed": 0, "items": []}

        for m in models:
            safe_name = "".join(c for c in m.name if c.isalnum() or c in (" ", "-", "_")).strip()
            model_dir = out_root_path / f"{safe_name}_{m.id}"
            model_dir.mkdir(parents=True, exist_ok=True)
            try:
                dest = self.download_tmdl(workspace_id, m.id, str(model_dir / "definition"))
                summary["items"].append({"id": m.id, "name": m.name, "path": str(dest)})
                summary["exported"] += 1
            except Exception as e:
                summary["items"].append({"id": m.id, "name": m.name, "error": str(e)})
                summary["failed"] += 1

        return summary


