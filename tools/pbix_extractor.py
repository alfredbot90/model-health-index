import argparse
import io
import json
import os
import re
import sys
import time
import zipfile
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set

import requests
from requests import Response
from tqdm import tqdm


API_ROOT = "https://api.powerbi.com/v1.0/myorg"


def get_az_token() -> str:
    try:
        is_windows = sys.platform.startswith("win")
        result = subprocess.run(
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "https://analysis.windows.net/powerbi/api",
            ],
            capture_output=True,
            text=True,
            shell=is_windows,
            check=True,
        )
        data = json.loads(result.stdout or "{}")
        token = data.get("accessToken")
        if not token:
            raise RuntimeError("Received empty token from az. Run 'az login' and try again.")
        return token
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "Failed to acquire token via Azure CLI. Ensure az is installed and run 'az login'."
        ) from exc


def pbi_request(
    method: str,
    url: str,
    token: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Any] = None,
    stream: bool = False,
    max_retries: int = 5,
) -> Response:
    headers = {"Authorization": f"Bearer {token}"}
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        resp = requests.request(method, url, headers=headers, params=params, data=data, stream=stream)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            time.sleep(max(retry_after, backoff))
            backoff = min(backoff * 2, 30)
            continue
        return resp
    return resp


def pbi_get(path: str, token: str, **kwargs) -> Response:
    return pbi_request("GET", f"{API_ROOT}{path}", token, **kwargs)


def ensure_ok(resp: Response, context: str) -> None:
    if 200 <= resp.status_code < 300:
        return
    if resp.status_code in (401, 403):
        raise SystemExit(f"{context} failed: {resp.status_code}. Check permissions and workspace settings.")
    if resp.status_code == 404:
        raise SystemExit(f"{context} failed: 404 Not Found. Validate group/report IDs.")
    if resp.status_code == 409:
        raise SystemExit(f"{context} failed: 409 Conflict. PBIX export may be blocked by tenant settings.")
    try:
        detail = resp.text[:500]
    except Exception:
        detail = str(resp)
    raise SystemExit(f"{context} failed: HTTP {resp.status_code}: {detail}")


def list_pages(group_id: str, report_id: str, token: str) -> List[Dict[str, Any]]:
    resp = pbi_get(f"/groups/{group_id}/reports/{report_id}/pages", token)
    ensure_ok(resp, "List report pages")
    data = resp.json()
    return data.get("value", [])


def download_report_pbix(group_id: str, report_id: str, token: str, out_pbix: str) -> None:
    url = f"{API_ROOT}/groups/{group_id}/reports/{report_id}/Export"
    resp = pbi_request("GET", url, token, stream=True)
    if resp.status_code == 401:
        raise SystemExit("Export PBIX failed: 401 Unauthorized. Ensure your account has access.")
    if resp.status_code == 403:
        raise SystemExit(
            "Export PBIX failed: 403 Forbidden. PBIX export may be disabled by admin. See README for JS fallback."
        )
    ensure_ok(resp, "Export PBIX")

    total = int(resp.headers.get("Content-Length", 0))
    with open(out_pbix, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc="Downloading PBIX") as pbar:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))


def read_layout_from_pbix(pbix_path: str) -> Dict[str, Any]:
    with zipfile.ZipFile(pbix_path, "r") as zf:
        try:
            with zf.open("Report/Layout") as layout_file:
                raw = layout_file.read()
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    # Some PBIX store with utf-16
                    text = raw.decode("utf-16")
                return json.loads(text)
        except KeyError as exc:
            raise SystemExit("PBIX parsing failed: 'Report/Layout' not found in the PBIX.") from exc


def _load_visual_config(vc: Dict[str, Any]) -> Dict[str, Any]:
    cfg = vc.get("config")
    if isinstance(cfg, str):
        try:
            return json.loads(cfg)
        except Exception:
            return {}
    return cfg or {}


def _extract_title(single_visual: Dict[str, Any]) -> Optional[str]:
    objects = single_visual.get("objects", {}) or {}
    title_arr = objects.get("title") or []
    if not isinstance(title_arr, list) or not title_arr:
        return None
    props = (title_arr[0] or {}).get("properties", {})
    text = props.get("text") or {}
    # Common shape: { 'expr': { 'Literal': { 'Value': "'Revenue by Month'" } } }
    expr = text.get("expr") or {}
    lit = expr.get("Literal") or {}
    val = lit.get("Value")
    if isinstance(val, str):
        return val.strip("'\"")
    return None


def _scan_selects_for_refs(selects: List[Dict[str, Any]]) -> Dict[str, str]:
    """Map queryRef/Name -> fully qualified Table[FieldOrMeasure]."""
    mapping: Dict[str, str] = {}

    def fq_from_node(node: Dict[str, Any]) -> Optional[str]:
        # Measure
        if isinstance(node.get("Measure"), str):
            entity = None
            src = node.get("SourceRef") or node.get("Expression", {}).get("SourceRef")
            if isinstance(src, dict):
                entity = src.get("Entity")
            measure = node.get("Measure")
            if entity and measure:
                return f"{entity}[{measure}]"

        # Column
        if isinstance(node.get("Column"), str):
            entity = None
            src = node.get("SourceRef") or node.get("Expression", {}).get("SourceRef")
            if isinstance(src, dict):
                entity = src.get("Entity")
            column = node.get("Column")
            if entity and column:
                return f"{entity}[{column}]"
        return None

    for sel in selects or []:
        name = sel.get("Name") or sel.get("QueryRef") or sel.get("queryRef")
        if not name:
            continue
        for key in ("Expression", "Measure", "Column", "SourceRef"):
            if key in sel and isinstance(sel[key], dict):
                fq = fq_from_node(sel[key])
                if fq:
                    mapping[name] = fq
                    break
        # Nested under Expression
        expr = sel.get("Expression")
        if isinstance(expr, dict):
            fq = fq_from_node(expr)
            if fq:
                mapping[name] = fq
                continue
    return mapping


def _collect_from_node(node: Any, refs: Set[str], unresolved: Set[str]) -> None:
    if isinstance(node, dict):
        # direct measure/column refs
        if ("Measure" in node or "Column" in node) and ("SourceRef" in node or "Expression" in node or "Entity" in node):
            entity = None
            src = node.get("SourceRef") or node.get("Expression", {}).get("SourceRef")
            if isinstance(src, dict):
                entity = src.get("Entity") or src.get("Source")
            target = node.get("Measure") or node.get("Column")
            if isinstance(entity, str) and isinstance(target, str):
                refs.add(f"{entity}[{target}]")
            else:
                unresolved.add(json.dumps(node)[:200])
        # Recurse
        for v in node.values():
            _collect_from_node(v, refs, unresolved)
    elif isinstance(node, list):
        for v in node:
            _collect_from_node(v, refs, unresolved)


def extract_bindings_from_layout(layout: Dict[str, Any]) -> List[Dict[str, Any]]:
    pages_out: List[Dict[str, Any]] = []
    sections = layout.get("sections") or []
    for sec in sections:
        page_obj: Dict[str, Any] = {
            "pageName": sec.get("name"),
            "pageDisplayName": sec.get("displayName") or sec.get("name"),
        }
        if "isHidden" in sec:
            page_obj["isHidden"] = bool(sec.get("isHidden"))

        visuals_out: List[Dict[str, Any]] = []
        for vc in sec.get("visualContainers") or []:
            visual: Dict[str, Any] = {
                "visualType": None,
                "visualName": vc.get("name"),
            }
            cfg = _load_visual_config(vc)
            single = (cfg or {}).get("singleVisual") or {}
            visual["visualType"] = single.get("visualType")
            title = _extract_title(single)
            if title:
                visual["visualTitle"] = title

            measures: List[Dict[str, Any]] = []
            fields: List[str] = []
            unresolved: Set[str] = set()

            # Build queryRef -> fq map from prototypeQuery
            proto = single.get("prototypeQuery") or {}
            ref_map = _scan_selects_for_refs(proto.get("Select") or proto.get("select"))

            # Projections often refer to queryRef names
            projections = single.get("projections") or {}
            for role, arr in (projections.items() if isinstance(projections, dict) else []):
                for item in arr or []:
                    ref = item.get("queryRef")
                    if isinstance(ref, str):
                        fq = ref_map.get(ref)
                        if fq:
                            # Heuristic: if appears to be a measure (capitalization not guaranteed)
                            if "]" in fq and any(token in fq.lower() for token in ("total", "sum", "measure")):
                                measures.append({"fullyQualifiedName": fq})
                            else:
                                fields.append(fq)
                        else:
                            unresolved.add(ref)

            # Also scan other parts for direct refs
            for key in ("prototypeQuery", "dataTransforms", "drillFilter", "filters"):
                _collect_from_node(single.get(key), set(fields), unresolved)

            # Deduplicate while preserving order
            def dedupe_list(items: Iterable[str]) -> List[str]:
                seen: Set[str] = set()
                out: List[str] = []
                for x in items:
                    if x and x not in seen:
                        seen.add(x)
                        out.append(x)
                return out

            fields = dedupe_list(fields)
            # Ensure measures not duplicated and not also in fields
            measure_names = dedupe_list([m.get("fullyQualifiedName") for m in measures if m.get("fullyQualifiedName")])
            fields = [f for f in fields if f not in set(measure_names)]
            measures = [{"fullyQualifiedName": m} for m in measure_names]

            visual["measures"] = measures
            visual["fields"] = fields
            if unresolved:
                visual["unresolvedBindings"] = sorted(unresolved)
            visuals_out.append(visual)

        page_obj["visuals"] = visuals_out
        pages_out.append(page_obj)
    return pages_out


def read_dmvs(workspace: str, dataset_name: str, token: str) -> Dict[str, Dict[str, Any]]:
    try:
        from pyadomd import Pyadomd  # type: ignore
    except Exception as exc:
        raise SystemExit("XMLA requested but 'pyadomd' is not installed. Run: pip install pyadomd") from exc

    conn_str = f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};Initial Catalog={dataset_name}"

    # Try different constructor signatures for token support
    conn = None
    try:
        conn = Pyadomd(conn_str, token=token)  # type: ignore
    except TypeError:
        conn = Pyadomd(conn_str, azure_token=token)  # type: ignore

    with conn:
        def query(sql: str) -> List[Dict[str, Any]]:
            with conn.cursor().execute(sql) as cur:
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

        tables = query("SELECT * FROM $SYSTEM.TMSCHEMA_TABLES")
        tbl_by_id = {t.get("ID"): t.get("Name") for t in tables}

        measures = query("SELECT * FROM $SYSTEM.TMSCHEMA_MEASURES")
        meas_map: Dict[str, Dict[str, Any]] = {}
        for m in measures:
            table_name = tbl_by_id.get(m.get("TableID"))
            name = m.get("Name")
            expr = m.get("Expression")
            if table_name and name:
                fq = f"{table_name}[{name}]"
                meas_map[fq] = {"dax": expr}

        # Columns not strictly needed for dax, but can be returned for future enrichment
        columns = query("SELECT * FROM $SYSTEM.TMSCHEMA_COLUMNS")
        col_map: Dict[str, Dict[str, Any]] = {}
        for c in columns:
            table_name = tbl_by_id.get(c.get("TableID"))
            name = c.get("Name")
            if table_name and name:
                fq = f"{table_name}[{name}]"
                col_map[fq] = {}

    return {"measures": meas_map, "columns": col_map}


def attach_dax_to_measures(structure: List[Dict[str, Any]], dmvs: Dict[str, Dict[str, Any]]) -> None:
    m = dmvs.get("measures", {})
    for page in structure:
        for vis in page.get("visuals", []):
            for meas in vis.get("measures", []):
                fq = meas.get("fullyQualifiedName")
                if fq in m and dmvs["measures"][fq].get("dax"):
                    meas["dax"] = dmvs["measures"][fq]["dax"]


def _flatten_bindings(structure: List[Dict[str, Any]]) -> Set[str]:
    """Create a report signature: set of fully-qualified bindings (measures + fields)."""
    sig: Set[str] = set()
    for page in structure:
        for vis in page.get("visuals", []) or []:
            for m in vis.get("measures", []) or []:
                fq = m.get("fullyQualifiedName")
                if isinstance(fq, str):
                    sig.add(fq)
            for f in vis.get("fields", []) or []:
                if isinstance(f, str):
                    sig.add(f)
    return sig


def scan_cached_pbix(cache_dir: str) -> List[Dict[str, Any]]:
    """Scan a cache directory for .pbix files and extract layout bindings."""
    results: List[Dict[str, Any]] = []
    p = Path(cache_dir)
    for pbix_path in p.glob("*.pbix"):
        try:
            layout = read_layout_from_pbix(str(pbix_path))
            structure = extract_bindings_from_layout(layout)
            signature = _flatten_bindings(structure)
            visuals_count = sum(len(pg.get("visuals", [])) for pg in structure)
            results.append({
                "report_id": pbix_path.stem,
                "pbix_path": str(pbix_path),
                "pages": len(structure),
                "visuals": visuals_count,
                "unique_bindings": len(signature),
                "structure": structure,
            })
        except Exception:
            # Skip unreadable PBIX files
            continue
    return results


def find_similar_reports(cache_dir: str, target_report_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
    """Find cached reports similar to the target by Jaccard similarity of bindings."""
    scans = scan_cached_pbix(cache_dir)
    by_id = {r["report_id"]: r for r in scans}
    target = by_id.get(target_report_id)
    if not target:
        return []
    target_sig = _flatten_bindings(target.get("structure", []))

    sims: List[Tuple[str, float, int, int]] = []
    for rid, info in by_id.items():
        if rid == target_report_id:
            continue
        sig = _flatten_bindings(info.get("structure", []))
        if not sig or not target_sig:
            continue
        inter = len(target_sig & sig)
        union = len(target_sig | sig)
        jacc = inter / union if union else 0.0
        sims.append((rid, jacc, inter, union))

    sims.sort(key=lambda x: x[1], reverse=True)
    out: List[Dict[str, Any]] = []
    for rid, score, inter, union in sims[:top_k]:
        item = by_id[rid]
        out.append({
            "report_id": rid,
            "similarity": round(score, 4),
            "intersection": inter,
            "union": union,
            "pages": item.get("pages"),
            "visuals": item.get("visuals"),
            "unique_bindings": item.get("unique_bindings"),
        })
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Power BI report structure and bindings from PBIX")
    parser.add_argument("--group-id", required=True, help="Power BI group/workspace ID (GUID)")
    parser.add_argument("--report-id", required=True, help="Power BI report ID (GUID)")
    parser.add_argument("--workspace", help="Workspace name or ID for XMLA (optional)")
    parser.add_argument("--dataset-name", help="Dataset name for XMLA (optional)")
    parser.add_argument("--out-json", default="report_structure.json", help="Output JSON path")
    parser.add_argument("--out-pbix", default="report.pbix", help="Output PBIX path")
    args = parser.parse_args()

    token = get_az_token()

    # Sanity: list pages via REST
    try:
        _ = list_pages(args.group_id, args.report_id, token)
    except SystemExit as e:
        raise
    except Exception as exc:
        raise SystemExit(f"Failed to list pages: {exc}")

    # Download PBIX
    download_report_pbix(args.group_id, args.report_id, token, args.out_pbix)

    # Parse layout and extract bindings
    layout = read_layout_from_pbix(args.out_pbix)
    structure = extract_bindings_from_layout(layout)

    # Optional XMLA enrichment
    if args.workspace and args.dataset_name:
        try:
            dmvs = read_dmvs(args.workspace, args.dataset_name, token)
            attach_dax_to_measures(structure, dmvs)
        except SystemExit:
            raise
        except Exception as exc:
            raise SystemExit(
                f"XMLA enrichment failed. Ensure XMLA read access and pyadomd installed. Details: {exc}"
            )

    # Write output
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(structure, f, indent=2)
    print(f"Wrote structure to {args.out_json}")


if __name__ == "__main__":
    main()


