"""Deploy the semantic model and report definitions to a Fabric workspace (deployment as code).

The definitions are code (``mfgops.model_def`` and ``mfgops.report_def``,
also written to ``fabric/`` for review). This module publishes them through
the Fabric REST API (``items/{id}/updateDefinition`` or create), fills in the
environment-specific values (SQL endpoint of the lakehouse, semantic model id)
and refreshes the model so Direct Lake picks up the new definition.

Run it from a Fabric notebook (``06_deploy_model_and_report``)::

    from mfgops.deploy import deploy
    deploy()                       # current workspace

Inside Fabric it authenticates as the notebook user through semantic-link
(``sempy.fabric.FabricRestClient``). Pointing the same call at a test and a
production workspace is the deployment path between environments.
"""

from __future__ import annotations

import base64
import time

from mfgops.model_def import model_files
from mfgops.report_def import report_files

MODEL_NAME = "sm_mfg_ops"
REPORT_NAME = "rpt_mfg_ops"
LAKEHOUSE_NAME = "lh_mfg_ops"
PLACEHOLDERS = ("SQL_ENDPOINT", "SQL_ENDPOINT_ID", "SEMANTIC_MODEL_CONNECTION")


def load_parts(files: dict[str, str], replacements: dict[str, str]) -> list[dict]:
    """Item files (relative path → text) as definition parts, with ``{{PLACEHOLDERS}}`` filled in."""
    parts = []
    for rel, text in sorted(files.items()):
        if rel.endswith(".platform"):
            continue
        for k, v in replacements.items():
            text = text.replace("{{" + k + "}}", v)
        left = [k for k in PLACEHOLDERS if "{{" + k + "}}" in text]
        if left:
            raise ValueError(f"unfilled placeholder {left} in {rel}")
        parts.append(
            {
                "path": rel,
                "payload": base64.b64encode(text.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            }
        )
    return parts


def connection_string(workspace_name: str, model_id: str) -> str:
    return (
        f'Data Source="powerbi://api.powerbi.com/v1.0/myorg/{workspace_name}";'
        f"initial catalog={MODEL_NAME};access mode=readonly;integrated security=ClaimsToken;"
        f"semanticmodelid={model_id}"
    )


class Fabric:
    """Thin wrapper over the Fabric REST API with long-running-operation polling."""

    def __init__(self, client=None, poll_s: float = 3.0, timeout_s: float = 600.0):
        if client is None:
            import sempy.fabric as fabric  # only available inside Fabric

            client = fabric.FabricRestClient()
        self.c, self.poll_s, self.timeout_s = client, poll_s, timeout_s

    def _wait(self, resp) -> None:
        if resp.status_code not in (200, 201, 202):
            raise RuntimeError(f"{resp.status_code}: {resp.text[:2000]}")
        if resp.status_code != 202:
            return
        url = resp.headers.get("Location")
        deadline = time.time() + self.timeout_s
        while time.time() < deadline:
            time.sleep(float(resp.headers.get("Retry-After", self.poll_s)))
            r = self.c.get(url)
            body = r.json() if r.content else {}
            status = body.get("status")
            if status == "Succeeded":
                return
            if status in ("Failed", "Cancelled"):
                raise RuntimeError(f"operation {status}: {body.get('error')}")
        raise TimeoutError(f"operation did not finish within {self.timeout_s}s: {url}")

    def items(self, ws: str, kind: str) -> dict[str, dict]:
        r = self.c.get(f"v1/workspaces/{ws}/{kind}")
        r.raise_for_status()
        return {i["displayName"]: i for i in r.json().get("value", [])}

    def upsert(self, ws: str, kind: str, name: str, parts: list[dict], fmt: str | None = None) -> str:
        definition = {"parts": parts}
        if fmt:
            definition["format"] = fmt
        existing = self.items(ws, kind).get(name)
        if existing:
            self._wait(
                self.c.post(
                    f"v1/workspaces/{ws}/{kind}/{existing['id']}/updateDefinition",
                    json={"definition": definition},
                )
            )
            return existing["id"]
        self._wait(self.c.post(f"v1/workspaces/{ws}/{kind}", json={"displayName": name, "definition": definition}))
        return self.items(ws, kind)[name]["id"]


def deploy(workspace_id: str | None = None, client=None, refresh: bool = True) -> dict:
    """Publish the model and report defined in ``model_def`` / ``report_def`` into the workspace."""
    api = Fabric(client)
    if workspace_id is None:
        import sempy.fabric as fabric

        workspace_id = fabric.get_workspace_id()
    ws_info = api.c.get(f"v1/workspaces/{workspace_id}").json()
    lakehouse = api.items(workspace_id, "lakehouses")[LAKEHOUSE_NAME]
    sql = api.c.get(f"v1/workspaces/{workspace_id}/lakehouses/{lakehouse['id']}").json()["properties"][
        "sqlEndpointProperties"
    ]
    model_parts = load_parts(
        model_files(),
        {"SQL_ENDPOINT": sql["connectionString"], "SQL_ENDPOINT_ID": sql["id"]},
    )
    model_id = api.upsert(workspace_id, "semanticModels", MODEL_NAME, model_parts, fmt="TMDL")
    if refresh:
        import sempy.fabric as fabric

        fabric.refresh_dataset(MODEL_NAME, workspace=workspace_id)
    report_parts = load_parts(
        report_files(),
        {"SEMANTIC_MODEL_CONNECTION": connection_string(ws_info["displayName"], model_id).replace('"', '\\"')},
    )
    report_id = api.upsert(workspace_id, "reports", REPORT_NAME, report_parts, fmt="PBIR")
    return {"workspace": workspace_id, "semantic_model": model_id, "report": report_id}
