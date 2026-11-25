"""Consistency of the Fabric item definitions in fabric/ and the deploy logic (no Fabric needed)."""

from __future__ import annotations

import base64
import json
import re

import pytest

from mfgops.deploy import Fabric, connection_string, load_parts
from mfgops.gold_schema import GOLD_SCHEMA
from mfgops.model_def import model_files
from mfgops.report_def import report_files

MODEL = model_files()
REPORT = report_files()


def _model_objects():
    tables, measures = {}, set()
    for path, text in MODEL.items():
        if not path.startswith("definition/tables/"):
            continue
        t = re.search(r"^table (\S+)", text, re.M).group(1)
        tables[t] = set(re.findall(r"^\tcolumn (\S+)", text, re.M))
        measures |= {(t, m.strip("'")) for m in re.findall(r"^\tmeasure ('[^']+'|\S+) =", text, re.M)}
    return tables, measures


def _report_fields():
    for path, text in REPORT.items():
        if path.endswith(".json"):
            yield from _walk(json.loads(text))


def _walk(o):
    if isinstance(o, dict):
        for kind in ("Column", "Measure"):
            if kind in o and isinstance(o[kind], dict) and "Property" in o[kind]:
                yield kind, o[kind]["Expression"]["SourceRef"]["Entity"], o[kind]["Property"]
        for v in o.values():
            yield from _walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from _walk(v)


def test_model_matches_gold_schema():
    tables, _ = _model_objects()
    assert set(tables) == set(GOLD_SCHEMA)
    for t, cols in GOLD_SCHEMA.items():
        assert tables[t] == {c for c, _ in cols}, t


def test_every_report_field_exists_in_the_model():
    tables, measures = _model_objects()
    missing = set()
    for kind, entity, prop in _report_fields():
        if kind == "Column" and prop not in tables.get(entity, set()):
            missing.add((entity, prop))
        if kind == "Measure" and (entity, prop) not in measures:
            missing.add((entity, prop))
    assert not missing


def test_relationships_point_at_real_columns():
    tables, _ = _model_objects()
    text = MODEL["definition/relationships.tmdl"]
    for t, c in re.findall(r"(?:fromColumn|toColumn): (\w+)\.(\w+)", text):
        assert c in tables[t], f"{t}.{c}"


def test_placeholders_are_filled_and_unfilled_ones_rejected():
    parts = load_parts(MODEL, {"SQL_ENDPOINT": "x.datawarehouse.fabric.microsoft.com", "SQL_ENDPOINT_ID": "abc"})
    expr = next(p for p in parts if p["path"] == "definition/expressions.tmdl")
    assert 'Sql.Database("x.datawarehouse.fabric.microsoft.com", "abc")' in base64.b64decode(expr["payload"]).decode()
    assert not any(p["path"] == ".platform" for p in parts)
    with pytest.raises(ValueError):
        load_parts(MODEL, {})


def test_report_points_at_the_model():
    cs = connection_string("mfg-ops-intelligence", "1234").replace('"', '\\"')
    parts = load_parts(REPORT, {"SEMANTIC_MODEL_CONNECTION": cs})
    pbir = json.loads(base64.b64decode(next(p for p in parts if p["path"] == "definition.pbir")["payload"]))
    assert pbir["datasetReference"]["byConnection"]["connectionString"].endswith("semanticmodelid=1234")


class _Resp:
    def __init__(self, code=200, body=None, headers=None):
        self.status_code, self._body, self.headers = code, body or {}, headers or {}
        self.content, self.text = b"x", json.dumps(body or {})

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


class _Client:
    def __init__(self, existing):
        self.existing, self.calls = existing, []

    def get(self, url):
        self.calls.append(("GET", url))
        if url.startswith("ops/"):
            return _Resp(body={"status": "Succeeded"})
        return _Resp(body={"value": [{"displayName": n, "id": i} for n, i in self.existing.items()]})

    def post(self, url, json=None):
        self.calls.append(("POST", url))
        if url.endswith("/semanticModels"):
            self.existing["sm"] = "new-id"
        return _Resp(202, headers={"Location": "ops/1", "Retry-After": "0"})


def test_upsert_updates_existing_and_creates_missing():
    c = _Client({"sm": "old-id"})
    api = Fabric(c, poll_s=0)
    assert api.upsert("ws", "semanticModels", "sm", [], "TMDL") == "old-id"
    assert ("POST", "v1/workspaces/ws/semanticModels/old-id/updateDefinition") in c.calls
    c = _Client({})
    assert Fabric(c, poll_s=0).upsert("ws", "semanticModels", "sm", [], "TMDL") == "new-id"
