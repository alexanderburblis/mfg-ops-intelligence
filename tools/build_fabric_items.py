"""Write the Fabric item definitions to ``fabric/`` (for review and Fabric Git integration).

python tools/build_fabric_items.py                       # write fabric/*.SemanticModel and *.Report
python tools/build_fabric_items.py --validate <dir>      # also validate the report against the PBIR
                                                         # schemas in a clone of microsoft/json-schemas
python tools/build_fabric_items.py --capture-schema <lakehouse dir>
                                                         # refresh mfgops/gold_schema.py from a local run
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mfgops import model_def, report_def  # noqa: E402


def write_folder(name: str, files: dict[str, str]) -> Path:
    out = ROOT / "fabric" / name
    if out.exists():
        shutil.rmtree(out)
    for rel, text in files.items():
        p = out / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    return out


def validate(report_dir: Path, schemas: Path) -> int:
    from jsonschema import Draft7Validator
    from referencing import Registry, Resource

    prefix = "https://developer.microsoft.com/json-schemas/"

    def retrieve(uri: str):
        return Resource.from_contents(json.loads((schemas / uri.removeprefix(prefix)).read_text()))

    registry = Registry(retrieve=retrieve)
    files = sorted((report_dir / "definition").rglob("*.json"))
    errors = 0
    for f in files:
        doc = json.loads(f.read_text())
        for e in Draft7Validator(retrieve(doc["$schema"]).contents, registry=registry).iter_errors(doc):
            errors += 1
            print(f"{f.relative_to(report_dir)}: {'/'.join(map(str, e.absolute_path))}: {e.message[:200]}")
    print(f"validated {len(files)} report files against the PBIR schemas: {errors} errors")
    return errors


def capture_schema(lakehouse: Path) -> None:
    from mfgops.lakehouse import get_spark

    spark = get_spark()
    out = {}
    for layer in ("gold", "meta"):
        for t in sorted(p.name for p in (lakehouse / layer).iterdir()):
            name = f"{layer}_{t}"
            if layer == "meta" and t not in ("run_summary", "pipeline_runs"):
                continue
            df = spark.read.parquet(str(lakehouse / layer / t))
            out[name] = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    lines = [model_def.__doc__ and "", ""]
    src = ROOT / "src" / "mfgops" / "gold_schema.py"
    head = src.read_text().split("GOLD_SCHEMA")[0]
    lines = [head + "GOLD_SCHEMA: dict[str, list[tuple[str, str]]] = {"]
    for k, cols in out.items():
        lines.append(f'    "{k}": [')
        lines += [f'        ("{c}", "{t}"),' for c, t in cols]
        lines.append("    ],")
    lines.append("}")
    src.write_text("\n".join(lines) + "\n")
    print("updated", src.relative_to(ROOT))


if __name__ == "__main__":
    if "--capture-schema" in sys.argv:
        capture_schema(Path(sys.argv[sys.argv.index("--capture-schema") + 1]))
    m = write_folder(model_def.FOLDER, model_def.model_files())
    r = write_folder(report_def.FOLDER, report_def.report_files())
    print("wrote", m.relative_to(ROOT), "and", r.relative_to(ROOT))
    if "--validate" in sys.argv:
        raise SystemExit(1 if validate(r, Path(sys.argv[sys.argv.index("--validate") + 1])) else 0)
