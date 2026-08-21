#!/usr/bin/env python3
"""Assemble the self-contained GenieOne installer notebook from source assets."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "templates"
DATA_NOTEBOOK = ROOT / "src" / "data_generator.py"
OUTPUT = ROOT / "src" / "install_genieone.py"
SPACE_ORDER = [
    "bid_management",
    "financial_analysis",
    "supplier_intelligence",
    "market_intelligence",
    "contract_performance",
    "risk_assessment",
]


def data_cells() -> str:
    text = DATA_NOTEBOOK.read_text(encoding="utf-8")
    marker = "\n# COMMAND ----------\n"
    _, remainder = text.split(marker, 1)
    return marker + remainder.strip() + "\n"


def space_bundle() -> dict:
    metadata = json.loads((TEMPLATES / "spaces.json").read_text(encoding="utf-8"))
    bundle = {}
    for name in SPACE_ORDER:
        definition = json.loads(
            (TEMPLATES / f"{name}.geniespace.json").read_text(encoding="utf-8")
        )
        bundle[name] = {
            "title": metadata[name]["title"],
            "description": metadata[name]["description"],
            "serialized_space": definition,
        }
    return bundle


HEADER = r'''# Databricks notebook source
# MAGIC %md
# MAGIC # GenieOne complete installer
# MAGIC
# MAGIC Self-contained notebook. No CLI and no `install.sh` required.
# MAGIC
# MAGIC 1. Set the widgets below (`warehouse_id` is required).
# MAGIC 2. Attach **serverless** or any Spark cluster.
# MAGIC 3. **Run all**.
# MAGIC
# MAGIC This notebook creates the 16 demo tables first, then creates or updates
# MAGIC the six Genie spaces. Tables must exist before Genie space creation.

# COMMAND ----------
# mypy: ignore-errors
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup - Catalog, schema, warehouse
import json
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import *

dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "bid_evaluation_demo", "Schema")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("parent_path", "/Shared/al-ghurair-procurement", "Genie folder")
dbutils.widgets.text("can_run_group", "users", "Group granted CAN_RUN")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
PARENT_PATH = dbutils.widgets.get("parent_path").strip() or "/Shared/al-ghurair-procurement"
CAN_RUN_GROUP = dbutils.widgets.get("can_run_group").strip() or "users"

if not CATALOG or not SCHEMA:
    raise ValueError("Set the catalog and schema widgets")
if not WAREHOUSE_ID:
    raise ValueError(
        "Set warehouse_id. In SQL Warehouses, open the warehouse and copy the ID "
        "from the URL (.../sql/warehouses/<id>)."
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Schema ready: {CATALOG}.{SCHEMA}")
print(f"Warehouse: {WAREHOUSE_ID}")
print(f"Genie folder: {PARENT_PATH}")
print("Al Ghurair Group - Bid Evaluation Demo")
'''

GENIE_CELL = r'''
# COMMAND ----------
# DBTITLE 1,Create or update the six Genie spaces
import json

from databricks.sdk import WorkspaceClient

SPACE_BUNDLE = json.loads(r"""__SPACE_BUNDLE_JSON__""")


def render_definition(definition: dict) -> dict:
    payload = json.dumps(definition)
    payload = payload.replace("__CATALOG__", CATALOG).replace("__SCHEMA__", SCHEMA)
    return json.loads(payload)


w = WorkspaceClient()
w.workspace.mkdirs(PARENT_PATH)

listed = w.api_client.do("GET", "/api/2.0/genie/spaces")
existing = {space.get("title"): space for space in listed.get("spaces", [])}

results = []
for key, spec in SPACE_BUNDLE.items():
    definition = render_definition(spec["serialized_space"])
    body = {
        "title": spec["title"],
        "description": spec["description"],
        "warehouse_id": WAREHOUSE_ID,
        "parent_path": PARENT_PATH,
        "serialized_space": json.dumps(definition),
    }
    current = existing.get(spec["title"])
    if current and current.get("space_id"):
        space_id = current["space_id"]
        try:
            created = w.api_client.do(
                "PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body
            )
            action = "updated"
        except Exception as patch_exc:
            created = w.api_client.do(
                "POST", f"/api/2.0/genie/spaces/{space_id}", body=body
            )
            action = f"updated (POST fallback after PATCH: {patch_exc})"
    else:
        created = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
        action = "created"
        space_id = created.get("space_id")

    space_id = created.get("space_id") or (current or {}).get("space_id")
    if CAN_RUN_GROUP and space_id:
        try:
            w.api_client.do(
                "PATCH",
                f"/api/2.0/permissions/genie/{space_id}",
                body={
                    "access_control_list": [
                        {
                            "group_name": CAN_RUN_GROUP,
                            "permission_level": "CAN_RUN",
                        }
                    ]
                },
            )
        except Exception as perm_exc:
            print(f"Could not grant CAN_RUN on {spec['title']}: {perm_exc}")

    host = (w.config.host or "").rstrip("/")
    url = f"{host}/genie/rooms/{space_id}" if space_id else ""
    results.append(
        {
            "key": key,
            "title": spec["title"],
            "action": action,
            "space_id": space_id,
            "url": url,
        }
    )
    print(f"{action}: {spec['title']} -> {url}")

print(
    f"""
Installation complete.
Data:   {CATALOG}.{SCHEMA}
Genie:  {PARENT_PATH}
"""
)
display(spark.createDataFrame(results))
'''


def main() -> None:
    bundle_json = json.dumps(space_bundle(), ensure_ascii=False, indent=2)
    genie = GENIE_CELL.replace("__SPACE_BUNDLE_JSON__", bundle_json)
    OUTPUT.write_text(HEADER + "\n" + data_cells() + genie, encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
