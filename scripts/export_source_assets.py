#!/usr/bin/env python3
"""Export the canonical Al Ghurair Genie spaces from a Databricks workspace."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


SPACES = {
    "bid_management": "01f1957c932213ddb14107386cb53d9a",
    "financial_analysis": "01f1957c967119f79cc3ea6ba863e395",
    "supplier_intelligence": "01f1957c8fc715f3921016393739707d",
    "market_intelligence": "01f1957ca00d1ea8a0879760a46f9678",
    "contract_performance": "01f1957c9cdb125d99317cc8991aae99",
    "risk_assessment": "01f1957c99ac1a9b8896e3b8281ef4a9",
}

SOURCE_NAMESPACE = "mehdi_wissad.bid_evaluation_demo"
TARGET_NAMESPACE = "__CATALOG__.__SCHEMA__"


def export_space(profile: str, space_id: str) -> dict:
    command = [
        "databricks",
        "api",
        "get",
        f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
        "--profile",
        profile,
    ]
    response = subprocess.run(
        command, check=True, capture_output=True, text=True
    )
    return json.loads(response.stdout)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="inspire-deploy")
    parser.add_argument("--output", type=Path, default=Path("templates"))
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    metadata = {}

    for name, space_id in SPACES.items():
        response = export_space(args.profile, space_id)
        serialized = response["serialized_space"].replace(
            SOURCE_NAMESPACE, TARGET_NAMESPACE
        )
        definition = json.loads(serialized)
        output_path = args.output / f"{name}.geniespace.json"
        output_path.write_text(
            json.dumps(definition, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        metadata[name] = {
            "source_space_id": space_id,
            "title": response["title"],
            "description": response.get("description", ""),
        }
        print(f"Exported {response['title']} -> {output_path}")

    metadata_path = args.output / "spaces.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
