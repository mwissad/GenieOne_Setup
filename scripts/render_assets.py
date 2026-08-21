#!/usr/bin/env python3
"""Render workspace-specific Genie definitions from committed templates."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str) -> None:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise SystemExit(
            f"{label} must contain only letters, numbers, and underscores "
            "and cannot start with a number"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--templates", type=Path, default=Path("templates"))
    parser.add_argument("--output", type=Path, default=Path(".generated"))
    args = parser.parse_args()

    validate_identifier(args.catalog, "catalog")
    validate_identifier(args.schema, "schema")
    args.output.mkdir(parents=True, exist_ok=True)

    rendered_count = 0
    for template_path in sorted(args.templates.glob("*.geniespace.json")):
        content = template_path.read_text(encoding="utf-8")
        content = content.replace("__CATALOG__", args.catalog)
        content = content.replace("__SCHEMA__", args.schema)
        definition = json.loads(content)

        output_path = args.output / template_path.name
        output_path.write_text(
            json.dumps(definition, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        rendered_count += 1

    if rendered_count != 6:
        raise SystemExit(
            f"Expected 6 Genie templates, rendered {rendered_count}"
        )

    print(
        f"Rendered {rendered_count} Genie spaces for "
        f"{args.catalog}.{args.schema}"
    )


if __name__ == "__main__":
    main()
