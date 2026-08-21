#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Install the complete GenieOne Al Ghurair demo.

Usage:
  ./install.sh --profile PROFILE --warehouse-id ID --catalog CATALOG [options]

Required:
  --profile NAME         Databricks CLI profile for the target workspace
  --warehouse-id ID      SQL warehouse used by all six Genie spaces
  --catalog NAME         Existing Unity Catalog

Options:
  --schema NAME          Schema to create (default: bid_evaluation_demo)
  --can-run-group NAME   Group granted CAN_RUN (default: users)
  --target NAME          Bundle target (default: production)
  --validate-only        Render and validate without deploying
  -h, --help             Show this help
EOF
}

PROFILE=""
WAREHOUSE_ID=""
CATALOG=""
SCHEMA="bid_evaluation_demo"
CAN_RUN_GROUP="users"
TARGET="production"
VALIDATE_ONLY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="${2:-}"; shift 2 ;;
    --warehouse-id) WAREHOUSE_ID="${2:-}"; shift 2 ;;
    --catalog) CATALOG="${2:-}"; shift 2 ;;
    --schema) SCHEMA="${2:-}"; shift 2 ;;
    --can-run-group) CAN_RUN_GROUP="${2:-}"; shift 2 ;;
    --target) TARGET="${2:-}"; shift 2 ;;
    --validate-only) VALIDATE_ONLY="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$PROFILE" || -z "$WAREHOUSE_ID" || -z "$CATALOG" ]]; then
  echo "Missing a required option." >&2
  usage >&2
  exit 2
fi

for command in databricks python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Required command not found: $command" >&2
    exit 1
  fi
done

CLI_VERSION="$(databricks version | awk '{print $NF}' | tr -d 'v')"
python3 - "$CLI_VERSION" <<'PY'
import re
import sys

current = tuple(int(value) for value in re.findall(r"\d+", sys.argv[1])[:3])
minimum = (1, 3, 0)
if current < minimum:
    raise SystemExit(
        f"Databricks CLI v{sys.argv[1]} is too old; v1.3.0+ is required "
        "for Genie Space bundle resources"
    )
PY

echo "[1/6] Checking Databricks authentication..."
databricks current-user me --profile "$PROFILE" >/dev/null

echo "[2/6] Checking target catalog and SQL warehouse..."
databricks catalogs get "$CATALOG" --profile "$PROFILE" >/dev/null
databricks warehouses get "$WAREHOUSE_ID" --profile "$PROFILE" >/dev/null

echo "[3/6] Rendering Genie definitions for ${CATALOG}.${SCHEMA}..."
python3 scripts/render_assets.py --catalog "$CATALOG" --schema "$SCHEMA"

BUNDLE_ARGS=(
  --profile "$PROFILE"
  --target "$TARGET"
  --var "warehouse_id=$WAREHOUSE_ID"
  --var "catalog=$CATALOG"
  --var "schema=$SCHEMA"
  --var "can_run_group=$CAN_RUN_GROUP"
)

echo "[4/6] Validating the Databricks Asset Bundle..."
databricks bundle validate "${BUNDLE_ARGS[@]}"

if [[ "$VALIDATE_ONLY" == "true" ]]; then
  echo "Validation successful. No resources were deployed."
  exit 0
fi

echo "[5/6] Deploying the setup job and six Genie spaces..."
databricks bundle deploy "${BUNDLE_ARGS[@]}"

echo "[6/6] Creating the 16 Unity Catalog tables..."
databricks bundle run setup_data "${BUNDLE_ARGS[@]}"

cat <<EOF

Installation complete.
Data:   ${CATALOG}.${SCHEMA}
Genie:  /Shared/al-ghurair-procurement
Target: ${TARGET}
EOF
