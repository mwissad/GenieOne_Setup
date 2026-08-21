# Architecture

## Deployment flow

There are two installers. Both create tables before Genie spaces.

### Notebook (`src/install_genieone.py`)

1. Widgets collect catalog, schema, warehouse, and folder.
2. The notebook creates the schema and overwrites the 16 synthetic tables.
3. It creates or updates the six Genie spaces with the workspace Genie API.

### CLI (`install.sh`)

1. `install.sh` verifies the target Databricks profile, catalog, and warehouse.
2. `scripts/render_assets.py` replaces the namespace tokens in the six
   committed Genie templates.
3. Databricks Asset Bundles deploys only the setup job.
4. The setup job runs `src/data_generator.py` on serverless compute and
   overwrites all 16 synthetic Delta tables.
5. Databricks Asset Bundles deploys the six Genie spaces after the tables
   exist. Genie rejects space creation when a referenced table is missing.

Regenerate the notebook after template changes:

```bash
python3 scripts/build_install_notebook.py
```

## Agent-to-data mapping

- Bid Management: `rfp_projects`, `bids`, `bid_line_items`
- Financial Analysis: `pricing_benchmarks`, `budget_allocations`
- Supplier Intelligence: `suppliers`, `supplier_ratings`,
  `supplier_certifications`
- Market Intelligence: `market_competitor_bids`, `market_indices`
- Contract Performance: `historical_contracts`, `sla_performance`,
  `delivery_metrics`
- Risk Assessment: `risk_scores`, `compliance_checks`,
  `supplier_financial_health`

## Portability

Workspace-specific values are supplied at installation time:

- Databricks CLI profile
- SQL warehouse ID
- Unity Catalog
- Schema
- Consumer group

The committed Genie files use `__CATALOG__.__SCHEMA__` tokens. Rendered files
are written to `.generated/`, which is intentionally excluded from Git.

## Source provenance

The data notebook and Genie definitions were exported from:

- Workspace: `https://adb-3642885996758754.14.azuredatabricks.net`
- Workspace project:
  `/Users/mehdi.wissad@databricks.com/al-ghurair-genie-bundle`
- Source namespace: `mehdi_wissad.bid_evaluation_demo`

`scripts/export_source_assets.py` can refresh the six templates when the source
Genie spaces change.
