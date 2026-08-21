# GenieOne Setup

Reusable installer for the Al Ghurair procurement bid-evaluation demo. One
command deploys six Genie spaces, a serverless setup job, and 16 synthetic
Unity Catalog tables.

## Install

Prerequisites:

- Databricks CLI with Genie Space bundle support (v1.3.0 or newer)
- An authenticated Databricks CLI profile
- An existing Unity Catalog and SQL warehouse
- Permission to create schemas, tables, jobs, and Genie spaces

```bash
git clone https://github.com/mwissad/GenieOne_Setup.git
cd GenieOne_Setup

./install.sh \
  --profile YOUR_PROFILE \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --catalog YOUR_CATALOG
```

Optional arguments:

```text
--schema NAME          Default: bid_evaluation_demo
--can-run-group NAME   Default: users
--target NAME          Default: production
--validate-only        Validate without deploying
```

The installer checks access, renders all table references for the target
catalog and schema, validates and deploys the bundle, then runs the data setup
job. It is safe to rerun: the job recreates the synthetic tables and the bundle
updates its managed resources.

## Included Genie spaces

- Bid Management
- Financial Analysis
- Supplier Intelligence
- Market Intelligence
- Contract Performance
- Risk Assessment

The spaces are installed under `/Shared/al-ghurair-procurement` and use the
warehouse supplied to `install.sh`.

## Included data

The serverless setup job creates:

- Supplier: `suppliers`, `supplier_ratings`, `supplier_certifications`
- Bid: `rfp_projects`, `bids`, `bid_line_items`
- Financial: `pricing_benchmarks`, `budget_allocations`
- Risk: `risk_scores`, `compliance_checks`, `supplier_financial_health`
- Contract: `historical_contracts`, `sla_performance`, `delivery_metrics`
- Market: `market_competitor_bids`, `market_indices`

All records are synthetic and intended for demos only.

## Repository structure

```text
install.sh                 Complete installer
databricks.yml             Bundle resources and targets
src/data_generator.py      Parameterized synthetic data notebook
templates/                 Canonical Genie templates
scripts/render_assets.py   Renders target catalog/schema references
scripts/export_source_assets.py
                            Refreshes templates from the source workspace
docs/                      Architecture and troubleshooting
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the component model and
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for common deployment
issues.
