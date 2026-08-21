# GenieOne Setup

Reusable installer for the Al Ghurair procurement bid-evaluation demo.

One command creates:

- 16 synthetic Unity Catalog tables
- 6 Genie agents
- A serverless setup job
- The supporting documentation in this repo

The data, Genie spaces, and docs are installed together. You do not need a
separate data job after deploy.

## How to install

### 1. Install the Databricks CLI (v1.3.0 or newer)

Genie Space bundle resources require CLI **v1.3.0+**.

macOS:

```bash
brew tap databricks/tap
brew install databricks
databricks version
```

Linux / other:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks version
```

If `databricks version` reports `v0.x`, upgrade before continuing.

### 2. Authenticate to the target workspace

```bash
databricks auth login https://YOUR-WORKSPACE-HOST --profile YOUR_PROFILE
databricks current-user me --profile YOUR_PROFILE
```

Use that same profile name in the installer.

### 3. Collect three required values

| Value | How to find it |
| --- | --- |
| CLI profile | The name you used in `databricks auth login` |
| Catalog | An existing Unity Catalog you can write to |
| Warehouse ID | SQL warehouse used by all six Genie spaces |

List catalogs:

```bash
databricks catalogs list --profile YOUR_PROFILE
```

List warehouses and copy the `id` field:

```bash
databricks warehouses list --profile YOUR_PROFILE
```

You also need permission to create schemas, tables, jobs, and Genie spaces.

### 4. Clone the repo

```bash
git clone https://github.com/mwissad/GenieOne_Setup.git
cd GenieOne_Setup
chmod +x install.sh
```

### 5. Run the installer

```bash
./install.sh \
  --profile YOUR_PROFILE \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --catalog YOUR_CATALOG
```

This is the full install. It:

1. Checks CLI version, authentication, catalog, and warehouse
2. Renders the six Genie definitions for `YOUR_CATALOG.bid_evaluation_demo`
3. Validates the Databricks Asset Bundle
4. Deploys the six Genie spaces and the setup job
5. Runs the job that creates all 16 tables

Optional arguments:

```text
--schema NAME          Schema to create (default: bid_evaluation_demo)
--can-run-group NAME   Group granted CAN_RUN (default: users)
--target NAME          Bundle target (default: production)
--validate-only        Check the bundle without deploying
```

Example with a custom schema and group:

```bash
./install.sh \
  --profile YOUR_PROFILE \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --catalog YOUR_CATALOG \
  --schema bid_evaluation_demo \
  --can-run-group users
```

Dry run (no deploy):

```bash
./install.sh \
  --profile YOUR_PROFILE \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --catalog YOUR_CATALOG \
  --validate-only
```

The installer is safe to rerun. Tables are overwritten with the same
synthetic dataset, and bundle resources are updated in place.

### 6. Verify

After a successful run you should see:

```text
Installation complete.
Data:   YOUR_CATALOG.bid_evaluation_demo
Genie:  /Shared/al-ghurair-procurement
Target: production
```

Confirm in the workspace:

- Tables in `YOUR_CATALOG.bid_evaluation_demo`
- Six Genie spaces under `/Shared/al-ghurair-procurement`

## Included Genie spaces

- Bid Management
- Financial Analysis
- Supplier Intelligence
- Market Intelligence
- Contract Performance
- Risk Assessment

All six share the SQL warehouse you pass to `install.sh`.

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
docs/                      Architecture and troubleshooting
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the component model and
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) if install fails.
