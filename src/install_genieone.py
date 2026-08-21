# Databricks notebook source
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


# COMMAND ----------
# DBTITLE 1,Supplier Intelligence Agent - Suppliers & Ratings
# ============================================================
# GENIE AGENT 1: SUPPLIER INTELLIGENCE
# Serves: Supplier profiles, ratings, certifications, capacity
# ============================================================

import random
from datetime import datetime, timedelta

random.seed(42)

# UAE-specific suppliers for Al Ghurair's industries (construction, food, real estate)
suppliers_data = [
    # Construction & Infrastructure
    ("SUP-001", "Al Jaber Engineering", "Construction & Infrastructure", "Abu Dhabi", "UAE", "Tier 1", "Large", 2500, 1998, "active"),
    ("SUP-002", "Drake & Scull International", "MEP Services", "Dubai", "UAE", "Tier 1", "Large", 3200, 1996, "active"),
    ("SUP-003", "Khansaheb Civil Engineering", "Civil Works", "Dubai", "UAE", "Tier 1", "Large", 1800, 1935, "active"),
    ("SUP-004", "Al Naboodah Construction", "General Contracting", "Dubai", "UAE", "Tier 1", "Large", 4000, 1958, "active"),
    ("SUP-005", "Trojan General Contracting", "Building Construction", "Abu Dhabi", "UAE", "Tier 2", "Medium", 1200, 2003, "active"),
    ("SUP-006", "Gulf Precast Concrete", "Precast Manufacturing", "Ajman", "UAE", "Tier 2", "Medium", 600, 2007, "active"),
    ("SUP-007", "Emirates Steel Industries", "Steel Supply", "Abu Dhabi", "UAE", "Tier 1", "Large", 2800, 2001, "active"),
    ("SUP-008", "National Cement Company", "Cement & Aggregates", "Al Ain", "UAE", "Tier 1", "Large", 900, 1977, "active"),
    ("SUP-009", "Al Shirawi Contracting", "Fit-out & Interiors", "Dubai", "UAE", "Tier 2", "Medium", 750, 2005, "active"),
    ("SUP-010", "Consolidated Contractors Co", "Heavy Construction", "Dubai", "UAE", "Tier 1", "Large", 5000, 1952, "active"),
    # MEP & Electrical
    ("SUP-011", "Al Futtaim Engineering", "Electrical Systems", "Dubai", "UAE", "Tier 1", "Large", 1500, 1992, "active"),
    ("SUP-012", "Voltas MEP", "HVAC & Plumbing", "Dubai", "UAE", "Tier 2", "Medium", 800, 2010, "active"),
    ("SUP-013", "Leminar Air Conditioning", "HVAC Solutions", "Dubai", "UAE", "Tier 2", "Medium", 450, 1999, "active"),
    # IT & Technology
    ("SUP-014", "Etisalat Digital", "IT Infrastructure", "Abu Dhabi", "UAE", "Tier 1", "Large", 6000, 2016, "active"),
    ("SUP-015", "Gulf Business Machines", "Systems Integration", "Dubai", "UAE", "Tier 2", "Medium", 350, 2000, "active"),
    # Facilities Management
    ("SUP-016", "Emrill Services", "Facilities Management", "Dubai", "UAE", "Tier 1", "Large", 7500, 2002, "active"),
    ("SUP-017", "Farnek Services", "FM & Environment", "Dubai", "UAE", "Tier 1", "Large", 5000, 1980, "active"),
    ("SUP-018", "Imdaad LLC", "Integrated FM", "Dubai", "UAE", "Tier 2", "Medium", 3000, 2007, "active"),
    # Food & FMCG (Al Ghurair Foods context)
    ("SUP-019", "Al Khaleej Sugar", "Raw Materials - Sugar", "Dubai", "UAE", "Tier 1", "Large", 400, 1995, "active"),
    ("SUP-020", "Agthia Group", "Flour & Ingredients", "Abu Dhabi", "UAE", "Tier 1", "Large", 2000, 2004, "active"),
    # International suppliers
    ("SUP-021", "Samsung C&T Corporation", "EPC Contractor", "Seoul", "South Korea", "Tier 1", "Large", 15000, 1938, "active"),
    ("SUP-022", "Larsen & Toubro Ltd", "Engineering & Construction", "Mumbai", "India", "Tier 1", "Large", 45000, 1946, "active"),
    ("SUP-023", "Siemens Energy", "Power & Energy Systems", "Munich", "Germany", "Tier 1", "Large", 30000, 1847, "active"),
    ("SUP-024", "BESIX Group", "Construction & Concessions", "Brussels", "Belgium", "Tier 1", "Large", 12000, 1909, "active"),
    ("SUP-025", "China State Construction", "General Contracting", "Beijing", "China", "Tier 1", "Large", 80000, 1982, "active"),
]

suppliers_schema = StructType([
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("specialization", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("tier", StringType()),
    StructField("company_size", StringType()),
    StructField("employee_count", IntegerType()),
    StructField("established_year", IntegerType()),
    StructField("status", StringType()),
])

df_suppliers = spark.createDataFrame(suppliers_data, schema=suppliers_schema)

# Supplier Ratings (quarterly assessments)
ratings_data = []
for sup in suppliers_data:
    for quarter in ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4", "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2"]:
        base_quality = random.uniform(3.2, 4.9)
        ratings_data.append((
            sup[0], quarter,
            round(base_quality, 2),  # quality_score
            round(random.uniform(3.0, 5.0), 2),  # delivery_score
            round(random.uniform(3.0, 5.0), 2),  # communication_score
            round(random.uniform(2.8, 5.0), 2),  # value_score
            round(random.uniform(3.0, 5.0), 2),  # safety_score
            round((base_quality + random.uniform(-0.3, 0.3)), 2),  # overall_score
            random.choice(["Mehdi Wissad", "Ahmed Al Ghurair", "Fatima Hassan", "Ravi Patel", "Sarah Chen"]),
        ))

ratings_schema = StructType([
    StructField("supplier_id", StringType()),
    StructField("assessment_quarter", StringType()),
    StructField("quality_score", FloatType()),
    StructField("delivery_score", FloatType()),
    StructField("communication_score", FloatType()),
    StructField("value_score", FloatType()),
    StructField("safety_score", FloatType()),
    StructField("overall_score", FloatType()),
    StructField("assessed_by", StringType()),
])

df_ratings = spark.createDataFrame(ratings_data, schema=ratings_schema)

# Supplier Certifications
cert_types = ["ISO 9001:2015", "ISO 14001:2015", "ISO 45001:2018", "OHSAS 18001", "Dubai Municipality Approved", "Abu Dhabi DOT Certified", "ESMA Certified", "ICV Certified", "Green Building Certified", "CBUAE Compliant"]

certs_data = []
for sup in suppliers_data:
    num_certs = random.randint(2, 6)
    selected_certs = random.sample(cert_types, num_certs)
    for cert in selected_certs:
        issue_date = datetime(random.randint(2020, 2024), random.randint(1, 12), random.randint(1, 28))
        expiry_date = issue_date + timedelta(days=random.randint(365, 1095))
        certs_data.append((
            sup[0], cert, issue_date.strftime("%Y-%m-%d"), expiry_date.strftime("%Y-%m-%d"),
            "active" if expiry_date > datetime(2026, 8, 1) else "expired",
            random.choice(["Bureau Veritas", "DNV GL", "TUV SUD", "SGS", "Intertek"])
        ))

certs_schema = StructType([
    StructField("supplier_id", StringType()),
    StructField("certification_name", StringType()),
    StructField("issue_date", StringType()),
    StructField("expiry_date", StringType()),
    StructField("cert_status", StringType()),
    StructField("issuing_body", StringType()),
])

df_certs = spark.createDataFrame(certs_data, schema=certs_schema)

# Write tables
df_suppliers.write.mode("overwrite").saveAsTable("suppliers")
df_ratings.write.mode("overwrite").saveAsTable("supplier_ratings")
df_certs.write.mode("overwrite").saveAsTable("supplier_certifications")

print(f"✓ Suppliers: {df_suppliers.count()} records")
print(f"✓ Supplier Ratings: {df_ratings.count()} records")
print(f"✓ Supplier Certifications: {df_certs.count()} records")

# COMMAND ----------

# DBTITLE 1,Bid Management Agent - RFPs & Bids
# ============================================================
# GENIE AGENT 2: BID MANAGEMENT
# Serves: RFP projects, bids submitted, bid line items, evaluations
# ============================================================

random.seed(123)

# Al Ghurair Group RFP Projects (realistic UAE projects)
rfp_projects = [
    ("RFP-2026-001", "Al Ghurair HQ Tower Renovation - DIFC", "Renovation & Fit-out", "Al Ghurair Properties", "Dubai", 45000000, "2026-01-15", "2026-03-15", "evaluation", "high"),
    ("RFP-2026-002", "Deira Waterfront Mixed-Use Development", "New Construction", "Al Ghurair Real Estate", "Dubai", 320000000, "2026-02-01", "2026-04-30", "evaluation", "critical"),
    ("RFP-2026-003", "Al Ghurair Foods Factory Expansion - KIZAD", "Industrial Construction", "Al Ghurair Foods", "Abu Dhabi", 85000000, "2026-01-20", "2026-03-20", "shortlisted", "high"),
    ("RFP-2026-004", "Smart Building IoT Infrastructure", "Technology", "Al Ghurair Properties", "Dubai", 12000000, "2026-03-01", "2026-05-01", "open", "medium"),
    ("RFP-2026-005", "Facilities Management - Portfolio Wide", "Services", "Al Ghurair Properties", "Dubai", 28000000, "2026-02-15", "2026-04-15", "evaluation", "high"),
    ("RFP-2026-006", "MEP Works - Reem Island Residential Tower", "MEP", "Al Ghurair Real Estate", "Abu Dhabi", 55000000, "2026-03-10", "2026-05-30", "open", "high"),
    ("RFP-2026-007", "ERP System Upgrade & Integration", "IT Services", "Al Ghurair Group IT", "Dubai", 8500000, "2026-04-01", "2026-06-01", "draft", "medium"),
    ("RFP-2026-008", "Warehouse Automation - Jebel Ali", "Logistics & Automation", "Al Ghurair Foods", "Dubai", 22000000, "2026-02-20", "2026-04-20", "evaluation", "high"),
    ("RFP-2026-009", "Solar Panel Installation - Industrial Portfolio", "Sustainability", "Al Ghurair Energy", "Dubai", 35000000, "2026-03-15", "2026-06-15", "open", "medium"),
    ("RFP-2026-010", "Interior Fit-out - Al Ghurair Centre Mall", "Retail Fit-out", "Al Ghurair Properties", "Dubai", 18000000, "2026-01-10", "2026-02-28", "awarded", "medium"),
    ("RFP-2025-011", "Structural Steel Supply - Dubai Creek Tower", "Materials Supply", "Al Ghurair Construction", "Dubai", 42000000, "2025-10-01", "2025-12-15", "awarded", "critical"),
    ("RFP-2025-012", "Security Systems Upgrade", "Security", "Al Ghurair Properties", "Dubai", 6500000, "2025-11-15", "2026-01-15", "awarded", "low"),
]

rfp_schema = StructType([
    StructField("rfp_id", StringType()),
    StructField("project_name", StringType()),
    StructField("category", StringType()),
    StructField("business_unit", StringType()),
    StructField("location", StringType()),
    StructField("estimated_budget_aed", LongType()),
    StructField("issue_date", StringType()),
    StructField("deadline", StringType()),
    StructField("status", StringType()),
    StructField("priority", StringType()),
])

df_rfps = spark.createDataFrame(rfp_projects, schema=rfp_schema)

# Bids submitted against RFPs
bids_data = []
bid_counter = 1
for rfp in rfp_projects:
    if rfp[8] in ["evaluation", "shortlisted", "awarded"]:
        num_bids = random.randint(3, 7)
        bidding_suppliers = random.sample(suppliers_data, num_bids)
        for sup in bidding_suppliers:
            bid_amount = rfp[5] * random.uniform(0.85, 1.25)
            submission_date = datetime.strptime(rfp[7], "%Y-%m-%d") - timedelta(days=random.randint(1, 15))
            bids_data.append((
                f"BID-{bid_counter:04d}",
                rfp[0],
                sup[0],
                sup[1],
                round(bid_amount, 2),
                "AED",
                submission_date.strftime("%Y-%m-%d"),
                random.randint(12, 36),  # proposed_duration_months
                random.choice(["submitted", "under_review", "shortlisted", "rejected", "awarded"]),
                round(random.uniform(55, 98), 1),  # technical_score
                round(random.uniform(60, 95), 1),  # commercial_score
                round(random.uniform(50, 100), 1),  # overall_weighted_score
                random.choice(["fixed_price", "cost_plus", "unit_rate", "lump_sum"]),
            ))
            bid_counter += 1

bids_schema = StructType([
    StructField("bid_id", StringType()),
    StructField("rfp_id", StringType()),
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("bid_amount_aed", DoubleType()),
    StructField("currency", StringType()),
    StructField("submission_date", StringType()),
    StructField("proposed_duration_months", IntegerType()),
    StructField("bid_status", StringType()),
    StructField("technical_score", FloatType()),
    StructField("commercial_score", FloatType()),
    StructField("overall_weighted_score", FloatType()),
    StructField("pricing_model", StringType()),
])

df_bids = spark.createDataFrame(bids_data, schema=bids_schema)

# Bid Line Items (detailed breakdown)
line_items_data = []
work_packages = {
    "New Construction": ["Piling & Foundation", "Structural Frame", "Facade & Cladding", "MEP Rough-in", "Interior Fit-out", "External Works", "Preliminaries"],
    "Renovation & Fit-out": ["Demolition", "Structural Modifications", "M&E Works", "Finishes", "FF&E", "IT Infrastructure", "Preliminaries"],
    "MEP": ["HVAC Systems", "Electrical LV", "Electrical HV", "Plumbing & Drainage", "Fire Fighting", "BMS Controls", "Testing & Commissioning"],
    "Technology": ["Hardware Supply", "Software Licensing", "Installation", "Integration", "Training", "Support & Maintenance"],
    "Services": ["Hard FM", "Soft FM", "Specialized Services", "Emergency Response", "Sustainability Programs"],
    "Industrial Construction": ["Site Preparation", "Steel Structure", "Process Equipment", "Utilities", "Clean Room Fit-out", "Commissioning"],
    "Logistics & Automation": ["Conveyor Systems", "Robotic Arms", "WMS Software", "Racking Systems", "Integration & Testing"],
    "Sustainability": ["Solar Panels", "Inverters & BOS", "Installation Labor", "Grid Connection", "Monitoring System"],
    "Retail Fit-out": ["Structural Works", "Flooring", "Ceiling & Lighting", "Shopfront", "M&E Services"],
    "Materials Supply": ["Structural Steel", "Connectors & Fasteners", "Surface Treatment", "Delivery & Logistics"],
    "Security": ["CCTV Systems", "Access Control", "Intrusion Detection", "Command Center", "Installation"],
}

line_id = 1
for bid in bids_data:
    rfp_cat = next((r[2] for r in rfp_projects if r[0] == bid[1]), "Services")
    packages = work_packages.get(rfp_cat, ["General Works", "Preliminaries", "Contingency"])
    total_bid = bid[4]
    remaining = total_bid
    for i, pkg in enumerate(packages):
        if i == len(packages) - 1:
            pkg_amount = remaining
        else:
            pkg_amount = total_bid * random.uniform(0.08, 0.30)
            remaining -= pkg_amount
        line_items_data.append((
            f"LI-{line_id:05d}", bid[0], bid[1], pkg, round(pkg_amount, 2), "AED",
            random.randint(1, 6),  # estimated_months
            random.choice(["local", "imported", "mixed"]),  # sourcing
        ))
        line_id += 1

line_items_schema = StructType([
    StructField("line_item_id", StringType()),
    StructField("bid_id", StringType()),
    StructField("rfp_id", StringType()),
    StructField("work_package", StringType()),
    StructField("amount_aed", DoubleType()),
    StructField("currency", StringType()),
    StructField("estimated_duration_months", IntegerType()),
    StructField("sourcing_type", StringType()),
])

df_line_items = spark.createDataFrame(line_items_data, schema=line_items_schema)

df_rfps.write.mode("overwrite").saveAsTable("rfp_projects")
df_bids.write.mode("overwrite").saveAsTable("bids")
df_line_items.write.mode("overwrite").saveAsTable("bid_line_items")

print(f"✓ RFP Projects: {df_rfps.count()} records")
print(f"✓ Bids: {df_bids.count()} records")
print(f"✓ Bid Line Items: {df_line_items.count()} records")

# COMMAND ----------

# DBTITLE 1,Financial Analysis Agent - Pricing & Budgets
# ============================================================
# GENIE AGENT 3: FINANCIAL ANALYSIS
# Serves: Pricing benchmarks, cost breakdowns, budget tracking
# ============================================================

random.seed(456)

# Pricing Benchmarks (UAE market rates)
benchmark_categories = [
    ("Concrete Works", "AED/m³", 850, 1400),
    ("Structural Steel", "AED/ton", 5500, 8500),
    ("Facade - Curtain Wall", "AED/m²", 1200, 2800),
    ("Facade - Stone Cladding", "AED/m²", 800, 1600),
    ("MEP - HVAC", "AED/m²", 280, 550),
    ("MEP - Electrical", "AED/m²", 180, 380),
    ("MEP - Plumbing", "AED/m²", 120, 280),
    ("Interior Fit-out - Grade A", "AED/m²", 1500, 3500),
    ("Interior Fit-out - Standard", "AED/m²", 600, 1200),
    ("Piling - Bored", "AED/lin.m", 2500, 5000),
    ("Landscaping", "AED/m²", 150, 450),
    ("IT Infrastructure - Cat6", "AED/point", 350, 750),
    ("Solar PV Installation", "AED/kWp", 3200, 5500),
    ("Fire Fighting Systems", "AED/m²", 80, 180),
    ("Facilities Management", "AED/m²/year", 45, 120),
    ("Security Systems", "AED/m²", 60, 150),
    ("Waterproofing", "AED/m²", 80, 200),
    ("Painting & Decorating", "AED/m²", 25, 75),
    ("Floor Finishes - Marble", "AED/m²", 400, 1200),
    ("Floor Finishes - Porcelain", "AED/m²", 150, 400),
]

benchmarks_data = []
for cat, unit, low, high in benchmark_categories:
    for year in [2024, 2025, 2026]:
        for quarter in ["Q1", "Q2", "Q3", "Q4"]:
            if year == 2026 and quarter in ["Q3", "Q4"]:
                continue
            inflation = 1 + (year - 2024) * 0.035  # ~3.5% annual inflation
            benchmarks_data.append((
                cat, unit, f"{year}-{quarter}",
                round(low * inflation * random.uniform(0.95, 1.05), 2),
                round(high * inflation * random.uniform(0.95, 1.05), 2),
                round((low + high) / 2 * inflation * random.uniform(0.97, 1.03), 2),
                "Dubai" if random.random() > 0.3 else "Abu Dhabi",
                random.choice(["Turner & Townsend", "AECOM", "Faithful+Gould", "Currie & Brown", "RLB"]),
            ))

benchmarks_schema = StructType([
    StructField("category", StringType()),
    StructField("unit", StringType()),
    StructField("period", StringType()),
    StructField("low_rate", DoubleType()),
    StructField("high_rate", DoubleType()),
    StructField("median_rate", DoubleType()),
    StructField("market", StringType()),
    StructField("source", StringType()),
])

df_benchmarks = spark.createDataFrame(benchmarks_data, schema=benchmarks_schema)

# Budget Allocations per project
budget_data = []
for rfp in rfp_projects:
    total_budget = rfp[5]
    allocations = [
        ("Construction / Direct Costs", 0.65),
        ("Consultancy & Design", 0.08),
        ("Project Management", 0.05),
        ("Contingency", 0.10),
        ("Authority Fees & Permits", 0.03),
        ("Insurance & Bonds", 0.02),
        ("Escalation Provision", 0.04),
        ("Owner's Reserve", 0.03),
    ]
    for alloc_name, pct in allocations:
        allocated = total_budget * pct * random.uniform(0.9, 1.1)
        spent = allocated * random.uniform(0.0, 0.7) if rfp[8] in ["evaluation", "awarded"] else 0
        budget_data.append((
            rfp[0], rfp[1], alloc_name,
            round(allocated, 2), round(spent, 2), round(allocated - spent, 2),
            round(pct * 100, 1),
            "2026",
        ))

budget_schema = StructType([
    StructField("rfp_id", StringType()),
    StructField("project_name", StringType()),
    StructField("cost_category", StringType()),
    StructField("allocated_amount_aed", DoubleType()),
    StructField("spent_amount_aed", DoubleType()),
    StructField("remaining_amount_aed", DoubleType()),
    StructField("allocation_pct", FloatType()),
    StructField("fiscal_year", StringType()),
])

df_budgets = spark.createDataFrame(budget_data, schema=budget_schema)

df_benchmarks.write.mode("overwrite").saveAsTable("pricing_benchmarks")
df_budgets.write.mode("overwrite").saveAsTable("budget_allocations")

print(f"✓ Pricing Benchmarks: {df_benchmarks.count()} records")
print(f"✓ Budget Allocations: {df_budgets.count()} records")

# COMMAND ----------

# DBTITLE 1,Risk Assessment Agent - Risk Scores & Compliance
# ============================================================
# GENIE AGENT 4: RISK ASSESSMENT
# Serves: Risk scores, compliance checks, financial health
# ============================================================

random.seed(789)

# Risk Scores per supplier per project
risk_categories = ["Financial Risk", "Delivery Risk", "Quality Risk", "Safety Risk", "Geopolitical Risk", "Capacity Risk", "Subcontractor Risk", "Legal/Compliance Risk"]

risk_data = []
for rfp in rfp_projects:
    if rfp[8] in ["evaluation", "shortlisted", "awarded"]:
        bidding_sups = random.sample(suppliers_data, random.randint(3, 6))
        for sup in bidding_sups:
            for risk_cat in risk_categories:
                # UAE local suppliers generally lower risk
                base_risk = random.uniform(1, 8) if sup[4] == "UAE" else random.uniform(2, 9)
                risk_data.append((
                    rfp[0], sup[0], sup[1], risk_cat,
                    round(base_risk, 1),  # risk_score (1-10, 10=highest risk)
                    random.choice(["low", "medium", "high", "critical"]) if base_risk > 6 else random.choice(["low", "medium"]),
                    random.choice([
                        "Historical performance within acceptable range",
                        "Minor concerns noted in recent audit",
                        "Strong track record in similar projects",
                        "Requires additional due diligence",
                        "ICV score meets threshold",
                        "Cash flow concerns identified",
                        "Excellent safety record",
                        "Subcontractor dependencies noted",
                        "Capacity constraints during peak season",
                    ]),
                    "2026-Q2",
                    random.choice(["Mehdi Wissad", "Omar Al Mazrouei", "Priya Sharma"]),
                ))

risk_schema = StructType([
    StructField("rfp_id", StringType()),
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("risk_category", StringType()),
    StructField("risk_score", FloatType()),
    StructField("risk_level", StringType()),
    StructField("assessment_notes", StringType()),
    StructField("assessment_period", StringType()),
    StructField("assessed_by", StringType()),
])

df_risks = spark.createDataFrame(risk_data, schema=risk_schema)

# Compliance Checks
compliance_items = [
    "Trade License Valid", "DEWA NOC", "Civil Defense Approval", "DM Health & Safety",
    "Labour Card Compliance", "WPS Compliance", "Insurance Coverage", "Bank Guarantee",
    "ICV Certificate", "Anti-Bribery Declaration", "Conflict of Interest Declaration",
    "Environmental Compliance", "Data Protection (PDPL)", "Emiratisation Quota",
    "VAT Registration", "RERA Registration", "JAFZA License"
]

compliance_data = []
for sup in suppliers_data:
    applicable_checks = random.sample(compliance_items, random.randint(8, 14))
    for check in applicable_checks:
        passed = random.random() > 0.12  # 88% pass rate
        compliance_data.append((
            sup[0], sup[1], check,
            "passed" if passed else random.choice(["failed", "pending", "expired"]),
            f"2026-{random.randint(1,8):02d}-{random.randint(1,28):02d}",
            None if passed else random.choice([
                "Document expired - renewal in progress",
                "Pending government authority response",
                "Non-conformance identified - corrective action required",
                "Awaiting updated documentation",
            ]),
            random.choice(["Compliance Team", "Procurement Team", "External Auditor"]),
        ))

compliance_schema = StructType([
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("compliance_item", StringType()),
    StructField("status", StringType()),
    StructField("check_date", StringType()),
    StructField("notes", StringType()),
    StructField("verified_by", StringType()),
])

df_compliance = spark.createDataFrame(compliance_data, schema=compliance_schema)

# Financial Health Indicators
financial_health_data = []
for sup in suppliers_data:
    revenue_base = sup[7] * random.uniform(200000, 500000)  # estimate from employee count
    financial_health_data.append((
        sup[0], sup[1],
        round(revenue_base, 0),  # annual_revenue_aed
        round(random.uniform(1.1, 3.5), 2),  # current_ratio
        round(random.uniform(0.2, 0.8), 2),  # debt_to_equity
        round(random.uniform(0.03, 0.18), 2),  # profit_margin
        round(random.uniform(25, 120), 0),  # days_payable
        round(random.uniform(30, 90), 0),  # days_receivable
        random.choice(["A+", "A", "A-", "B+", "B", "B-", "C+"]),  # credit_rating
        round(random.uniform(5000000, 200000000), 0),  # bonding_capacity_aed
        random.choice(["Dun & Bradstreet", "S&P", "Moody's", "Local Bank Assessment"]),
        "2026-Q2",
    ))

financial_schema = StructType([
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("annual_revenue_aed", DoubleType()),
    StructField("current_ratio", FloatType()),
    StructField("debt_to_equity", FloatType()),
    StructField("profit_margin", FloatType()),
    StructField("days_payable_outstanding", FloatType()),
    StructField("days_receivable_outstanding", FloatType()),
    StructField("credit_rating", StringType()),
    StructField("bonding_capacity_aed", DoubleType()),
    StructField("rating_source", StringType()),
    StructField("assessment_period", StringType()),
])

df_financial = spark.createDataFrame(financial_health_data, schema=financial_schema)

df_risks.write.mode("overwrite").saveAsTable("risk_scores")
df_compliance.write.mode("overwrite").saveAsTable("compliance_checks")
df_financial.write.mode("overwrite").saveAsTable("supplier_financial_health")

print(f"✓ Risk Scores: {df_risks.count()} records")
print(f"✓ Compliance Checks: {df_compliance.count()} records")
print(f"✓ Financial Health: {df_financial.count()} records")

# COMMAND ----------

# DBTITLE 1,Contract Performance Agent - Historical Delivery
# ============================================================
# GENIE AGENT 5: CONTRACT PERFORMANCE
# Serves: Historical contracts, SLA performance, delivery metrics
# ============================================================

random.seed(321)

# Historical Contracts (past 5 years with Al Ghurair)
historical_contracts = [
    ("CON-2021-001", "SUP-001", "Al Jaber Engineering", "Al Ghurair Centre Extension", 28000000, "2021-03-15", "2022-09-30", "completed", 18, 20, "Dubai"),
    ("CON-2021-002", "SUP-004", "Al Naboodah Construction", "Staff Accommodation - DIP", 15000000, "2021-06-01", "2022-06-30", "completed", 12, 12, "Dubai"),
    ("CON-2022-001", "SUP-002", "Drake & Scull International", "HVAC Upgrade - Al Ghurair Towers", 8500000, "2022-01-10", "2022-12-15", "completed", 11, 13, "Dubai"),
    ("CON-2022-002", "SUP-010", "Consolidated Contractors Co", "Parking Structure - Deira", 42000000, "2022-04-01", "2023-10-30", "completed", 18, 19, "Dubai"),
    ("CON-2022-003", "SUP-016", "Emrill Services", "FM Contract - Properties Portfolio", 12000000, "2022-07-01", "2024-06-30", "completed", 24, 24, "Dubai"),
    ("CON-2023-001", "SUP-007", "Emirates Steel Industries", "Steel Supply - Mixed Use Tower", 35000000, "2023-02-01", "2024-02-28", "completed", 12, 14, "Abu Dhabi"),
    ("CON-2023-002", "SUP-011", "Al Futtaim Engineering", "Electrical Fit-out - Office Tower", 6200000, "2023-05-15", "2024-01-31", "completed", 8, 9, "Dubai"),
    ("CON-2023-003", "SUP-022", "Larsen & Toubro Ltd", "Industrial Facility - KIZAD", 95000000, "2023-03-01", "2025-03-31", "completed", 24, 26, "Abu Dhabi"),
    ("CON-2023-004", "SUP-003", "Khansaheb Civil Engineering", "Road Works & Utilities", 18000000, "2023-08-01", "2024-08-31", "completed", 12, 12, "Dubai"),
    ("CON-2024-001", "SUP-005", "Trojan General Contracting", "Residential Villa Cluster", 22000000, "2024-01-15", "2025-07-30", "completed", 18, 19, "Abu Dhabi"),
    ("CON-2024-002", "SUP-014", "Etisalat Digital", "Network Infrastructure Upgrade", 4500000, "2024-03-01", "2024-09-30", "completed", 6, 7, "Dubai"),
    ("CON-2024-003", "SUP-017", "Farnek Services", "FM - Al Ghurair Centre", 8000000, "2024-04-01", "2026-03-31", "active", 24, None, "Dubai"),
    ("CON-2024-004", "SUP-009", "Al Shirawi Contracting", "Retail Fit-out Phase 1", 11000000, "2024-06-01", "2025-03-31", "completed", 9, 10, "Dubai"),
    ("CON-2025-001", "SUP-024", "BESIX Group", "High-Rise Foundation Works", 55000000, "2025-01-15", "2026-06-30", "active", 17, None, "Dubai"),
    ("CON-2025-002", "SUP-021", "Samsung C&T Corporation", "Tower Superstructure", 180000000, "2025-03-01", "2027-12-31", "active", 33, None, "Dubai"),
    ("CON-2025-003", "SUP-006", "Gulf Precast Concrete", "Precast Supply - Towers", 14000000, "2025-04-01", "2026-04-30", "active", 12, None, "Ajman"),
]

contracts_schema = StructType([
    StructField("contract_id", StringType()),
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("project_name", StringType()),
    StructField("contract_value_aed", LongType()),
    StructField("start_date", StringType()),
    StructField("end_date", StringType()),
    StructField("status", StringType()),
    StructField("planned_duration_months", IntegerType()),
    StructField("actual_duration_months", IntegerType()),
    StructField("location", StringType()),
])

df_contracts = spark.createDataFrame(historical_contracts, schema=contracts_schema)

# SLA Performance (monthly tracking)
sla_metrics = ["Response Time", "Defect Rectification", "Safety Incidents", "Progress Reporting", "Workforce Availability", "Material Quality", "Site Cleanliness"]

sla_data = []
for con in historical_contracts:
    months_active = con[8] if con[9] is None else con[9]
    start = datetime.strptime(con[5], "%Y-%m-%d")
    for m in range(min(months_active, 24)):  # cap at 24 months of records
        month_date = start + timedelta(days=30 * m)
        for metric in sla_metrics:
            target = random.uniform(85, 99)
            actual = target * random.uniform(0.85, 1.05)
            sla_data.append((
                con[0], con[1], con[2], metric,
                month_date.strftime("%Y-%m"),
                round(target, 1),
                round(min(actual, 100), 1),
                "met" if actual >= target else "breached",
                round(max(0, actual - target), 1) if actual >= target else round(target - actual, 1),
            ))

sla_schema = StructType([
    StructField("contract_id", StringType()),
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("sla_metric", StringType()),
    StructField("month", StringType()),
    StructField("target_pct", FloatType()),
    StructField("actual_pct", FloatType()),
    StructField("status", StringType()),
    StructField("variance_pct", FloatType()),
])

df_sla = spark.createDataFrame(sla_data, schema=sla_schema)

# Delivery Metrics (variation orders, claims, penalties)
delivery_data = []
for con in historical_contracts:
    num_vos = random.randint(0, 8)
    total_vo_value = 0
    for vo in range(num_vos):
        vo_value = con[4] * random.uniform(0.01, 0.08)
        total_vo_value += vo_value
        delivery_data.append((
            con[0], con[1], con[2], "variation_order",
            f"VO-{vo+1:02d}", round(vo_value, 2),
            random.choice(["approved", "pending", "rejected"]),
            random.choice(["scope_change", "design_change", "unforeseen_conditions", "client_request"]),
        ))
    # Penalties
    if con[9] and con[9] > con[8]:  # delayed
        delay_penalty = con[4] * 0.001 * (con[9] - con[8]) * 30  # 0.1% per day
        delivery_data.append((
            con[0], con[1], con[2], "delay_penalty",
            "DP-01", round(delay_penalty, 2), "applied", "schedule_delay",
        ))

delivery_schema = StructType([
    StructField("contract_id", StringType()),
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("item_type", StringType()),
    StructField("item_ref", StringType()),
    StructField("value_aed", DoubleType()),
    StructField("status", StringType()),
    StructField("reason", StringType()),
])

df_delivery = spark.createDataFrame(delivery_data, schema=delivery_schema)

df_contracts.write.mode("overwrite").saveAsTable("historical_contracts")
df_sla.write.mode("overwrite").saveAsTable("sla_performance")
df_delivery.write.mode("overwrite").saveAsTable("delivery_metrics")

print(f"✓ Historical Contracts: {df_contracts.count()} records")
print(f"✓ SLA Performance: {df_sla.count()} records")
print(f"✓ Delivery Metrics: {df_delivery.count()} records")

# COMMAND ----------

# DBTITLE 1,Market Intelligence Agent - Market Rates & Competitor Analysis
# ============================================================
# GENIE AGENT 6: MARKET INTELLIGENCE
# Serves: Market rates, competitor bids, industry trends
# ============================================================

random.seed(654)

# Market Intelligence - Competitor bid analysis (anonymized)
competitor_data = []
project_types = ["High-Rise Tower", "Mixed-Use Development", "Industrial Facility", "Retail Mall", "Residential Complex", "Office Building", "Hotel", "Warehouse"]
emirates = ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "RAK"]

for i in range(150):
    project_type = random.choice(project_types)
    gfa = random.randint(5000, 200000)  # gross floor area
    base_rate = {
        "High-Rise Tower": random.uniform(4500, 8000),
        "Mixed-Use Development": random.uniform(4000, 7000),
        "Industrial Facility": random.uniform(2000, 4000),
        "Retail Mall": random.uniform(5000, 9000),
        "Residential Complex": random.uniform(3500, 6000),
        "Office Building": random.uniform(4000, 7500),
        "Hotel": random.uniform(6000, 12000),
        "Warehouse": random.uniform(1500, 3000),
    }[project_type]
    total_value = gfa * base_rate
    award_date = datetime(random.randint(2023, 2026), random.randint(1, 12), random.randint(1, 28))
    
    competitor_data.append((
        f"MKT-{i+1:04d}",
        project_type,
        random.choice(emirates),
        gfa,
        round(base_rate, 2),  # rate_per_sqm_aed
        round(total_value, 2),  # total_contract_value_aed
        random.randint(3, 8),  # num_bidders
        round(total_value * random.uniform(0.85, 0.95), 2),  # winning_bid_aed
        round(total_value * random.uniform(1.0, 1.15), 2),  # highest_bid_aed
        round(total_value * random.uniform(0.75, 0.88), 2),  # lowest_bid_aed
        random.choice(["Tier 1 - International", "Tier 1 - Local", "Tier 2 - Local", "JV"]),  # winner_type
        award_date.strftime("%Y-%m-%d"),
        random.choice(["private", "government", "semi-government"]),  # client_type
        random.randint(12, 48),  # duration_months
    ))

competitor_schema = StructType([
    StructField("record_id", StringType()),
    StructField("project_type", StringType()),
    StructField("emirate", StringType()),
    StructField("gfa_sqm", IntegerType()),
    StructField("rate_per_sqm_aed", DoubleType()),
    StructField("estimated_value_aed", DoubleType()),
    StructField("num_bidders", IntegerType()),
    StructField("winning_bid_aed", DoubleType()),
    StructField("highest_bid_aed", DoubleType()),
    StructField("lowest_bid_aed", DoubleType()),
    StructField("winner_profile", StringType()),
    StructField("award_date", StringType()),
    StructField("client_type", StringType()),
    StructField("duration_months", IntegerType()),
])

df_competitor = spark.createDataFrame(competitor_data, schema=competitor_schema)

# Market Trends & Indices
trend_data = []
indices = ["Construction Cost Index", "Steel Price Index", "Cement Price Index", "Labour Cost Index", "Equipment Rental Index", "Energy Cost Index"]

for idx in indices:
    base_value = 100  # base year 2023
    for year in range(2023, 2027):
        for month in range(1, 13):
            if year == 2026 and month > 8:
                break
            monthly_change = random.uniform(-0.5, 1.2)
            base_value = max(80, base_value + monthly_change)
            trend_data.append((
                idx, f"{year}-{month:02d}",
                round(base_value, 2),
                round(monthly_change, 2),
                round((base_value - 100) / 100 * 100, 2),  # cumulative_change_pct
                "UAE National",
            ))

trend_schema = StructType([
    StructField("index_name", StringType()),
    StructField("month", StringType()),
    StructField("index_value", DoubleType()),
    StructField("monthly_change_pct", DoubleType()),
    StructField("cumulative_change_pct", DoubleType()),
    StructField("market", StringType()),
])

df_trends = spark.createDataFrame(trend_data, schema=trend_schema)

df_competitor.write.mode("overwrite").saveAsTable("market_competitor_bids")
df_trends.write.mode("overwrite").saveAsTable("market_indices")

print(f"✓ Market Competitor Bids: {df_competitor.count()} records")
print(f"✓ Market Indices: {df_trends.count()} records")

# COMMAND ----------

# DBTITLE 1,Summary - All Tables Created
# ============================================================
# SUMMARY: All tables created for Bid Evaluation Demo
# ============================================================

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").select("tableName")
display(tables)

print(f"""
╔══════════════════════════════════════════════════════════════╗
║   AL GHURAIR GROUP - BID EVALUATION DEMO DATA READY        ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  GENIE AGENT 1: Supplier Intelligence                        ║
║    → suppliers, supplier_ratings, supplier_certifications     ║
║                                                              ║
║  GENIE AGENT 2: Bid Management                               ║
║    → rfp_projects, bids, bid_line_items                      ║
║                                                              ║
║  GENIE AGENT 3: Financial Analysis                           ║
║    → pricing_benchmarks, budget_allocations                  ║
║                                                              ║
║  GENIE AGENT 4: Risk Assessment                              ║
║    → risk_scores, compliance_checks, supplier_financial_health║
║                                                              ║
║  GENIE AGENT 5: Contract Performance                         ║
║    → historical_contracts, sla_performance, delivery_metrics ║
║                                                              ║
║  GENIE AGENT 6: Market Intelligence                          ║
║    → market_competitor_bids, market_indices                   ║
║                                                              ║
║  Schema: {CATALOG}.{SCHEMA}
║  Company: Al Ghurair Group (UAE)                             ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------
# DBTITLE 1,Create or update the six Genie spaces
import json

from databricks.sdk import WorkspaceClient

SPACE_BUNDLE = json.loads(r"""{
  "bid_management": {
    "title": "Bid Management Agent - Al Ghurair",
    "description": "You are the Bid Management Agent for Al Ghurair Group's procurement team. You are the primary interface for bid managers evaluating active procurement opportunities across all Al Ghurair business units.\n\nYour role is to provide insights on:\n- Active RFP pipeline: status tracking across draft, open, evaluation, shortlisted, awarded stages\n- Bid submissions: who bid, how much, technical/commercial scores, pricing models\n- Bid line item breakdowns by work package\n- Comparative analysis between bidders on the same RFP\n- Scoring and ranking of bids\n\nKey business rules:\n- All monetary values are in AED (UAE Dirhams)\n- RFP statuses flow: draft → open → evaluation → shortlisted → awarded\n- Priority levels: critical, high, medium, low\n- Overall weighted score = combination of technical_score and commercial_score (typically 60/40 or 70/30 weighting)\n- Pricing models: fixed_price, cost_plus, unit_rate, lump_sum\n- \"Bid spread\" = highest bid - lowest bid for the same RFP\n- \"Bid premium\" = (bid_amount - estimated_budget) / estimated_budget * 100\n- Business units: Al Ghurair Properties, Al Ghurair Real Estate, Al Ghurair Foods, Al Ghurair Group IT, Al Ghurair Energy, Al Ghurair Construction\n- The \"Deira Waterfront\" project (RFP-2026-002) is the current critical priority evaluation\n- A bid is \"competitive\" if it is within 10% of the estimated budget\n- Sourcing types in line items: local, imported, mixed",
    "serialized_space": {
      "version": 2,
      "data_sources": {
        "tables": [
          {
            "identifier": "__CATALOG__.__SCHEMA__.bid_line_items"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.bids"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.rfp_projects"
          }
        ]
      }
    }
  },
  "financial_analysis": {
    "title": "Financial Analysis Agent - Al Ghurair",
    "description": "You are the Financial Analysis Agent for Al Ghurair Group's procurement team. You help bid managers validate commercial proposals against market benchmarks and track project budget consumption.\n\nYour role is to provide insights on:\n- UAE construction market pricing benchmarks by category (concrete, steel, facade, MEP, fit-out, piling, solar, FM, etc.)\n- Rate trends over time with inflation tracking (~3.5% annual in UAE construction)\n- Budget allocation and spend tracking per project\n- Cost category breakdowns: Direct Costs, Consultancy, PM, Contingency, Fees, Insurance, Escalation, Owner's Reserve\n- Market comparisons between Dubai and Abu Dhabi\n\nKey business rules:\n- All monetary values are in AED (UAE Dirhams)\n- Pricing benchmarks are sourced from: Turner & Townsend, AECOM, Faithful+Gould, Currie & Brown, RLB\n- Periods follow format: YYYY-QN (e.g., 2026-Q1)\n- \"Current rate\" means the most recent period available in the data\n- Budget utilization % = spent_amount_aed / allocated_amount_aed * 100\n- Remaining budget = allocated_amount_aed - spent_amount_aed\n- A bid line item rate is \"above market\" if it exceeds the high_rate benchmark for that category\n- A bid line item rate is \"below market\" if it is below the low_rate benchmark (potential scope gap risk)\n- Contingency allocation is typically 10% of total project budget\n- Dubai and Abu Dhabi markets can differ by 10-20% on rates",
    "serialized_space": {
      "version": 2,
      "data_sources": {
        "tables": [
          {
            "identifier": "__CATALOG__.__SCHEMA__.budget_allocations"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.pricing_benchmarks"
          }
        ]
      }
    }
  },
  "supplier_intelligence": {
    "title": "Supplier Intelligence Agent - Al Ghurair",
    "description": "You are the Supplier Intelligence Agent for Al Ghurair Group's procurement team. You provide comprehensive supplier profiles, performance ratings, and certification status to help bid managers assess supplier capability and reliability.\n\nYour role is to provide insights on:\n- Supplier profiles: specialization, location, tier, size, employee count, establishment year\n- Quarterly performance ratings: quality, delivery, communication, value, safety, and overall scores\n- Certification tracking: ISO standards, UAE government approvals, ICV, environmental compliance\n- Rating trends over time to identify improving or declining suppliers\n- Supplier comparisons across multiple dimensions\n\nKey business rules:\n- All scores are on a 1-5 scale (5 = excellent)\n- Suppliers are classified as Tier 1 (strategic, large-scale) or Tier 2 (specialist, medium-scale)\n- Company sizes: Large (>1000 employees), Medium (200-1000)\n- Assessment quarters follow format: YYYY-QN (e.g., 2025-Q3)\n- A \"top-rated\" supplier has overall_score >= 4.5\n- A supplier is \"at risk\" if their overall_score dropped by more than 0.5 points in consecutive quarters\n- Certification status: \"active\" (valid and current) or \"expired\" (past expiry date)\n- Key UAE certifications: ICV Certified (mandatory for government work), Dubai Municipality Approved, Abu Dhabi DOT Certified, ESMA Certified\n- Issuing bodies: Bureau Veritas, DNV GL, TUV SUD, SGS, Intertek\n- Local suppliers (country = 'UAE') are preferred for ICV scoring purposes\n- Assessors include the procurement team leads: Mehdi Wissad, Ahmed Al Ghurair, Fatima Hassan, Ravi Patel, Sarah Chen",
    "serialized_space": {
      "version": 2,
      "config": {
        "sample_questions": [
          {
            "id": "01f1957d066e1056b89fa3829ba20c0b",
            "question": [
              "What's the overall rating trend for Al Naboodah Construction over the last 6 quarters?"
            ]
          },
          {
            "id": "01f1957d069111588d889980bbfe0125",
            "question": [
              "Which Tier 1 suppliers have all certifications current and valid?"
            ]
          },
          {
            "id": "01f1957d06b4187180ccf8ec2e2b1e8d",
            "question": [
              "Compare delivery and safety scores between our top 5 rated suppliers"
            ]
          },
          {
            "id": "01f1957d06da1aa08da75da73e497994",
            "question": [
              "Show me suppliers established before 2000 with more than 2000 employees"
            ]
          },
          {
            "id": "01f1957d07041f019f0fc2dc4abf085d",
            "question": [
              "Which suppliers have ISO 45001 certification expiring in the next 12 months?"
            ]
          },
          {
            "id": "01f1957d1de5102893d10374ab27a5d9",
            "question": [
              "Show me all bids for the Deira Waterfront project ranked by overall weighted score"
            ]
          },
          {
            "id": "01f1957d1e0a1664a7787537c40a241f",
            "question": [
              "What's the spread between highest and lowest bid for each active RFP?"
            ]
          },
          {
            "id": "01f1957d1e30195795697b289c1e2bb1",
            "question": [
              "Which suppliers proposed lump sum pricing vs cost-plus for RFP-2026-002?"
            ]
          },
          {
            "id": "01f1957d1e54159ea4f1cfb7f5bc396f",
            "question": [
              "List all RFPs in evaluation status with their total number of bids received"
            ]
          },
          {
            "id": "01f1957d1e7814a690c10647c9be8853",
            "question": [
              "Break down bid line items for the top-scoring bid on the Deira Waterfront project"
            ]
          },
          {
            "id": "01f1957d288416a5a1484226c6e9b5c1",
            "question": [
              "What's the current market rate for structural steel and concrete in Dubai?"
            ]
          },
          {
            "id": "01f1957d28a810efacf90b71b09a3c63",
            "question": [
              "How have MEP HVAC rates trended over the last 2 years in the Dubai market?"
            ]
          },
          {
            "id": "01f1957d28cf1ac1b0e75cbc831fb110",
            "question": [
              "Show me budget allocation breakdown for the Deira Waterfront project"
            ]
          },
          {
            "id": "01f1957d28f311a6a0de000f6b1ce813",
            "question": [
              "Which projects have spent more than 50% of their contingency allocation?"
            ]
          },
          {
            "id": "01f1957d2916135894c05543ff5b3a7e",
            "question": [
              "Compare median rates for interior fit-out Grade A vs Standard across all periods"
            ]
          },
          {
            "id": "01f1957d787d17c982034e962ecbae35",
            "question": [
              "Show me SLA breach rate by supplier across all historical contracts"
            ]
          },
          {
            "id": "01f1957d78a313a5aac63fc71fd66b9e",
            "question": [
              "Which suppliers delivered on time vs had delays, and what penalties were applied?"
            ]
          },
          {
            "id": "01f1957d78c719fb96d3efc165dfeffd",
            "question": [
              "What was the total value of variation orders for Larsen & Toubro on the KIZAD project?"
            ]
          },
          {
            "id": "01f1957d78f01b1bae3350b925dbcddb",
            "question": [
              "Compare average SLA performance for safety incidents across all active contracts"
            ]
          },
          {
            "id": "01f1957d79141d2bb500284c35d61418",
            "question": [
              "Which contracts had more than 5 variation orders and what were the reasons?"
            ]
          },
          {
            "id": "01f1957d83201be495aecaa8b4ac62a1",
            "question": [
              "What's the average winning bid discount to estimated value for mixed-use developments in Dubai?"
            ]
          },
          {
            "id": "01f1957d834417bda4172f3517b4b387",
            "question": [
              "How many bidders typically compete for projects above 300M AED?"
            ]
          },
          {
            "id": "01f1957d83681fefafc261e7286fe3b1",
            "question": [
              "What profile of contractor wins government vs private projects?"
            ]
          },
          {
            "id": "01f1957d838f1371a91d9c2ad2772262",
            "question": [
              "Show me the construction cost index and steel price index trend over the last 12 months"
            ]
          },
          {
            "id": "01f1957d83b4103eb28dd444e9cb65d9",
            "question": [
              "What's the average project duration for high-rise towers vs warehouses in the UAE?"
            ]
          }
        ]
      },
      "data_sources": {
        "tables": [
          {
            "identifier": "__CATALOG__.__SCHEMA__.supplier_certifications"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.supplier_ratings"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.suppliers"
          }
        ]
      }
    }
  },
  "market_intelligence": {
    "title": "Market Intelligence Agent - Al Ghurair",
    "description": "You are the Market Intelligence Agent for Al Ghurair Group's procurement team. You help bid managers understand competitive market dynamics in the UAE construction and infrastructure sector.\n\nYour role is to provide insights on:\n- Competitor bid analysis: winning bid discounts, bid spreads, number of bidders\n- Market rates and construction cost indices over time\n- Project benchmarking by type (high-rise, mixed-use, industrial, retail, hotel, warehouse)\n- Winner profiles by tier (Tier 1 International, Tier 1 Local, Tier 2, JV) and client type (private, government, semi-government)\n- Geographic trends across emirates (Dubai, Abu Dhabi, Sharjah, Ajman, RAK)\n\nKey business rules:\n- All monetary values are in AED (UAE Dirhams)\n- \"Winning bid discount\" = (estimated_value - winning_bid) / estimated_value * 100\n- Market indices use 2023 as base year (index = 100)\n- Project types: High-Rise Tower, Mixed-Use Development, Industrial Facility, Retail Mall, Residential Complex, Office Building, Hotel, Warehouse\n- When comparing rates, always specify the emirate as markets vary significantly between Dubai and Abu Dhabi",
    "serialized_space": {
      "version": 2,
      "config": {
        "sample_questions": [
          {
            "id": "01f1957d9c461736aa463232f851d416",
            "question": [
              "Show me all bids for the Deira Waterfront project ranked by overall weighted score"
            ]
          },
          {
            "id": "01f1957d9c6a1e94860ff0ba6a60b947",
            "question": [
              "What's the spread between highest and lowest bid for each active RFP?"
            ]
          },
          {
            "id": "01f1957d9c921df5ab5ec98f9e931af4",
            "question": [
              "Which suppliers proposed lump sum pricing vs cost-plus for RFP-2026-002?"
            ]
          },
          {
            "id": "01f1957d9cb61c9f90bacca7ceefde21",
            "question": [
              "List all RFPs in evaluation status with their total number of bids received"
            ]
          },
          {
            "id": "01f1957d9cda1b03a842413d6d67ba52",
            "question": [
              "Break down bid line items for the top-scoring bid on the Deira Waterfront project"
            ]
          }
        ]
      },
      "data_sources": {
        "tables": [
          {
            "identifier": "__CATALOG__.__SCHEMA__.market_competitor_bids"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.market_indices"
          }
        ]
      },
      "instructions": {
        "join_specs": [
          {
            "id": "01f1957e289018bdb2ec38c85536d33f",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.historical_contracts"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.sla_performance"
            },
            "sql": [
              "historical_contracts.contract_id = sla_performance.contract_id",
              "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"
            ],
            "comment": [
              "Contracts to SLA Performance"
            ]
          },
          {
            "id": "01f1957e28901bb88ad0b94981b96325",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.historical_contracts"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.delivery_metrics"
            },
            "sql": [
              "historical_contracts.contract_id = delivery_metrics.contract_id",
              "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"
            ],
            "comment": [
              "Contracts to Delivery Metrics"
            ]
          },
          {
            "id": "01f19580c9191f5cb3d86b882c120bc9",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.risk_scores"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.supplier_financial_health"
            },
            "sql": [
              "risk_scores.supplier_id = supplier_financial_health.supplier_id",
              "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"
            ],
            "comment": [
              "Risk Scores to Financial Health"
            ]
          },
          {
            "id": "01f19580c91a125aa95e34cd01646b00",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.risk_scores"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.compliance_checks"
            },
            "sql": [
              "risk_scores.supplier_id = compliance_checks.supplier_id",
              "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_MANY--"
            ],
            "comment": [
              "Risk Scores to Compliance Checks"
            ]
          },
          {
            "id": "01f1958100d41bee89e144ce187a8c55",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.rfp_projects"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.bids"
            },
            "sql": [
              "rfp_projects.rfp_id = bids.rfp_id",
              "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"
            ],
            "comment": [
              "RFPs to Bids"
            ]
          },
          {
            "id": "01f1958100d41f3ba76c4959210cb986",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.bids"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.bid_line_items"
            },
            "sql": [
              "bids.bid_id = bid_line_items.bid_id",
              "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"
            ],
            "comment": [
              "Bids to Line Items"
            ]
          },
          {
            "id": "01f1958121761051afdca4625577c0a8",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.suppliers"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.supplier_ratings"
            },
            "sql": [
              "suppliers.supplier_id = supplier_ratings.supplier_id",
              "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"
            ],
            "comment": [
              "Suppliers to Ratings"
            ]
          },
          {
            "id": "01f19581217613c9a6f2178f26b6b955",
            "left": {
              "identifier": "__CATALOG__.__SCHEMA__.suppliers"
            },
            "right": {
              "identifier": "__CATALOG__.__SCHEMA__.supplier_certifications"
            },
            "sql": [
              "suppliers.supplier_id = supplier_certifications.supplier_id",
              "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"
            ],
            "comment": [
              "Suppliers to Certifications"
            ]
          }
        ],
        "sql_snippets": {
          "filters": [
            {
              "id": "01f1957e289a1296b134e607f1555a62",
              "sql": [
                "sla_performance.status = 'breached'"
              ],
              "display_name": "SLA Breached"
            },
            {
              "id": "01f1957e289a1531bec32eee11c6561b",
              "sql": [
                "delivery_metrics.item_type = 'variation_order'"
              ],
              "display_name": "Variation Orders Only"
            },
            {
              "id": "01f1957e289a178090f1db119457cc98",
              "sql": [
                "delivery_metrics.item_type = 'delay_penalty'"
              ],
              "display_name": "Delay Penalties Only"
            },
            {
              "id": "01f19580c9221ab4bbe8953d5d43e6f9",
              "sql": [
                "risk_scores.risk_level IN ('high', 'critical')"
              ],
              "display_name": "High or Critical Risk"
            },
            {
              "id": "01f19580c9221de6b0d0abb110d38922",
              "sql": [
                "compliance_checks.status IN ('failed', 'expired')"
              ],
              "display_name": "Failed or Expired Compliance"
            },
            {
              "id": "01f19580ea83126cbf0cf96ec511a98e",
              "sql": [
                "pricing_benchmarks.period = (SELECT MAX(period) FROM __CATALOG__.__SCHEMA__.pricing_benchmarks)"
              ],
              "display_name": "Current Period Rates"
            },
            {
              "id": "01f1958100db183ba37d23b7ca0924e7",
              "sql": [
                "rfp_projects.status = 'evaluation'"
              ],
              "display_name": "RFPs Under Evaluation"
            },
            {
              "id": "01f1958100db1a29bfe7fa7539ce199c",
              "sql": [
                "rfp_projects.priority IN ('critical', 'high')"
              ],
              "display_name": "Critical & High Priority RFPs"
            },
            {
              "id": "01f19581218611b896c206c2ba0cc0d7",
              "sql": [
                "suppliers.tier = 'Tier 1'"
              ],
              "display_name": "Tier 1 Suppliers Only"
            },
            {
              "id": "01f195812186167d98fd7c233d97bd3a",
              "sql": [
                "supplier_certifications.cert_status = 'active'"
              ],
              "display_name": "Active Certifications Only"
            },
            {
              "id": "01f1958121861ad1a450ac739c2bc38e",
              "sql": [
                "suppliers.country = 'UAE'"
              ],
              "display_name": "UAE Local Suppliers"
            }
          ],
          "expressions": [
            {
              "id": "01f1957e09041e07b3143bdb3c6cfdef",
              "sql": [
                "(market_competitor_bids.estimated_value_aed - market_competitor_bids.winning_bid_aed) / market_competitor_bids.estimated_value_aed * 100"
              ],
              "display_name": "Winning Bid Discount %"
            },
            {
              "id": "01f1957e090511199a1d8fdc49de52f6",
              "sql": [
                "market_competitor_bids.highest_bid_aed - market_competitor_bids.lowest_bid_aed"
              ],
              "display_name": "Bid Spread (Highest vs Lowest)"
            },
            {
              "id": "01f1957e28991eff9b0c00f5aaf235cf",
              "sql": [
                "(historical_contracts.actual_duration_months - historical_contracts.planned_duration_months) * 1.0 / historical_contracts.planned_duration_months * 100"
              ],
              "display_name": "Schedule Overrun %"
            },
            {
              "id": "01f19580c923121ca281b79a9336e534",
              "sql": [
                "CASE WHEN supplier_financial_health.current_ratio >= 1.5 AND supplier_financial_health.debt_to_equity <= 0.7 AND supplier_financial_health.credit_rating IN ('A+', 'A', 'A-') THEN 'Strong' WHEN supplier_financial_health.current_ratio >= 1.0 THEN 'Adequate' ELSE 'Weak' END"
              ],
              "display_name": "Financial Health Classification"
            },
            {
              "id": "01f19580ea82182b82eaa044695cddd1",
              "sql": [
                "budget_allocations.spent_amount_aed / budget_allocations.allocated_amount_aed * 100"
              ],
              "display_name": "Budget Utilization %"
            },
            {
              "id": "01f19580ea8314d5900befe7bfbaae9c",
              "sql": [
                "(pricing_benchmarks.high_rate - pricing_benchmarks.low_rate) / pricing_benchmarks.median_rate * 100"
              ],
              "display_name": "Rate Variance % (market spread)"
            },
            {
              "id": "01f1958100db12719c312b7b68fb87bc",
              "sql": [
                "(bids.bid_amount_aed - rfp_projects.estimated_budget_aed) / rfp_projects.estimated_budget_aed * 100"
              ],
              "display_name": "Bid Premium %"
            },
            {
              "id": "01f1958100db15d3a38358a53ab08aba",
              "sql": [
                "bids.technical_score * 0.6 + bids.commercial_score * 0.4"
              ],
              "display_name": "Weighted Score (60/40)"
            },
            {
              "id": "01f1958121861ff48e7d0f634aac71f7",
              "sql": [
                "CASE WHEN supplier_ratings.overall_score >= 4.5 THEN 'Top Rated' WHEN supplier_ratings.overall_score >= 3.5 THEN 'Good' WHEN supplier_ratings.overall_score >= 2.5 THEN 'Average' ELSE 'Below Average' END"
              ],
              "display_name": "Supplier Rating Tier"
            }
          ],
          "measures": [
            {
              "id": "01f1957e09051346a7c98379a4b71178",
              "sql": [
                "AVG(market_competitor_bids.num_bidders)"
              ],
              "display_name": "Average Number of Bidders"
            },
            {
              "id": "01f1957e0905155fa70276d8a6e44bfc",
              "sql": [
                "AVG((market_competitor_bids.estimated_value_aed - market_competitor_bids.winning_bid_aed) / market_competitor_bids.estimated_value_aed * 100)"
              ],
              "display_name": "Average Winning Bid Discount %"
            },
            {
              "id": "01f1957e090517a2b6d073f52d897987",
              "sql": [
                "market_indices.index_value - LAG(market_indices.index_value) OVER (PARTITION BY market_indices.index_name ORDER BY market_indices.month)"
              ],
              "display_name": "Year-over-Year Index Change"
            },
            {
              "id": "01f19580c9231046ad6c223c0cbae99e",
              "sql": [
                "AVG(risk_scores.risk_score)"
              ],
              "display_name": "Average Risk Score"
            },
            {
              "id": "01f19580ea821b3c8c369822fe5b4616",
              "sql": [
                "SUM(budget_allocations.allocated_amount_aed)"
              ],
              "display_name": "Total Budget Allocated"
            },
            {
              "id": "01f19580ea821da0a4d0d4ebb616ae24",
              "sql": [
                "SUM(budget_allocations.spent_amount_aed)"
              ],
              "display_name": "Total Budget Spent"
            },
            {
              "id": "01f19580ea83109ba1dedc19361f3490",
              "sql": [
                "SUM(budget_allocations.remaining_amount_aed)"
              ],
              "display_name": "Total Budget Remaining"
            },
            {
              "id": "01f1958100db1ee2a636f3e6ff9430a1",
              "sql": [
                "MAX(bids.bid_amount_aed) - MIN(bids.bid_amount_aed)"
              ],
              "display_name": "Bid Spread (Max - Min)"
            },
            {
              "id": "01f1958121861daa8685d661e8170b0b",
              "sql": [
                "AVG(supplier_ratings.overall_score)"
              ],
              "display_name": "Average Overall Rating"
            }
          ]
        }
      }
    }
  },
  "contract_performance": {
    "title": "Contract Performance Agent - Al Ghurair",
    "description": "You are the Contract Performance Agent for Al Ghurair Group's procurement team. You provide historical performance data on suppliers who have previously worked with Al Ghurair, helping bid managers make evidence-based decisions.\n\nYour role is to provide insights on:\n- Historical contract delivery: planned vs actual durations, delays\n- SLA performance: monthly tracking of response time, defect rectification, safety, workforce availability, material quality, site cleanliness, progress reporting\n- Variation orders (VOs): frequency, value, reasons (scope change, design change, unforeseen conditions, client request)\n- Delay penalties applied to suppliers\n- Overall contractor reliability and performance patterns\n\nKey business rules:\n- All monetary values are in AED (UAE Dirhams)\n- SLA status is \"met\" when actual_pct >= target_pct, \"breached\" otherwise\n- SLA breach rate = number of breached months / total months * 100\n- Delay penalty is typically 0.1% of contract value per day of delay\n- Variation orders can be \"approved\", \"pending\", or \"rejected\"\n- Schedule overrun % = (actual_duration - planned_duration) / planned_duration * 100\n- Contracts with status \"active\" have NULL actual_duration_months\n- A supplier with consistently low variance_pct on SLAs is considered reliable",
    "serialized_space": {
      "version": 2,
      "data_sources": {
        "tables": [
          {
            "identifier": "__CATALOG__.__SCHEMA__.delivery_metrics"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.historical_contracts"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.sla_performance"
          }
        ]
      }
    }
  },
  "risk_assessment": {
    "title": "Risk Assessment Agent - Al Ghurair",
    "description": "You are the Risk Assessment Agent for Al Ghurair Group's procurement team. You help bid managers evaluate supplier risk profiles, compliance status, and financial health to ensure sound procurement decisions.\n\nYour role is to provide insights on:\n- Multi-dimensional risk scoring across 8 categories: Financial Risk, Delivery Risk, Quality Risk, Safety Risk, Geopolitical Risk, Capacity Risk, Subcontractor Risk, Legal/Compliance Risk\n- UAE regulatory compliance: Trade License, DEWA NOC, Civil Defense, Labour/WPS compliance, ICV certification, Emiratisation, VAT, RERA, JAFZA, PDPL (data protection)\n- Supplier financial health: current ratio, debt-to-equity, profit margin, credit ratings, bonding capacity\n- Risk level classification and trends\n\nKey business rules:\n- Risk scores range from 1-10 (10 = highest risk). Scores above 6 are concerning.\n- Risk levels: \"low\" (1-3), \"medium\" (4-6), \"high\" (7-8), \"critical\" (9-10)\n- UAE local suppliers (country = 'UAE') generally carry lower geopolitical risk\n- Compliance status: \"passed\", \"failed\", \"pending\", \"expired\"\n- A supplier is \"fully compliant\" when ALL their compliance checks have status = 'passed'\n- Current ratio above 1.5 indicates good liquidity; below 1.0 is concerning\n- Debt-to-equity above 0.7 is considered high leverage\n- Credit ratings: A+ is best, C+ is worst\n- Bonding capacity must exceed the project value for the supplier to be eligible\n- ICV (In-Country Value) certification is mandatory for government contracts in the UAE",
    "serialized_space": {
      "version": 2,
      "config": {
        "sample_questions": [
          {
            "id": "01f1957ce8f61687a32608d5360e4fee",
            "question": [
              "Show me all high or critical risk scores for suppliers bidding on the Deira Waterfront project"
            ]
          },
          {
            "id": "01f1957ce91816c7a070ebe1ea0fef4d",
            "question": [
              "Which suppliers have failed or expired compliance checks in 2026?"
            ]
          },
          {
            "id": "01f1957ce93c1de1a2ce545bf9873bac",
            "question": [
              "Compare financial health — current ratio and credit rating — for all Tier 1 suppliers"
            ]
          },
          {
            "id": "01f1957ce9601a17945a4d262c737356",
            "question": [
              "What are the top risk categories across all active RFP evaluations?"
            ]
          },
          {
            "id": "01f1957ce9841fa2acde3afb9f629601",
            "question": [
              "Which suppliers have a debt-to-equity ratio above 0.6 and bonding capacity below 50M AED?"
            ]
          }
        ]
      },
      "data_sources": {
        "tables": [
          {
            "identifier": "__CATALOG__.__SCHEMA__.compliance_checks"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.risk_scores"
          },
          {
            "identifier": "__CATALOG__.__SCHEMA__.supplier_financial_health"
          }
        ]
      }
    }
  }
}""")


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
