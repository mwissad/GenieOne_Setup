# Databricks notebook source
# mypy: ignore-errors
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup - Imports & Configuration
# Bid Evaluation Demo - Al Ghurair Group (UAE)
# Generates fake data for multiple Genie agents serving a Bid Manager

import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configuration
dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "bid_evaluation_demo", "Schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()

if not CATALOG or not SCHEMA:
    raise ValueError("Both catalog and schema parameters are required")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Schema ready: {CATALOG}.{SCHEMA}")
print("Al Ghurair Group - Bid Evaluation Demo Data Generator")

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