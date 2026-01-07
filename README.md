# Healthcare Analytics: OLTP to Star Schema

A comprehensive data engineering project demonstrating the transformation from normalized OLTP database design to an optimized OLAP star schema for healthcare analytics.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Step-by-Step Guide](#step-by-step-guide)
- [Understanding the Data](#understanding-the-data)
- [Key Files Reference](#key-files-reference)

---

## Overview

This project simulates a real-world data engineering scenario at HealthTech Analytics. It includes:

- **OLTP Schema**: Normalized (3NF) transactional database with 10 tables
- **OLAP Schema**: Optimized star schema with dimension tables, fact tables, and bridge tables
- **Data Generators**: Python scripts to generate 10,000 rows per table
- **Analysis Documents**: Query performance analysis, design decisions, and reflections

---

## Project Structure

```
Data_Modelling/
├── data/
│   ├── oltp/                    # OLTP INSERT statements (10 SQL files)
│   └── olap/                    # OLAP INSERT statements (11 SQL files)
│
├── oltp/
│   ├── schema/
│   │   └── oltp_schema.sql      # Normalized OLTP tables (3NF)
│   └── description.md           # OLTP design explanation
│
├── olap/
│   ├── schema/
│   │   └── star_schema.sql      # Star schema DDL
│   └── description.md           # OLAP design explanation
│
├── logs/
│   ├── oltp.log                 # Transaction logs
│   ├── etl.log                  # ETL transformation logs
│   └── error.log                # Error logs
│
├── generate_oltp_data.py        # OLTP data generator
├── generate_olap_data.py        # OLAP data generator
├── task.md                      # Original project requirements
├── query_analysis.txt           # Query performance analysis
├── design_decisions.txt         # Star schema design rationale
├── star_schema_queries.txt      # Optimized star schema queries
├── etl_design.txt               # ETL pipeline documentation
├── reflection.md                # Project analysis & learnings
└── README.md                    # This file
```

---

## Prerequisites

| Requirement | Version | Purpose |
|-------------|---------|---------|
| Python      | 3.8+    | Running data generators |
| SQL Database | MySQL/PostgreSQL/SQLite | Executing schema and queries |

> [!NOTE]
> No external Python packages are required. The generators use only built-in libraries (`random`, `datetime`, `os`).

---

## Quick Start

```bash
# 1. Navigate to project directory
cd "Data Modelling"

# 2. Generate OLTP data (creates data/oltp/*.sql files)
python generate_oltp_data.py

# 3. Generate OLAP data (creates data/olap/*.sql files)
python generate_olap_data.py

# 4. Execute SQL files in your preferred database
```

---

## Step-by-Step Guide

### Step 1: Generate OLTP Data

Run the OLTP data generator to create INSERT statements for all 10 normalized tables:

```bash
python generate_oltp_data.py
```

**Output**: Creates 10 SQL files in `data/oltp/`:
| File | Description | Rows |
|------|-------------|------|
| `patients.sql` | Patient demographics | 10,000 |
| `specialties.sql` | Medical specialties | 10,000 |
| `departments.sql` | Hospital departments | 10,000 |
| `providers.sql` | Healthcare providers | 10,000 |
| `diagnoses.sql` | ICD-10 diagnosis codes | 10,000 |
| `procedures.sql` | CPT procedure codes | 10,000 |
| `encounters.sql` | Patient encounters | 10,000 |
| `encounter_diagnoses.sql` | Encounter-diagnosis mapping | 10,000 |
| `encounter_procedures.sql` | Encounter-procedure mapping | 10,000 |
| `billing.sql` | Billing records | 10,000 |

---

### Step 2: Create OLTP Database

1. Open your SQL client (MySQL Workbench, pgAdmin, DBeaver, etc.)
2. Create a new database:
   ```sql
   CREATE DATABASE healthcare_oltp;
   USE healthcare_oltp;
   ```
3. Execute the schema: `oltp/schema/oltp_schema.sql`
4. Load the data by executing each file in `data/oltp/` in this order:
   1. `patients.sql`
   2. `specialties.sql`
   3. `departments.sql`
   4. `providers.sql`
   5. `diagnoses.sql`
   6. `procedures.sql`
   7. `encounters.sql`
   8. `encounter_diagnoses.sql`
   9. `encounter_procedures.sql`
   10. `billing.sql`

---

### Step 3: Run OLTP Query Analysis

Execute the queries from `query_analysis.txt` to experience performance issues:

```sql
-- Example: Monthly Encounters by Specialty (requires 4 JOINs)
SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY month, s.specialty_name, e.encounter_type;
```

---

### Step 4: Generate OLAP Data

Run the OLAP data generator to create INSERT statements for the star schema:

```bash
python generate_olap_data.py
```

**Output**: Creates 11 SQL files in `data/olap/`:
| File | Table Type | Description |
|------|-----------|-------------|
| `dim_date.sql` | Dimension | Calendar dates (2 years) |
| `dim_patient.sql` | Dimension | Patient dimension |
| `dim_specialty.sql` | Dimension | Specialty dimension |
| `dim_department.sql` | Dimension | Department dimension |
| `dim_encounter_type.sql` | Dimension | Encounter type dimension |
| `dim_diagnosis.sql` | Dimension | Diagnosis dimension |
| `dim_procedure.sql` | Dimension | Procedure dimension |
| `dim_provider.sql` | Dimension | Provider dimension |
| `fact_encounters.sql` | Fact | Central fact table |
| `bridge_encounter_diagnoses.sql` | Bridge | Many-to-many diagnoses |
| `bridge_encounter_procedures.sql` | Bridge | Many-to-many procedures |

---

### Step 5: Create OLAP Database

1. Create a new database:
   ```sql
   CREATE DATABASE healthcare_olap;
   USE healthcare_olap;
   ```
2. Execute the schema: `olap/schema/star_schema.sql`
3. Load dimension tables first (order matters!):
   1. `dim_date.sql`
   2. `dim_patient.sql`
   3. `dim_specialty.sql`
   4. `dim_department.sql`
   5. `dim_encounter_type.sql`
   6. `dim_diagnosis.sql`
   7. `dim_procedure.sql`
   8. `dim_provider.sql`
4. Then load the fact table: `fact_encounters.sql`
5. Finally load bridge tables:
   - `bridge_encounter_diagnoses.sql`
   - `bridge_encounter_procedures.sql`

---

### Step 6: Run Optimized Queries

Execute queries from `star_schema_queries.txt` and compare performance:

```sql
-- Same query on Star Schema (only 2 JOINs)
SELECT 
    d.year,
    d.month,
    s.specialty_name,
    et.encounter_type_name,
    SUM(f.encounter_count) AS total_encounters,
    COUNT(DISTINCT f.patient_key) AS unique_patients
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY d.year, d.month, s.specialty_name, et.encounter_type_name;
```

---

## Understanding the Data

### OLTP vs OLAP Comparison

| Aspect | OLTP (Normalized) | OLAP (Star Schema) |
|--------|-------------------|-------------------|
| **Purpose** | Transactional operations | Analytical queries |
| **Tables** | 10 normalized tables | 8 dimensions + 1 fact + 2 bridges |
| **Typical Joins** | 4-6 per query | 2-3 per query |
| **Query Speed** | Slower for analytics | ~10x faster |
| **Data Redundancy** | Minimal | Controlled duplication |

### Key Performance Improvements

| Query | OLTP Time | OLAP Time | Improvement |
|-------|-----------|-----------|-------------|
| Monthly Encounters | ~1.8s | ~150ms | **12x faster** |
| Diagnosis-Procedure Pairs | ~2.5s | ~200ms | **12x faster** |
| 30-Day Readmissions | ~3.2s | ~300ms | **10x faster** |
| Revenue by Specialty | ~1.5s | ~120ms | **12x faster** |

---

## Key Files Reference

| File | Purpose |
|------|---------|
| [task.md](./task.md) | Original project requirements |
| [design_decisions.txt](./design_decisions.txt) | Star schema design rationale |
| [query_analysis.txt](./query_analysis.txt) | OLTP query performance analysis |
| [star_schema_queries.txt](./star_schema_queries.txt) | Optimized OLAP queries |
| [etl_design.txt](./etl_design.txt) | ETL pipeline documentation |
| [reflection.md](./reflection.md) | Project learnings & analysis |
| [oltp/description.md](./oltp/description.md) | OLTP schema explanation |
| [olap/description.md](./olap/description.md) | OLAP schema explanation |

---

## Troubleshooting

> [!WARNING]
> **Foreign Key Errors**: Ensure you load tables in the correct order (dimensions before facts).

> [!TIP]
> **Large Data Sets**: If importing is slow, consider disabling indexes before bulk insert, then re-enable after.

```sql
-- Disable foreign key checks temporarily (MySQL)
SET FOREIGN_KEY_CHECKS = 0;
-- Run your INSERT statements
SET FOREIGN_KEY_CHECKS = 1;
```

---

## Author

Created as part of a Healthcare Analytics data engineering learning project.

---

*Last updated: January 2026*
