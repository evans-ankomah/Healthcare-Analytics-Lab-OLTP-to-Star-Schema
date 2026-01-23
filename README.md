# Healthcare Analytics: OLTP to Star Schema

A comprehensive data engineering project demonstrating the transformation from normalized OLTP database design to an optimized OLAP star schema for healthcare analytics.

---

## Table of Contents

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
- **Analysis Documents**: Query performance analysis, design decisions, and reflections

---

## Project Structure

```
Data_Modelling/
├── data/
│   ├── oltp/                    # OLTP INSERT statements (10 SQL files)
│   └── olap/                    # OLAP INSERT statements (11 SQL files)
│
├── oltp_schema/
│   ├── oltp_schema.sql          # Normalized OLTP tables (3NF)
│   └── description.md           # OLTP design explanation
│
├── olap_schema/
│   ├── star_schema.sql          # Star schema DDL
│   └── description.md           # OLAP design explanation
│
├── scripts/
│   └── generate_realistic_data.py  # Data generator script
│
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
> No external Python packages are required. The generators use only built-in libraries (`random`, `datetime`, `pathlib`).

---

## Quick Start

```bash
# 1. Navigate to project directory
cd "Data Modelling"

# 2. Generate realistic OLTP data (optional - data files already included)
python scripts/generate_realistic_data.py

# 3. Execute SQL files in your preferred database
```

---

## Step-by-Step Guide

### Step 1: Generate OLTP Data

The project includes pre-generated realistic healthcare data. To regenerate:

```bash
python scripts/generate_realistic_data.py
```

**Data Distribution**: The OLTP data follows a realistic structure with:

| Table | Rows | Type | Description |
|-------|------|------|-------------|
| `specialties.sql` | 25 | Lookup | Medical specialties (Cardiology, Neurology, etc.) |
| `departments.sql` | 20 | Lookup | Hospital departments (ICU, ER, Oncology, etc.) |
| `diagnoses.sql` | 72 | Reference | ICD-10 diagnosis codes across 10 categories |
| `procedures.sql` | 60 | Reference | CPT procedure codes (E&M, Surgery, Lab, etc.) |
| `patients.sql` | 10,000 | Entity | Patient demographics |
| `providers.sql` | 500 | Entity | Healthcare providers with specialty/department FKs |
| `encounters.sql` | 10,000 | Transaction | Patient visits (60% outpatient, 25% inpatient, 15% ER) |
| `encounter_diagnoses.sql` | ~25,000 | Junction | 2-3 diagnoses per encounter average |
| `encounter_procedures.sql` | ~14,000 | Junction | 1-2 procedures per encounter average |
| `billing.sql` | 10,000 | Transaction | Claims with realistic amounts by encounter type |

> [!IMPORTANT]
> **Realistic Data Design**: Unlike synthetic data with artificial duplicates, this dataset uses proper lookup tables (20 departments, 25 specialties) referenced by larger transactional tables. This mirrors real-world healthcare database design.

---

### Step 2: Create OLTP Database

1. Open your SQL client (MySQL Workbench, pgAdmin, DBeaver, etc.)
2. Create a new database:
   ```sql
   CREATE DATABASE healthcare_oltp;
   USE healthcare_oltp;
   ```
3. Execute the schema: `oltp_schema/oltp_schema.sql`
4. Load the data by executing each file in `data/oltp/` in this order:
   1. `specialties.sql` (25 rows - lookup table)
   2. `departments.sql` (20 rows - lookup table)
   3. `diagnoses.sql` (72 rows - reference table)
   4. `procedures.sql` (60 rows - reference table)
   5. `patients.sql` (10,000 rows)
   6. `providers.sql` (500 rows)
   7. `encounters.sql` (10,000 rows)
   8. `encounter_diagnoses.sql` (~25,000 rows)
   9. `encounter_procedures.sql` (~14,000 rows)
   10. `billing.sql` (10,000 rows)

---

### Step 3: Run OLTP Query Analysis

Execute the queries from `query_analysis.txt` to experience performance issues:

```sql
-- Example: Monthly Encounters by Specialty (requires 2 JOINs)
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

**Description**: 11 SQL insertion files in `data/olap/`:

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
2. Execute the schema: `olap_schema/star_schema.sql`
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
-- Same query on Star Schema (Zero JOINs)
SELECT 
    encounter_year AS year,
    encounter_month AS month,
    encounter_month_name AS month_name,
    specialty_name,
    encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_key) AS unique_patients
FROM fact_encounters
GROUP BY 
    encounter_year,
    encounter_month,
    encounter_month_name,
    specialty_name,
    encounter_type
ORDER BY encounter_year, encounter_month, specialty_name;
```

---

## Understanding the Data

### OLTP Data Model

The OLTP schema uses a **realistic data distribution**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    LOOKUP/REFERENCE TABLES                      │
├─────────────────┬───────┬───────────────────────────────────────┤
│ specialties     │   25  │ Medical specialties (fixed list)      │
│ departments     │   20  │ Hospital departments (fixed list)     │
│ diagnoses       │   72  │ ICD-10 codes (reference data)         │
│ procedures      │   60  │ CPT codes (reference data)            │
└─────────────────┴───────┴───────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    TRANSACTIONAL TABLES                         │
├─────────────────┬────────┬──────────────────────────────────────┤
│ patients        │ 10,000 │ Unique patients with demographics    │
│ providers       │    500 │ Doctors → reference specialties/depts│
│ encounters      │ 10,000 │ Patient visits with FK relationships │
│ encounter_diag  │~25,000 │ Many-to-many (2-3 per encounter)     │
│ encounter_proc  │~14,000 │ Many-to-many (1-2 per encounter)     │
│ billing         │ 10,000 │ One claim per encounter              │
└─────────────────┴────────┴──────────────────────────────────────┘
```

### OLTP vs OLAP Comparison

| Aspect | OLTP (Normalized) | OLAP (Star Schema) |
|--------|-------------------|-------------------|
| **Purpose** | Transactional operations | Analytical queries |
| **Tables** | 10 normalized tables | 8 dimensions + 1 fact + 2 bridges |
| **Typical Joins** | 4-6 per query | 2-3 per query |
| **Query Speed** | Slower for analytics | ~10x faster |
| **Data Redundancy** | Minimal | Controlled duplication |

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
| [oltp_schema/description.md](./oltp_schema/description.md) | OLTP schema explanation |
| [olap_schema/description.md](./olap_schema/description.md) | OLAP schema explanation |

---

## Data Generation

The data generator (`scripts/generate_realistic_data.py`) creates realistic healthcare data with:

- **Weighted distributions**: 60% outpatient, 25% inpatient, 15% ER encounters
- **Realistic ICD-10 codes**: Organized by category (Circulatory, Respiratory, etc.)
- **Realistic CPT codes**: E&M visits, surgeries, radiology, lab tests, etc.
- **Proper foreign key relationships**: All transactional data references lookup tables
- **Reproducible output**: Uses random seed for consistent generation

To regenerate data with different parameters, edit and run:
```bash
python scripts/generate_realistic_data.py
```

---

## Troubleshooting

> [!WARNING]
> **Foreign Key Errors**: Ensure you load tables in the correct order (lookup tables → entity tables → transactional tables).

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
