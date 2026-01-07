# OLAP Star Schema Description

## Overview

This document describes the **Online Analytical Processing (OLAP)** star schema for the HealthTech Analytics healthcare system. The schema is designed to optimize analytical query performance by:

- **Denormalizing** dimension attributes to reduce JOINs
- **Pre-aggregating** common metrics in the fact table
- **Creating surrogate keys** for efficient joins
- **Using bridge tables** for many-to-many relationships

---

## Schema Diagram

```mermaid
erDiagram
    dim_date ||--o{ fact_encounters : "encounter_date"
    dim_date ||--o{ fact_encounters : "discharge_date"
    dim_patient ||--o{ fact_encounters : "has"
    dim_provider ||--o{ fact_encounters : "conducts"
    dim_department ||--o{ fact_encounters : "hosts"
    dim_specialty ||--o{ fact_encounters : "for"
    dim_encounter_type ||--o{ fact_encounters : "classified_as"
    
    fact_encounters ||--o{ bridge_encounter_diagnoses : "has"
    fact_encounters ||--o{ bridge_encounter_procedures : "has"
    
    dim_diagnosis ||--o{ bridge_encounter_diagnoses : "links"
    dim_procedure ||--o{ bridge_encounter_procedures : "links"
    
    dim_specialty ||--o{ dim_provider : "denormalized_into"

    dim_date {
        int date_key PK
        date calendar_date
        int year
        int quarter
        int month
        varchar month_name
        int week_of_year
        int day_of_month
        boolean is_weekend
    }
    
    dim_patient {
        int patient_key PK
        int patient_id
        varchar full_name
        date date_of_birth
        int age
        varchar age_group
        char gender
        varchar mrn
    }
    
    dim_provider {
        int provider_key PK
        int provider_id
        varchar full_name
        varchar credential
        varchar specialty_name
        varchar department_name
    }
    
    dim_specialty {
        int specialty_key PK
        int specialty_id
        varchar specialty_name
        varchar specialty_code
    }
    
    dim_department {
        int department_key PK
        int department_id
        varchar department_name
        int floor
        int capacity
    }
    
    dim_encounter_type {
        int encounter_type_key PK
        varchar type_code
        varchar type_name
        boolean is_inpatient
    }
    
    dim_diagnosis {
        int diagnosis_key PK
        int diagnosis_id
        varchar icd10_code
        varchar icd10_description
    }
    
    dim_procedure {
        int procedure_key PK
        int procedure_id
        varchar cpt_code
        varchar cpt_description
    }
    
    fact_encounters {
        int encounter_key PK
        int encounter_id
        int encounter_date_key FK
        int patient_key FK
        int provider_key FK
        int specialty_key FK
        int diagnosis_count
        int procedure_count
        decimal total_allowed_amount
        int length_of_stay_hours
        boolean is_readmission
    }
    
    bridge_encounter_diagnoses {
        int bridge_id PK
        int encounter_key FK
        int diagnosis_key FK
        int diagnosis_sequence
    }
    
    bridge_encounter_procedures {
        int bridge_id PK
        int encounter_key FK
        int procedure_key FK
        date procedure_date
    }
```

---

## Dimension Tables

### Core Dimensions

| Dimension | Purpose | Key Attributes |
|-----------|---------|----------------|
| `dim_date` | Time analysis | year, month, quarter, is_weekend |
| `dim_patient` | Patient demographics | age_group, gender |
| `dim_provider` | Provider info | credential, specialty_name (denormalized) |
| `dim_specialty` | Medical specialties | specialty_name, specialty_code |
| `dim_department` | Hospital locations | department_name, floor, capacity |
| `dim_encounter_type` | Visit categories | type_name, is_inpatient |

### Reference Dimensions

| Dimension | Purpose | Key Attributes |
|-----------|---------|----------------|
| `dim_diagnosis` | ICD-10 codes | icd10_code, icd10_description |
| `dim_procedure` | CPT codes | cpt_code, cpt_description |

---

## Fact Table: fact_encounters

### Grain
**One row per patient encounter**

### Dimension Keys
- `encounter_date_key` → dim_date
- `discharge_date_key` → dim_date
- `patient_key` → dim_patient
- `provider_key` → dim_provider
- `department_key` → dim_department
- `encounter_type_key` → dim_encounter_type
- `specialty_key` → dim_specialty
- `primary_diagnosis_key` → dim_diagnosis

### Pre-Aggregated Metrics

| Metric | Type | Source |
|--------|------|--------|
| `diagnosis_count` | INT | COUNT from encounter_diagnoses |
| `procedure_count` | INT | COUNT from encounter_procedures |
| `total_claim_amount` | DECIMAL | SUM from billing |
| `total_allowed_amount` | DECIMAL | SUM from billing |
| `length_of_stay_hours` | INT | DATEDIFF(discharge, encounter) |
| `is_readmission` | BOOLEAN | Computed 30-day flag |

---

## Bridge Tables

### bridge_encounter_diagnoses
Links encounters to all their diagnoses (many-to-many).

### bridge_encounter_procedures
Links encounters to all their procedures (many-to-many).

---

## Performance Improvements

### Before (OLTP) vs After (Star Schema)

| Query | OLTP Joins | Star Schema Joins | Improvement |
|-------|------------|-------------------|-------------|
| Q1: Monthly Encounters | 2 | 1 | 50% fewer joins |
| Q2: Diagnosis-Procedure | 3 | 2 | + no row explosion |
| Q3: Readmissions | Self-join | 0 (pre-computed) | ~10x faster |
| Q4: Revenue by Specialty | 3 | 1 | 66% fewer joins |

---

## Data Files

OLAP dimension and fact table INSERT statements are located in `data/olap/`:

| File | Table | Description |
|------|-------|-------------|
| `dim_date.sql` | dim_date | Date dimension (2 years) |
| `dim_patient.sql` | dim_patient | Patient dimension |
| `dim_provider.sql` | dim_provider | Provider dimension |
| `dim_specialty.sql` | dim_specialty | Specialty dimension |
| `dim_department.sql` | dim_department | Department dimension |
| `dim_encounter_type.sql` | dim_encounter_type | Encounter types |
| `dim_diagnosis.sql` | dim_diagnosis | Diagnosis dimension |
| `dim_procedure.sql` | dim_procedure | Procedure dimension |
| `fact_encounters.sql` | fact_encounters | Fact table |
| `bridge_diagnoses.sql` | bridge_encounter_diagnoses | Diagnosis bridge |
| `bridge_procedures.sql` | bridge_encounter_procedures | Procedure bridge |
