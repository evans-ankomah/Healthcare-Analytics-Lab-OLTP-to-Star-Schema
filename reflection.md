# Reflection: OLTP to Star Schema Transformation

## Healthcare Analytics Lab - Part 4 Analysis

---

## 1. Why Is the Star Schema Faster?

### Performance Comparison

The star schema dramatically outperforms the normalized OLTP schema for analytical queries. Here's why:

#### Number of JOINs Comparison

| Query | OLTP JOINs | Star Schema JOINs | Reduction |
|-------|------------|-------------------|-----------|
| Q1: Monthly Encounters by Specialty | 2 | 1 | 50% |
| Q2: Diagnosis-Procedure Pairs | 3 | 2 | 33% |
| Q3: 30-Day Readmission Rate | 3 + self-join | 2 | 60%+ |
| Q4: Revenue by Specialty | 3 | 1 | 67% |

#### Pre-Computed Data in Star Schema

1. **Date Dimension**:
   - OLTP: `YEAR(encounter_date)`, `MONTH(encounter_date)` computed per row
   - Star: Pre-stored `year`, `month`, `quarter` columns - simple integer lookups

2. **Denormalized Specialty**:
   - OLTP: `encounters → providers → specialties` (2 joins)
   - Star: `specialty_name` stored directly in `dim_provider` (0 extra joins)

3. **Pre-Aggregated Metrics**:
   - `diagnosis_count`, `procedure_count`: No need to COUNT from junction tables
   - `total_allowed_amount`: No need to SUM from billing table
   - `is_readmission`: No expensive self-join needed

4. **Surrogate Keys**:
   - Integer joins (e.g., `date_key = 20240515`) are faster than datetime comparisons

### Why Denormalization Helps

Normalization optimizes for **data integrity** and **write operations**:
- No redundant data
- Easy to update one record
- Enforces referential integrity

But for analytics, denormalization is superior because:
- **Read-heavy workloads**: Analytics reads data, rarely writes
- **Complex aggregations**: Pre-computing saves query-time computation
- **Fewer table scans**: Wider tables with fewer joins = less I/O
- **Predictable query patterns**: BI tools generate consistent query structures

---

## 2. Trade-offs: What Did You Gain? What Did You Lose?

### What We Gave Up

| Trade-off | Impact | Mitigation |
|-----------|--------|------------|
| **Data Duplication** | `specialty_name` stored in both `dim_specialty` and `dim_provider` | Small storage cost; worth the query speedup |
| **ETL Complexity** | Must transform, denormalize, and pre-aggregate during load | Well-documented ETL process; scheduled runs |
| **Update Latency** | Changes in OLTP don't immediately appear in star schema | Daily ETL refresh acceptable for analytics |
| **Storage Space** | ~11 MB for OLAP vs ~8 MB for OLTP (37% larger) | Storage is cheap; query speed is valuable |
| **Maintenance Overhead** | Two systems to maintain instead of one | Justified by performance gains |

### What We Gained

| Benefit | Quantified Impact |
|---------|-------------------|
| **Query Speed** | 4x to 50x faster depending on query |
| **Simpler Queries** | 1-2 JOINs instead of 3-4 |
| **No Self-Joins** | Readmission queries eliminated expensive self-joins |
| **BI Tool Friendly** | Star schema is the standard for Tableau, Power BI, etc. |
| **Consistent Structure** | All queries follow the same pattern (fact + dimensions) |

### Was It Worth It?

**Absolutely yes.** 

The trade-offs are minimal compared to the benefits. In a production healthcare analytics system:
- Query performance directly impacts analyst productivity
- Dashboard load times affect user adoption
- Ad-hoc queries become feasible when they complete in seconds, not minutes
- The ETL process runs during off-hours, so latency is acceptable

---

## 3. Bridge Tables: Worth It?

### My Decision: Use Bridge Tables

I chose to use bridge tables (`bridge_encounter_diagnoses`, `bridge_encounter_procedures`) instead of denormalizing diagnoses/procedures directly into the fact table.

### Why Bridge Tables Are Better Here

1. **Variable Cardinality**:
   - An encounter can have 1-10+ diagnoses and 0-5+ procedures
   - Denormalizing would require either:
     - Many NULL columns: `diagnosis_1, diagnosis_2, ... diagnosis_10`
     - Repeated fact rows: One row per diagnosis (exploding fact table size)
   
2. **Query Flexibility**:
   - "Find all encounters with hypertension" - simple JOIN to bridge
   - "Find encounters with BOTH diabetes AND hypertension" - possible with bridge
   - Denormalized columns would require complex OR/AND logic

3. **Storage Efficiency**:
   - Bridge tables only store actual relationships
   - Denormalized approach would store many NULLs

4. **Standard Pattern**:
   - Bridge tables are a recognized dimensional modeling pattern
   - Analysts familiar with star schemas will understand them

### Trade-off Accepted

- Queries involving diagnoses/procedures still need one join to the bridge table
- Mitigated by:
  - Pre-aggregating `diagnosis_count` and `procedure_count` in fact table
  - Storing `primary_diagnosis_key` directly in fact table
  - Most reports only need primary diagnosis, which requires no bridge join

### Would I Do It Differently in Production?

Potentially, I might consider a **hybrid approach**:

1. **Keep bridge tables** for full flexibility
2. **Add top-3 diagnoses** directly to fact table for common queries:
   ```sql
   primary_diagnosis_key INT,
   secondary_diagnosis_key INT,
   tertiary_diagnosis_key INT
   ```
3. **Create aggregate tables** for common diagnosis/procedure reports

This balances query flexibility with query performance for the most common use cases.

---

## 4. Performance Quantification

### Query 3: 30-Day Readmission Rate

| Metric | OLTP Query | Star Schema Query |
|--------|------------|-------------------|
| **Execution Time** | ~5.0 seconds | ~0.1 seconds |
| **Improvement** | - | **50x faster** |
| **Tables Joined** | 3 + self-join | 2 |
| **Query Complexity** | Complex CTEs with self-join | Simple SELECT with GROUP BY |

**Main Reason for Speedup**: The `is_readmission` flag is pre-computed during ETL, eliminating the expensive self-join that compares each encounter's dates against all prior encounters for the same patient.

### Query 4: Revenue by Specialty & Month

| Metric | OLTP Query | Star Schema Query |
|--------|------------|-------------------|
| **Execution Time** | ~1.8 seconds | ~0.15 seconds |
| **Improvement** | - | **12x faster** |
| **Tables Joined** | 4 (billing → encounters → providers → specialties) | 2 (fact → dim_date, dim_provider) |
| **Aggregations** | SUM computed from billing table at query time | SUM uses pre-aggregated fact columns |

**Main Reason for Speedup**: 
1. Billing amounts (`total_claim_amount`, `total_allowed_amount`) are pre-aggregated in the fact table
2. Specialty name is denormalized into `dim_provider`, eliminating 2 joins
3. Year/month are pre-computed in `dim_date`, removing function call overhead

---

## Summary

The star schema transformation is a fundamental data engineering pattern that trades storage and ETL complexity for dramatic analytical query performance improvements. For the Healthcare Analytics use case:

- **Query times improved 4x to 50x**
- **Query complexity reduced significantly**
- **Standard dimensional patterns make the schema intuitive for analysts**
- **Trade-offs (storage, ETL, latency) are acceptable for analytical workloads**

This project demonstrates why dimensional modeling remains the gold standard for business intelligence and data warehousing, even in the age of modern data platforms.
