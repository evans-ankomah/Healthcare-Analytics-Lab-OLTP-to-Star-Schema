-- ============================================================================
-- STAR SCHEMA for Healthcare Analytics (OLAP)
-- ============================================================================
-- This schema represents a dimensional model optimized for analytical queries.
-- It denormalizes data to reduce JOINs and pre-aggregates common metrics
-- for faster query performance.
-- ============================================================================

-- Drop tables if they exist (in reverse order of dependencies)
DROP TABLE IF EXISTS bridge_encounter_procedures;
DROP TABLE IF EXISTS bridge_encounter_diagnoses;
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS dim_procedure;
DROP TABLE IF EXISTS dim_diagnosis;
DROP TABLE IF EXISTS dim_encounter_type;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_specialty;
DROP TABLE IF EXISTS dim_provider;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_date;

-- ============================================================================
-- DIM_DATE (Time Dimension)
-- ============================================================================
-- Pre-computed date attributes to eliminate YEAR()/MONTH() function calls.
-- Enables efficient filtering and grouping by any time period.
-- ============================================================================
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,              -- Surrogate key (YYYYMMDD format)
    calendar_date DATE NOT NULL UNIQUE,    -- Actual date value
    year INT NOT NULL,                     -- 4-digit year
    quarter INT NOT NULL,                  -- Quarter (1-4)
    month INT NOT NULL,                    -- Month (1-12)
    month_name VARCHAR(20) NOT NULL,       -- Full month name
    week_of_year INT NOT NULL,             -- Week number (1-53)
    day_of_month INT NOT NULL,             -- Day (1-31)
    day_of_week INT NOT NULL,              -- Day (1=Monday, 7=Sunday)
    day_name VARCHAR(20) NOT NULL,         -- Full day name
    is_weekend BOOLEAN NOT NULL,           -- TRUE if Saturday/Sunday
    fiscal_year INT NOT NULL,              -- Fiscal year
    fiscal_quarter INT NOT NULL,           -- Fiscal quarter (1-4)
    
    INDEX idx_date_year_month (year, month),
    INDEX idx_date_calendar (calendar_date)
);

-- ============================================================================
-- DIM_PATIENT
-- ============================================================================
-- Patient demographics with computed age groups for easier analysis.
-- ============================================================================
CREATE TABLE dim_patient (
    patient_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    patient_id INT NOT NULL UNIQUE,              -- Natural key from OLTP
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),                      -- Concatenated name
    date_of_birth DATE,
    age INT,                                     -- Current age (computed)
    age_group VARCHAR(20),                       -- Age bracket (0-17, 18-34, etc.)
    gender CHAR(1),
    gender_desc VARCHAR(10),                     -- Male/Female
    mrn VARCHAR(20) UNIQUE,                      -- Medical Record Number
    
    INDEX idx_patient_natural (patient_id),
    INDEX idx_patient_age_group (age_group)
);

-- ============================================================================
-- DIM_SPECIALTY
-- ============================================================================
-- Medical specialty dimension for specialty-level analysis.
-- ============================================================================
CREATE TABLE dim_specialty (
    specialty_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    specialty_id INT NOT NULL UNIQUE,              -- Natural key from OLTP
    specialty_name VARCHAR(100) NOT NULL,
    specialty_code VARCHAR(10) NOT NULL,
    
    INDEX idx_specialty_natural (specialty_id)
);

-- ============================================================================
-- DIM_DEPARTMENT
-- ============================================================================
-- Hospital department information for location-based analysis.
-- ============================================================================
CREATE TABLE dim_department (
    department_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    department_id INT NOT NULL UNIQUE,              -- Natural key from OLTP
    department_name VARCHAR(100) NOT NULL,
    floor INT,
    capacity INT,
    
    INDEX idx_department_natural (department_id)
);

-- ============================================================================
-- DIM_PROVIDER
-- ============================================================================
-- Provider dimension with DENORMALIZED specialty and department names.
-- This eliminates 2 joins in most analytical queries.
-- ============================================================================
CREATE TABLE dim_provider (
    provider_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    provider_id INT NOT NULL UNIQUE,              -- Natural key from OLTP
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),                       -- Concatenated name
    credential VARCHAR(20),                       -- MD, DO, NP, etc.
    specialty_key INT,                            -- FK to dim_specialty
    specialty_id INT,                             -- Natural key for drill-through
    specialty_name VARCHAR(100),                  -- DENORMALIZED
    specialty_code VARCHAR(10),                   -- DENORMALIZED
    department_key INT,                           -- FK to dim_department
    department_id INT,                            -- Natural key for drill-through
    department_name VARCHAR(100),                 -- DENORMALIZED
    
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    INDEX idx_provider_natural (provider_id),
    INDEX idx_provider_specialty (specialty_key)
);

-- ============================================================================
-- DIM_ENCOUNTER_TYPE
-- ============================================================================
-- Encounter type dimension with useful derived attributes.
-- ============================================================================
CREATE TABLE dim_encounter_type (
    encounter_type_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    type_code VARCHAR(20) NOT NULL UNIQUE,              -- Short code
    type_name VARCHAR(50) NOT NULL,                     -- Full name
    is_inpatient BOOLEAN NOT NULL,                      -- Admission required?
    avg_los_hours DECIMAL(10, 2),                       -- Typical length of stay
    
    INDEX idx_encounter_type_code (type_code)
);

-- ============================================================================
-- DIM_DIAGNOSIS
-- ============================================================================
-- ICD-10 diagnosis codes dimension.
-- ============================================================================
CREATE TABLE dim_diagnosis (
    diagnosis_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    diagnosis_id INT NOT NULL UNIQUE,              -- Natural key from OLTP
    icd10_code VARCHAR(10) NOT NULL,
    icd10_description VARCHAR(200),
    
    INDEX idx_diagnosis_natural (diagnosis_id),
    INDEX idx_diagnosis_code (icd10_code)
);

-- ============================================================================
-- DIM_PROCEDURE
-- ============================================================================
-- CPT procedure codes dimension.
-- ============================================================================
CREATE TABLE dim_procedure (
    procedure_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    procedure_id INT NOT NULL UNIQUE,              -- Natural key from OLTP
    cpt_code VARCHAR(10) NOT NULL,
    cpt_description VARCHAR(200),
    
    INDEX idx_procedure_natural (procedure_id),
    INDEX idx_procedure_code (cpt_code)
);

-- ============================================================================
-- FACT_ENCOUNTERS (Central Fact Table)
-- ============================================================================
-- One row per encounter with FK to all dimensions and pre-aggregated metrics.
-- Grain: One row per patient encounter.
--
-- DENORMALIZATION STRATEGY:
-- We store commonly-queried dimension attributes DIRECTLY in the fact table
-- to enable ZERO-JOIN analytical queries. Dimension tables are still available
-- for drill-through and detailed analysis.
-- ============================================================================
CREATE TABLE fact_encounters (
    encounter_key INT PRIMARY KEY AUTO_INCREMENT,     -- Surrogate key
    encounter_id INT NOT NULL UNIQUE,                 -- Natural key from OLTP
    
    -- ========================================================================
    -- DIMENSION FOREIGN KEYS (for drill-through to dimension details)
    -- ========================================================================
    encounter_date_key INT NOT NULL,                  -- FK to dim_date
    discharge_date_key INT,                           -- FK to dim_date
    patient_key INT NOT NULL,                         -- FK to dim_patient
    provider_key INT NOT NULL,                        -- FK to dim_provider
    department_key INT NOT NULL,                      -- FK to dim_department
    encounter_type_key INT NOT NULL,                  -- FK to dim_encounter_type
    specialty_key INT NOT NULL,                       -- FK to dim_specialty
    primary_diagnosis_key INT,                        -- FK to dim_diagnosis
    
    -- ========================================================================
    -- DENORMALIZED DATE ATTRIBUTES (eliminates dim_date join)
    -- ========================================================================
    encounter_date DATETIME NOT NULL,                 -- Original datetime
    discharge_date DATETIME,                          -- Original datetime
    encounter_year INT NOT NULL,                      -- DENORMALIZED from dim_date
    encounter_month INT NOT NULL,                     -- DENORMALIZED: 1-12
    encounter_month_name VARCHAR(20),                 -- DENORMALIZED: 'January', etc.
    encounter_quarter INT,                            -- DENORMALIZED: 1-4
    encounter_day_of_week INT,                        -- DENORMALIZED: 1-7
    is_weekend BOOLEAN,                               -- DENORMALIZED from dim_date
    
    -- ========================================================================
    -- DENORMALIZED DIMENSION ATTRIBUTES (eliminates dimension joins)
    -- ========================================================================
    -- Specialty (eliminates dim_specialty join)
    specialty_name VARCHAR(100) NOT NULL,             -- DENORMALIZED
    specialty_code VARCHAR(10),                       -- DENORMALIZED
    
    -- Department (eliminates dim_department join)
    department_name VARCHAR(100),                     -- DENORMALIZED
    
    -- Provider (eliminates dim_provider join for common attributes)
    provider_name VARCHAR(200),                       -- DENORMALIZED (full name)
    
    -- Encounter Type (degenerate dimension - no separate table needed)
    encounter_type VARCHAR(50) NOT NULL,              -- 'Outpatient', 'Inpatient', 'Emergency'
    is_inpatient BOOLEAN DEFAULT FALSE,               -- DENORMALIZED flag
    
    -- Primary Diagnosis (eliminates bridge join for primary diagnosis)
    primary_icd10_code VARCHAR(10),                   -- DENORMALIZED
    primary_icd10_description VARCHAR(200),           -- DENORMALIZED
    
    -- ========================================================================
    -- PRE-AGGREGATED METRICS (eliminates COUNT/SUM from bridge tables)
    -- ========================================================================
    diagnosis_count INT DEFAULT 0,                    -- Count of diagnoses
    procedure_count INT DEFAULT 0,                    -- Count of procedures
    
    -- Billing Metrics (eliminates billing table join)
    total_claim_amount DECIMAL(12, 2) DEFAULT 0,      -- Sum of claims
    total_allowed_amount DECIMAL(12, 2) DEFAULT 0,    -- Sum of allowed amounts
    claim_count INT DEFAULT 0,                        -- Number of claims
    
    -- ========================================================================
    -- PRE-COMPUTED DERIVED METRICS (eliminates complex calculations)
    -- ========================================================================
    length_of_stay_hours INT,                         -- Hours between dates
    length_of_stay_days INT,                          -- Days between dates
    is_readmission BOOLEAN DEFAULT FALSE,             -- 30-day readmission flag
    days_since_last_visit INT,                        -- For readmission analysis
    
    -- ========================================================================
    -- FOREIGN KEY CONSTRAINTS (for referential integrity)
    -- ========================================================================
    FOREIGN KEY (encounter_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (primary_diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    
    -- ========================================================================
    -- INDEXES FOR COMMON QUERY PATTERNS
    -- ========================================================================
    -- Date-based queries (zero-join)
    INDEX idx_fact_year_month (encounter_year, encounter_month),
    INDEX idx_fact_quarter (encounter_year, encounter_quarter),
    
    -- Specialty-based queries (zero-join)
    INDEX idx_fact_specialty_name (specialty_name),
    
    -- Combined indexes for common GROUP BY patterns
    INDEX idx_fact_specialty_year_month (specialty_name, encounter_year, encounter_month),
    INDEX idx_fact_type_specialty (encounter_type, specialty_name),
    
    -- Readmission queries
    INDEX idx_fact_readmission (is_readmission, is_inpatient),
    
    -- Legacy FK indexes (for drill-through queries)
    INDEX idx_fact_enc_date_key (encounter_date_key),
    INDEX idx_fact_enc_patient (patient_key),
    INDEX idx_fact_enc_provider (provider_key)
);

-- ============================================================================
-- BRIDGE_ENCOUNTER_DIAGNOSES
-- ============================================================================
-- Bridge table for many-to-many relationship between encounters and diagnoses.
-- ============================================================================
CREATE TABLE bridge_encounter_diagnoses (
    bridge_id INT PRIMARY KEY AUTO_INCREMENT,
    encounter_key INT NOT NULL,                       -- FK to fact_encounters
    diagnosis_key INT NOT NULL,                       -- FK to dim_diagnosis
    diagnosis_sequence INT NOT NULL,                  -- Order (1=primary, 2=secondary, etc.)
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    
    INDEX idx_bridge_diag_encounter (encounter_key),
    INDEX idx_bridge_diag_diagnosis (diagnosis_key),
    UNIQUE KEY uk_encounter_diagnosis (encounter_key, diagnosis_key)
);

-- ============================================================================
-- BRIDGE_ENCOUNTER_PROCEDURES
-- ============================================================================
-- Bridge table for many-to-many relationship between encounters and procedures.
-- ============================================================================
CREATE TABLE bridge_encounter_procedures (
    bridge_id INT PRIMARY KEY AUTO_INCREMENT,
    encounter_key INT NOT NULL,                       -- FK to fact_encounters
    procedure_key INT NOT NULL,                       -- FK to dim_procedure
    procedure_date DATE,                              -- When procedure was performed
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    
    INDEX idx_bridge_proc_encounter (encounter_key),
    INDEX idx_bridge_proc_procedure (procedure_key),
    UNIQUE KEY uk_encounter_procedure (encounter_key, procedure_key)
);
