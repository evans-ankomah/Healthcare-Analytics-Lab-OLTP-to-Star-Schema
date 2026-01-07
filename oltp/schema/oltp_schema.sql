-- ============================================================================
-- OLTP Schema for Healthcare Analytics
-- ============================================================================
-- This schema represents a normalized (3NF) transactional database design
-- for a healthcare system. It is optimized for data integrity and write
-- operations but may suffer from performance issues for complex analytical
-- queries due to the need for multiple JOINs.
-- ============================================================================

-- Drop tables if they exist (in reverse order of dependencies)
DROP TABLE IF EXISTS billing;
DROP TABLE IF EXISTS encounter_procedures;
DROP TABLE IF EXISTS encounter_diagnoses;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS procedures;
DROP TABLE IF EXISTS diagnoses;
DROP TABLE IF EXISTS providers;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS specialties;
DROP TABLE IF EXISTS patients;

-- ============================================================================
-- PATIENTS TABLE
-- ============================================================================
-- Stores patient demographic information.
-- Primary Key: patient_id
-- Unique Constraint: mrn (Medical Record Number)
-- ============================================================================
CREATE TABLE patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender CHAR(1),
    mrn VARCHAR(20) UNIQUE
);

-- ============================================================================
-- SPECIALTIES TABLE
-- ============================================================================
-- Stores medical specialty information (e.g., Cardiology, Internal Medicine).
-- Primary Key: specialty_id
-- ============================================================================
CREATE TABLE specialties (
    specialty_id INT PRIMARY KEY,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10)
);

-- ============================================================================
-- DEPARTMENTS TABLE
-- ============================================================================
-- Stores hospital department information.
-- Primary Key: department_id
-- ============================================================================
CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(100),
    floor INT,
    capacity INT
);

-- ============================================================================
-- PROVIDERS TABLE
-- ============================================================================
-- Stores healthcare provider (doctor) information.
-- Primary Key: provider_id
-- Foreign Keys: specialty_id -> specialties, department_id -> departments
-- ============================================================================
CREATE TABLE providers (
    provider_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    credential VARCHAR(20),
    specialty_id INT,
    department_id INT,
    FOREIGN KEY (specialty_id) REFERENCES specialties(specialty_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

-- ============================================================================
-- DIAGNOSES TABLE
-- ============================================================================
-- Stores ICD-10 diagnosis codes and descriptions.
-- Primary Key: diagnosis_id
-- ============================================================================
CREATE TABLE diagnoses (
    diagnosis_id INT PRIMARY KEY,
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200)
);

-- ============================================================================
-- PROCEDURES TABLE
-- ============================================================================
-- Stores CPT procedure codes and descriptions.
-- Primary Key: procedure_id
-- ============================================================================
CREATE TABLE procedures (
    procedure_id INT PRIMARY KEY,
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200)
);

-- ============================================================================
-- ENCOUNTERS TABLE
-- ============================================================================
-- Stores patient visit/encounter information.
-- This is the central transactional table linking patients to providers.
-- Primary Key: encounter_id
-- Foreign Keys: patient_id -> patients, provider_id -> providers,
--               department_id -> departments
-- Index: encounter_date for date-based queries
-- ============================================================================
CREATE TABLE encounters (
    encounter_id INT PRIMARY KEY,
    patient_id INT,
    provider_id INT,
    encounter_type VARCHAR(50),  -- 'Outpatient', 'Inpatient', 'ER'
    encounter_date DATETIME,
    discharge_date DATETIME,
    department_id INT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id),
    INDEX idx_encounter_date (encounter_date)
);

-- ============================================================================
-- ENCOUNTER_DIAGNOSES TABLE (Junction/Bridge Table)
-- ============================================================================
-- Links encounters to diagnoses (many-to-many relationship).
-- An encounter can have multiple diagnoses, each with a sequence number.
-- Primary Key: encounter_diagnosis_id
-- Foreign Keys: encounter_id -> encounters, diagnosis_id -> diagnoses
-- ============================================================================
CREATE TABLE encounter_diagnoses (
    encounter_diagnosis_id INT PRIMARY KEY,
    encounter_id INT,
    diagnosis_id INT,
    diagnosis_sequence INT,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (diagnosis_id) REFERENCES diagnoses(diagnosis_id)
);

-- ============================================================================
-- ENCOUNTER_PROCEDURES TABLE (Junction/Bridge Table)
-- ============================================================================
-- Links encounters to procedures (many-to-many relationship).
-- Primary Key: encounter_procedure_id
-- Foreign Keys: encounter_id -> encounters, procedure_id -> procedures
-- ============================================================================
CREATE TABLE encounter_procedures (
    encounter_procedure_id INT PRIMARY KEY,
    encounter_id INT,
    procedure_id INT,
    procedure_date DATE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (procedure_id) REFERENCES procedures(procedure_id)
);

-- ============================================================================
-- BILLING TABLE
-- ============================================================================
-- Stores billing/claims information for each encounter.
-- Primary Key: billing_id
-- Foreign Key: encounter_id -> encounters
-- Index: claim_date for date-based financial queries
-- ============================================================================
CREATE TABLE billing (
    billing_id INT PRIMARY KEY,
    encounter_id INT,
    claim_amount DECIMAL(12, 2),
    allowed_amount DECIMAL(12, 2),
    claim_date DATE,
    claim_status VARCHAR(50),
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    INDEX idx_claim_date (claim_date)
);
