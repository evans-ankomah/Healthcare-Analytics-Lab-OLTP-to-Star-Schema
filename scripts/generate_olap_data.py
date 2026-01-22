"""
OLAP Star Schema Data Generator
================================
Generates OLAP dimension and fact table data from OLTP source data.
Includes aggressive denormalization for zero-join queries.
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(42)

# Paths
OLTP_DIR = Path(__file__).parent.parent / "data" / "oltp"
OLAP_DIR = Path(__file__).parent.parent / "data" / "olap"

# =============================================================================
# STATIC DATA (copied from OLTP generator for consistency)
# =============================================================================

SPECIALTIES = [
    ("Cardiology", "CARD"),
    ("Internal Medicine", "IM"),
    ("Emergency Medicine", "ER"),
    ("Orthopedics", "ORTH"),
    ("Neurology", "NEUR"),
    ("Pediatrics", "PED"),
    ("Oncology", "ONC"),
    ("Dermatology", "DERM"),
    ("Psychiatry", "PSYCH"),
    ("Radiology", "RAD"),
    ("Anesthesiology", "ANES"),
    ("Pathology", "PATH"),
    ("Gastroenterology", "GI"),
    ("Pulmonology", "PULM"),
    ("Nephrology", "NEPH"),
    ("Endocrinology", "ENDO"),
    ("Rheumatology", "RHEUM"),
    ("Urology", "URO"),
    ("Ophthalmology", "OPH"),
    ("Otolaryngology", "ENT"),
    ("Family Medicine", "FM"),
    ("General Surgery", "GS"),
    ("Plastic Surgery", "PS"),
    ("Vascular Surgery", "VS"),
    ("Infectious Disease", "ID"),
]

DEPARTMENTS = [
    ("Emergency Department", 1, 50),
    ("Cardiology Unit", 3, 30),
    ("Internal Medicine Ward", 4, 45),
    ("Orthopedic Surgery", 5, 25),
    ("Neurology Center", 6, 20),
    ("Pediatric Ward", 2, 35),
    ("Oncology Department", 7, 28),
    ("Dermatology Clinic", 2, 15),
    ("Psychiatric Unit", 8, 22),
    ("Radiology Department", 1, 10),
    ("Surgical Suite", 5, 12),
    ("ICU", 3, 18),
    ("NICU", 2, 12),
    ("CCU", 3, 15),
    ("Outpatient Clinic", 1, 40),
    ("Rehabilitation Center", 4, 30),
    ("Laboratory", 1, 8),
    ("Pharmacy", 1, 5),
    ("Physical Therapy", 4, 20),
    ("Labor and Delivery", 2, 25),
]

ENCOUNTER_TYPES = [
    ("OP", "Outpatient", False, 2.0),
    ("IP", "Inpatient", True, 96.0),
    ("ER", "Emergency", False, 6.0),
]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
]

MONTH_NAMES = ["", "January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def escape_sql(value):
    """Escape single quotes in SQL strings."""
    if value is None:
        return "NULL"
    return str(value).replace("'", "''")


def write_sql_file(filename, table_name, rows, total_rows):
    """Write SQL INSERT statements to a file."""
    filepath = OLAP_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"-- ============================================================================\n")
        f.write(f"-- {table_name.upper()} - INSERT statements\n")
        f.write(f"-- ============================================================================\n")
        f.write(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Total rows: {total_rows:,}\n")
        f.write(f"-- ============================================================================\n\n")
        
        for row in rows:
            f.write(row + "\n")
    
    print(f"  [OK] {filename}: {total_rows:,} rows")


def generate_date(start_year=2020, end_year=2025):
    """Generate a random date."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def calculate_age_group(dob):
    """Calculate age group from date of birth."""
    today = datetime(2025, 1, 1)
    age = (today - dob).days // 365
    if age < 18:
        return "0-17"
    elif age < 35:
        return "18-34"
    elif age < 50:
        return "35-49"
    elif age < 65:
        return "50-64"
    elif age < 75:
        return "65-74"
    else:
        return "75+"


def main():
    print("\n" + "=" * 70)
    print("OLAP Star Schema Data Generator")
    print("=" * 70 + "\n")
    
    # Ensure output directory exists
    OLAP_DIR.mkdir(parents=True, exist_ok=True)
    
    # -------------------------------------------------------------------------
    # 1. DIM_DATE (2 years of dates)
    # -------------------------------------------------------------------------
    print("Generating dimension tables...")
    
    date_rows = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2026, 12, 31)
    current_date = start_date
    
    while current_date <= end_date:
        date_key = int(current_date.strftime("%Y%m%d"))
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        month_name = MONTH_NAMES[month]
        week_of_year = current_date.isocalendar()[1]
        day_of_month = current_date.day
        day_of_week = current_date.weekday() + 1  # 1=Monday
        day_name = DAY_NAMES[current_date.weekday()]
        is_weekend = "TRUE" if day_of_week >= 6 else "FALSE"
        
        date_rows.append(
            f"INSERT INTO dim_date VALUES ({date_key}, '{current_date.strftime('%Y-%m-%d')}', "
            f"{year}, {quarter}, {month}, '{month_name}', {week_of_year}, {day_of_month}, "
            f"{day_of_week}, '{day_name}', {is_weekend}, {year}, {quarter});"
        )
        current_date += timedelta(days=1)
    
    write_sql_file("dim_date.sql", "dim_date", date_rows, len(date_rows))
    
    # -------------------------------------------------------------------------
    # 2. DIM_SPECIALTY (25 rows)
    # -------------------------------------------------------------------------
    specialty_rows = []
    for i, (name, code) in enumerate(SPECIALTIES, 1):
        specialty_rows.append(
            f"INSERT INTO dim_specialty VALUES ({i}, {i}, '{escape_sql(name)}', '{code}');"
        )
    write_sql_file("dim_specialty.sql", "dim_specialty", specialty_rows, len(SPECIALTIES))
    
    # -------------------------------------------------------------------------
    # 3. DIM_DEPARTMENT (20 rows)
    # -------------------------------------------------------------------------
    dept_rows = []
    for i, (name, floor, capacity) in enumerate(DEPARTMENTS, 1):
        dept_rows.append(
            f"INSERT INTO dim_department VALUES ({i}, {i}, '{escape_sql(name)}', {floor}, {capacity});"
        )
    write_sql_file("dim_department.sql", "dim_department", dept_rows, len(DEPARTMENTS))
    
    # -------------------------------------------------------------------------
    # 4. DIM_ENCOUNTER_TYPE (3 rows)
    # -------------------------------------------------------------------------
    enc_type_rows = []
    for i, (code, name, is_inpatient, avg_los) in enumerate(ENCOUNTER_TYPES, 1):
        is_inp = "TRUE" if is_inpatient else "FALSE"
        enc_type_rows.append(
            f"INSERT INTO dim_encounter_type VALUES ({i}, '{code}', '{name}', {is_inp}, {avg_los});"
        )
    write_sql_file("dim_encounter_type.sql", "dim_encounter_type", enc_type_rows, len(ENCOUNTER_TYPES))
    
    # -------------------------------------------------------------------------
    # 5. DIM_DIAGNOSIS (72 rows - from OLTP)
    # -------------------------------------------------------------------------
    # Read from OLTP diagnoses
    ICD10_CATEGORIES = {
        "I": [
            ("I10", "Essential hypertension"),
            ("I11.9", "Hypertensive heart disease without heart failure"),
            ("I21.0", "ST elevation myocardial infarction of anterior wall"),
            ("I21.3", "ST elevation myocardial infarction unspecified site"),
            ("I25.10", "Atherosclerotic heart disease native coronary artery"),
            ("I48.0", "Paroxysmal atrial fibrillation"),
            ("I48.91", "Unspecified atrial fibrillation"),
            ("I50.9", "Heart failure unspecified"),
            ("I63.9", "Cerebral infarction unspecified"),
            ("I70.0", "Atherosclerosis of aorta"),
        ],
        "J": [
            ("J06.9", "Acute upper respiratory infection unspecified"),
            ("J18.9", "Pneumonia unspecified organism"),
            ("J20.9", "Acute bronchitis unspecified"),
            ("J44.1", "COPD with acute exacerbation"),
            ("J45.20", "Mild intermittent asthma uncomplicated"),
            ("J45.40", "Moderate persistent asthma uncomplicated"),
            ("J96.00", "Acute respiratory failure"),
            ("J98.4", "Other disorders of lung"),
        ],
        "E": [
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("E11.65", "Type 2 diabetes mellitus with hyperglycemia"),
            ("E11.21", "Type 2 diabetes mellitus with diabetic nephropathy"),
            ("E11.40", "Type 2 diabetes mellitus with diabetic neuropathy"),
            ("E78.00", "Pure hypercholesterolemia unspecified"),
            ("E78.5", "Hyperlipidemia unspecified"),
            ("E03.9", "Hypothyroidism unspecified"),
            ("E66.9", "Obesity unspecified"),
        ],
        "K": [
            ("K21.0", "GERD with esophagitis"),
            ("K29.70", "Gastritis unspecified without bleeding"),
            ("K35.80", "Unspecified acute appendicitis"),
            ("K40.90", "Unilateral inguinal hernia"),
            ("K57.30", "Diverticulosis of large intestine"),
            ("K80.20", "Calculus of gallbladder"),
            ("K92.0", "Hematemesis"),
            ("K92.1", "Melena"),
        ],
        "M": [
            ("M54.5", "Low back pain"),
            ("M17.11", "Primary osteoarthritis right knee"),
            ("M17.12", "Primary osteoarthritis left knee"),
            ("M25.561", "Pain in right knee"),
            ("M25.562", "Pain in left knee"),
            ("M79.3", "Panniculitis unspecified"),
            ("M81.0", "Age-related osteoporosis"),
            ("M62.830", "Muscle spasm of back"),
        ],
        "N": [
            ("N18.3", "Chronic kidney disease stage 3"),
            ("N18.4", "Chronic kidney disease stage 4"),
            ("N18.5", "Chronic kidney disease stage 5"),
            ("N39.0", "Urinary tract infection"),
            ("N40.0", "Benign prostatic hyperplasia"),
            ("N17.9", "Acute kidney failure unspecified"),
            ("N20.0", "Calculus of kidney"),
        ],
        "F": [
            ("F32.1", "Major depressive disorder moderate"),
            ("F32.9", "Major depressive disorder unspecified"),
            ("F41.1", "Generalized anxiety disorder"),
            ("F41.9", "Anxiety disorder unspecified"),
            ("F10.10", "Alcohol use disorder mild"),
            ("F17.210", "Nicotine dependence cigarettes"),
            ("F33.0", "Major depressive disorder recurrent"),
        ],
        "G": [
            ("G43.909", "Migraine unspecified"),
            ("G47.00", "Insomnia unspecified"),
            ("G20", "Parkinson disease"),
            ("G35", "Multiple sclerosis"),
            ("G40.909", "Epilepsy unspecified"),
            ("G89.29", "Other chronic pain"),
        ],
        "S": [
            ("S06.0X0A", "Concussion initial"),
            ("S52.501A", "Fracture right radius initial"),
            ("S82.001A", "Fracture right tibia initial"),
            ("S42.001A", "Fracture right clavicle initial"),
            ("S72.001A", "Fracture right femur initial"),
        ],
        "Z": [
            ("Z23", "Encounter for immunization"),
            ("Z00.00", "General adult medical examination"),
            ("Z12.11", "Screening for colon cancer"),
            ("Z87.891", "Personal history of nicotine dependence"),
            ("Z96.1", "Presence of intraocular lens"),
        ],
    }
    
    diag_rows = []
    diag_id = 1
    all_diagnoses = []
    for category, codes in ICD10_CATEGORIES.items():
        for code, desc in codes:
            all_diagnoses.append((diag_id, code, desc))
            diag_rows.append(
                f"INSERT INTO dim_diagnosis VALUES ({diag_id}, {diag_id}, '{code}', '{escape_sql(desc)}');"
            )
            diag_id += 1
    write_sql_file("dim_diagnosis.sql", "dim_diagnosis", diag_rows, len(all_diagnoses))
    num_diagnoses = len(all_diagnoses)
    
    # -------------------------------------------------------------------------
    # 6. DIM_PROCEDURE (60 rows)
    # -------------------------------------------------------------------------
    CPT_CATEGORIES = {
        "Evaluation": [
            ("99201", "Office visit new patient level 1"),
            ("99202", "Office visit new patient level 2"),
            ("99203", "Office visit new patient level 3"),
            ("99204", "Office visit new patient level 4"),
            ("99205", "Office visit new patient level 5"),
            ("99211", "Office visit established level 1"),
            ("99212", "Office visit established level 2"),
            ("99213", "Office visit established level 3"),
            ("99214", "Office visit established level 4"),
            ("99215", "Office visit established level 5"),
            ("99281", "ED visit level 1"),
            ("99282", "ED visit level 2"),
            ("99283", "ED visit level 3"),
            ("99284", "ED visit level 4"),
            ("99285", "ED visit level 5"),
        ],
        "Surgery": [
            ("27447", "Total knee arthroplasty"),
            ("27130", "Total hip arthroplasty"),
            ("47562", "Laparoscopic cholecystectomy"),
            ("49505", "Inguinal hernia repair"),
            ("44950", "Appendectomy"),
            ("33533", "CABG single arterial"),
            ("35301", "Carotid endarterectomy"),
            ("63030", "Lumbar laminotomy"),
            ("29881", "Knee arthroscopy"),
            ("47600", "Cholecystectomy"),
        ],
        "Radiology": [
            ("71046", "Chest X-ray 2 views"),
            ("71250", "CT thorax without contrast"),
            ("71260", "CT thorax with contrast"),
            ("74176", "CT abdomen pelvis without"),
            ("74177", "CT abdomen pelvis with"),
            ("70553", "MRI brain with and without"),
            ("73721", "MRI joint lower extremity"),
            ("76700", "Ultrasound abdominal"),
            ("76856", "Ultrasound pelvic"),
            ("77067", "Screening mammography"),
        ],
        "Cardiology": [
            ("93000", "ECG complete"),
            ("93306", "Echocardiography complete"),
            ("93458", "Coronary angiography"),
            ("93510", "Left heart catheterization"),
            ("92928", "PCI single vessel"),
            ("33249", "ICD insertion"),
            ("33208", "Pacemaker dual chamber"),
        ],
        "Laboratory": [
            ("80053", "Comprehensive metabolic panel"),
            ("80061", "Lipid panel"),
            ("85025", "CBC with differential"),
            ("84443", "TSH"),
            ("83036", "Hemoglobin A1c"),
            ("82947", "Glucose blood"),
            ("80048", "Basic metabolic panel"),
            ("82565", "Creatinine blood"),
        ],
    }
    
    proc_rows = []
    proc_id = 1
    all_procedures = []
    for category, codes in CPT_CATEGORIES.items():
        for code, desc in codes:
            all_procedures.append((proc_id, code, desc))
            proc_rows.append(
                f"INSERT INTO dim_procedure VALUES ({proc_id}, {proc_id}, '{code}', '{escape_sql(desc)}');"
            )
            proc_id += 1
    write_sql_file("dim_procedure.sql", "dim_procedure", proc_rows, len(all_procedures))
    num_procedures = len(all_procedures)
    
    # -------------------------------------------------------------------------
    # 7. DIM_PATIENT (10,000 rows)
    # -------------------------------------------------------------------------
    print("\nGenerating patient dimension...")
    NUM_PATIENTS = 10000
    patient_rows = []
    patients = []
    
    for i in range(1, NUM_PATIENTS + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        full_name = f"{first_name} {last_name}"
        
        # Generate DOB (ages 1-95)
        age_days = random.randint(365, 365 * 95)
        dob = datetime(2025, 1, 1) - timedelta(days=age_days)
        age = age_days // 365
        age_group = calculate_age_group(dob)
        
        gender = random.choice(["M", "F"])
        gender_desc = "Male" if gender == "M" else "Female"
        mrn = f"MRN{i:08d}"
        
        patients.append({
            "key": i,
            "id": i,
            "first_name": first_name,
            "last_name": last_name,
            "dob": dob,
            "age": age,
            "gender": gender
        })
        
        patient_rows.append(
            f"INSERT INTO dim_patient VALUES ({i}, {i}, '{first_name}', '{last_name}', "
            f"'{full_name}', '{dob.strftime('%Y-%m-%d')}', {age}, '{age_group}', "
            f"'{gender}', '{gender_desc}', '{mrn}');"
        )
    
    write_sql_file("dim_patient.sql", "dim_patient", patient_rows, NUM_PATIENTS)
    
    # -------------------------------------------------------------------------
    # 8. DIM_PROVIDER (500 rows with denormalized specialty/department)
    # -------------------------------------------------------------------------
    print("Generating provider dimension...")
    NUM_PROVIDERS = 500
    provider_rows = []
    providers = []
    
    for i in range(1, NUM_PROVIDERS + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        full_name = f"{first_name} {last_name}"
        credential = random.choice(["MD", "DO", "MD", "MD", "DO"])
        
        specialty_idx = random.randint(0, len(SPECIALTIES) - 1)
        specialty_id = specialty_idx + 1
        specialty_name, specialty_code = SPECIALTIES[specialty_idx]
        
        dept_idx = random.randint(0, len(DEPARTMENTS) - 1)
        dept_id = dept_idx + 1
        dept_name = DEPARTMENTS[dept_idx][0]
        
        providers.append({
            "key": i,
            "id": i,
            "name": full_name,
            "specialty_id": specialty_id,
            "specialty_name": specialty_name,
            "specialty_code": specialty_code,
            "dept_id": dept_id,
            "dept_name": dept_name
        })
        
        provider_rows.append(
            f"INSERT INTO dim_provider VALUES ({i}, {i}, '{first_name}', '{last_name}', "
            f"'{full_name}', '{credential}', {specialty_id}, {specialty_id}, "
            f"'{specialty_name}', '{specialty_code}', {dept_id}, {dept_id}, '{escape_sql(dept_name)}');"
        )
    
    write_sql_file("dim_provider.sql", "dim_provider", provider_rows, NUM_PROVIDERS)
    
    # -------------------------------------------------------------------------
    # 9. FACT_ENCOUNTERS (10,000 rows with denormalized attributes)
    # -------------------------------------------------------------------------
    print("\nGenerating fact table (with denormalized attributes)...")
    NUM_ENCOUNTERS = 10000
    fact_rows = []
    encounters = []
    
    # Create a map of patient -> previous encounters for readmission detection
    patient_encounters = {}
    
    for i in range(1, NUM_ENCOUNTERS + 1):
        # Select patient and provider
        patient_idx = random.randint(0, NUM_PATIENTS - 1)
        patient = patients[patient_idx]
        
        provider_idx = random.randint(0, NUM_PROVIDERS - 1)
        provider = providers[provider_idx]
        
        # Select encounter type (60% outpatient, 25% inpatient, 15% ER)
        enc_type_weights = [60, 25, 15]
        enc_type_idx = random.choices([0, 1, 2], weights=enc_type_weights)[0]
        enc_type_code, enc_type_name, is_inpatient, avg_los = ENCOUNTER_TYPES[enc_type_idx]
        
        # Generate dates
        encounter_date = generate_date(2020, 2025)
        
        if is_inpatient:
            los_days = random.randint(1, 14)
            discharge_date = encounter_date + timedelta(days=los_days)
        elif enc_type_name == "Emergency":
            los_hours = random.randint(1, 24)
            discharge_date = encounter_date + timedelta(hours=los_hours)
        else:
            los_hours = random.randint(1, 4)
            discharge_date = encounter_date + timedelta(hours=los_hours)
        
        # Calculate derived values
        date_key = int(encounter_date.strftime("%Y%m%d"))
        discharge_key = int(discharge_date.strftime("%Y%m%d"))
        year = encounter_date.year
        month = encounter_date.month
        month_name = MONTH_NAMES[month]
        quarter = (month - 1) // 3 + 1
        day_of_week = encounter_date.weekday() + 1
        is_weekend = "TRUE" if day_of_week >= 6 else "FALSE"
        
        los_hours_calc = int((discharge_date - encounter_date).total_seconds() // 3600)
        los_days_calc = int((discharge_date - encounter_date).days)
        
        # Counts (random but realistic)
        diagnosis_count = random.choices([1, 2, 3, 4, 5], weights=[15, 40, 30, 10, 5])[0]
        procedure_count = random.choices([0, 1, 2, 3, 4], weights=[20, 40, 25, 10, 5])[0]
        
        # Primary diagnosis
        primary_diag_idx = random.randint(0, num_diagnoses - 1)
        primary_diag = all_diagnoses[primary_diag_idx]
        primary_diag_key = primary_diag[0]
        primary_icd10_code = primary_diag[1]
        primary_icd10_desc = primary_diag[2]
        
        # Billing amounts
        if enc_type_name == "Outpatient":
            claim_amount = round(random.uniform(100, 2000), 2)
        elif enc_type_name == "Emergency":
            claim_amount = round(random.uniform(500, 10000), 2)
        else:
            claim_amount = round(random.uniform(5000, 100000), 2)
        
        allowed_amount = round(claim_amount * random.uniform(0.6, 0.9), 2)
        
        # Readmission detection
        is_readmission = "FALSE"
        days_since_last = "NULL"
        
        if is_inpatient and patient["id"] in patient_encounters:
            prev_encounters = patient_encounters[patient["id"]]
            for prev in prev_encounters:
                if prev["is_inpatient"] and prev["discharge_date"]:
                    days_diff = (encounter_date - prev["discharge_date"]).days
                    if 0 < days_diff <= 30:
                        is_readmission = "TRUE"
                        days_since_last = days_diff
                        break
        
        # Track for future readmission detection
        if patient["id"] not in patient_encounters:
            patient_encounters[patient["id"]] = []
        patient_encounters[patient["id"]].append({
            "encounter_date": encounter_date,
            "discharge_date": discharge_date,
            "is_inpatient": is_inpatient
        })
        
        encounters.append({
            "key": i,
            "patient_key": patient["key"],
            "provider_key": provider["key"],
            "date": encounter_date
        })
        
        # Build the INSERT statement with all denormalized attributes
        is_inp_str = "TRUE" if is_inpatient else "FALSE"
        
        fact_rows.append(
            f"INSERT INTO fact_encounters VALUES ("
            f"{i}, {i}, "  # encounter_key, encounter_id
            f"{date_key}, {discharge_key}, "  # date keys
            f"{patient['key']}, {provider['key']}, {provider['dept_id']}, "  # FK to dims
            f"{enc_type_idx + 1}, {provider['specialty_id']}, {primary_diag_key}, "  # more FKs
            f"'{encounter_date.strftime('%Y-%m-%d %H:%M:%S')}', "  # encounter_date
            f"'{discharge_date.strftime('%Y-%m-%d %H:%M:%S')}', "  # discharge_date
            f"{year}, {month}, '{month_name}', {quarter}, {day_of_week}, {is_weekend}, "  # date attrs
            f"'{provider['specialty_name']}', '{provider['specialty_code']}', "  # specialty
            f"'{escape_sql(provider['dept_name'])}', '{provider['name']}', "  # dept, provider name
            f"'{enc_type_name}', {is_inp_str}, "  # encounter type
            f"'{primary_icd10_code}', '{escape_sql(primary_icd10_desc)}', "  # primary diag
            f"{diagnosis_count}, {procedure_count}, "  # counts
            f"{claim_amount}, {allowed_amount}, 1, "  # billing
            f"{los_hours_calc}, {los_days_calc}, {is_readmission}, {days_since_last}"  # derived
            f");"
        )
    
    write_sql_file("fact_encounters.sql", "fact_encounters", fact_rows, NUM_ENCOUNTERS)
    
    # -------------------------------------------------------------------------
    # 10. BRIDGE_ENCOUNTER_DIAGNOSES
    # -------------------------------------------------------------------------
    print("\nGenerating bridge tables...")
    bridge_diag_rows = []
    bridge_id = 1
    
    for enc in encounters:
        # 1-5 diagnoses per encounter
        num_diags = random.choices([1, 2, 3, 4, 5], weights=[15, 40, 30, 10, 5])[0]
        used_diags = set()
        
        for seq in range(1, num_diags + 1):
            diag_key = random.randint(1, num_diagnoses)
            while diag_key in used_diags:
                diag_key = random.randint(1, num_diagnoses)
            used_diags.add(diag_key)
            
            bridge_diag_rows.append(
                f"INSERT INTO bridge_encounter_diagnoses VALUES ({bridge_id}, {enc['key']}, {diag_key}, {seq});"
            )
            bridge_id += 1
    
    write_sql_file("bridge_diagnoses.sql", "bridge_encounter_diagnoses", bridge_diag_rows, bridge_id - 1)
    
    # -------------------------------------------------------------------------
    # 11. BRIDGE_ENCOUNTER_PROCEDURES
    # -------------------------------------------------------------------------
    bridge_proc_rows = []
    bridge_id = 1
    
    for enc in encounters:
        # 0-4 procedures per encounter
        num_procs = random.choices([0, 1, 2, 3, 4], weights=[20, 40, 25, 10, 5])[0]
        used_procs = set()
        
        for _ in range(num_procs):
            proc_key = random.randint(1, num_procedures)
            while proc_key in used_procs:
                proc_key = random.randint(1, num_procedures)
            used_procs.add(proc_key)
            
            proc_date = enc["date"] + timedelta(days=random.randint(0, 3))
            
            bridge_proc_rows.append(
                f"INSERT INTO bridge_encounter_procedures VALUES ({bridge_id}, {enc['key']}, {proc_key}, '{proc_date.strftime('%Y-%m-%d')}');"
            )
            bridge_id += 1
    
    write_sql_file("bridge_procedures.sql", "bridge_encounter_procedures", bridge_proc_rows, bridge_id - 1)
    
    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("OLAP DATA GENERATION COMPLETE")
    print("=" * 70)
    print(f"\nFiles written to: {OLAP_DIR.absolute()}")
    print()


if __name__ == "__main__":
    main()
