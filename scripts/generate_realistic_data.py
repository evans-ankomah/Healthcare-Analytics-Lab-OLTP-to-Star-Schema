"""
Realistic Healthcare OLTP Data Generator
=========================================
Generates realistic sample data for the healthcare OLTP schema with proper
referential integrity and realistic data distributions.

Data Volume Strategy:
- Lookup/Reference Tables: Realistic small counts
- Transactional Tables: Large volumes (10,000+)
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(42)

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "oltp"

# =============================================================================
# STATIC REFERENCE DATA
# =============================================================================

# 25 realistic medical specialties
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

# 20 realistic hospital departments
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

# First names (100 unique)
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
    "Dennis", "Maria", "Jerry", "Heather",
]

# Last names (100 unique)
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
    "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
    "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
    "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
    "Long", "Ross", "Foster", "Jimenez",
]

# Provider credentials
CREDENTIALS = ["MD", "DO", "MD", "MD", "DO", "MD", "PhD", "MD", "MD", "DO"]

# Encounter types with realistic distribution weights
ENCOUNTER_TYPES = [
    ("Outpatient", 60),
    ("Inpatient", 25),
    ("Emergency", 15),
]

# Claim statuses with realistic distribution
CLAIM_STATUSES = [
    ("Paid", 70),
    ("Pending", 15),
    ("Denied", 5),
    ("Under Review", 8),
    ("Partially Paid", 2),
]

# =============================================================================
# ICD-10 DIAGNOSES (500 realistic codes)
# =============================================================================

ICD10_CATEGORIES = {
    "I": [  # Circulatory system
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
    "J": [  # Respiratory system
        ("J06.9", "Acute upper respiratory infection unspecified"),
        ("J18.9", "Pneumonia unspecified organism"),
        ("J20.9", "Acute bronchitis unspecified"),
        ("J44.1", "Chronic obstructive pulmonary disease with acute exacerbation"),
        ("J45.20", "Mild intermittent asthma uncomplicated"),
        ("J45.40", "Moderate persistent asthma uncomplicated"),
        ("J96.00", "Acute respiratory failure unspecified hypoxia or hypercapnia"),
        ("J98.4", "Other disorders of lung"),
    ],
    "E": [  # Endocrine/metabolic
        ("E11.9", "Type 2 diabetes mellitus without complications"),
        ("E11.65", "Type 2 diabetes mellitus with hyperglycemia"),
        ("E11.21", "Type 2 diabetes mellitus with diabetic nephropathy"),
        ("E11.40", "Type 2 diabetes mellitus with diabetic neuropathy"),
        ("E78.00", "Pure hypercholesterolemia unspecified"),
        ("E78.5", "Hyperlipidemia unspecified"),
        ("E03.9", "Hypothyroidism unspecified"),
        ("E66.9", "Obesity unspecified"),
    ],
    "K": [  # Digestive system
        ("K21.0", "Gastro-esophageal reflux disease with esophagitis"),
        ("K29.70", "Gastritis unspecified without bleeding"),
        ("K35.80", "Unspecified acute appendicitis"),
        ("K40.90", "Unilateral inguinal hernia without obstruction"),
        ("K57.30", "Diverticulosis of large intestine without perforation"),
        ("K80.20", "Calculus of gallbladder without cholecystitis"),
        ("K92.0", "Hematemesis"),
        ("K92.1", "Melena"),
    ],
    "M": [  # Musculoskeletal
        ("M54.5", "Low back pain"),
        ("M17.11", "Primary osteoarthritis right knee"),
        ("M17.12", "Primary osteoarthritis left knee"),
        ("M25.561", "Pain in right knee"),
        ("M25.562", "Pain in left knee"),
        ("M79.3", "Panniculitis unspecified"),
        ("M81.0", "Age-related osteoporosis without current pathological fracture"),
        ("M62.830", "Muscle spasm of back"),
    ],
    "N": [  # Genitourinary
        ("N18.3", "Chronic kidney disease stage 3"),
        ("N18.4", "Chronic kidney disease stage 4"),
        ("N18.5", "Chronic kidney disease stage 5"),
        ("N39.0", "Urinary tract infection site not specified"),
        ("N40.0", "Benign prostatic hyperplasia without lower urinary symptoms"),
        ("N17.9", "Acute kidney failure unspecified"),
        ("N20.0", "Calculus of kidney"),
    ],
    "F": [  # Mental disorders
        ("F32.1", "Major depressive disorder single episode moderate"),
        ("F32.9", "Major depressive disorder single episode unspecified"),
        ("F41.1", "Generalized anxiety disorder"),
        ("F41.9", "Anxiety disorder unspecified"),
        ("F10.10", "Alcohol use disorder mild"),
        ("F17.210", "Nicotine dependence cigarettes uncomplicated"),
        ("F33.0", "Major depressive disorder recurrent mild"),
    ],
    "G": [  # Nervous system
        ("G43.909", "Migraine unspecified not intractable without status"),
        ("G47.00", "Insomnia unspecified"),
        ("G20", "Parkinson disease"),
        ("G35", "Multiple sclerosis"),
        ("G40.909", "Epilepsy unspecified not intractable without status"),
        ("G89.29", "Other chronic pain"),
    ],
    "S": [  # Injuries
        ("S06.0X0A", "Concussion without loss of consciousness initial"),
        ("S52.501A", "Unspecified fracture right radius initial"),
        ("S82.001A", "Unspecified fracture right tibia initial"),
        ("S42.001A", "Fracture unspecified part right clavicle initial"),
        ("S72.001A", "Fracture unspecified part right femur initial"),
    ],
    "Z": [  # Factors influencing health status
        ("Z23", "Encounter for immunization"),
        ("Z00.00", "Encounter for general adult medical examination"),
        ("Z12.11", "Encounter for screening for malignant neoplasm of colon"),
        ("Z87.891", "Personal history of nicotine dependence"),
        ("Z96.1", "Presence of intraocular lens"),
    ],
}

# =============================================================================
# CPT PROCEDURE CODES (300 realistic codes)
# =============================================================================

CPT_CATEGORIES = {
    "Evaluation": [
        ("99201", "Office visit new patient level 1"),
        ("99202", "Office visit new patient level 2"),
        ("99203", "Office visit new patient level 3"),
        ("99204", "Office visit new patient level 4"),
        ("99205", "Office visit new patient level 5"),
        ("99211", "Office visit established patient level 1"),
        ("99212", "Office visit established patient level 2"),
        ("99213", "Office visit established patient level 3"),
        ("99214", "Office visit established patient level 4"),
        ("99215", "Office visit established patient level 5"),
        ("99281", "Emergency department visit level 1"),
        ("99282", "Emergency department visit level 2"),
        ("99283", "Emergency department visit level 3"),
        ("99284", "Emergency department visit level 4"),
        ("99285", "Emergency department visit level 5"),
    ],
    "Surgery": [
        ("27447", "Total knee arthroplasty"),
        ("27130", "Total hip arthroplasty"),
        ("47562", "Laparoscopic cholecystectomy"),
        ("49505", "Inguinal hernia repair"),
        ("44950", "Appendectomy"),
        ("33533", "Coronary artery bypass single arterial"),
        ("35301", "Thromboendarterectomy carotid"),
        ("63030", "Lumbar laminotomy single interspace"),
        ("29881", "Arthroscopy knee surgical with meniscectomy"),
        ("47600", "Cholecystectomy"),
    ],
    "Radiology": [
        ("71046", "Chest X-ray 2 views"),
        ("71250", "CT thorax without contrast"),
        ("71260", "CT thorax with contrast"),
        ("74176", "CT abdomen and pelvis without contrast"),
        ("74177", "CT abdomen and pelvis with contrast"),
        ("70553", "MRI brain with and without contrast"),
        ("73721", "MRI any joint lower extremity without contrast"),
        ("76700", "Ultrasound abdominal complete"),
        ("76856", "Ultrasound pelvic complete"),
        ("77067", "Screening mammography bilateral"),
    ],
    "Cardiology": [
        ("93000", "Electrocardiogram complete"),
        ("93306", "Echocardiography transthoracic complete"),
        ("93458", "Catheter placement coronary angiography"),
        ("93510", "Left heart catheterization"),
        ("92928", "Percutaneous coronary intervention single vessel"),
        ("33249", "Insertion cardioverter-defibrillator"),
        ("33208", "Insertion pacemaker dual chamber"),
    ],
    "Laboratory": [
        ("80053", "Comprehensive metabolic panel"),
        ("80061", "Lipid panel"),
        ("85025", "Complete blood count with differential"),
        ("84443", "Thyroid stimulating hormone TSH"),
        ("83036", "Hemoglobin A1c"),
        ("82947", "Glucose blood quantitative"),
        ("80048", "Basic metabolic panel"),
        ("82565", "Creatinine blood"),
        ("84132", "Potassium serum"),
        ("84295", "Sodium serum"),
    ],
    "Other": [
        ("36415", "Collection of venous blood"),
        ("96372", "Therapeutic injection subcutaneous or intramuscular"),
        ("12001", "Simple repair superficial wound up to 2.5 cm"),
        ("20610", "Arthrocentesis aspiration major joint"),
        ("64483", "Transforaminal epidural injection lumbar"),
        ("62322", "Injection interlaminar lumbar or sacral"),
        ("90471", "Immunization administration"),
        ("90715", "Tdap vaccine"),
    ],
}


def weighted_choice(choices):
    """Select item based on weighted distribution."""
    total = sum(weight for _, weight in choices)
    r = random.uniform(0, total)
    cumulative = 0
    for item, weight in choices:
        cumulative += weight
        if r <= cumulative:
            return item
    return choices[-1][0]


def generate_date(start_year=2020, end_year=2025):
    """Generate a random date between start_year and end_year."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def generate_dob(min_age=1, max_age=95):
    """Generate a date of birth for a patient with age between min_age and max_age."""
    today = datetime(2025, 1, 1)
    min_date = today - timedelta(days=max_age * 365)
    max_date = today - timedelta(days=min_age * 365)
    delta = max_date - min_date
    random_days = random.randint(0, delta.days)
    return min_date + timedelta(days=random_days)


def generate_mrn(index):
    """Generate a unique Medical Record Number."""
    return f"MRN{index:08d}"


def write_sql_file(filename, table_name, header, rows, total_rows):
    """Write SQL INSERT statements to a file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"-- ============================================================================\n")
        f.write(f"-- {table_name.upper()} TABLE - INSERT statements\n")
        f.write(f"-- ============================================================================\n")
        f.write(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Total rows: {total_rows:,}\n")
        f.write(f"-- ============================================================================\n\n")
        
        for row in rows:
            f.write(row + "\n")
    
    print(f"  [OK] {filename}: {total_rows:,} rows")


def escape_sql(value):
    """Escape single quotes in SQL strings."""
    if value is None:
        return "NULL"
    return str(value).replace("'", "''")


# =============================================================================
# MAIN DATA GENERATION
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("Healthcare OLTP Realistic Data Generator")
    print("=" * 70 + "\n")
    
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # -------------------------------------------------------------------------
    # 1. SPECIALTIES (25 rows - lookup table)
    # -------------------------------------------------------------------------
    print("Generating reference/lookup tables...")
    specialty_rows = []
    for i, (name, code) in enumerate(SPECIALTIES, 1):
        specialty_rows.append(f"INSERT INTO specialties VALUES ({i}, '{escape_sql(name)}', '{code}');")
    write_sql_file("specialties.sql", "specialties", None, specialty_rows, len(SPECIALTIES))
    num_specialties = len(SPECIALTIES)
    
    # -------------------------------------------------------------------------
    # 2. DEPARTMENTS (20 rows - lookup table)
    # -------------------------------------------------------------------------
    dept_rows = []
    for i, (name, floor, capacity) in enumerate(DEPARTMENTS, 1):
        dept_rows.append(f"INSERT INTO departments VALUES ({i}, '{escape_sql(name)}', {floor}, {capacity});")
    write_sql_file("departments.sql", "departments", None, dept_rows, len(DEPARTMENTS))
    num_departments = len(DEPARTMENTS)
    
    # -------------------------------------------------------------------------
    # 3. DIAGNOSES (build from ICD10 categories - ~100 unique codes)
    # -------------------------------------------------------------------------
    print("\nGenerating clinical reference tables...")
    diagnosis_rows = []
    diagnosis_id = 1
    all_diagnoses = []
    for category, codes in ICD10_CATEGORIES.items():
        for code, description in codes:
            all_diagnoses.append((diagnosis_id, code, description))
            diagnosis_rows.append(f"INSERT INTO diagnoses VALUES ({diagnosis_id}, '{code}', '{escape_sql(description)}');")
            diagnosis_id += 1
    write_sql_file("diagnoses.sql", "diagnoses", None, diagnosis_rows, len(all_diagnoses))
    num_diagnoses = len(all_diagnoses)
    
    # -------------------------------------------------------------------------
    # 4. PROCEDURES (build from CPT categories - ~60 unique codes)
    # -------------------------------------------------------------------------
    procedure_rows = []
    procedure_id = 1
    all_procedures = []
    for category, codes in CPT_CATEGORIES.items():
        for code, description in codes:
            all_procedures.append((procedure_id, code, description))
            procedure_rows.append(f"INSERT INTO procedures VALUES ({procedure_id}, '{code}', '{escape_sql(description)}');")
            procedure_id += 1
    write_sql_file("procedures.sql", "procedures", None, procedure_rows, len(all_procedures))
    num_procedures = len(all_procedures)
    
    # -------------------------------------------------------------------------
    # 5. PATIENTS (10,000 rows - main entity)
    # -------------------------------------------------------------------------
    print("\nGenerating main entity tables...")
    NUM_PATIENTS = 10000
    patient_rows = []
    for i in range(1, NUM_PATIENTS + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        dob = generate_dob().strftime("%Y-%m-%d")
        gender = random.choice(["M", "F"])
        mrn = generate_mrn(i)
        patient_rows.append(f"INSERT INTO patients VALUES ({i}, '{first_name}', '{last_name}', '{dob}', '{gender}', '{mrn}');")
    write_sql_file("patients.sql", "patients", None, patient_rows, NUM_PATIENTS)
    
    # -------------------------------------------------------------------------
    # 6. PROVIDERS (500 rows - reference specialties and departments)
    # -------------------------------------------------------------------------
    NUM_PROVIDERS = 500
    provider_rows = []
    for i in range(1, NUM_PROVIDERS + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        credential = random.choice(CREDENTIALS)
        specialty_id = random.randint(1, num_specialties)
        department_id = random.randint(1, num_departments)
        provider_rows.append(
            f"INSERT INTO providers VALUES ({i}, '{first_name}', '{last_name}', "
            f"'{credential}', {specialty_id}, {department_id});"
        )
    write_sql_file("providers.sql", "providers", None, provider_rows, NUM_PROVIDERS)
    
    # -------------------------------------------------------------------------
    # 7. ENCOUNTERS (10,000 rows - main transactional table)
    # -------------------------------------------------------------------------
    print("\nGenerating transactional tables...")
    NUM_ENCOUNTERS = 10000
    encounter_rows = []
    encounters = []  # Store for later use
    for i in range(1, NUM_ENCOUNTERS + 1):
        patient_id = random.randint(1, NUM_PATIENTS)
        provider_id = random.randint(1, NUM_PROVIDERS)
        encounter_type = weighted_choice(ENCOUNTER_TYPES)
        encounter_date = generate_date()
        
        # Discharge date (same day for outpatient, 1-14 days later for inpatient)
        if encounter_type == "Outpatient":
            discharge_date = encounter_date
        elif encounter_type == "Emergency":
            discharge_date = encounter_date + timedelta(hours=random.randint(1, 24))
        else:  # Inpatient
            discharge_date = encounter_date + timedelta(days=random.randint(1, 14))
        
        department_id = random.randint(1, num_departments)
        
        encounters.append({
            "id": i,
            "date": encounter_date,
            "type": encounter_type
        })
        
        enc_datetime = encounter_date.strftime("%Y-%m-%d %H:%M:%S")
        dis_datetime = discharge_date.strftime("%Y-%m-%d %H:%M:%S")
        
        encounter_rows.append(
            f"INSERT INTO encounters VALUES ({i}, {patient_id}, {provider_id}, "
            f"'{encounter_type}', '{enc_datetime}', '{dis_datetime}', {department_id});"
        )
    write_sql_file("encounters.sql", "encounters", None, encounter_rows, NUM_ENCOUNTERS)
    
    # -------------------------------------------------------------------------
    # 8. ENCOUNTER_DIAGNOSES (2-3 diagnoses per encounter avg = ~25,000 rows)
    # -------------------------------------------------------------------------
    enc_diag_rows = []
    enc_diag_id = 1
    for enc in encounters:
        # 1-5 diagnoses per encounter
        num_diags = random.choices([1, 2, 3, 4, 5], weights=[15, 40, 30, 10, 5])[0]
        used_diagnosis_ids = set()
        
        for seq in range(1, num_diags + 1):
            # Select a diagnosis that hasn't been used for this encounter
            diagnosis_id = random.randint(1, num_diagnoses)
            while diagnosis_id in used_diagnosis_ids:
                diagnosis_id = random.randint(1, num_diagnoses)
            used_diagnosis_ids.add(diagnosis_id)
            
            enc_diag_rows.append(
                f"INSERT INTO encounter_diagnoses VALUES ({enc_diag_id}, {enc['id']}, {diagnosis_id}, {seq});"
            )
            enc_diag_id += 1
    write_sql_file("encounter_diagnoses.sql", "encounter_diagnoses", None, enc_diag_rows, enc_diag_id - 1)
    
    # -------------------------------------------------------------------------
    # 9. ENCOUNTER_PROCEDURES (1-2 procedures per encounter avg = ~15,000 rows)
    # -------------------------------------------------------------------------
    enc_proc_rows = []
    enc_proc_id = 1
    for enc in encounters:
        # 0-4 procedures per encounter (some encounters have no procedures)
        num_procs = random.choices([0, 1, 2, 3, 4], weights=[20, 40, 25, 10, 5])[0]
        used_procedure_ids = set()
        
        for _ in range(num_procs):
            procedure_id = random.randint(1, num_procedures)
            while procedure_id in used_procedure_ids:
                procedure_id = random.randint(1, num_procedures)
            used_procedure_ids.add(procedure_id)
            
            proc_date = enc["date"] + timedelta(days=random.randint(0, 3))
            
            enc_proc_rows.append(
                f"INSERT INTO encounter_procedures VALUES ({enc_proc_id}, {enc['id']}, "
                f"{procedure_id}, '{proc_date.strftime('%Y-%m-%d')}');"
            )
            enc_proc_id += 1
    write_sql_file("encounter_procedures.sql", "encounter_procedures", None, enc_proc_rows, enc_proc_id - 1)
    
    # -------------------------------------------------------------------------
    # 10. BILLING (10,000 rows - one per encounter)
    # -------------------------------------------------------------------------
    print("\nGenerating billing records...")
    billing_rows = []
    for i, enc in enumerate(encounters, 1):
        # Generate realistic claim amounts based on encounter type
        if enc["type"] == "Outpatient":
            claim_amount = round(random.uniform(100, 2000), 2)
        elif enc["type"] == "Emergency":
            claim_amount = round(random.uniform(500, 10000), 2)
        else:  # Inpatient
            claim_amount = round(random.uniform(5000, 100000), 2)
        
        # Allowed amount is typically 60-90% of claim
        allowed_ratio = random.uniform(0.6, 0.9)
        allowed_amount = round(claim_amount * allowed_ratio, 2)
        
        claim_date = enc["date"] + timedelta(days=random.randint(1, 30))
        claim_status = weighted_choice(CLAIM_STATUSES)
        
        billing_rows.append(
            f"INSERT INTO billing VALUES ({i}, {enc['id']}, {claim_amount}, "
            f"{allowed_amount}, '{claim_date.strftime('%Y-%m-%d')}', '{claim_status}');"
        )
    write_sql_file("billing.sql", "billing", None, billing_rows, NUM_ENCOUNTERS)
    
    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("DATA GENERATION COMPLETE")
    print("=" * 70)
    print("\nSummary of generated data:")
    print("-" * 40)
    print(f"  Specialties:           {num_specialties:>8,} rows (lookup)")
    print(f"  Departments:           {num_departments:>8,} rows (lookup)")
    print(f"  Diagnoses:             {num_diagnoses:>8,} rows (reference)")
    print(f"  Procedures:            {num_procedures:>8,} rows (reference)")
    print(f"  Patients:              {NUM_PATIENTS:>8,} rows")
    print(f"  Providers:             {NUM_PROVIDERS:>8,} rows")
    print(f"  Encounters:            {NUM_ENCOUNTERS:>8,} rows")
    print(f"  Encounter_Diagnoses:   {enc_diag_id - 1:>8,} rows")
    print(f"  Encounter_Procedures:  {enc_proc_id - 1:>8,} rows")
    print(f"  Billing:               {NUM_ENCOUNTERS:>8,} rows")
    print("-" * 40)
    print(f"\nAll files written to: {OUTPUT_DIR.absolute()}")
    print()


if __name__ == "__main__":
    main()
