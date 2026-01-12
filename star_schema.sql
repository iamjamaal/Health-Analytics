
----- HEALTHCARE ANALYTICS STAR SCHEMA


-- CLEANUP: Drop existing objects 
-- Drop in reverse dependency order
DROP TABLE IF EXISTS bridge_encounter_procedures;
DROP TABLE IF EXISTS bridge_encounter_diagnoses;
DROP TABLE IF EXISTS fact_encounters_monthly_summary;
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS dim_procedure;
DROP TABLE IF EXISTS dim_diagnosis;
DROP TABLE IF EXISTS dim_encounter_type;
DROP TABLE IF EXISTS dim_provider;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_specialty;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_date;

-- Drop views if they exist
DROP VIEW IF EXISTS vw_encounter_summary;
DROP VIEW IF EXISTS vw_readmission_analysis;
DROP VIEW IF EXISTS vw_financial_summary;





-- DIMENSION TABLES

-- DIM_DATE: Pre-computed date dimension
-- Type 0 SCD: Loaded once, never changes
CREATE TABLE dim_date (
    -- Primary Key
    date_key INT PRIMARY KEY 
        COMMENT 'Surrogate key in YYYYMMDD format (e.g., 20240115)',
    
    -- Natural date
    calendar_date DATE NOT NULL UNIQUE 
        COMMENT 'Actual date value for joining',
    
    -- Calendar hierarchy
    year INT NOT NULL 
        COMMENT 'Calendar year (e.g., 2024)',
    quarter INT NOT NULL 
        COMMENT 'Calendar quarter: 1, 2, 3, 4',
    month INT NOT NULL 
        COMMENT 'Month number: 1-12',
    month_name VARCHAR(20) NOT NULL 
        COMMENT 'Full month name: January, February, etc.',
    month_abbr CHAR(3) NOT NULL
        COMMENT 'Month abbreviation: Jan, Feb, etc.',
    week_of_year INT NOT NULL 
        COMMENT 'ISO week of year: 1-53',
    day_of_month INT NOT NULL 
        COMMENT 'Day of month: 1-31',
    day_of_week INT NOT NULL 
        COMMENT 'Day of week: 1=Monday, 7=Sunday',
    day_name VARCHAR(20) NOT NULL 
        COMMENT 'Day name: Monday, Tuesday, etc.',
    day_abbr CHAR(3) NOT NULL
        COMMENT 'Day abbreviation: Mon, Tue, etc.',
    
    -- Boolean flags
    is_weekend BOOLEAN NOT NULL 
        COMMENT 'TRUE if Saturday or Sunday',
    is_holiday BOOLEAN DEFAULT FALSE 
        COMMENT 'TRUE if recognized holiday',
    is_weekday BOOLEAN NOT NULL
        COMMENT 'TRUE if Monday-Friday',
    
    -- Fiscal calendar (assuming fiscal year starts July 1)
    fiscal_year INT NOT NULL 
        COMMENT 'Fiscal year (FY starts July 1)',
    fiscal_quarter INT NOT NULL 
        COMMENT 'Fiscal quarter: 1-4',
    fiscal_month INT NOT NULL
        COMMENT 'Fiscal month: 1-12',
    fiscal_week INT NOT NULL
        COMMENT 'Fiscal week of year',
    
    -- Additional useful attributes
    day_of_year INT NOT NULL
        COMMENT 'Day number in year: 1-366',
    week_of_month INT NOT NULL
        COMMENT 'Week of month: 1-5',
    
    -- Indexes for common query patterns
    INDEX idx_calendar_date (calendar_date),
    INDEX idx_year_month (year, month),
    INDEX idx_year_quarter (year, quarter),
    INDEX idx_fiscal_year_quarter (fiscal_year, fiscal_quarter),
    INDEX idx_year_week (year, week_of_year),
    INDEX idx_day_of_week (day_of_week)
    
) COMMENT 'Date dimension with pre-computed time hierarchies and attributes';


-- DIM_PATIENT: Patient demographics
-- Type 1 SCD: Overwrite changes (demographics update rarely)
CREATE TABLE dim_patient (
    -- Surrogate Key
    patient_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for patient dimension',
    
    -- Business Key
    patient_id INT NOT NULL UNIQUE 
        COMMENT 'Natural key from OLTP system',
    
    -- Patient identifiers
    mrn VARCHAR(20) NOT NULL UNIQUE 
        COMMENT 'Medical Record Number - unique patient identifier',
    
    -- Demographics
    first_name VARCHAR(100) NOT NULL 
        COMMENT 'Patient first name',
    last_name VARCHAR(100) NOT NULL 
        COMMENT 'Patient last name',
    full_name VARCHAR(200) NOT NULL 
        COMMENT 'Concatenated full name for reporting',
    
    date_of_birth DATE NOT NULL 
        COMMENT 'Patient date of birth',
    
    -- Calculated age attributes (updated during ETL)
    age INT NOT NULL 
        COMMENT 'Current age in years',
    age_group VARCHAR(20) NOT NULL 
        COMMENT 'Age grouping: 0-17, 18-35, 36-50, 51-65, 66+',
    age_decade VARCHAR(20) NOT NULL
        COMMENT 'Age by decade: 0-9, 10-19, 20-29, etc.',
    
    -- Gender
    gender CHAR(1) NOT NULL 
        COMMENT 'Gender code: M, F, O (Other), U (Unknown)',
    gender_description VARCHAR(20) NOT NULL 
        COMMENT 'Gender description: Male, Female, Other, Unknown',
    
    -- SCD attributes
    effective_date DATE NOT NULL 
        COMMENT 'Date this record became effective',
    end_date DATE DEFAULT '9999-12-31'
        COMMENT 'Date this record expired (9999-12-31 for current)',
    is_current BOOLEAN DEFAULT TRUE 
        COMMENT 'TRUE if this is the current version',
    
    -- Data quality flags
    is_test_patient BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if test/training patient',
    
    -- Indexes
    INDEX idx_patient_id (patient_id),
    INDEX idx_mrn (mrn),
    INDEX idx_age_group (age_group),
    INDEX idx_gender (gender),
    INDEX idx_full_name (full_name),
    INDEX idx_is_current (is_current),
    INDEX idx_effective_date (effective_date)
    
) COMMENT 'Patient demographics with calculated age attributes (Type 1 SCD)';

-- --------------------------------------------
-- DIM_SPECIALTY: Medical specialties
-- Type 1 SCD: Reference data, updates rare
-- --------------------------------------------
CREATE TABLE dim_specialty (
    -- Surrogate Key
    specialty_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for specialty dimension',
    
    -- Business Key
    specialty_id INT NOT NULL UNIQUE 
        COMMENT 'Natural key from OLTP system',
    
    -- Specialty attributes
    specialty_name VARCHAR(100) NOT NULL 
        COMMENT 'Full specialty name',
    specialty_code VARCHAR(10) NOT NULL 
        COMMENT 'Specialty abbreviation code',
    specialty_category VARCHAR(50) 
        COMMENT 'Higher-level grouping: Surgical, Medical, Diagnostic, Emergency',
    specialty_description TEXT
        COMMENT 'Detailed description of specialty',
    
    -- Operational attributes
    is_surgical BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if surgical specialty',
    is_primary_care BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if primary care specialty',
    requires_referral BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if typically requires referral',
    
    -- Indexes
    INDEX idx_specialty_id (specialty_id),
    INDEX idx_specialty_name (specialty_name),
    INDEX idx_specialty_code (specialty_code),
    INDEX idx_specialty_category (specialty_category)
    
) COMMENT 'Medical specialties reference dimension';



-- DIM_DEPARTMENT: Hospital departments
-- Type 1 SCD: Organizational structure
CREATE TABLE dim_department (
    -- Surrogate Key
    department_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for department dimension',
    
    -- Business Key
    department_id INT NOT NULL UNIQUE 
        COMMENT 'Natural key from OLTP system',
    
    -- Department attributes
    department_name VARCHAR(100) NOT NULL 
        COMMENT 'Full department name',
    department_code VARCHAR(20)
        COMMENT 'Department code/abbreviation',
    
    -- Physical attributes
    floor INT 
        COMMENT 'Physical floor location in hospital',
    building VARCHAR(50)
        COMMENT 'Building name or identifier',
    capacity INT 
        COMMENT 'Patient capacity (beds or slots)',
    
    -- Categorization
    department_type VARCHAR(50) 
        COMMENT 'Type: Clinical, Surgical, Diagnostic, Support, Administrative',
    service_line VARCHAR(100)
        COMMENT 'Service line: Cardiology, Orthopedics, etc.',
    
    -- Operational attributes
    is_inpatient BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if provides inpatient services',
    is_outpatient BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if provides outpatient services',
    is_emergency BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if emergency department',
    is_critical_care BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if ICU or critical care',
    
    -- Contact information
    phone VARCHAR(20)
        COMMENT 'Department phone number',
    email VARCHAR(100)
        COMMENT 'Department email',
    
    -- Indexes
    INDEX idx_department_id (department_id),
    INDEX idx_department_name (department_name),
    INDEX idx_department_type (department_type),
    INDEX idx_floor (floor)
    
) COMMENT 'Hospital departments with physical and operational attributes';


-- DIM_PROVIDER: Healthcare providers
-- Type 1 SCD with denormalized specialty/department
CREATE TABLE dim_provider (
    -- Surrogate Key
    provider_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for provider dimension',
    
    -- Business Key
    provider_id INT NOT NULL UNIQUE 
        COMMENT 'Natural key from OLTP system',
    
    -- Provider identification
    first_name VARCHAR(100) NOT NULL 
        COMMENT 'Provider first name',
    last_name VARCHAR(100) NOT NULL 
        COMMENT 'Provider last name',
    full_name VARCHAR(200) NOT NULL 
        COMMENT 'Concatenated full name for reporting',
    
    credential VARCHAR(20) NOT NULL 
        COMMENT 'Professional credential: MD, DO, NP, PA, RN',
    npi VARCHAR(10)
        COMMENT 'National Provider Identifier',
    license_number VARCHAR(50)
        COMMENT 'State medical license number',
    
    -- Denormalized specialty (reduces JOINs)
    specialty_key INT NOT NULL 
        COMMENT 'FK to dim_specialty',
    specialty_name VARCHAR(100) NOT NULL 
        COMMENT 'Denormalized specialty name for query convenience',
    specialty_code VARCHAR(10) NOT NULL
        COMMENT 'Denormalized specialty code',
    
    -- Denormalized department (reduces JOINs)
    department_key INT NOT NULL 
        COMMENT 'FK to dim_department',
    department_name VARCHAR(100) NOT NULL 
        COMMENT 'Denormalized department name for query convenience',
    
    -- Provider attributes
    provider_type VARCHAR(50)
        COMMENT 'Provider type: Attending, Resident, Fellow, APP',
    is_active BOOLEAN DEFAULT TRUE
        COMMENT 'TRUE if currently active',
    hire_date DATE
        COMMENT 'Date provider joined organization',
    
    -- Contact information
    email VARCHAR(100)
        COMMENT 'Provider email',
    phone VARCHAR(20)
        COMMENT 'Provider phone',
    
    -- SCD attributes
    effective_date DATE NOT NULL 
        COMMENT 'Date this record became effective',
    end_date DATE DEFAULT '9999-12-31'
        COMMENT 'Date this record expired',
    is_current BOOLEAN DEFAULT TRUE 
        COMMENT 'TRUE if this is the current version',
    
    -- Indexes
    INDEX idx_provider_id (provider_id),
    INDEX idx_specialty_key (specialty_key),
    INDEX idx_department_key (department_key),
    INDEX idx_full_name (full_name),
    INDEX idx_credential (credential),
    INDEX idx_npi (npi),
    INDEX idx_is_active (is_active),
    
    -- Foreign Keys
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
    
) COMMENT 'Provider dimension with denormalized specialty and department (Type 1 SCD)';


-- DIM_ENCOUNTER_TYPE: Encounter type junk dimension
-- Type 0: Static reference data
CREATE TABLE dim_encounter_type (
    -- Surrogate Key
    encounter_type_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for encounter type',
    
    -- Type attributes
    encounter_type VARCHAR(50) NOT NULL UNIQUE 
        COMMENT 'Encounter type: Inpatient, Outpatient, ER, Telehealth',
    encounter_type_code VARCHAR(10) NOT NULL
        COMMENT 'Short code: IP, OP, ER, TH',
    encounter_type_category VARCHAR(50) NOT NULL 
        COMMENT 'Category: Acute, Ambulatory, Emergency, Virtual',
    encounter_type_description TEXT
        COMMENT 'Detailed description',
    
    -- Business rules (pre-defined attributes)
    expected_duration_hours INT 
        COMMENT 'Typical duration in hours',
    expected_duration_days DECIMAL(5,2)
        COMMENT 'Typical duration in days',
    
    is_admission BOOLEAN NOT NULL 
        COMMENT 'TRUE if type requires hospital admission',
    is_same_day BOOLEAN NOT NULL 
        COMMENT 'TRUE if typically completed same day',
    is_observation BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if observation status',
    
    requires_bed BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if requires inpatient bed',
    requires_pre_auth BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if typically requires pre-authorization',
    
    -- Financial attributes
    typical_claim_range_min DECIMAL(10,2)
        COMMENT 'Typical minimum claim amount',
    typical_claim_range_max DECIMAL(10,2)
        COMMENT 'Typical maximum claim amount',
    
    -- Indexes
    INDEX idx_encounter_type (encounter_type),
    INDEX idx_encounter_type_code (encounter_type_code),
    INDEX idx_encounter_type_category (encounter_type_category)
    
) COMMENT 'Encounter type junk dimension with business rules and attributes';


-- DIM_DIAGNOSIS: ICD-10 diagnosis codes
-- Type 0: Reference data from external standard
CREATE TABLE dim_diagnosis (
    -- Surrogate Key
    diagnosis_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for diagnosis dimension',
    
    -- Business Key
    diagnosis_id INT NOT NULL UNIQUE 
        COMMENT 'Natural key from OLTP system',
    
    -- ICD-10 attributes
    icd10_code VARCHAR(10) NOT NULL 
        COMMENT 'ICD-10 diagnosis code (e.g., I10, E11.9)',
    icd10_description VARCHAR(200) NOT NULL 
        COMMENT 'Official ICD-10 description',
    icd10_code_short VARCHAR(7)
        COMMENT 'Short ICD-10 code without decimal',
    
    -- Hierarchical categorization
    diagnosis_category VARCHAR(100) 
        COMMENT 'High-level category based on ICD-10 chapter',
    diagnosis_subcategory VARCHAR(100)
        COMMENT 'Sub-category within main category',
    body_system VARCHAR(100)
        COMMENT 'Affected body system: Cardiovascular, Respiratory, etc.',
    
    -- Clinical attributes
    is_chronic BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if chronic condition',
    is_acute BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if acute condition',
    severity_level VARCHAR(20)
        COMMENT 'Severity: Mild, Moderate, Severe, Critical',
    
    -- Reporting attributes
    is_reportable BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if reportable to public health',
    is_preventable BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if preventable condition',
    
    -- Indexes
    INDEX idx_diagnosis_id (diagnosis_id),
    INDEX idx_icd10_code (icd10_code),
    INDEX idx_diagnosis_category (diagnosis_category),
    INDEX idx_body_system (body_system),
    INDEX idx_is_chronic (is_chronic)
    
) COMMENT 'ICD-10 diagnosis codes reference dimension';


-- DIM_PROCEDURE: CPT procedure codes
-- Type 0: Reference data from external standard
CREATE TABLE dim_procedure (
    -- Surrogate Key
    procedure_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for procedure dimension',
    
    -- Business Key
    procedure_id INT NOT NULL UNIQUE 
        COMMENT 'Natural key from OLTP system',
    
    -- CPT attributes
    cpt_code VARCHAR(10) NOT NULL 
        COMMENT 'CPT procedure code (e.g., 99213, 93000)',
    cpt_description VARCHAR(200) NOT NULL 
        COMMENT 'Official CPT description',
    cpt_code_short VARCHAR(5)
        COMMENT 'Short CPT code',
    
    -- Hierarchical categorization
    procedure_category VARCHAR(100) 
        COMMENT 'High-level category: E&M, Surgery, Radiology, Lab, etc.',
    procedure_subcategory VARCHAR(100)
        COMMENT 'Sub-category within main category',
    procedure_type VARCHAR(50)
        COMMENT 'Type: Diagnostic, Therapeutic, Preventive',
    
    -- Clinical attributes
    body_region VARCHAR(100)
        COMMENT 'Body region: Head, Chest, Abdomen, Extremity',
    requires_anesthesia BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if typically requires anesthesia',
    is_surgical BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if surgical procedure',
    is_invasive BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if invasive procedure',
    
    -- Financial attributes
    relative_value_units DECIMAL(10,2) 
        COMMENT 'RVUs for Medicare reimbursement',
    typical_reimbursement DECIMAL(10,2)
        COMMENT 'Typical reimbursement amount',
    
    -- Operational attributes
    typical_duration_minutes INT
        COMMENT 'Typical procedure duration in minutes',
    requires_pre_auth BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if typically requires pre-authorization',
    
    -- Indexes
    INDEX idx_procedure_id (procedure_id),
    INDEX idx_cpt_code (cpt_code),
    INDEX idx_procedure_category (procedure_category),
    INDEX idx_procedure_type (procedure_type),
    INDEX idx_is_surgical (is_surgical)
    
) COMMENT 'CPT procedure codes reference dimension';





-- FACT TABLE
-- FACT_ENCOUNTERS: Central fact table
-- Grain: One row per patient encounter

CREATE TABLE fact_encounters (
    -- Surrogate Key
    encounter_key BIGINT AUTO_INCREMENT
        COMMENT 'Surrogate key for fact table',
    
    -- Business Key (degenerate dimension)
    encounter_id INT NOT NULL
        COMMENT 'Natural key from OLTP system - uniqueness enforced by ETL',



    -- DIMENSION FOREIGN KEYS
    -- Date dimensions
    date_key INT NOT NULL 
        COMMENT 'FK to dim_date for encounter date',
    discharge_date_key INT 
        COMMENT 'FK to dim_date for discharge date (NULL for same-day)',
    
    -- Entity dimensions
    patient_key INT NOT NULL 
        COMMENT 'FK to dim_patient',
    provider_key INT NOT NULL 
        COMMENT 'FK to dim_provider',
    
    -- Denormalized dimensions (for query optimization)
    specialty_key INT NOT NULL 
        COMMENT 'FK to dim_specialty (denormalized from provider)',
    department_key INT NOT NULL 
        COMMENT 'FK to dim_department',
    encounter_type_key INT NOT NULL 
        COMMENT 'FK to dim_encounter_type',
    
    -- DEGENERATE DIMENSIONS
    encounter_date DATETIME NOT NULL 
        COMMENT 'Original encounter timestamp (precise time)',
    discharge_date DATETIME 
        COMMENT 'Original discharge timestamp',
    
    -- PRE-AGGREGATED METRICS
    -- (Computed during ETL to avoid expensive JOINs)
    
    -- Clinical counts
    diagnosis_count INT NOT NULL DEFAULT 0 
        COMMENT 'Count of diagnoses for this encounter',
    procedure_count INT NOT NULL DEFAULT 0 
        COMMENT 'Count of procedures performed',
    
    -- Primary diagnosis (most important)
    primary_diagnosis_key INT
        COMMENT 'FK to dim_diagnosis for primary diagnosis',
    
    -- FINANCIAL MEASURES
    claim_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00 
        COMMENT 'Total billed amount',
    allowed_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00 
        COMMENT 'Total allowed/paid amount',
    patient_responsibility DECIMAL(12,2) DEFAULT 0.00
        COMMENT 'Patient out-of-pocket amount',
    insurance_payment DECIMAL(12,2) DEFAULT 0.00
        COMMENT 'Insurance payment amount',
    
    claim_status VARCHAR(50) 
        COMMENT 'Current claim status: Paid, Pending, Denied, Processing',
    claim_date DATE
        COMMENT 'Date claim submitted',
    payment_date DATE
        COMMENT 'Date payment received',
    
    -- OPERATIONAL MEASURES
    -- Length of stay
    length_of_stay_hours INT 
        COMMENT 'Hours between admission and discharge',
    length_of_stay_days DECIMAL(10,2) 
        COMMENT 'Days between admission and discharge (fractional)',
    
    -- Wait times
    wait_time_minutes INT
        COMMENT 'Wait time from arrival to see provider',
    
    -- PRE-COMPUTED ANALYTICS METRICS
    -- (These eliminate expensive self-joins and window functions)
    -- Readmission analysis
    is_readmission_30day BOOLEAN NOT NULL DEFAULT FALSE 
        COMMENT 'TRUE if patient readmitted within 30 days of previous discharge',
    is_readmission_7day BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT 'TRUE if readmitted within 7 days',
    is_readmission_90day BOOLEAN NOT NULL DEFAULT FALSE
        COMMENT 'TRUE if readmitted within 90 days',
    
    days_since_last_encounter INT 
        COMMENT 'Days since previous encounter for same patient (NULL if first)',
    previous_encounter_key BIGINT 
        COMMENT 'FK to previous encounter for same patient',
    
    -- Visit frequency
    encounter_sequence_number INT
        COMMENT 'Sequential number of this encounter for patient (1, 2, 3...)',
    is_first_visit BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if patient first visit to organization',
    
    -- Quality metrics
    is_planned_readmission BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if readmission was planned',
    had_complication BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if complication occurred',
    
    -- AUDIT AND ETL METADATA
    etl_batch_id INT 
        COMMENT 'ETL batch identifier for tracking',
    etl_loaded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        COMMENT 'When loaded into data warehouse',
    etl_updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        COMMENT 'When last updated',
    source_system VARCHAR(50) DEFAULT 'OLTP'
        COMMENT 'Source system identifier',
    
    -- PRIMARY KEY (must include partitioning column for MySQL)
    PRIMARY KEY (encounter_key, date_key),
    
    -- INDEXES FOR QUERY PERFORMANCE
    -- Single column indexes
    INDEX idx_encounter_id (encounter_id),
    INDEX idx_date_key (date_key),
    INDEX idx_discharge_date_key (discharge_date_key),
    INDEX idx_patient_key (patient_key),
    INDEX idx_provider_key (provider_key),
    INDEX idx_specialty_key (specialty_key),
    INDEX idx_department_key (department_key),
    INDEX idx_encounter_type_key (encounter_type_key),
    INDEX idx_primary_diagnosis_key (primary_diagnosis_key),
    INDEX idx_encounter_date (encounter_date),
    INDEX idx_claim_status (claim_status),
    
    -- Boolean flags (bitmap indexes work well)
    INDEX idx_is_readmission_30day (is_readmission_30day),
    INDEX idx_is_readmission_7day (is_readmission_7day),
    INDEX idx_is_first_visit (is_first_visit),
    
    -- Composite indexes for common query patterns
    INDEX idx_date_specialty (date_key, specialty_key),
    INDEX idx_date_department (date_key, department_key),
    INDEX idx_date_encounter_type (date_key, encounter_type_key),
    INDEX idx_patient_encounter_date (patient_key, encounter_date),
    INDEX idx_specialty_encounter_type (specialty_key, encounter_type_key),
    INDEX idx_department_encounter_type (department_key, encounter_type_key),
    
    -- Financial analysis
    INDEX idx_claim_status_date (claim_status, date_key),
    INDEX idx_allowed_amount (allowed_amount),
    
    -- ETL tracking
    INDEX idx_etl_batch_id (etl_batch_id),
    INDEX idx_etl_loaded_date (etl_loaded_date)
    
) COMMENT 'Central fact table: one row per patient encounter with pre-computed metrics'
PARTITION BY RANGE (date_key) (
    PARTITION p_2023_q1 VALUES LESS THAN (20230401) COMMENT '2023 Q1',
    PARTITION p_2023_q2 VALUES LESS THAN (20230701) COMMENT '2023 Q2',
    PARTITION p_2023_q3 VALUES LESS THAN (20231001) COMMENT '2023 Q3',
    PARTITION p_2023_q4 VALUES LESS THAN (20240101) COMMENT '2023 Q4',
    PARTITION p_2024_q1 VALUES LESS THAN (20240401) COMMENT '2024 Q1',
    PARTITION p_2024_q2 VALUES LESS THAN (20240701) COMMENT '2024 Q2',
    PARTITION p_2024_q3 VALUES LESS THAN (20241001) COMMENT '2024 Q3',
    PARTITION p_2024_q4 VALUES LESS THAN (20250101) COMMENT '2024 Q4',
    PARTITION p_2025_q1 VALUES LESS THAN (20250401) COMMENT '2025 Q1',
    PARTITION p_2025_q2 VALUES LESS THAN (20250701) COMMENT '2025 Q2',
    PARTITION p_2025_q3 VALUES LESS THAN (20251001) COMMENT '2025 Q3',
    PARTITION p_2025_q4 VALUES LESS THAN (20260101) COMMENT '2025 Q4',
    PARTITION p_future VALUES LESS THAN MAXVALUE COMMENT 'Future dates'
);

-- BRIDGE TABLES (Many-to-Many Relationships)
-- BRIDGE_ENCOUNTER_DIAGNOSES
-- Handles many-to-many between encounters and diagnoses

CREATE TABLE bridge_encounter_diagnoses (
    -- Primary Key
    bridge_key BIGINT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for bridge table',
    
    -- Foreign Keys
    encounter_key BIGINT NOT NULL 
        COMMENT 'FK to fact_encounters',
    diagnosis_key INT NOT NULL 
        COMMENT 'FK to dim_diagnosis',
    
    -- Attributes
    diagnosis_sequence INT NOT NULL 
        COMMENT 'Sequence: 1=primary, 2+=secondary diagnoses',
    diagnosis_group_key INT NOT NULL 
        COMMENT 'Group key for weighting in many-to-many queries',
    
    -- Present on admission (clinical attribute)
    present_on_admission CHAR(1)
        COMMENT 'POA indicator: Y, N, U, W (Yes, No, Unknown, Undetermined)',
    
    -- Audit
    etl_loaded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_diagnosis_key (diagnosis_key),
    INDEX idx_sequence (diagnosis_sequence),
    INDEX idx_composite (encounter_key, diagnosis_sequence),
    INDEX idx_diagnosis_encounter (diagnosis_key, encounter_key),
    
    -- Foreign Keys
    -- Note: FK to fact_encounters removed due to MySQL partitioning limitation
    -- Referential integrity enforced in ETL process
    -- FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key)
    --     ON DELETE CASCADE,
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    
    -- Unique constraint
    UNIQUE KEY uk_encounter_diagnosis (encounter_key, diagnosis_key, diagnosis_sequence)
    
) COMMENT 'Bridge table linking encounters to multiple diagnoses';

-- BRIDGE_ENCOUNTER_PROCEDURES
-- Handles many-to-many between encounters and procedures
CREATE TABLE bridge_encounter_procedures (
    -- Primary Key
    bridge_key BIGINT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for bridge table',
    
    -- Foreign Keys
    encounter_key BIGINT NOT NULL 
        COMMENT 'FK to fact_encounters',
    procedure_key INT NOT NULL 
        COMMENT 'FK to dim_procedure',
    
    -- Attributes
    procedure_date DATE NOT NULL 
        COMMENT 'Date procedure was performed',
    procedure_sequence INT NOT NULL 
        COMMENT 'Order of procedures within encounter',
    procedure_group_key INT NOT NULL 
        COMMENT 'Group key for weighting in many-to-many queries',
    
    -- Procedure details (continued)
    procedure_quantity INT DEFAULT 1
        COMMENT 'Number of times procedure performed',
    procedure_modifier VARCHAR(10)
        COMMENT 'CPT modifier codes (e.g., -50, -51)',
    
    -- Financial
    procedure_charge DECIMAL(10,2)
        COMMENT 'Charge for this specific procedure',
    procedure_payment DECIMAL(10,2)
        COMMENT 'Payment received for this procedure',
    
    -- Provider
    performing_provider_key INT
        COMMENT 'FK to dim_provider who performed procedure',
    
    -- Audit
    etl_loaded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_procedure_key (procedure_key),
    INDEX idx_procedure_date (procedure_date),
    INDEX idx_sequence (procedure_sequence),
    INDEX idx_composite (encounter_key, procedure_sequence),
    INDEX idx_procedure_encounter (procedure_key, encounter_key),
    INDEX idx_performing_provider (performing_provider_key),
    
    -- Foreign Keys
    -- Note: FK to fact_encounters removed due to MySQL partitioning limitation
    -- Referential integrity enforced in ETL process
    -- FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key)
    --     ON DELETE CASCADE,
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    FOREIGN KEY (performing_provider_key) REFERENCES dim_provider(provider_key),
    
    -- Unique constraint
    UNIQUE KEY uk_encounter_procedure (encounter_key, procedure_key, procedure_sequence)
    
) COMMENT 'Bridge table linking encounters to multiple procedures with details';


-- AGGREGATE/SUMMARY TABLES
-- Pre-aggregated for common queries
-- FACT_ENCOUNTERS_MONTHLY_SUMMARY
-- Pre-aggregated monthly metrics for fast dashboards
CREATE TABLE fact_encounters_monthly_summary (
    -- Primary Key
    summary_key INT AUTO_INCREMENT PRIMARY KEY 
        COMMENT 'Surrogate key for summary table',
    
    -- Dimensions (grain: month + specialty + department + encounter_type)
    date_key INT NOT NULL 
        COMMENT 'FK to dim_date - first day of month',
    specialty_key INT NOT NULL 
        COMMENT 'FK to dim_specialty',
    department_key INT NOT NULL 
        COMMENT 'FK to dim_department',
    encounter_type_key INT NOT NULL 
        COMMENT 'FK to dim_encounter_type',
    
    -- Aggregated measures
    encounter_count INT NOT NULL DEFAULT 0
        COMMENT 'Total encounters in this month',
    unique_patients INT NOT NULL DEFAULT 0
        COMMENT 'Count of unique patients',
    unique_providers INT NOT NULL DEFAULT 0
        COMMENT 'Count of unique providers',
    
    -- Financial aggregates
    total_claim_amount DECIMAL(14,2) DEFAULT 0.00
        COMMENT 'Sum of all claim amounts',
    total_allowed_amount DECIMAL(14,2) DEFAULT 0.00
        COMMENT 'Sum of all allowed amounts',
    avg_claim_amount DECIMAL(12,2) DEFAULT 0.00
        COMMENT 'Average claim amount per encounter',
    avg_allowed_amount DECIMAL(12,2) DEFAULT 0.00
        COMMENT 'Average allowed amount per encounter',
    
    -- Clinical aggregates
    total_diagnoses INT DEFAULT 0
        COMMENT 'Sum of all diagnoses',
    total_procedures INT DEFAULT 0
        COMMENT 'Sum of all procedures',
    avg_diagnoses_per_encounter DECIMAL(5,2) DEFAULT 0.00
        COMMENT 'Average diagnoses per encounter',
    avg_procedures_per_encounter DECIMAL(5,2) DEFAULT 0.00
        COMMENT 'Average procedures per encounter',
    
    -- Quality metrics
    readmission_count INT DEFAULT 0
        COMMENT 'Count of 30-day readmissions',
    readmission_rate DECIMAL(5,2) DEFAULT 0.00
        COMMENT 'Readmission rate percentage',
    
    -- Length of stay
    avg_length_of_stay_days DECIMAL(6,2) DEFAULT 0.00
        COMMENT 'Average length of stay in days',
    total_patient_days INT DEFAULT 0
        COMMENT 'Sum of all patient days',
    
    -- Metadata
    etl_batch_id INT
        COMMENT 'ETL batch that created this summary',
    etl_loaded_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        COMMENT 'When this summary was created',
    
    -- Indexes
    INDEX idx_date_specialty (date_key, specialty_key),
    INDEX idx_date_department (date_key, department_key),
    INDEX idx_date_encounter_type (date_key, encounter_type_key),
    INDEX idx_specialty_key (specialty_key),
    INDEX idx_department_key (department_key),
    INDEX idx_encounter_type_key (encounter_type_key),
    
    -- Foreign Keys
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    
    -- Unique constraint (one summary per combination)
    UNIQUE KEY uk_monthly_summary (date_key, specialty_key, department_key, encounter_type_key)
    
) COMMENT 'Pre-aggregated monthly summary for fast dashboard queries';


-- POPULATE UNKNOWN/DEFAULT ROWS
-- Required for handling NULL foreign keys gracefully
-- Insert Unknown Date
INSERT IGNORE INTO dim_date (
    date_key, calendar_date, year, quarter, month, month_name, month_abbr,
    week_of_year, day_of_month, day_of_week, day_name, day_abbr,
    is_weekend, is_holiday, is_weekday,
    fiscal_year, fiscal_quarter, fiscal_month, fiscal_week,
    day_of_year, week_of_month
) VALUES (
    -1, '1900-01-01', 1900, 1, 1, 'Unknown', 'Unk',
    1, 1, 1, 'Unknown', 'Unk',
    FALSE, FALSE, TRUE,
    1900, 1, 1, 1,
    1, 1
);

-- Insert Unknown Patient
INSERT IGNORE INTO dim_patient (
    patient_key, patient_id, mrn, first_name, last_name, full_name,
    date_of_birth, age, age_group, age_decade,
    gender, gender_description,
    effective_date, end_date, is_current, is_test_patient
) VALUES (
    -1, -1, 'UNKNOWN', 'Unknown', 'Patient', 'Unknown Patient',
    '1900-01-01', 0, 'Unknown', 'Unknown',
    'U', 'Unknown',
    '1900-01-01', '9999-12-31', TRUE, FALSE
);

-- Insert Unknown Specialty
INSERT IGNORE INTO dim_specialty (
    specialty_key, specialty_id, specialty_name, specialty_code,
    specialty_category, is_surgical, is_primary_care, requires_referral
) VALUES (
    -1, -1, 'Unknown Specialty', 'UNK',
    'Unknown', FALSE, FALSE, FALSE
);

-- Insert Unknown Department
INSERT IGNORE INTO dim_department (
    department_key, department_id, department_name, department_code,
    floor, capacity, department_type,
    is_inpatient, is_outpatient, is_emergency, is_critical_care
) VALUES (
    -1, -1, 'Unknown Department', 'UNK',
    NULL, NULL, 'Unknown',
    FALSE, FALSE, FALSE, FALSE
);

-- Insert Unknown Provider
INSERT IGNORE INTO dim_provider (
    provider_key, provider_id, first_name, last_name, full_name,
    credential, npi,
    specialty_key, specialty_name, specialty_code,
    department_key, department_name,
    provider_type, is_active,
    effective_date, end_date, is_current
) VALUES (
    -1, -1, 'Unknown', 'Provider', 'Unknown Provider',
    'UNK', NULL,
    -1, 'Unknown Specialty', 'UNK',
    -1, 'Unknown Department',
    'Unknown', FALSE,
    '1900-01-01', '9999-12-31', TRUE
);

-- Insert Unknown Encounter Type
INSERT IGNORE INTO dim_encounter_type (
    encounter_type_key, encounter_type, encounter_type_code,
    encounter_type_category, expected_duration_hours,
    is_admission, is_same_day, is_observation,
    requires_bed, requires_pre_auth
) VALUES (
    -1, 'Unknown', 'UNK',
    'Unknown', NULL,
    FALSE, FALSE, FALSE,
    FALSE, FALSE
);

-- Insert Unknown Diagnosis
INSERT IGNORE INTO dim_diagnosis (
    diagnosis_key, diagnosis_id, icd10_code, icd10_description,
    diagnosis_category, is_chronic, is_acute, is_reportable, is_preventable
) VALUES (
    -1, -1, 'UNK', 'Unknown Diagnosis',
    'Unknown', FALSE, FALSE, FALSE, FALSE
);

-- Insert Unknown Procedure
INSERT IGNORE INTO dim_procedure (
    procedure_key, procedure_id, cpt_code, cpt_description,
    procedure_category, procedure_type,
    requires_anesthesia, is_surgical, is_invasive, requires_pre_auth
) VALUES (
    -1, -1, 'UNK', 'Unknown Procedure',
    'Unknown', 'Unknown',
    FALSE, FALSE, FALSE, FALSE
);


-- ANALYTICAL VIEWS
-- Simplified views for common reporting needs
-- VW_ENCOUNTER_SUMMARY
-- Denormalized view joining fact to all dimensions
CREATE OR REPLACE VIEW vw_encounter_summary AS
SELECT 
    -- Fact identifiers
    f.encounter_key,
    f.encounter_id,
    f.encounter_date,
    f.discharge_date,
    
    -- Date attributes
    d.calendar_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_name,
    d.is_weekend,
    d.fiscal_year,
    d.fiscal_quarter,
    
    -- Patient attributes
    p.patient_key,
    p.patient_id,
    p.mrn,
    p.full_name AS patient_name,
    p.age,
    p.age_group,
    p.gender,
    p.gender_description,
    
    -- Provider attributes
    pr.provider_key,
    pr.provider_id,
    pr.full_name AS provider_name,
    pr.credential,
    pr.npi,
    
    -- Specialty attributes
    s.specialty_key,
    s.specialty_name,
    s.specialty_code,
    s.specialty_category,
    
    -- Department attributes
    dep.department_key,
    dep.department_name,
    dep.department_type,
    dep.floor,
    
    -- Encounter type attributes
    et.encounter_type,
    et.encounter_type_category,
    et.is_admission,
    
    -- Pre-computed metrics
    f.diagnosis_count,
    f.procedure_count,
    f.claim_amount,
    f.allowed_amount,
    f.patient_responsibility,
    f.insurance_payment,
    f.claim_status,
    f.length_of_stay_hours,
    f.length_of_stay_days,
    f.is_readmission_30day,
    f.is_readmission_7day,
    f.days_since_last_encounter,
    f.encounter_sequence_number,
    f.is_first_visit,
    
    -- Calculated measures
    ROUND((f.allowed_amount / NULLIF(f.claim_amount, 0)) * 100, 2) AS allowed_rate_pct,
    ROUND(f.allowed_amount / NULLIF(f.diagnosis_count, 0), 2) AS allowed_per_diagnosis,
    ROUND(f.allowed_amount / NULLIF(f.procedure_count, 0), 2) AS allowed_per_procedure,
    
    -- Metadata
    f.etl_batch_id,
    f.etl_loaded_date
    
FROM fact_encounters f
INNER JOIN dim_date d ON f.date_key = d.date_key
INNER JOIN dim_patient p ON f.patient_key = p.patient_key
INNER JOIN dim_provider pr ON f.provider_key = pr.provider_key
INNER JOIN dim_specialty s ON f.specialty_key = s.specialty_key
INNER JOIN dim_department dep ON f.department_key = dep.department_key
INNER JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key;


-- VW_READMISSION_ANALYSIS
-- Specialized view for readmission analysis
CREATE OR REPLACE VIEW vw_readmission_analysis AS
SELECT 
    -- Time dimensions
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    
    -- Entity dimensions
    s.specialty_name,
    dep.department_name,
    et.encounter_type,
    
    -- Readmission metrics
    COUNT(DISTINCT f.encounter_key) AS total_discharges,
    COUNT(DISTINCT CASE WHEN f.is_readmission_7day THEN f.encounter_key END) AS readmissions_7day,
    COUNT(DISTINCT CASE WHEN f.is_readmission_30day THEN f.encounter_key END) AS readmissions_30day,
    COUNT(DISTINCT CASE WHEN f.is_readmission_90day THEN f.encounter_key END) AS readmissions_90day,
    
    -- Readmission rates
    ROUND(
        (COUNT(DISTINCT CASE WHEN f.is_readmission_7day THEN f.encounter_key END) * 100.0) / 
        NULLIF(COUNT(DISTINCT f.encounter_key), 0), 
        2
    ) AS readmission_rate_7day_pct,
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN f.is_readmission_30day THEN f.encounter_key END) * 100.0) / 
        NULLIF(COUNT(DISTINCT f.encounter_key), 0), 
        2
    ) AS readmission_rate_30day_pct,
    
    ROUND(
        (COUNT(DISTINCT CASE WHEN f.is_readmission_90day THEN f.encounter_key END) * 100.0) / 
        NULLIF(COUNT(DISTINCT f.encounter_key), 0), 
        2
    ) AS readmission_rate_90day_pct,
    
    -- Average days between encounters
    AVG(f.days_since_last_encounter) AS avg_days_between_encounters
    
FROM fact_encounters f
INNER JOIN dim_date d ON f.date_key = d.date_key
INNER JOIN dim_specialty s ON f.specialty_key = s.specialty_key
INNER JOIN dim_department dep ON f.department_key = dep.department_key
INNER JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
WHERE et.encounter_type = 'Inpatient'  -- Only inpatient encounters for readmission analysis
    AND f.discharge_date IS NOT NULL
GROUP BY 
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    s.specialty_name,
    dep.department_name,
    et.encounter_type;


-- VW_FINANCIAL_SUMMARY
-- Financial metrics by various dimensions
CREATE OR REPLACE VIEW vw_financial_summary AS
SELECT 
    -- Time dimensions
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    
    -- Entity dimensions
    s.specialty_name,
    dep.department_name,
    et.encounter_type,
    f.claim_status,
    
    -- Volume metrics
    COUNT(DISTINCT f.encounter_key) AS encounter_count,
    COUNT(DISTINCT f.patient_key) AS unique_patients,
    
    -- Financial metrics
    SUM(f.claim_amount) AS total_claimed,
    SUM(f.allowed_amount) AS total_allowed,
    SUM(f.patient_responsibility) AS total_patient_responsibility,
    SUM(f.insurance_payment) AS total_insurance_payment,
    
    -- Average metrics
    AVG(f.claim_amount) AS avg_claim_amount,
    AVG(f.allowed_amount) AS avg_allowed_amount,
    
    -- Rates
    ROUND((SUM(f.allowed_amount) / NULLIF(SUM(f.claim_amount), 0)) * 100, 2) AS allowed_rate_pct,
    
    -- Per encounter metrics
    ROUND(SUM(f.allowed_amount) / NULLIF(COUNT(DISTINCT f.encounter_key), 0), 2) AS revenue_per_encounter,
    ROUND(SUM(f.allowed_amount) / NULLIF(COUNT(DISTINCT f.patient_key), 0), 2) AS revenue_per_patient
    
FROM fact_encounters f
INNER JOIN dim_date d ON f.date_key = d.date_key
INNER JOIN dim_specialty s ON f.specialty_key = s.specialty_key
INNER JOIN dim_department dep ON f.department_key = dep.department_key
INNER JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY 
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    s.specialty_name,
    dep.department_name,
    et.encounter_type,
    f.claim_status;



-- UTILITY STORED PROCEDURES
-- SP_REFRESH_MONTHLY_SUMMARY
-- Refresh monthly summary table for a specific month

DELIMITER //

CREATE PROCEDURE sp_refresh_monthly_summary(
    IN p_year INT,
    IN p_month INT
)
BEGIN
    DECLARE v_date_key INT;
    DECLARE v_start_date DATE;
    DECLARE v_end_date DATE;
    
    -- Calculate date range
    SET v_start_date = DATE(CONCAT(p_year, '-', LPAD(p_month, 2, '0'), '-01'));
    SET v_end_date = LAST_DAY(v_start_date);
    SET v_date_key = CAST(DATE_FORMAT(v_start_date, '%Y%m%d') AS UNSIGNED);
    
    -- Delete existing summary for this month
    DELETE FROM fact_encounters_monthly_summary
    WHERE date_key = v_date_key;
    
    -- Insert new summary
    INSERT INTO fact_encounters_monthly_summary (
        date_key, specialty_key, department_key, encounter_type_key,
        encounter_count, unique_patients, unique_providers,
        total_claim_amount, total_allowed_amount,
        avg_claim_amount, avg_allowed_amount,
        total_diagnoses, total_procedures,
        avg_diagnoses_per_encounter, avg_procedures_per_encounter,
        readmission_count, readmission_rate,
        avg_length_of_stay_days, total_patient_days
    )
    SELECT 
        v_date_key AS date_key,
        f.specialty_key,
        f.department_key,
        f.encounter_type_key,
        
        -- Volume metrics
        COUNT(DISTINCT f.encounter_key) AS encounter_count,
        COUNT(DISTINCT f.patient_key) AS unique_patients,
        COUNT(DISTINCT f.provider_key) AS unique_providers,
        
        -- Financial metrics
        SUM(f.claim_amount) AS total_claim_amount,
        SUM(f.allowed_amount) AS total_allowed_amount,
        AVG(f.claim_amount) AS avg_claim_amount,
        AVG(f.allowed_amount) AS avg_allowed_amount,
        
        -- Clinical metrics
        SUM(f.diagnosis_count) AS total_diagnoses,
        SUM(f.procedure_count) AS total_procedures,
        AVG(f.diagnosis_count) AS avg_diagnoses_per_encounter,
        AVG(f.procedure_count) AS avg_procedures_per_encounter,
        
        -- Quality metrics
        SUM(CASE WHEN f.is_readmission_30day THEN 1 ELSE 0 END) AS readmission_count,
        ROUND(
            (SUM(CASE WHEN f.is_readmission_30day THEN 1 ELSE 0 END) * 100.0) / 
            NULLIF(COUNT(DISTINCT f.encounter_key), 0),
            2
        ) AS readmission_rate,
        
        -- Length of stay
        AVG(f.length_of_stay_days) AS avg_length_of_stay_days,
        SUM(COALESCE(f.length_of_stay_days, 0)) AS total_patient_days
        
    FROM fact_encounters f
    INNER JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.calendar_date BETWEEN v_start_date AND v_end_date
    GROUP BY 
        f.specialty_key,
        f.department_key,
        f.encounter_type_key;
    
    -- Return success message
    SELECT CONCAT('Summary refreshed for ', p_year, '-', LPAD(p_month, 2, '0')) AS result;
    
END //

DELIMITER ;


-- SP_GET_TABLE_STATS
-- Get row counts and statistics for all tables
DELIMITER //

CREATE PROCEDURE sp_get_table_stats()
BEGIN
    SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
    UNION ALL
    SELECT 'dim_patient', COUNT(*) FROM dim_patient
    UNION ALL
    SELECT 'dim_specialty', COUNT(*) FROM dim_specialty
    UNION ALL
    SELECT 'dim_department', COUNT(*) FROM dim_department
    UNION ALL
    SELECT 'dim_provider', COUNT(*) FROM dim_provider
    UNION ALL
    SELECT 'dim_encounter_type', COUNT(*) FROM dim_encounter_type
    UNION ALL
    SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis
    UNION ALL
    SELECT 'dim_procedure', COUNT(*) FROM dim_procedure
    UNION ALL
    SELECT 'fact_encounters', COUNT(*) FROM fact_encounters
    UNION ALL
    SELECT 'bridge_encounter_diagnoses', COUNT(*) FROM bridge_encounter_diagnoses
    UNION ALL
    SELECT 'bridge_encounter_procedures', COUNT(*) FROM bridge_encounter_procedures
    UNION ALL
    SELECT 'fact_encounters_monthly_summary', COUNT(*) FROM fact_encounters_monthly_summary
    ORDER BY table_name;
END //

DELIMITER ;







-- SCHEMA VALIDATION QUERIES


-- Verify all dimension tables have unknown rows
SELECT 'Dimension Unknown Rows Verification' AS check_name;
SELECT 'dim_date' AS table_name, COUNT(*) AS unknown_count FROM dim_date WHERE date_key = -1
UNION ALL
SELECT 'dim_patient', COUNT(*) FROM dim_patient WHERE patient_key = -1
UNION ALL
SELECT 'dim_specialty', COUNT(*) FROM dim_specialty WHERE specialty_key = -1
UNION ALL
SELECT 'dim_department', COUNT(*) FROM dim_department WHERE department_key = -1
UNION ALL
SELECT 'dim_provider', COUNT(*) FROM dim_provider WHERE provider_key = -1
UNION ALL
SELECT 'dim_encounter_type', COUNT(*) FROM dim_encounter_type WHERE encounter_type_key = -1
UNION ALL
SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis WHERE diagnosis_key = -1
UNION ALL
SELECT 'dim_procedure', COUNT(*) FROM dim_procedure WHERE procedure_key = -1;

-- Verify foreign key relationships exist
SELECT 'Foreign Key Verification' AS check_name;
SELECT 
    TABLE_NAME,
    CONSTRAINT_NAME,
    REFERENCED_TABLE_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = DATABASE()
    AND REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY TABLE_NAME, CONSTRAINT_NAME;




