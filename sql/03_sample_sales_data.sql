-- Comprehensive Sample Data for Sales Hierarchy and Compensation System
-- This script populates the sales tables with realistic hierarchy and comprehensive data
-- Covers Jan 2023 to Aug 2025 with proper executive structure

USE SLACK_SALES_DEMO.SLACK_SCHEMA;

-- Clear existing data
DELETE FROM SALES_PERFORMANCE;
DELETE FROM SALES_TRANSACTIONS;
DELETE FROM DATA_ENTITLEMENTS;
DELETE FROM CUSTOMER_ASSIGNMENTS;
DELETE FROM SALES_EMPLOYEES;

-- 1. Insert Sales Employees (Realistic hierarchy: 1 CRO, 2 VPs, rest as before)
INSERT INTO SALES_EMPLOYEES VALUES
-- CRO (Chief Revenue Officer) - Increased quota to achieve ~90% attainment
('EMP001', 'Sarah', 'Johnson', 'sarah.johnson@company.com', 'CRO', 'Global', 'Global', '2020-01-15', 250000.00, 0.0100, 45000000.00, NULL, TRUE, CURRENT_TIMESTAMP()),

-- VP Sales (2 VPs reporting to CRO) - Increased quotas for ~90% attainment
('EMP002', 'Michael', 'Chen', 'michael.chen@company.com', 'VP Sales', 'West Coast', 'West', '2020-03-01', 180000.00, 0.0120, 16000000.00, 'EMP001', TRUE, CURRENT_TIMESTAMP()),
('EMP003', 'Jennifer', 'Davis', 'jennifer.davis@company.com', 'VP Sales', 'East/Central', 'East', '2020-04-15', 175000.00, 0.0120, 15000000.00, 'EMP001', TRUE, CURRENT_TIMESTAMP()),

-- Regional Managers (Report to VPs) - Increased quotas for ~90% attainment
('EMP004', 'Robert', 'Wilson', 'robert.wilson@company.com', 'Regional Manager', 'West Coast', 'West', '2021-01-10', 120000.00, 0.0150, 11000000.00, 'EMP002', TRUE, CURRENT_TIMESTAMP()),
('EMP005', 'Lisa', 'Anderson', 'lisa.anderson@company.com', 'Regional Manager', 'Pacific Northwest', 'West', '2021-06-01', 118000.00, 0.0150, 10000000.00, 'EMP002', TRUE, CURRENT_TIMESTAMP()),
('EMP006', 'David', 'Martinez', 'david.martinez@company.com', 'Regional Manager', 'Nevada/Arizona', 'West', '2021-08-15', 115000.00, 0.0150, 9500000.00, 'EMP002', TRUE, CURRENT_TIMESTAMP()),
('EMP007', 'Amanda', 'Taylor', 'amanda.taylor@company.com', 'Regional Manager', 'East Coast', 'East', '2021-05-20', 122000.00, 0.0150, 11500000.00, 'EMP003', TRUE, CURRENT_TIMESTAMP()),
('EMP008', 'James', 'Brown', 'james.brown@company.com', 'Regional Manager', 'Southeast', 'East', '2021-09-10', 118000.00, 0.0150, 10000000.00, 'EMP003', TRUE, CURRENT_TIMESTAMP()),
('EMP009', 'Michelle', 'Garcia', 'michelle.garcia@company.com', 'Regional Manager', 'Central', 'Central', '2022-01-05', 120000.00, 0.0150, 11000000.00, 'EMP003', TRUE, CURRENT_TIMESTAMP()),
('EMP010', 'Christopher', 'Lee', 'christopher.lee@company.com', 'Regional Manager', 'Midwest', 'Central', '2022-02-15', 116000.00, 0.0150, 10000000.00, 'EMP003', TRUE, CURRENT_TIMESTAMP()),

-- Sales Managers (Report to Regional Managers) - Increased quotas for ~90% attainment
('EMP011', 'Patricia', 'Kim', 'patricia.kim@company.com', 'Sales Manager', 'California North', 'West', '2022-03-01', 85000.00, 0.0200, 4300000.00, 'EMP004', TRUE, CURRENT_TIMESTAMP()),
('EMP012', 'William', 'Zhang', 'william.zhang@company.com', 'Sales Manager', 'California South', 'West', '2022-04-01', 83000.00, 0.0200, 4200000.00, 'EMP004', TRUE, CURRENT_TIMESTAMP()),
('EMP013', 'Maria', 'Rodriguez', 'maria.rodriguez@company.com', 'Sales Manager', 'Seattle', 'West', '2022-05-01', 82000.00, 0.0200, 4000000.00, 'EMP005', TRUE, CURRENT_TIMESTAMP()),
('EMP014', 'Kevin', 'White', 'kevin.white@company.com', 'Sales Manager', 'Portland', 'West', '2022-07-01', 80000.00, 0.0200, 3900000.00, 'EMP005', TRUE, CURRENT_TIMESTAMP()),
('EMP015', 'Nicole', 'Harris', 'nicole.harris@company.com', 'Sales Manager', 'Las Vegas', 'West', '2022-08-15', 81000.00, 0.0200, 4000000.00, 'EMP006', TRUE, CURRENT_TIMESTAMP()),
('EMP016', 'Daniel', 'Martin', 'daniel.martin@company.com', 'Sales Manager', 'New York', 'East', '2022-05-01', 88000.00, 0.0200, 4600000.00, 'EMP007', TRUE, CURRENT_TIMESTAMP()),
('EMP017', 'Stephanie', 'Thompson', 'stephanie.thompson@company.com', 'Sales Manager', 'Boston', 'East', '2022-06-15', 86000.00, 0.0200, 4400000.00, 'EMP007', TRUE, CURRENT_TIMESTAMP()),
('EMP018', 'Mark', 'Garcia', 'mark.garcia@company.com', 'Sales Manager', 'Florida', 'East', '2022-09-01', 84000.00, 0.0200, 4300000.00, 'EMP008', TRUE, CURRENT_TIMESTAMP()),
('EMP019', 'Rachel', 'Martinez', 'rachel.martinez@company.com', 'Sales Manager', 'Atlanta', 'East', '2022-10-01', 82000.00, 0.0200, 4200000.00, 'EMP008', TRUE, CURRENT_TIMESTAMP()),
('EMP020', 'Andrew', 'Robinson', 'andrew.robinson@company.com', 'Sales Manager', 'Texas', 'Central', '2022-11-15', 85000.00, 0.0200, 4400000.00, 'EMP009', TRUE, CURRENT_TIMESTAMP()),
('EMP021', 'Lauren', 'Clark', 'lauren.clark@company.com', 'Sales Manager', 'Illinois', 'Central', '2023-01-01', 83000.00, 0.0200, 4250000.00, 'EMP010', TRUE, CURRENT_TIMESTAMP());

-- Sales Representatives (Report to Sales Managers) - Generate realistic numbers
INSERT INTO SALES_EMPLOYEES 
WITH realistic_names AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY first_name, last_name) as name_id,
        first_name, 
        last_name,
        LOWER(first_name) || '.' || LOWER(last_name) || '@company.com' as email
    FROM (
        SELECT 'Alex' as first_name, 'Thompson' as last_name UNION ALL
        SELECT 'Jordan', 'Mitchell' UNION ALL SELECT 'Casey', 'Parker' UNION ALL
        SELECT 'Morgan', 'Bennett' UNION ALL SELECT 'Riley', 'Cooper' UNION ALL
        SELECT 'Avery', 'Reed' UNION ALL SELECT 'Quinn', 'Bailey' UNION ALL
        SELECT 'Sage', 'Rivera' UNION ALL SELECT 'Blake', 'Torres' UNION ALL
        SELECT 'Drew', 'Ward' UNION ALL SELECT 'Emery', 'Brooks' UNION ALL
        SELECT 'Finley', 'Gray' UNION ALL SELECT 'Hayden', 'Watson' UNION ALL
        SELECT 'Jamie', 'Kelly' UNION ALL SELECT 'Kai', 'Sanders' UNION ALL
        SELECT 'Lane', 'Price' UNION ALL SELECT 'Max', 'Bennett' UNION ALL
        SELECT 'Nova', 'Wood' UNION ALL SELECT 'Oakley', 'Barnes' UNION ALL
        SELECT 'Parker', 'Ross' UNION ALL SELECT 'River', 'Henderson' UNION ALL
        SELECT 'Skyler', 'Coleman' UNION ALL SELECT 'Tatum', 'Jenkins' UNION ALL
        SELECT 'Val', 'Perry' UNION ALL SELECT 'Winter', 'Powell' UNION ALL
        SELECT 'Zion', 'Long' UNION ALL SELECT 'Ari', 'Patterson' UNION ALL
        SELECT 'Bay', 'Hughes' UNION ALL SELECT 'Cameron', 'Flores' UNION ALL
        SELECT 'Dakota', 'Washington' UNION ALL SELECT 'Ellis', 'Butler' UNION ALL
        SELECT 'Frances', 'Simmons' UNION ALL SELECT 'Gray', 'Foster' UNION ALL
        SELECT 'Harper', 'Gonzales' UNION ALL SELECT 'Indigo', 'Bryant' UNION ALL
        SELECT 'Jules', 'Alexander' UNION ALL SELECT 'Kris', 'Russell' UNION ALL
        SELECT 'Lou', 'Griffin' UNION ALL SELECT 'Micah', 'Diaz' UNION ALL
        SELECT 'Nico', 'Hayes' UNION ALL SELECT 'Ocean', 'Myers' UNION ALL
        SELECT 'Phoenix', 'Ford' UNION ALL SELECT 'Reese', 'Hamilton' UNION ALL
        SELECT 'Sam', 'Graham' UNION ALL SELECT 'True', 'Sullivan' UNION ALL
        SELECT 'Unity', 'Wallace' UNION ALL SELECT 'Vale', 'Woods' UNION ALL
        SELECT 'Wren', 'Cole' UNION ALL SELECT 'Sage', 'West' UNION ALL
        SELECT 'Rowan', 'Jordan' UNION ALL SELECT 'Peyton', 'Owens' UNION ALL
        SELECT 'Marlowe', 'Reynolds' UNION ALL SELECT 'Kendall', 'Fisher' UNION ALL
        SELECT 'Jaden', 'Ellis' UNION ALL SELECT 'Hayden', 'Harrison' UNION ALL
        SELECT 'Eden', 'Gibson' UNION ALL SELECT 'Darcy', 'McDonald' UNION ALL
        SELECT 'Bryce', 'Cruz' UNION ALL SELECT 'Aubrey', 'Marshall' UNION ALL
        SELECT 'Aspen', 'Ortiz' UNION ALL SELECT 'Arden', 'Gomez' UNION ALL
        SELECT 'Amari', 'Murray' UNION ALL SELECT 'Aiden', 'Freeman' UNION ALL
        SELECT 'Addison', 'Wells' UNION ALL SELECT 'Adrian', 'Webb' UNION ALL
        SELECT 'Charlie', 'Simpson' UNION ALL SELECT 'Devon', 'Stevens' UNION ALL
        SELECT 'Emerson', 'Tucker' UNION ALL SELECT 'Finley', 'Porter' UNION ALL
        SELECT 'Glenn', 'Hunter' UNION ALL SELECT 'Haven', 'Hicks' UNION ALL
        SELECT 'Iris', 'Crawford' UNION ALL SELECT 'Justice', 'Henry' UNION ALL
        SELECT 'Kellen', 'Boyd' UNION ALL SELECT 'Lennox', 'Mason' UNION ALL
        SELECT 'Memphis', 'Morales' UNION ALL SELECT 'Noel', 'Kennedy' UNION ALL
        SELECT 'Orion', 'Warren' UNION ALL SELECT 'Palmer', 'Dixon' UNION ALL
        SELECT 'Quincy', 'Ramos' UNION ALL SELECT 'Remy', 'Reeves' UNION ALL
        SELECT 'Shay', 'Burns' UNION ALL SELECT 'Tanner', 'Gordon' UNION ALL
        SELECT 'Urban', 'Shaw' UNION ALL SELECT 'Vesper', 'Holmes' UNION ALL
        SELECT 'Wynn', 'Rice' UNION ALL SELECT 'Xander', 'Robertson' UNION ALL
        SELECT 'Yale', 'Hunt' UNION ALL SELECT 'Zara', 'Black'
    ) names
),
base_combinations AS (
    SELECT 
        sm.EMPLOYEE_ID as MANAGER_ID,
        sm.TERRITORY,
        sm.REGION,
        gen.seq,
        ROW_NUMBER() OVER (ORDER BY sm.EMPLOYEE_ID, gen.seq) as rep_sequence
    FROM (
        SELECT EMPLOYEE_ID, TERRITORY, REGION
        FROM SALES_EMPLOYEES 
        WHERE ROLE = 'Sales Manager'
    ) sm
    CROSS JOIN (
        SELECT 1 as seq UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    ) gen -- 9 reps per manager = ~99 total reps
),
rep_generator AS (
    SELECT 
        'EMP' || LPAD(100 + bc.rep_sequence, 3, '0') as EMPLOYEE_ID,
        rn.first_name as FIRST_NAME,
        rn.last_name as LAST_NAME,
        rn.email as EMAIL,
        'Sales Rep' as ROLE,
        bc.TERRITORY as TERRITORY,
        bc.REGION as REGION,
        DATEADD('day', UNIFORM(0, 365, RANDOM()), DATE('2022-01-01')) as HIRE_DATE,
        UNIFORM(55000, 75000, RANDOM()) as BASE_SALARY,
        0.0300 as COMMISSION_RATE,
        UNIFORM(1600000, 2700000, RANDOM()) as QUOTA_AMOUNT,
        bc.MANAGER_ID as MANAGER_ID,
        TRUE as ACTIVE,
        CURRENT_TIMESTAMP() as CREATED_DATE
    FROM base_combinations bc
    JOIN realistic_names rn ON rn.name_id = MOD(bc.rep_sequence - 1, (SELECT COUNT(*) FROM realistic_names)) + 1
)
SELECT 
    EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, ROLE, TERRITORY, REGION, 
    HIRE_DATE, BASE_SALARY, COMMISSION_RATE, QUOTA_AMOUNT, MANAGER_ID, 
    ACTIVE, CREATED_DATE
FROM rep_generator;

-- 2. Create Customer Assignments
INSERT INTO CUSTOMER_ASSIGNMENTS 
WITH customer_generator AS (
    SELECT 
        'ASSIGN' || LPAD(ROW_NUMBER() OVER (ORDER BY se.EMPLOYEE_ID, cust.seq), 6, '0') as ASSIGNMENT_ID,
        se.EMPLOYEE_ID,
        'CUST' || LPAD(cust.seq + (ABS(HASH(se.EMPLOYEE_ID)) % 1000), 4, '0') as CUSTOMER_ID,
        DATEADD('day', UNIFORM(0, 400, RANDOM()), DATE('2023-01-01')) as ASSIGNED_DATE,
        TRUE as IS_PRIMARY,
        CURRENT_TIMESTAMP() as CREATED_DATE
    FROM SALES_EMPLOYEES se
    CROSS JOIN (
        SELECT 1 as seq UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL
        SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL
        SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
    ) cust
    WHERE se.ACTIVE = TRUE 
      AND se.ROLE IN ('Sales Rep', 'Sales Manager', 'Regional Manager', 'VP Sales', 'CRO')
      AND UNIFORM(0, 1, RANDOM()) < 0.4  -- 40% chance per employee-customer combination
)
SELECT * FROM customer_generator;

-- 3. Generate Sales Transactions (Jan 2023 - Aug 2025)
INSERT INTO SALES_TRANSACTIONS
WITH all_months AS (
    SELECT '2023-01-01'::DATE as month_start, 2023 as year_num, 1 as month_num UNION ALL
    SELECT '2023-02-01'::DATE, 2023, 2 UNION ALL SELECT '2023-03-01'::DATE, 2023, 3 UNION ALL
    SELECT '2023-04-01'::DATE, 2023, 4 UNION ALL SELECT '2023-05-01'::DATE, 2023, 5 UNION ALL
    SELECT '2023-06-01'::DATE, 2023, 6 UNION ALL SELECT '2023-07-01'::DATE, 2023, 7 UNION ALL
    SELECT '2023-08-01'::DATE, 2023, 8 UNION ALL SELECT '2023-09-01'::DATE, 2023, 9 UNION ALL
    SELECT '2023-10-01'::DATE, 2023, 10 UNION ALL SELECT '2023-11-01'::DATE, 2023, 11 UNION ALL
    SELECT '2023-12-01'::DATE, 2023, 12 UNION ALL SELECT '2024-01-01'::DATE, 2024, 1 UNION ALL
    SELECT '2024-02-01'::DATE, 2024, 2 UNION ALL SELECT '2024-03-01'::DATE, 2024, 3 UNION ALL
    SELECT '2024-04-01'::DATE, 2024, 4 UNION ALL SELECT '2024-05-01'::DATE, 2024, 5 UNION ALL
    SELECT '2024-06-01'::DATE, 2024, 6 UNION ALL SELECT '2024-07-01'::DATE, 2024, 7 UNION ALL
    SELECT '2024-08-01'::DATE, 2024, 8 UNION ALL SELECT '2024-09-01'::DATE, 2024, 9 UNION ALL
    SELECT '2024-10-01'::DATE, 2024, 10 UNION ALL SELECT '2024-11-01'::DATE, 2024, 11 UNION ALL
    SELECT '2024-12-01'::DATE, 2024, 12 UNION ALL SELECT '2025-01-01'::DATE, 2025, 1 UNION ALL
    SELECT '2025-02-01'::DATE, 2025, 2 UNION ALL SELECT '2025-03-01'::DATE, 2025, 3 UNION ALL
    SELECT '2025-04-01'::DATE, 2025, 4 UNION ALL SELECT '2025-05-01'::DATE, 2025, 5 UNION ALL
    SELECT '2025-06-01'::DATE, 2025, 6 UNION ALL SELECT '2025-07-01'::DATE, 2025, 7 UNION ALL
    SELECT '2025-08-01'::DATE, 2025, 8
),
active_employees AS (
    SELECT EMPLOYEE_ID, REGION, COMMISSION_RATE
    FROM SALES_EMPLOYEES 
    WHERE ACTIVE = TRUE AND REGION IN ('West', 'East', 'Central', 'Global')
),
sample_customers AS (
    SELECT DISTINCT CUSTOMER_ID 
    FROM CUSTOMER_ASSIGNMENTS 
    LIMIT 100
),
transaction_base AS (
    SELECT 
        m.month_start,
        m.year_num,
        ae.EMPLOYEE_ID,
        ae.REGION,
        ae.COMMISSION_RATE,
        sc.CUSTOMER_ID,
        t.txn_seq
    FROM all_months m
    CROSS JOIN active_employees ae
    CROSS JOIN sample_customers sc
    CROSS JOIN (SELECT 1 as txn_seq UNION ALL SELECT 2 UNION ALL SELECT 3) t
    WHERE MOD(ABS(HASH(m.month_start, ae.EMPLOYEE_ID, sc.CUSTOMER_ID, t.txn_seq)), 100) < 8
)
SELECT 
    'TXN' || LPAD(ROW_NUMBER() OVER (ORDER BY month_start, EMPLOYEE_ID, txn_seq), 8, '0') as TRANSACTION_ID,
    NULL as ORIGINAL_TRANSACTION_ID,
    EMPLOYEE_ID,
    CUSTOMER_ID,
    month_start + MOD(ABS(HASH(EMPLOYEE_ID, CUSTOMER_ID, txn_seq)), 28) as TRANSACTION_DATE,
    CASE MOD(ABS(HASH(EMPLOYEE_ID, month_start, txn_seq)), 5)
        WHEN 0 THEN 'Data Warehouse'
        WHEN 1 THEN 'Cortex AI'
        WHEN 2 THEN 'Analytics Workbench'
        WHEN 3 THEN 'Data Engineering'
        ELSE 'Data Governance'
    END as PRODUCT_CATEGORY,
    5 + MOD(ABS(HASH(CUSTOMER_ID, txn_seq)), 20) as QUANTITY,
    (200 + MOD(ABS(HASH(EMPLOYEE_ID, CUSTOMER_ID)), 400)) * 
    CASE 
        WHEN year_num = 2023 THEN 1.0
        WHEN year_num = 2024 THEN 1.25
        WHEN year_num = 2025 THEN 1.55
        ELSE 1.0
    END as UNIT_PRICE,
    QUANTITY * UNIT_PRICE as TOTAL_AMOUNT,
    QUANTITY * UNIT_PRICE * COMMISSION_RATE as COMMISSION_AMOUNT,
    CASE MOD(ABS(HASH(month_start, CUSTOMER_ID)), 4)
        WHEN 0 THEN 'New Business'
        WHEN 1 THEN 'Upsell'
        WHEN 2 THEN 'Renewal'
        ELSE 'Cross-sell'
    END as DEAL_TYPE,
    CURRENT_TIMESTAMP() as CREATED_DATE
FROM transaction_base;

-- 4. Generate Sales Performance Data
INSERT INTO SALES_PERFORMANCE 
WITH monthly_aggregates AS (
    SELECT 
        st.EMPLOYEE_ID,
        YEAR(st.TRANSACTION_DATE) as PERIOD_YEAR,
        MONTH(st.TRANSACTION_DATE) as PERIOD_MONTH,
        QUARTER(st.TRANSACTION_DATE) as PERIOD_QUARTER,
        SUM(st.TOTAL_AMOUNT) as SALES_AMOUNT,
        SUM(st.QUANTITY) as UNITS_SOLD,
        COUNT(DISTINCT CASE WHEN st.DEAL_TYPE = 'New Business' THEN st.CUSTOMER_ID END) as NEW_CUSTOMERS,
        COUNT(st.TRANSACTION_ID) as DEALS_CLOSED,
        SUM(st.COMMISSION_AMOUNT) as COMMISSION_EARNED
    FROM SALES_TRANSACTIONS st
    GROUP BY st.EMPLOYEE_ID, YEAR(st.TRANSACTION_DATE), MONTH(st.TRANSACTION_DATE), QUARTER(st.TRANSACTION_DATE)
),
performance_with_quotas AS (
    SELECT 
        'PERF' || LPAD(ROW_NUMBER() OVER (ORDER BY ma.EMPLOYEE_ID, ma.PERIOD_YEAR, ma.PERIOD_MONTH), 6, '0') as PERFORMANCE_ID,
        ma.*,
        se.QUOTA_AMOUNT,
        se.COMMISSION_RATE,
        CASE 
            WHEN se.QUOTA_AMOUNT > 0 THEN 
                -- Use realistic distribution where ~90% of ALL employees meet quota (not 100%)
                -- This applies to both Sales Reps and Managers for consistent distribution
                CASE WHEN MOD(ABS(HASH(se.EMPLOYEE_ID)), 100) < 10 
                     THEN 
                        -- 10% underperform (0.75-0.99)
                        ROUND(0.75 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_MONTH)), 24) / 100.0), 4) 
                     ELSE 
                        -- 90% meet/exceed quota (1.0-1.50)
                        ROUND(1.0 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_YEAR)), 50) / 100.0), 4)
                END
            ELSE 0
        END as QUOTA_ATTAINMENT,
        CASE 
            -- Calculate bonus based on the generated quota attainment, not actual sales
            WHEN se.QUOTA_AMOUNT > 0 THEN
                CASE 
                    WHEN (CASE WHEN MOD(ABS(HASH(se.EMPLOYEE_ID)), 100) < 10 
                               THEN ROUND(0.75 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_MONTH)), 24) / 100.0), 4) 
                               ELSE ROUND(1.0 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_YEAR)), 50) / 100.0), 4)
                          END) > 1.5 THEN ROUND((se.QUOTA_AMOUNT / 12) * 0.05, 2)
                    WHEN (CASE WHEN MOD(ABS(HASH(se.EMPLOYEE_ID)), 100) < 10 
                               THEN ROUND(0.75 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_MONTH)), 24) / 100.0), 4) 
                               ELSE ROUND(1.0 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_YEAR)), 50) / 100.0), 4)
                          END) > 1.3 THEN ROUND((se.QUOTA_AMOUNT / 12) * 0.03, 2)
                    WHEN (CASE WHEN MOD(ABS(HASH(se.EMPLOYEE_ID)), 100) < 10 
                               THEN ROUND(0.75 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_MONTH)), 24) / 100.0), 4) 
                               ELSE ROUND(1.0 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_YEAR)), 50) / 100.0), 4)
                          END) > 1.2 THEN ROUND((se.QUOTA_AMOUNT / 12) * 0.02, 2)
                    WHEN (CASE WHEN MOD(ABS(HASH(se.EMPLOYEE_ID)), 100) < 10 
                               THEN ROUND(0.75 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_MONTH)), 24) / 100.0), 4) 
                               ELSE ROUND(1.0 + (MOD(ABS(HASH(se.EMPLOYEE_ID, ma.PERIOD_YEAR)), 50) / 100.0), 4)
                          END) > 1.0 THEN ROUND((se.QUOTA_AMOUNT / 12) * 0.01, 2)
                    ELSE 0
                END
            ELSE 0
        END as BONUS_EARNED
    FROM monthly_aggregates ma
    JOIN SALES_EMPLOYEES se ON ma.EMPLOYEE_ID = se.EMPLOYEE_ID
    WHERE se.ACTIVE = TRUE
)
SELECT 
    pwq.PERFORMANCE_ID,
    pwq.EMPLOYEE_ID,
    pwq.PERIOD_YEAR,
    pwq.PERIOD_MONTH,
    pwq.PERIOD_QUARTER,
    COALESCE(pwq.SALES_AMOUNT, 0) as SALES_AMOUNT,
    COALESCE(pwq.UNITS_SOLD, 0) as UNITS_SOLD,
    COALESCE(pwq.NEW_CUSTOMERS, 0) as NEW_CUSTOMERS,
    COALESCE(pwq.DEALS_CLOSED, 0) as DEALS_CLOSED,
    COALESCE(pwq.QUOTA_ATTAINMENT, 0) as QUOTA_ATTAINMENT,
    COALESCE(pwq.COMMISSION_EARNED, 0) as COMMISSION_EARNED,
    COALESCE(pwq.BONUS_EARNED, 0) as BONUS_EARNED,
    COALESCE(pwq.COMMISSION_EARNED, 0) + COALESCE(pwq.BONUS_EARNED, 0) as TOTAL_COMPENSATION,
    0 as RANK_IN_TEAM,
    0 as RANK_IN_REGION,
    CURRENT_TIMESTAMP() as CREATED_DATE
FROM performance_with_quotas pwq;

-- 5. Insert Data Entitlements
INSERT INTO DATA_ENTITLEMENTS VALUES
-- CRO - Can see everything
('ENT001', 'EMP001', 'ALL', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),

-- VP Sales - Can see everything
('ENT002', 'EMP002', 'ALL', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT003', 'EMP003', 'ALL', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),

-- Regional Managers - Can see their region
('ENT004', 'EMP004', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT005', 'EMP005', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT006', 'EMP006', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT007', 'EMP007', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT008', 'EMP008', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT009', 'EMP009', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT010', 'EMP010', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP());

-- Sales Managers and Sales Reps - Generate entitlements
INSERT INTO DATA_ENTITLEMENTS
SELECT 
    'ENT' || LPAD(ROW_NUMBER() OVER (ORDER BY EMPLOYEE_ID) + 100, 3, '0') as ENTITLEMENT_ID,
    EMPLOYEE_ID,
    CASE 
        WHEN ROLE = 'Sales Manager' THEN 'DIRECT_REPORTS'
        WHEN ROLE = 'Sales Rep' THEN 'SELF_ONLY'
        ELSE 'SELF_ONLY'
    END as ACCESS_LEVEL,
    CASE WHEN ROLE IN ('Sales Manager') THEN TRUE ELSE FALSE END as CAN_VIEW_COMPENSATION,
    TRUE as CAN_VIEW_INDIVIDUAL_PERFORMANCE,
    CASE WHEN ROLE IN ('Sales Manager') THEN TRUE ELSE FALSE END as CAN_VIEW_TEAM_PERFORMANCE,
    TRUE as CAN_VIEW_CUSTOMER_DATA,
    DATE('2023-01-01') as EFFECTIVE_DATE,
    NULL as EXPIRY_DATE,
    CURRENT_TIMESTAMP() as CREATED_DATE
FROM SALES_EMPLOYEES
WHERE ACTIVE = TRUE AND ROLE IN ('Sales Manager', 'Sales Rep');

-- Final Summary
SELECT 'DATA_GENERATION_COMPLETE' as status;

SELECT 
    'Final Hierarchy' as summary_type,
    ROLE,
    COUNT(*) as employee_count
FROM SALES_EMPLOYEES 
WHERE ACTIVE = TRUE
GROUP BY ROLE
ORDER BY 
    CASE ROLE 
        WHEN 'CRO' THEN 1
        WHEN 'VP Sales' THEN 2
        WHEN 'Regional Manager' THEN 3
        WHEN 'Sales Manager' THEN 4
        WHEN 'Sales Rep' THEN 5
        ELSE 6
    END;

SELECT 
    'Data Volume Summary' as summary_type,
    (SELECT COUNT(*) FROM SALES_EMPLOYEES WHERE ACTIVE = TRUE) as total_employees,
    (SELECT COUNT(*) FROM SALES_TRANSACTIONS) as total_transactions,
    (SELECT COUNT(*) FROM CUSTOMER_ASSIGNMENTS) as total_assignments,
    (SELECT COUNT(*) FROM SALES_PERFORMANCE) as total_performance_records,
    (SELECT COUNT(DISTINCT REGION) FROM SALES_EMPLOYEES WHERE REGION IN ('West', 'East', 'Central')) as distinct_regions,
    (SELECT COUNT(DISTINCT DATE_TRUNC('MONTH', TRANSACTION_DATE)) FROM SALES_TRANSACTIONS) as months_with_transactions;
