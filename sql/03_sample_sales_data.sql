-- Sample Data for Sales Hierarchy and Compensation System
-- This script populates the sales tables with realistic sample data

USE SLACK_SALES_DEMO.SLACK_SCHEMA;

-- 1. Insert Sales Employees (Building hierarchy from top down)
INSERT INTO SALES_EMPLOYEES VALUES
-- VP Sales (Top Level)
('EMP001', 'Sarah', 'Johnson', 'sarah.johnson@company.com', 'VP Sales', 'National', 'National', '2020-01-15', 180000.00, 0.0100, 5000000.00, NULL, TRUE, CURRENT_TIMESTAMP()),

-- Regional Managers (Report to VP)
('EMP002', 'Michael', 'Chen', 'michael.chen@company.com', 'Regional Manager', 'West Coast', 'West', '2020-03-01', 120000.00, 0.0150, 2000000.00, 'EMP001', TRUE, CURRENT_TIMESTAMP()),
('EMP003', 'Jennifer', 'Davis', 'jennifer.davis@company.com', 'Regional Manager', 'East Coast', 'East', '2020-04-15', 118000.00, 0.0150, 1800000.00, 'EMP001', TRUE, CURRENT_TIMESTAMP()),
('EMP004', 'Robert', 'Wilson', 'robert.wilson@company.com', 'Regional Manager', 'Central', 'Central', '2021-01-10', 115000.00, 0.0150, 1500000.00, 'EMP001', TRUE, CURRENT_TIMESTAMP()),

-- Sales Managers (Report to Regional Managers)
('EMP005', 'Lisa', 'Anderson', 'lisa.anderson@company.com', 'Sales Manager', 'California', 'West', '2021-06-01', 85000.00, 0.0200, 800000.00, 'EMP002', TRUE, CURRENT_TIMESTAMP()),
('EMP006', 'David', 'Martinez', 'david.martinez@company.com', 'Sales Manager', 'Pacific Northwest', 'West', '2021-08-15', 82000.00, 0.0200, 750000.00, 'EMP002', TRUE, CURRENT_TIMESTAMP()),
('EMP007', 'Amanda', 'Taylor', 'amanda.taylor@company.com', 'Sales Manager', 'New York', 'East', '2021-05-20', 88000.00, 0.0200, 900000.00, 'EMP003', TRUE, CURRENT_TIMESTAMP()),
('EMP008', 'James', 'Brown', 'james.brown@company.com', 'Sales Manager', 'Florida', 'East', '2021-09-10', 80000.00, 0.0200, 700000.00, 'EMP003', TRUE, CURRENT_TIMESTAMP()),
('EMP009', 'Michelle', 'Garcia', 'michelle.garcia@company.com', 'Sales Manager', 'Texas', 'Central', '2022-01-05', 83000.00, 0.0200, 750000.00, 'EMP004', TRUE, CURRENT_TIMESTAMP()),
('EMP010', 'Christopher', 'Lee', 'christopher.lee@company.com', 'Sales Manager', 'Illinois', 'Central', '2022-02-15', 81000.00, 0.0200, 700000.00, 'EMP004', TRUE, CURRENT_TIMESTAMP()),

-- Sales Representatives (Report to Sales Managers)
-- California Team
('EMP011', 'Jessica', 'Miller', 'jessica.miller@company.com', 'Sales Rep', 'San Francisco', 'West', '2022-03-01', 65000.00, 0.0300, 400000.00, 'EMP005', TRUE, CURRENT_TIMESTAMP()),
('EMP012', 'Ryan', 'Moore', 'ryan.moore@company.com', 'Sales Rep', 'Los Angeles', 'West', '2022-04-15', 62000.00, 0.0300, 380000.00, 'EMP005', TRUE, CURRENT_TIMESTAMP()),
('EMP013', 'Emily', 'Jackson', 'emily.jackson@company.com', 'Sales Rep', 'San Diego', 'West', '2022-06-01', 60000.00, 0.0300, 350000.00, 'EMP005', TRUE, CURRENT_TIMESTAMP()),

-- Pacific Northwest Team  
('EMP014', 'Kevin', 'White', 'kevin.white@company.com', 'Sales Rep', 'Seattle', 'West', '2022-07-01', 63000.00, 0.0300, 370000.00, 'EMP006', TRUE, CURRENT_TIMESTAMP()),
('EMP015', 'Nicole', 'Harris', 'nicole.harris@company.com', 'Sales Rep', 'Portland', 'West', '2022-08-15', 61000.00, 0.0300, 360000.00, 'EMP006', TRUE, CURRENT_TIMESTAMP()),

-- New York Team
('EMP016', 'Daniel', 'Martin', 'daniel.martin@company.com', 'Sales Rep', 'Manhattan', 'East', '2022-05-01', 68000.00, 0.0300, 420000.00, 'EMP007', TRUE, CURRENT_TIMESTAMP()),
('EMP017', 'Stephanie', 'Thompson', 'stephanie.thompson@company.com', 'Sales Rep', 'Brooklyn', 'East', '2022-06-15', 66000.00, 0.0300, 400000.00, 'EMP007', TRUE, CURRENT_TIMESTAMP()),
('EMP018', 'Mark', 'Garcia', 'mark.garcia@company.com', 'Sales Rep', 'Queens', 'East', '2022-09-01', 64000.00, 0.0300, 380000.00, 'EMP007', TRUE, CURRENT_TIMESTAMP()),

-- Florida Team
('EMP019', 'Rachel', 'Martinez', 'rachel.martinez@company.com', 'Sales Rep', 'Miami', 'East', '2022-10-01', 62000.00, 0.0300, 360000.00, 'EMP008', TRUE, CURRENT_TIMESTAMP()),
('EMP020', 'Andrew', 'Robinson', 'andrew.robinson@company.com', 'Sales Rep', 'Tampa', 'East', '2022-11-15', 60000.00, 0.0300, 340000.00, 'EMP008', TRUE, CURRENT_TIMESTAMP()),

-- Texas Team
('EMP021', 'Lauren', 'Clark', 'lauren.clark@company.com', 'Sales Rep', 'Houston', 'Central', '2023-01-01', 64000.00, 0.0300, 380000.00, 'EMP009', TRUE, CURRENT_TIMESTAMP()),
('EMP022', 'Tyler', 'Rodriguez', 'tyler.rodriguez@company.com', 'Sales Rep', 'Dallas', 'Central', '2023-02-15', 63000.00, 0.0300, 370000.00, 'EMP009', TRUE, CURRENT_TIMESTAMP()),

-- Illinois Team
('EMP023', 'Megan', 'Lewis', 'megan.lewis@company.com', 'Sales Rep', 'Chicago', 'Central', '2023-03-01', 65000.00, 0.0300, 390000.00, 'EMP010', TRUE, CURRENT_TIMESTAMP()),
('EMP024', 'Brandon', 'Walker', 'brandon.walker@company.com', 'Sales Rep', 'Springfield', 'Central', '2023-04-15', 58000.00, 0.0300, 320000.00, 'EMP010', TRUE, CURRENT_TIMESTAMP());

-- 2. Insert Customer Assignments (Link customers to sales reps)
INSERT INTO CUSTOMER_ASSIGNMENTS VALUES
-- Sample assignments - each customer gets assigned to a sales rep
('ASSIGN001', 'EMP011', 'CUST001', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN002', 'EMP011', 'CUST002', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN003', 'EMP012', 'CUST003', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN004', 'EMP012', 'CUST004', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN005', 'EMP013', 'CUST005', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN006', 'EMP013', 'CUST006', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN007', 'EMP014', 'CUST007', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN008', 'EMP014', 'CUST008', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN009', 'EMP015', 'CUST009', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN010', 'EMP015', 'CUST010', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN011', 'EMP016', 'CUST011', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN012', 'EMP016', 'CUST012', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN013', 'EMP017', 'CUST013', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN014', 'EMP017', 'CUST014', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN015', 'EMP018', 'CUST015', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN016', 'EMP018', 'CUST016', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN017', 'EMP019', 'CUST017', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN018', 'EMP019', 'CUST018', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN019', 'EMP020', 'CUST019', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN020', 'EMP020', 'CUST020', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN021', 'EMP021', 'CUST021', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN022', 'EMP021', 'CUST022', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN023', 'EMP022', 'CUST023', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN024', 'EMP022', 'CUST024', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN025', 'EMP023', 'CUST025', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN026', 'EMP023', 'CUST026', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN027', 'EMP024', 'CUST027', '2023-01-01', TRUE, CURRENT_TIMESTAMP()),
('ASSIGN028', 'EMP024', 'CUST028', '2023-01-01', TRUE, CURRENT_TIMESTAMP());

-- 3. Generate ~2000 Sales Transactions from Jan 2023 to Aug 2025
-- First, let's create a larger customer base
INSERT INTO CUSTOMER_ASSIGNMENTS 
WITH customer_numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as cust_num
    FROM TABLE(GENERATOR(ROWCOUNT => 200)) -- Generate 200 customers
),
sales_reps AS (
    SELECT EMPLOYEE_ID 
    FROM SALES_EMPLOYEES 
    WHERE ROLE = 'Sales Rep' AND ACTIVE = TRUE
),
expanded_customers AS (
    SELECT 
        'ASSIGN' || LPAD(ROW_NUMBER() OVER (ORDER BY sr.EMPLOYEE_ID, cn.cust_num), 6, '0') as ASSIGNMENT_ID,
        sr.EMPLOYEE_ID,
        'CUST' || LPAD(cn.cust_num, 4, '0') as CUSTOMER_ID,
        DATE('2023-01-01') + UNIFORM(0, 365, RANDOM()) as ASSIGNED_DATE
    FROM sales_reps sr
    CROSS JOIN customer_numbers cn
    WHERE cn.cust_num <= 200
)
SELECT 
    ASSIGNMENT_ID,
    EMPLOYEE_ID,
    CUSTOMER_ID,
    ASSIGNED_DATE,
    TRUE as IS_PRIMARY,
    CURRENT_TIMESTAMP() as CREATED_DATE
FROM expanded_customers;

-- 4. Generate ~2000 Sales Transactions
INSERT INTO SALES_TRANSACTIONS
WITH date_sequence AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) as day_number
    FROM TABLE(GENERATOR(ROWCOUNT => 950)) -- From Jan 1, 2023 to Aug 31, 2025 (2.67 years)
),
date_range AS (
    SELECT 
        DATE('2023-01-01') + day_number - 1 as transaction_date
    FROM date_sequence
    WHERE DATE('2023-01-01') + day_number - 1 <= DATE('2025-08-31')
),
transaction_data AS (
    SELECT 
        'TXN' || LPAD(ROW_NUMBER() OVER (ORDER BY dr.transaction_date, ca.EMPLOYEE_ID), 8, '0') as TRANSACTION_ID,
        NULL as ORIGINAL_TRANSACTION_ID, -- Will link to retail dataset later if needed
        ca.EMPLOYEE_ID,
        ca.CUSTOMER_ID,
        dr.transaction_date,
        -- Snowflake product categories with realistic distribution
        CASE UNIFORM(1, 10, RANDOM())
            WHEN 1 THEN 'Data Warehouse'
            WHEN 2 THEN 'Data Warehouse' 
            WHEN 3 THEN 'Cortex AI'
            WHEN 4 THEN 'Analytics Workbench'
            WHEN 5 THEN 'Data Engineering'
            WHEN 6 THEN 'Data Governance'
            WHEN 7 THEN 'Data Sharing'
            WHEN 8 THEN 'Developer Tools'
            WHEN 9 THEN 'Data Lake'
            ELSE 'Marketplace'
        END as PRODUCT_CATEGORY,
        -- Number of user seats/licenses (1-50 for SaaS)
        CASE 
            WHEN UNIFORM(1, 10, RANDOM()) <= 6 THEN UNIFORM(1, 10, RANDOM()) -- Small teams (60%)
            WHEN UNIFORM(1, 10, RANDOM()) <= 8 THEN UNIFORM(11, 25, RANDOM()) -- Medium teams (20%) 
            ELSE UNIFORM(26, 50, RANDOM()) -- Large teams (20%)
        END as QUANTITY,
        -- Monthly subscription price varies by Snowflake product category
        CASE 
            WHEN PRODUCT_CATEGORY = 'Data Warehouse' THEN UNIFORM(100, 600, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Cortex AI' THEN UNIFORM(200, 1000, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Analytics Workbench' THEN UNIFORM(150, 500, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Data Engineering' THEN UNIFORM(120, 450, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Data Governance' THEN UNIFORM(150, 500, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Data Sharing' THEN UNIFORM(50, 300, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Developer Tools' THEN UNIFORM(75, 400, RANDOM())
            WHEN PRODUCT_CATEGORY = 'Data Lake' THEN UNIFORM(80, 350, RANDOM())
            ELSE UNIFORM(100, 400, RANDOM()) -- Marketplace
        END as UNIT_PRICE,
        -- Deal types with realistic distribution
        CASE UNIFORM(1, 10, RANDOM())
            WHEN 1 THEN 'New Business'
            WHEN 2 THEN 'New Business'
            WHEN 3 THEN 'New Business'
            WHEN 4 THEN 'Upsell'
            WHEN 5 THEN 'Upsell'
            WHEN 6 THEN 'Renewal'
            WHEN 7 THEN 'Renewal'
            WHEN 8 THEN 'Renewal'
            WHEN 9 THEN 'Cross-sell'
            ELSE 'Expansion'
        END as DEAL_TYPE
    FROM date_range dr
    CROSS JOIN CUSTOMER_ASSIGNMENTS ca
    WHERE UNIFORM(0, 1, RANDOM()) < 0.0035 -- ~0.35% chance per customer per day for better distribution
)
SELECT 
    td.TRANSACTION_ID,
    td.ORIGINAL_TRANSACTION_ID,
    td.EMPLOYEE_ID,
    td.CUSTOMER_ID,
    td.transaction_date as TRANSACTION_DATE,
    td.PRODUCT_CATEGORY,
    td.QUANTITY,
    ROUND(td.UNIT_PRICE, 2) as UNIT_PRICE,
    ROUND(td.QUANTITY * td.UNIT_PRICE, 2) as TOTAL_AMOUNT,
    ROUND(td.QUANTITY * td.UNIT_PRICE * se.COMMISSION_RATE, 2) as COMMISSION_AMOUNT,
    td.DEAL_TYPE,
    CURRENT_TIMESTAMP() as CREATED_DATE
FROM transaction_data td
JOIN SALES_EMPLOYEES se ON td.EMPLOYEE_ID = se.EMPLOYEE_ID
LIMIT 2000; -- Ensure we get approximately 2000 transactions

-- 5. Generate Sales Performance Data based on actual transactions
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
        -- Calculate quota attainment (capped at 300%)
        LEAST(ROUND(ma.SALES_AMOUNT / (se.QUOTA_AMOUNT / 12), 4), 3.0000) as QUOTA_ATTAINMENT,
        -- Calculate bonus based on performance
        CASE 
            WHEN ma.SALES_AMOUNT / (se.QUOTA_AMOUNT / 12) > 1.3 THEN ROUND(ma.SALES_AMOUNT * 0.03, 2)
            WHEN ma.SALES_AMOUNT / (se.QUOTA_AMOUNT / 12) > 1.2 THEN ROUND(ma.SALES_AMOUNT * 0.02, 2)
            WHEN ma.SALES_AMOUNT / (se.QUOTA_AMOUNT / 12) > 1.0 THEN ROUND(ma.SALES_AMOUNT * 0.01, 2)
            ELSE 0
        END as BONUS_EARNED
    FROM monthly_aggregates ma
    JOIN SALES_EMPLOYEES se ON ma.EMPLOYEE_ID = se.EMPLOYEE_ID
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

-- 4. Insert Data Entitlements
INSERT INTO DATA_ENTITLEMENTS VALUES
-- VP Sales - Can see everything
('ENT001', 'EMP001', 'ALL', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),

-- Regional Managers - Can see their region
('ENT002', 'EMP002', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT003', 'EMP003', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT004', 'EMP004', 'REGION', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),

-- Sales Managers - Can see direct reports
('ENT005', 'EMP005', 'DIRECT_REPORTS', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT006', 'EMP006', 'DIRECT_REPORTS', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT007', 'EMP007', 'DIRECT_REPORTS', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT008', 'EMP008', 'DIRECT_REPORTS', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT009', 'EMP009', 'DIRECT_REPORTS', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT010', 'EMP010', 'DIRECT_REPORTS', TRUE, TRUE, TRUE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),

-- Sales Reps - Can see only their own data
('ENT011', 'EMP011', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT012', 'EMP012', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT013', 'EMP013', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT014', 'EMP014', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT015', 'EMP015', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT016', 'EMP016', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT017', 'EMP017', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT018', 'EMP018', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT019', 'EMP019', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT020', 'EMP020', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT021', 'EMP021', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT022', 'EMP022', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT023', 'EMP023', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP()),
('ENT024', 'EMP024', 'SELF_ONLY', FALSE, TRUE, FALSE, TRUE, '2023-01-01', NULL, CURRENT_TIMESTAMP());