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

-- 3. Insert Sales Performance Data (Monthly data for 2023)
-- This generates performance data for each sales rep for each month of 2023
INSERT INTO SALES_PERFORMANCE 
WITH monthly_performance AS (
    SELECT 
        'PERF' || LPAD(ROW_NUMBER() OVER (ORDER BY e.EMPLOYEE_ID, m.month_num), 6, '0') as PERFORMANCE_ID,
        e.EMPLOYEE_ID,
        2023 as PERIOD_YEAR,
        m.month_num as PERIOD_MONTH,
        CASE 
            WHEN m.month_num <= 3 THEN 1
            WHEN m.month_num <= 6 THEN 2  
            WHEN m.month_num <= 9 THEN 3
            ELSE 4
        END as PERIOD_QUARTER,
        -- Generate realistic sales amounts based on role and seasonality
        CASE e.ROLE
            WHEN 'Sales Rep' THEN 
                ROUND((e.QUOTA_AMOUNT / 12) * (0.8 + RANDOM() * 0.6) * 
                      CASE m.month_num WHEN 12 THEN 1.3 WHEN 11 THEN 1.2 WHEN 3 THEN 1.1 ELSE 1.0 END, 2)
            WHEN 'Sales Manager' THEN 
                ROUND((e.QUOTA_AMOUNT / 12) * (0.85 + RANDOM() * 0.4) * 
                      CASE m.month_num WHEN 12 THEN 1.3 WHEN 11 THEN 1.2 WHEN 3 THEN 1.1 ELSE 1.0 END, 2)
            ELSE 
                ROUND((e.QUOTA_AMOUNT / 12) * (0.9 + RANDOM() * 0.3) * 
                      CASE m.month_num WHEN 12 THEN 1.3 WHEN 11 THEN 1.2 WHEN 3 THEN 1.1 ELSE 1.0 END, 2)
        END as SALES_AMOUNT,
        -- Units sold (roughly proportional to sales amount)
        ROUND(CASE e.ROLE
            WHEN 'Sales Rep' THEN 15 + RANDOM() * 25
            WHEN 'Sales Manager' THEN 35 + RANDOM() * 40  
            ELSE 60 + RANDOM() * 80
        END) as UNITS_SOLD,
        -- New customers per month
        ROUND(CASE e.ROLE
            WHEN 'Sales Rep' THEN 1 + RANDOM() * 4
            WHEN 'Sales Manager' THEN 3 + RANDOM() * 6
            ELSE 8 + RANDOM() * 12  
        END) as NEW_CUSTOMERS,
        -- Deals closed
        ROUND(CASE e.ROLE
            WHEN 'Sales Rep' THEN 3 + RANDOM() * 8
            WHEN 'Sales Manager' THEN 8 + RANDOM() * 15
            ELSE 20 + RANDOM() * 30
        END) as DEALS_CLOSED,
        e.QUOTA_AMOUNT,
        e.COMMISSION_RATE
    FROM SALES_EMPLOYEES e
    CROSS JOIN (
        SELECT 1 as month_num UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL 
        SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    ) m
    WHERE e.ACTIVE = TRUE
)
SELECT 
    PERFORMANCE_ID,
    EMPLOYEE_ID,
    PERIOD_YEAR,
    PERIOD_MONTH,
    PERIOD_QUARTER,
    SALES_AMOUNT,
    UNITS_SOLD,
    NEW_CUSTOMERS,
    DEALS_CLOSED,
    ROUND(SALES_AMOUNT / (QUOTA_AMOUNT / 12), 4) as QUOTA_ATTAINMENT,
    ROUND(SALES_AMOUNT * COMMISSION_RATE, 2) as COMMISSION_EARNED,
    CASE 
        WHEN SALES_AMOUNT / (QUOTA_AMOUNT / 12) > 1.2 THEN ROUND(SALES_AMOUNT * 0.02, 2)
        WHEN SALES_AMOUNT / (QUOTA_AMOUNT / 12) > 1.0 THEN ROUND(SALES_AMOUNT * 0.01, 2)
        ELSE 0
    END as BONUS_EARNED,
    0 as TOTAL_COMPENSATION, -- Will be updated below
    0 as RANK_IN_TEAM,
    0 as RANK_IN_REGION,
    CURRENT_TIMESTAMP() as CREATED_DATE
FROM monthly_performance;

-- Update total compensation
UPDATE SALES_PERFORMANCE 
SET TOTAL_COMPENSATION = COMMISSION_EARNED + BONUS_EARNED;

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