-- Comprehensive Sales Semantic View for Cortex
-- This view combines all sales data for easy semantic modeling

USE SLACK_SALES_DEMO.SLACK_SCHEMA;

CREATE OR REPLACE VIEW SALES_SEMANTIC_VIEW AS
SELECT 
    -- Performance Identifiers
    sp.PERFORMANCE_ID,
    sp.EMPLOYEE_ID,
    sp.PERIOD_YEAR,
    sp.PERIOD_MONTH,
    sp.PERIOD_QUARTER,
    
    -- Employee Information
    se.FIRST_NAME,
    se.LAST_NAME,
    se.FIRST_NAME || ' ' || se.LAST_NAME as FULL_NAME,
    se.EMAIL,
    se.ROLE,
    se.TERRITORY,
    se.REGION,
    se.HIRE_DATE,
    
    -- Compensation & Quota
    se.BASE_SALARY,
    se.COMMISSION_RATE,
    se.QUOTA_AMOUNT,
    se.QUOTA_AMOUNT / 12 as MONTHLY_QUOTA,
    
    -- Performance Metrics
    sp.SALES_AMOUNT,
    sp.UNITS_SOLD,
    sp.NEW_CUSTOMERS,
    sp.DEALS_CLOSED,
    sp.QUOTA_ATTAINMENT,
    sp.COMMISSION_EARNED,
    sp.BONUS_EARNED,
    sp.TOTAL_COMPENSATION,
    sp.RANK_IN_TEAM,
    sp.RANK_IN_REGION,
    
    -- Hierarchy Information
    mh.LEVEL as HIERARCHY_LEVEL,
    mh.TOP_LEVEL_MANAGER,
    mh.PATH as REPORTING_PATH,
    
    -- Manager Information
    mgr.FIRST_NAME || ' ' || mgr.LAST_NAME as MANAGER_NAME,
    mgr.ROLE as MANAGER_ROLE,
    mgr.TERRITORY as MANAGER_TERRITORY,
    mgr.REGION as MANAGER_REGION,
    
    -- Calculated Fields
    CASE 
        WHEN sp.QUOTA_ATTAINMENT >= 1.2 THEN 'Exceeds'
        WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN 'Meets' 
        WHEN sp.QUOTA_ATTAINMENT >= 0.8 THEN 'Approaching'
        ELSE 'Below'
    END as QUOTA_PERFORMANCE_BAND,
    
    CASE 
        WHEN se.ROLE = 'Sales Rep' THEN 'Individual Contributor'
        WHEN se.ROLE IN ('Sales Manager', 'Regional Manager') THEN 'Manager'
        ELSE 'Executive'
    END as ROLE_CATEGORY,
    
    -- Time Dimensions
    DATE(sp.PERIOD_YEAR || '-' || LPAD(sp.PERIOD_MONTH, 2, '0') || '-01') as PERIOD_DATE,
    MONTHNAME(DATE(sp.PERIOD_YEAR || '-' || LPAD(sp.PERIOD_MONTH, 2, '0') || '-01')) as MONTH_NAME,
    'Q' || sp.PERIOD_QUARTER as QUARTER_LABEL,
    
    -- Tenure Calculations
    DATEDIFF('month', se.HIRE_DATE, CURRENT_DATE()) as TENURE_MONTHS,
    CASE 
        WHEN DATEDIFF('month', se.HIRE_DATE, CURRENT_DATE()) < 6 THEN 'New Hire'
        WHEN DATEDIFF('month', se.HIRE_DATE, CURRENT_DATE()) < 24 THEN 'Developing'
        ELSE 'Experienced'
    END as TENURE_CATEGORY

FROM SALES_PERFORMANCE sp
    JOIN SALES_EMPLOYEES se ON sp.EMPLOYEE_ID = se.EMPLOYEE_ID
    LEFT JOIN MANAGER_HIERARCHY mh ON se.EMPLOYEE_ID = mh.EMPLOYEE_ID
    LEFT JOIN SALES_EMPLOYEES mgr ON se.MANAGER_ID = mgr.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE;

-- Create a separate transaction-level semantic view
CREATE OR REPLACE VIEW TRANSACTION_SEMANTIC_VIEW AS
SELECT 
    -- Transaction Identifiers
    st.TRANSACTION_ID,
    st.TRANSACTION_DATE,
    YEAR(st.TRANSACTION_DATE) as TRANSACTION_YEAR,
    MONTH(st.TRANSACTION_DATE) as TRANSACTION_MONTH,
    QUARTER(st.TRANSACTION_DATE) as TRANSACTION_QUARTER,
    MONTHNAME(st.TRANSACTION_DATE) as MONTH_NAME,
    
    -- Employee Information
    st.EMPLOYEE_ID,
    se.FIRST_NAME || ' ' || se.LAST_NAME as EMPLOYEE_NAME,
    se.ROLE,
    se.TERRITORY,
    se.REGION,
    
    -- Customer Information  
    st.CUSTOMER_ID,
    ca.ASSIGNED_DATE as CUSTOMER_ASSIGNED_DATE,
    DATEDIFF('day', ca.ASSIGNED_DATE, st.TRANSACTION_DATE) as DAYS_SINCE_ASSIGNMENT,
    
    -- Product Information
    st.PRODUCT_CATEGORY,
    st.QUANTITY,
    st.UNIT_PRICE,
    st.TOTAL_AMOUNT,
    st.COMMISSION_AMOUNT,
    st.DEAL_TYPE,
    
    -- Hierarchy Information
    mh.LEVEL as HIERARCHY_LEVEL,
    mgr.FIRST_NAME || ' ' || mgr.LAST_NAME as MANAGER_NAME,
    
    -- Calculated Fields
    CASE 
        WHEN st.TOTAL_AMOUNT >= 10000 THEN 'Large Deal'
        WHEN st.TOTAL_AMOUNT >= 5000 THEN 'Medium Deal'
        ELSE 'Small Deal'
    END as DEAL_SIZE_CATEGORY,
    
    CASE 
        WHEN st.QUANTITY >= 25 THEN 'Enterprise'
        WHEN st.QUANTITY >= 10 THEN 'Mid-Market'
        ELSE 'SMB'
    END as CUSTOMER_SEGMENT

FROM SALES_TRANSACTIONS st
    JOIN SALES_EMPLOYEES se ON st.EMPLOYEE_ID = se.EMPLOYEE_ID
    LEFT JOIN CUSTOMER_ASSIGNMENTS ca ON st.CUSTOMER_ID = ca.CUSTOMER_ID 
        AND st.EMPLOYEE_ID = ca.EMPLOYEE_ID
    LEFT JOIN MANAGER_HIERARCHY mh ON se.EMPLOYEE_ID = mh.EMPLOYEE_ID
    LEFT JOIN SALES_EMPLOYEES mgr ON se.MANAGER_ID = mgr.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE;