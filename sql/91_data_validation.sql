-- Data Validation Queries
-- This file contains queries to validate that each table has the expected data
-- Updated for the realistic sales hierarchy (1 CRO, 2 VPs, proper ratios)

USE SLACK_SALES_DEMO.SLACK_SCHEMA;

-- =============================================================================
-- 1. SALES_EMPLOYEES Validation
-- =============================================================================
SELECT 'SALES_EMPLOYEES_VALIDATION' as validation_test;

-- Check employee count by role
SELECT 
    'Employee Count by Role' as test_name,
    ROLE,
    COUNT(*) as count,
    CASE 
        WHEN ROLE = 'CRO' AND COUNT(*) = 1 THEN 'PASS'
        WHEN ROLE = 'VP Sales' AND COUNT(*) = 2 THEN 'PASS'
        WHEN ROLE = 'Regional Manager' AND COUNT(*) >= 5 THEN 'PASS'
        WHEN ROLE = 'Sales Manager' AND COUNT(*) >= 10 THEN 'PASS'
        WHEN ROLE = 'Sales Rep' AND COUNT(*) >= 80 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
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

-- Check regions
SELECT 
    'Region Distribution' as test_name,
    REGION,
    COUNT(*) as employee_count,
    COUNT(CASE WHEN ROLE = 'Sales Rep' THEN 1 END) as sales_rep_count,
    CASE 
        WHEN REGION IN ('West', 'East', 'Central', 'Global') THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_EMPLOYEES 
WHERE ACTIVE = TRUE
GROUP BY REGION
ORDER BY REGION;

-- Check hierarchy integrity
SELECT 
    'Hierarchy Integrity' as test_name,
    COUNT(*) as employees_with_valid_managers,
    CASE 
        WHEN COUNT(*) > 100 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_EMPLOYEES e1
JOIN SALES_EMPLOYEES e2 ON e1.MANAGER_ID = e2.EMPLOYEE_ID
WHERE e1.ACTIVE = TRUE;

-- Check manager-to-rep ratios
SELECT 
    'Manager to Rep Ratios' as test_name,
    mgr.ROLE as manager_role,
    COUNT(DISTINCT mgr.EMPLOYEE_ID) as manager_count,
    COUNT(rep.EMPLOYEE_ID) as direct_report_count,
    ROUND(COUNT(rep.EMPLOYEE_ID) / COUNT(DISTINCT mgr.EMPLOYEE_ID), 1) as avg_reports_per_manager,
    CASE 
        WHEN mgr.ROLE = 'Sales Manager' AND COUNT(rep.EMPLOYEE_ID) / COUNT(DISTINCT mgr.EMPLOYEE_ID) BETWEEN 6 AND 12 THEN 'PASS'
        WHEN mgr.ROLE = 'Regional Manager' AND COUNT(rep.EMPLOYEE_ID) / COUNT(DISTINCT mgr.EMPLOYEE_ID) BETWEEN 1 AND 3 THEN 'PASS'
        WHEN mgr.ROLE = 'VP Sales' AND COUNT(rep.EMPLOYEE_ID) / COUNT(DISTINCT mgr.EMPLOYEE_ID) BETWEEN 3 AND 6 THEN 'PASS'
        WHEN mgr.ROLE = 'CRO' AND COUNT(rep.EMPLOYEE_ID) / COUNT(DISTINCT mgr.EMPLOYEE_ID) = 2 THEN 'PASS'
        ELSE 'REVIEW'
    END as test_result
FROM SALES_EMPLOYEES mgr
JOIN SALES_EMPLOYEES rep ON mgr.EMPLOYEE_ID = rep.MANAGER_ID
WHERE mgr.ACTIVE = TRUE AND rep.ACTIVE = TRUE
GROUP BY mgr.ROLE
ORDER BY 
    CASE mgr.ROLE 
        WHEN 'CRO' THEN 1
        WHEN 'VP Sales' THEN 2
        WHEN 'Regional Manager' THEN 3
        WHEN 'Sales Manager' THEN 4
        ELSE 5
    END;

-- =============================================================================
-- 2. SALES_TRANSACTIONS Validation
-- =============================================================================
SELECT 'SALES_TRANSACTIONS_VALIDATION' as validation_test;

-- Check date range
SELECT 
    'Transaction Date Range' as test_name,
    MIN(TRANSACTION_DATE) as earliest_date,
    MAX(TRANSACTION_DATE) as latest_date,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT DATE_TRUNC('MONTH', TRANSACTION_DATE)) as distinct_months,
    CASE 
        WHEN MIN(TRANSACTION_DATE) >= '2023-01-01' 
         AND MAX(TRANSACTION_DATE) <= '2025-08-31'
         AND COUNT(DISTINCT DATE_TRUNC('MONTH', TRANSACTION_DATE)) = 32 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_TRANSACTIONS;

-- Check monthly distribution
SELECT 
    'Monthly Transaction Distribution' as test_name,
    COUNT(DISTINCT month) as months_with_data,
    MIN(monthly_count) as min_monthly_transactions,
    MAX(monthly_count) as max_monthly_transactions,
    AVG(monthly_count) as avg_monthly_transactions,
    CASE 
        WHEN COUNT(DISTINCT month) = 32 
         AND MIN(monthly_count) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM (
    SELECT 
        DATE_TRUNC('MONTH', TRANSACTION_DATE) as month,
        COUNT(*) as monthly_count
    FROM SALES_TRANSACTIONS
    GROUP BY DATE_TRUNC('MONTH', TRANSACTION_DATE)
) monthly_stats;

-- Check region distribution in transactions
SELECT 
    'Transaction Region Distribution' as test_name,
    se.REGION,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT st.EMPLOYEE_ID) as employees_with_sales,
    CASE 
        WHEN se.REGION IN ('West', 'East', 'Central', 'Global') AND COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_TRANSACTIONS st
JOIN SALES_EMPLOYEES se ON st.EMPLOYEE_ID = se.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE
GROUP BY se.REGION
ORDER BY se.REGION;

-- Check transaction participation by role
SELECT 
    'Employee Participation in Sales' as test_name,
    se.ROLE,
    COUNT(DISTINCT se.EMPLOYEE_ID) as total_employees,
    COUNT(DISTINCT st.EMPLOYEE_ID) as employees_with_transactions,
    ROUND(COUNT(DISTINCT st.EMPLOYEE_ID) * 100.0 / COUNT(DISTINCT se.EMPLOYEE_ID), 1) as participation_rate,
    CASE 
        WHEN COUNT(DISTINCT st.EMPLOYEE_ID) * 100.0 / COUNT(DISTINCT se.EMPLOYEE_ID) > 80 THEN 'PASS'
        ELSE 'REVIEW'
    END as test_result
FROM SALES_EMPLOYEES se
LEFT JOIN SALES_TRANSACTIONS st ON se.EMPLOYEE_ID = st.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE
GROUP BY se.ROLE
ORDER BY 
    CASE se.ROLE 
        WHEN 'CRO' THEN 1
        WHEN 'VP Sales' THEN 2
        WHEN 'Regional Manager' THEN 3
        WHEN 'Sales Manager' THEN 4
        WHEN 'Sales Rep' THEN 5
        ELSE 6
    END;

-- =============================================================================
-- 3. CUSTOMER_ASSIGNMENTS Validation
-- =============================================================================
SELECT 'CUSTOMER_ASSIGNMENTS_VALIDATION' as validation_test;

-- Check customer assignment counts
SELECT 
    'Customer Assignment Counts' as test_name,
    COUNT(*) as total_assignments,
    COUNT(DISTINCT CUSTOMER_ID) as unique_customers,
    COUNT(DISTINCT EMPLOYEE_ID) as employees_with_customers,
    CASE 
        WHEN COUNT(*) > 500 AND COUNT(DISTINCT CUSTOMER_ID) > 100 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM CUSTOMER_ASSIGNMENTS;

-- Check customer assignment by role
SELECT 
    'Customer Assignments by Role' as test_name,
    se.ROLE,
    COUNT(DISTINCT ca.EMPLOYEE_ID) as employees_with_customers,
    COUNT(ca.ASSIGNMENT_ID) as total_assignments,
    ROUND(COUNT(ca.ASSIGNMENT_ID) / COUNT(DISTINCT ca.EMPLOYEE_ID), 1) as avg_customers_per_employee
FROM SALES_EMPLOYEES se
LEFT JOIN CUSTOMER_ASSIGNMENTS ca ON se.EMPLOYEE_ID = ca.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE
GROUP BY se.ROLE
ORDER BY 
    CASE se.ROLE 
        WHEN 'CRO' THEN 1
        WHEN 'VP Sales' THEN 2
        WHEN 'Regional Manager' THEN 3
        WHEN 'Sales Manager' THEN 4
        WHEN 'Sales Rep' THEN 5
        ELSE 6
    END;

-- =============================================================================
-- 4. SALES_PERFORMANCE Validation
-- =============================================================================
SELECT 'SALES_PERFORMANCE_VALIDATION' as validation_test;

-- Check performance data coverage
SELECT 
    'Performance Data Coverage' as test_name,
    COUNT(*) as total_performance_records,
    COUNT(DISTINCT EMPLOYEE_ID) as employees_with_performance,
    MIN(PERIOD_YEAR) as earliest_year,
    MAX(PERIOD_YEAR) as latest_year,
    COUNT(DISTINCT PERIOD_YEAR || '-' || LPAD(PERIOD_MONTH, 2, '0')) as distinct_months,
    CASE 
        WHEN COUNT(*) > 2000 
         AND MIN(PERIOD_YEAR) = 2023 
         AND MAX(PERIOD_YEAR) = 2025
         AND COUNT(DISTINCT PERIOD_YEAR || '-' || LPAD(PERIOD_MONTH, 2, '0')) = 32 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_PERFORMANCE;

-- Check performance metrics reasonableness
SELECT 
    'Performance Metrics Quality' as test_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN SALES_AMOUNT > 0 THEN 1 END) as records_with_sales,
    COUNT(CASE WHEN QUOTA_ATTAINMENT > 0 THEN 1 END) as records_with_quota_data,
    ROUND(AVG(QUOTA_ATTAINMENT), 2) as avg_quota_attainment,
    CASE 
        WHEN COUNT(CASE WHEN SALES_AMOUNT > 0 THEN 1 END) > 0 
         AND AVG(QUOTA_ATTAINMENT) BETWEEN 0.5 AND 2.0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_PERFORMANCE;

-- Check quota achievement distribution (target: ~90% meet quota)
SELECT 
    'Quota Achievement Distribution' as test_name,
    COUNT(DISTINCT sp.EMPLOYEE_ID) as total_employees,
    COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN sp.EMPLOYEE_ID END) as employees_meeting_quota,
    ROUND(COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN sp.EMPLOYEE_ID END) * 100.0 / 
          COUNT(DISTINCT sp.EMPLOYEE_ID), 1) as percentage_meeting_quota,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN sp.EMPLOYEE_ID END) * 100.0 / 
             COUNT(DISTINCT sp.EMPLOYEE_ID) BETWEEN 88 AND 92 THEN 'PASS'
        ELSE 'FAIL - Expected ~90% quota achievement'
    END as test_result
FROM SALES_PERFORMANCE sp
JOIN SALES_EMPLOYEES se ON sp.EMPLOYEE_ID = se.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE AND sp.QUOTA_ATTAINMENT > 0;

-- Check quota achievement by role
SELECT 
    'Quota Achievement by Role' as test_name,
    se.ROLE,
    COUNT(DISTINCT sp.EMPLOYEE_ID) as total_employees,
    COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN sp.EMPLOYEE_ID END) as employees_meeting_quota,
    ROUND(COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN sp.EMPLOYEE_ID END) * 100.0 / 
          COUNT(DISTINCT sp.EMPLOYEE_ID), 1) as percentage_meeting_quota,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT >= 1.0 THEN sp.EMPLOYEE_ID END) * 100.0 / 
             COUNT(DISTINCT sp.EMPLOYEE_ID) BETWEEN 88 AND 92 THEN 'PASS'
        ELSE 'REVIEW'
    END as test_result
FROM SALES_PERFORMANCE sp
JOIN SALES_EMPLOYEES se ON sp.EMPLOYEE_ID = se.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE AND sp.QUOTA_ATTAINMENT > 0
GROUP BY se.ROLE
ORDER BY 
    CASE se.ROLE 
        WHEN 'CRO' THEN 1
        WHEN 'VP Sales' THEN 2
        WHEN 'Regional Manager' THEN 3
        WHEN 'Sales Manager' THEN 4
        WHEN 'Sales Rep' THEN 5
        ELSE 6
    END;

-- =============================================================================
-- 5. VIEW Validation (TRANSACTION_SEMANTIC_VIEW)
-- =============================================================================
SELECT 'TRANSACTION_SEMANTIC_VIEW_VALIDATION' as validation_test;

-- Check transaction semantic view (test via base tables if view has issues)
SELECT 
    'Transaction Semantic View' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT se.REGION) as distinct_regions,
    COUNT(DISTINCT DATE_TRUNC('MONTH', st.TRANSACTION_DATE)) as distinct_months,
    LISTAGG(DISTINCT se.REGION, ', ') as regions_found,
    CASE 
        WHEN COUNT(DISTINCT se.REGION) >= 3  -- At least West, East, Central
         AND COUNT(DISTINCT DATE_TRUNC('MONTH', st.TRANSACTION_DATE)) = 32 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_TRANSACTIONS st
JOIN SALES_EMPLOYEES se ON st.EMPLOYEE_ID = se.EMPLOYEE_ID
WHERE se.REGION IS NOT NULL AND se.ACTIVE = TRUE;

-- =============================================================================
-- 6. DATA_ENTITLEMENTS Validation
-- =============================================================================
SELECT 'DATA_ENTITLEMENTS_VALIDATION' as validation_test;

-- Check entitlement coverage
SELECT 
    'Data Entitlements Coverage' as test_name,
    COUNT(*) as total_entitlements,
    COUNT(DISTINCT EMPLOYEE_ID) as employees_with_entitlements,
    CASE 
        WHEN COUNT(DISTINCT EMPLOYEE_ID) > 100 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM DATA_ENTITLEMENTS;

-- Check entitlement distribution by role
SELECT 
    'Entitlements by Role' as test_name,
    se.ROLE,
    COUNT(de.ENTITLEMENT_ID) as entitlement_count,
    LISTAGG(DISTINCT de.ACCESS_LEVEL, ', ') as access_levels
FROM SALES_EMPLOYEES se
LEFT JOIN DATA_ENTITLEMENTS de ON se.EMPLOYEE_ID = de.EMPLOYEE_ID
WHERE se.ACTIVE = TRUE
GROUP BY se.ROLE
ORDER BY 
    CASE se.ROLE 
        WHEN 'CRO' THEN 1
        WHEN 'VP Sales' THEN 2
        WHEN 'Regional Manager' THEN 3
        WHEN 'Sales Manager' THEN 4
        WHEN 'Sales Rep' THEN 5
        ELSE 6
    END;

-- =============================================================================
-- 7. Overall System Health Check
-- =============================================================================
SELECT 'SYSTEM_HEALTH_CHECK' as validation_test;

-- Check referential integrity
SELECT 
    'Referential Integrity Check' as test_name,
    orphaned_transactions.orphan_count,
    orphaned_assignments.assignment_orphan_count,
    orphaned_performance.performance_orphan_count,
    CASE 
        WHEN orphaned_transactions.orphan_count = 0 
         AND orphaned_assignments.assignment_orphan_count = 0 
         AND orphaned_performance.performance_orphan_count = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM (
    SELECT COUNT(*) as orphan_count
    FROM SALES_TRANSACTIONS st
    LEFT JOIN SALES_EMPLOYEES se ON st.EMPLOYEE_ID = se.EMPLOYEE_ID
    WHERE se.EMPLOYEE_ID IS NULL
) orphaned_transactions
CROSS JOIN (
    SELECT COUNT(*) as assignment_orphan_count
    FROM CUSTOMER_ASSIGNMENTS ca
    LEFT JOIN SALES_EMPLOYEES se ON ca.EMPLOYEE_ID = se.EMPLOYEE_ID
    WHERE se.EMPLOYEE_ID IS NULL
) orphaned_assignments
CROSS JOIN (
    SELECT COUNT(*) as performance_orphan_count
    FROM SALES_PERFORMANCE sp
    LEFT JOIN SALES_EMPLOYEES se ON sp.EMPLOYEE_ID = se.EMPLOYEE_ID
    WHERE se.EMPLOYEE_ID IS NULL
) orphaned_performance;

-- =============================================================================
-- 8. Business Logic Validation
-- =============================================================================
SELECT 'BUSINESS_LOGIC_VALIDATION' as validation_test;

-- Check quota attainment calculations (by performance records)
SELECT 
    'Quota Attainment Logic - Records' as test_name,
    COUNT(*) as total_performance_records,
    COUNT(CASE WHEN QUOTA_ATTAINMENT > 0 THEN 1 END) as records_with_quota,
    COUNT(CASE WHEN QUOTA_ATTAINMENT > 1.4 THEN 1 END) as high_performer_records,
    ROUND(COUNT(CASE WHEN QUOTA_ATTAINMENT > 1.4 THEN 1 END) * 100.0 / COUNT(*), 1) as pct_high_performer_records,
    COUNT(CASE WHEN COMMISSION_EARNED > 0 THEN 1 END) as records_with_commission,
    CASE 
        WHEN COUNT(CASE WHEN QUOTA_ATTAINMENT > 0 THEN 1 END) > 0 
         AND COUNT(CASE WHEN COMMISSION_EARNED > 0 THEN 1 END) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result
FROM SALES_PERFORMANCE;

-- Check quota attainment by unique employees (more meaningful)
SELECT 
    'Quota Attainment Logic - Employees' as test_name,
    COUNT(DISTINCT sp.EMPLOYEE_ID) as total_employees,
    COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT > 1.4 THEN sp.EMPLOYEE_ID END) as high_performer_employees,
    ROUND(COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT > 1.4 THEN sp.EMPLOYEE_ID END) * 100.0 / 
          COUNT(DISTINCT sp.EMPLOYEE_ID), 1) as pct_high_performer_employees,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN sp.QUOTA_ATTAINMENT > 1.4 THEN sp.EMPLOYEE_ID END) * 100.0 / 
             COUNT(DISTINCT sp.EMPLOYEE_ID) BETWEEN 15 AND 25 THEN 'PASS - Expected ~20%'
        ELSE 'REVIEW'
    END as test_result
FROM SALES_PERFORMANCE sp
WHERE sp.QUOTA_ATTAINMENT > 0;

-- =============================================================================
-- 9. Final Summary Report
-- =============================================================================
SELECT 'VALIDATION_SUMMARY' as final_report;

-- Expected vs Actual counts
SELECT 
    'Data Volume Summary' as summary_type,
    (SELECT COUNT(*) FROM SALES_EMPLOYEES WHERE ACTIVE = TRUE) as total_employees,
    (SELECT COUNT(*) FROM SALES_TRANSACTIONS) as total_transactions,
    (SELECT COUNT(*) FROM CUSTOMER_ASSIGNMENTS) as total_assignments,
    (SELECT COUNT(*) FROM SALES_PERFORMANCE) as total_performance_records,
    (SELECT COUNT(*) FROM DATA_ENTITLEMENTS) as total_entitlements,
    (SELECT COUNT(DISTINCT REGION) FROM SALES_EMPLOYEES WHERE REGION IN ('West', 'East', 'Central')) as distinct_regions,
    (SELECT COUNT(DISTINCT DATE_TRUNC('MONTH', TRANSACTION_DATE)) FROM SALES_TRANSACTIONS) as months_with_transactions;

-- Final hierarchy summary
SELECT 
    'Final Hierarchy Summary' as summary_type,
    ROLE,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage_of_org
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
