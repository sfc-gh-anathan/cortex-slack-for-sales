# Sales Hierarchy and Compensation System

This system provides a comprehensive sales data structure with manager hierarchies, performance tracking, and role-based data access controls.

## Database Structure

### Core Tables

#### 1. SALES_EMPLOYEES
Master table for all sales employees with hierarchy and compensation details.
- **Employee information**: ID, name, email, role, territory, region
- **Compensation**: Base salary, commission rate, annual quota
- **Hierarchy**: Manager relationships via self-referencing MANAGER_ID
- **Roles**: VP Sales → Regional Manager → Sales Manager → Sales Rep

#### 2. SALES_PERFORMANCE  
Monthly performance metrics for each employee.
- **Sales metrics**: Sales amount, units sold, new customers, deals closed
- **Compensation**: Commission earned, bonuses, total compensation
- **Performance**: Quota attainment, team/region rankings
- **Time periods**: Year, month, quarter tracking

#### 3. CUSTOMER_ASSIGNMENTS
Links customers to their assigned sales representatives.
- **Assignment tracking**: Which sales rep manages each customer
- **Primary ownership**: Distinguishes primary vs support roles
- **Assignment dates**: When customer relationships began

#### 4. SALES_TRANSACTIONS
Enhanced transaction table linking sales to employees.
- **Transaction details**: Amount, products, dates
- **Sales attribution**: Which employee gets credit
- **Commission tracking**: Commission amounts per transaction
- **Deal classification**: New business, upsell, renewal

#### 5. DATA_ENTITLEMENTS
Role-based access control for sensitive data.
- **Access levels**: SELF_ONLY, DIRECT_REPORTS, TEAM, REGION, ALL
- **Permissions**: Compensation view, performance view, customer data
- **Time-based**: Effective and expiry dates for access

### Security Views

#### MANAGER_HIERARCHY
Recursive view showing complete organizational structure.
- **Full hierarchy**: From VP Sales down to individual reps
- **Path tracking**: Complete reporting chain for each employee
- **Level indicators**: Organizational depth (1=top, 4=sales rep)

#### ACCESSIBLE_DATA
Security-filtered view respecting data entitlements.
- **Role-based filtering**: Shows only data user is entitled to see
- **Compensation protection**: Hides salary/commission for unauthorized users
- **Hierarchy enforcement**: Managers see direct reports, reps see only themselves

## Sample Data Overview

### Organizational Structure
```
VP Sales (Sarah Johnson)
├── Regional Manager West (Michael Chen)
│   ├── Sales Manager CA (Lisa Anderson)
│   │   ├── Sales Rep SF (Jessica Miller)
│   │   ├── Sales Rep LA (Ryan Moore)  
│   │   └── Sales Rep SD (Emily Jackson)
│   └── Sales Manager PNW (David Martinez)
│       ├── Sales Rep Seattle (Kevin White)
│       └── Sales Rep Portland (Nicole Harris)
├── Regional Manager East (Jennifer Davis)
│   ├── Sales Manager NY (Amanda Taylor)
│   │   ├── Sales Rep Manhattan (Daniel Martin)
│   │   ├── Sales Rep Brooklyn (Stephanie Thompson)
│   │   └── Sales Rep Queens (Mark Garcia)
│   └── Sales Manager FL (James Brown)
│       ├── Sales Rep Miami (Rachel Martinez)
│       └── Sales Rep Tampa (Andrew Robinson)
└── Regional Manager Central (Robert Wilson)
    ├── Sales Manager TX (Michelle Garcia)
    │   ├── Sales Rep Houston (Lauren Clark)
    │   └── Sales Rep Dallas (Tyler Rodriguez)
    └── Sales Manager IL (Christopher Lee)
        ├── Sales Rep Chicago (Megan Lewis)
        └── Sales Rep Springfield (Brandon Walker)
```

### Access Control Examples
- **Sales Reps**: Can only see their own performance and compensation
- **Sales Managers**: Can see direct reports' data including compensation
- **Regional Managers**: Can see all data within their region
- **VP Sales**: Can see all company data

### Key Compensation Metrics
- **Base Salary**: Fixed annual salary ($58K-$180K range)
- **Commission Rate**: 1%-3% of sales (higher rates for individual contributors)
- **Annual Quotas**: $320K-$5M depending on role and territory
- **Commission Earned**: Monthly variable pay based on sales performance
- **Bonus Earned**: Additional rewards for exceeding quota (>100% attainment)
- **Quota Attainment**: Performance vs target (realistic 80%-130% range)

## Setup Instructions

1. **Create Tables**: Run `sales_hierarchy_setup.sql` to create the table structure
2. **Load Sample Data**: Run `sample_sales_data.sql` to populate with realistic data
3. **Update Semantic Model**: Use `enhanced_retail_sales_data.yaml` for Cortex integration
4. **Test Queries**: Verify hierarchy and access controls work as expected

## Example Queries

### Manager's Direct Reports Performance
```sql
SELECT 
    se.first_name || ' ' || se.last_name as employee_name,
    SUM(sp.sales_amount) as total_sales,
    AVG(sp.quota_attainment) as avg_quota_attainment,
    SUM(sp.commission_earned) as total_commission
FROM sales_performance sp
JOIN sales_employees se ON sp.employee_id = se.employee_id  
WHERE se.manager_id = 'EMP005' -- Lisa Anderson's team
    AND sp.period_year = 2023
GROUP BY se.employee_id, se.first_name, se.last_name
ORDER BY total_sales DESC;
```

### Regional Performance Summary
```sql
SELECT 
    se.region,
    COUNT(DISTINCT se.employee_id) as team_size,
    SUM(sp.sales_amount) as total_regional_sales,
    AVG(sp.quota_attainment) as avg_quota_attainment
FROM sales_performance sp
JOIN sales_employees se ON sp.employee_id = se.employee_id
WHERE sp.period_year = 2023
GROUP BY se.region
ORDER BY total_regional_sales DESC;
```

### Individual Compensation Summary
```sql
SELECT 
    se.first_name || ' ' || se.last_name as employee_name,
    se.role,
    se.base_salary,
    SUM(sp.commission_earned) as annual_commission,
    SUM(sp.bonus_earned) as annual_bonus,
    se.base_salary + SUM(sp.commission_earned) + SUM(sp.bonus_earned) as total_annual_comp
FROM sales_employees se
JOIN sales_performance sp ON se.employee_id = sp.employee_id
WHERE sp.period_year = 2023
    AND se.employee_id = 'EMP011' -- Jessica Miller
GROUP BY se.employee_id, se.first_name, se.last_name, se.role, se.base_salary;
```

This structure provides exactly what you requested: a sales hierarchy with multiple management levels, comprehensive compensation tracking, role-based data access controls, and the ability for managers to see their direct reports' performance data.