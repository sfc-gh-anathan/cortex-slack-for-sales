# Sales Data Model Documentation

This document provides comprehensive documentation of the sales data model used in the Cortex Slack for Sales application. The data model simulates a realistic sales organization with hierarchical structures, performance tracking, and transaction details.

## Overview

The data model consists of multiple interconnected tables that represent:
- Sales employee hierarchy and organizational structure
- Monthly performance metrics and quota tracking
- Individual transaction details
- Customer assignments and territory management
- Data access entitlements and security

## Database Structure

### Database: SLACK_SALES_DEMO
### Schema: SLACK_SCHEMA
### Warehouse: SLACK_S

## Core Tables

### 1. SALES_EMPLOYEES
The master table containing all sales employees and their organizational information.

**Purpose**: Stores employee details, compensation structure, and hierarchical relationships.

**Key Fields**:
- `EMPLOYEE_ID` (VARCHAR(20), Primary Key): Unique identifier for each employee
- `FIRST_NAME`, `LAST_NAME` (VARCHAR(50)): Employee names
- `EMAIL` (VARCHAR(100), Unique): Employee email address
- `ROLE` (VARCHAR(30)): Job role (CRO, VP Sales, Regional Manager, Sales Manager, Sales Rep)
- `TERRITORY` (VARCHAR(50)): Geographic territory assignment
- `REGION` (VARCHAR(50)): Broader geographic region (West, East, Central)
- `HIRE_DATE` (DATE): Employee start date
- `BASE_SALARY` (DECIMAL(10,2)): Annual base salary
- `COMMISSION_RATE` (DECIMAL(5,4)): Commission percentage (e.g., 0.0250 = 2.5%)
- `QUOTA_AMOUNT` (DECIMAL(12,2)): Annual sales quota
- `MANAGER_ID` (VARCHAR(20)): References EMPLOYEE_ID of direct manager
- `ACTIVE` (BOOLEAN): Whether employee is currently active

**Hierarchy Structure**:
1. **CRO** (Chief Revenue Officer) - Top level, no manager
2. **VP Sales** - Reports to CRO, manages regions
3. **Regional Manager** - Reports to VP Sales, manages territories
4. **Sales Manager** - Reports to Regional Manager, manages teams
5. **Sales Rep** - Reports to Sales Manager, individual contributors

**Sample Organizational Chart**:
```
CRO (Sarah Johnson)
├── VP Sales West (Michael Chen)
│   ├── Regional Manager West Coast (Robert Wilson)
│   │   ├── Sales Manager CA North (Patricia Kim)
│   │   │   ├── Sales Rep SF (Addison Wells)
│   │   │   ├── Sales Rep Oakland (Alex Thompson)
│   │   │   └── Sales Rep San Jose (Jordan Mitchell)
│   │   └── Sales Manager CA South (William Zhang)
│   │       ├── Sales Rep LA (Casey Parker)
│   │       ├── Sales Rep San Diego (Morgan Bennett)
│   │       └── Sales Rep Orange County (Riley Cooper)
│   └── Regional Manager Pacific NW (Lisa Anderson)
│       ├── Sales Manager Seattle (Maria Rodriguez)
│       │   ├── Sales Rep Seattle Downtown (Avery Reed)
│       │   └── Sales Rep Seattle Tech (Quinn Bailey)
│       └── Sales Manager Portland (Kevin White)
│           ├── Sales Rep Portland (Sage Rivera)
│           └── Sales Rep Eugene (Blake Torres)
└── VP Sales East/Central (Jennifer Davis)
    ├── Regional Manager East Coast (Amanda Taylor)
    │   ├── Sales Manager New York (Daniel Martin)
    │   │   ├── Sales Rep Manhattan (Drew Ward)
    │   │   ├── Sales Rep Brooklyn (Emery Brooks)
    │   │   └── Sales Rep Queens (Finley Gray)
    │   └── Sales Manager Boston (Stephanie Thompson)
    │       ├── Sales Rep Boston (Hayden Watson)
    │       └── Sales Rep Cambridge (Jamie Kelly)
    └── Regional Manager Central (Michelle Garcia)
        ├── Sales Manager Texas (Andrew Robinson)
        │   ├── Sales Rep Houston (Kai Sanders)
        │   └── Sales Rep Dallas (Lane Price)
        └── Sales Manager Illinois (Lauren Clark)
            ├── Sales Rep Chicago (Max Bennett)
            └── Sales Rep Springfield (Nova Wood)
```

### 2. SALES_PERFORMANCE
Monthly performance metrics for each employee.

**Purpose**: Tracks monthly sales performance, quota attainment, and compensation.

**Key Fields**:
- `PERFORMANCE_ID` (VARCHAR(30), Primary Key): Unique performance record identifier
- `EMPLOYEE_ID` (VARCHAR(20)): Links to SALES_EMPLOYEES
- `PERIOD_YEAR`, `PERIOD_MONTH`, `PERIOD_QUARTER` (INTEGER): Time period identifiers
- `SALES_AMOUNT` (DECIMAL(12,2)): Total sales for the month
- `UNITS_SOLD` (INTEGER): Number of units/licenses sold
- `NEW_CUSTOMERS` (INTEGER): New customers acquired
- `DEALS_CLOSED` (INTEGER): Number of deals closed
- `QUOTA_ATTAINMENT` (DECIMAL(8,4)): Percentage of monthly quota achieved
- `COMMISSION_EARNED` (DECIMAL(10,2)): Commission earned for the month
- `BONUS_EARNED` (DECIMAL(10,2)): Bonus earned for the month
- `TOTAL_COMPENSATION` (DECIMAL(12,2)): Total variable compensation
- `RANK_IN_TEAM`, `RANK_IN_REGION` (INTEGER): Performance rankings

### 3. SALES_TRANSACTIONS
Individual transaction records with detailed deal information.

**Purpose**: Stores individual sales transactions with product and customer details.

**Key Fields**:
- `TRANSACTION_ID` (VARCHAR(30), Primary Key): Unique transaction identifier
- `EMPLOYEE_ID` (VARCHAR(20)): Sales rep who made the sale
- `CUSTOMER_ID` (VARCHAR(20)): Customer identifier
- `TRANSACTION_DATE` (DATE): Date of the transaction
- `PRODUCT_CATEGORY` (VARCHAR(50)): Snowflake product category
- `QUANTITY` (INTEGER): Number of seats/licenses sold
- `UNIT_PRICE` (DECIMAL(10,2)): Price per seat/license
- `TOTAL_AMOUNT` (DECIMAL(12,2)): Total transaction value
- `COMMISSION_AMOUNT` (DECIMAL(10,2)): Commission on this transaction
- `DEAL_TYPE` (VARCHAR(20)): Type of deal (New Business, Upsell, Renewal)

### 4. CUSTOMER_ASSIGNMENTS
Tracks which customers are assigned to which sales representatives.

**Purpose**: Manages customer-to-salesperson assignments and territories.

**Key Fields**:
- `ASSIGNMENT_ID` (VARCHAR(30), Primary Key): Unique assignment identifier
- `EMPLOYEE_ID` (VARCHAR(20)): Assigned sales representative
- `CUSTOMER_ID` (VARCHAR(20)): Customer identifier
- `ASSIGNED_DATE` (DATE): Date of assignment
- `IS_PRIMARY` (BOOLEAN): Whether this is the primary account owner

### 5. DATA_ENTITLEMENTS
Controls data access permissions for each employee.

**Purpose**: Manages what data each user can access based on their role and level.

**Key Fields**:
- `ENTITLEMENT_ID` (VARCHAR(30), Primary Key): Unique entitlement identifier
- `EMPLOYEE_ID` (VARCHAR(20)): Employee the entitlement applies to
- `ACCESS_LEVEL` (VARCHAR(20)): Level of access (SELF_ONLY, DIRECT_REPORTS, TEAM, REGION, ALL)
- `CAN_VIEW_COMPENSATION` (BOOLEAN): Permission to view compensation data
- `CAN_VIEW_INDIVIDUAL_PERFORMANCE` (BOOLEAN): Permission to view individual metrics
- `CAN_VIEW_TEAM_PERFORMANCE` (BOOLEAN): Permission to view team metrics
- `CAN_VIEW_CUSTOMER_DATA` (BOOLEAN): Permission to view customer information

## Views and Semantic Models

### SALES_SEMANTIC_VIEW
A comprehensive view that combines employee, performance, and hierarchy data for analytics.

**Purpose**: Primary view for sales performance analysis and reporting.

**Key Features**:
- Combines data from SALES_PERFORMANCE and SALES_EMPLOYEES
- Includes calculated fields for quota performance bands
- Provides hierarchy information and manager details
- Includes tenure calculations and role categorizations

**Important Calculated Fields**:
- `FULL_NAME`: Concatenated first and last name
- `MONTHLY_QUOTA`: Annual quota divided by 12
- `QUOTA_PERFORMANCE_BAND`: Performance categorization (Below, Approaching, Meets, Exceeds)
- `ROLE_CATEGORY`: Grouped roles (Individual Contributor, Manager, Executive)
- `TENURE_CATEGORY`: Experience level (New Hire, Developing, Experienced)
- `PERIOD_DATE`: First day of the performance month

### TRANSACTION_SEMANTIC_VIEW
A view that provides transaction-level data with employee and hierarchy context.

**Purpose**: Detailed transaction analysis with organizational context.

**Key Features**:
- Individual transaction records with employee information
- Product category and deal type analysis
- Customer segment classification
- Sales cycle tracking (days since assignment)

**Important Calculated Fields**:
- `EMPLOYEE_NAME`: Full name of the sales representative
- `DEAL_SIZE_CATEGORY`: Deal size grouping (Small, Medium, Large Deal)
- `CUSTOMER_SEGMENT`: Customer classification (SMB, Mid-Market, Enterprise)
- `DAYS_SINCE_ASSIGNMENT`: Sales cycle length calculation

### MANAGER_HIERARCHY
A recursive view showing the complete organizational structure.

**Purpose**: Provides hierarchical relationships for reporting and access control.

**Key Features**:
- Recursive CTE showing full reporting chains
- Level indicators for hierarchy depth
- Path tracking showing reporting relationships

## Data Ranges and Coverage

### Time Periods
- **Historical Data**: January 2023 - August 2025
- **Primary Analysis Period**: 2024 (most complete data)
- **Monthly Granularity**: Performance data tracked monthly
- **Transaction Level**: Daily transaction records

### Organizational Scale
- **Total Employees**: Approximately 150+ sales professionals
- **Hierarchy Levels**: 5 levels from CRO to Sales Rep
- **Geographic Coverage**: West, East, and Central regions
- **Territories**: Multiple territories within each region

### Product Categories
The data includes realistic Snowflake product categories:
- Data Warehouse
- Cortex AI
- Analytics Workbench
- Data Engineering
- Data Governance
- Data Sharing
- Developer Tools
- Data Lake
- Marketplace

## Key Metrics and KPIs

### Performance Metrics
- **Sales Amount**: Monthly and annual sales figures
- **Quota Attainment**: Percentage of quota achieved
- **Units Sold**: Number of licenses/seats sold
- **New Customers**: Customer acquisition metrics
- **Deals Closed**: Transaction count metrics

### Compensation Metrics
- **Base Salary**: Annual fixed compensation ($58K-$250K range depending on role)
- **Commission Rate**: 1%-3% of sales (higher rates for individual contributors)
- **Annual Quotas**: $320K-$45M depending on role and territory
- **Commission Earned**: Monthly variable pay based on sales performance
- **Bonus Earned**: Additional rewards for exceeding quota (>100% attainment)
- **Total Compensation**: Combined variable compensation (commission + bonus)
- **Quota Attainment**: Performance vs target (realistic 80%-130% range with seasonal variation)

### Ranking Metrics
- **Rank in Team**: Performance ranking within immediate team
- **Rank in Region**: Performance ranking within broader region

## Security and Access Control

### Entitlement-Based Filtering
The application implements sophisticated row-level security:

**Access Levels**:
- `SELF_ONLY`: Individual can only see their own data
- `DIRECT_REPORTS`: Manager can see direct reports' data
- `TEAM`: Access to team-level data
- `REGION`: Access to regional data
- `ALL`: Full access to all data (CRO level)

**Implementation**:
- Recursive CTE traverses manager-subordinate relationships
- Real-time filtering applied to all queries
- Hierarchical access based on organizational structure

### Sample Access Patterns
- **CRO (Sarah Johnson)**: Can see all data across the organization
- **VP Sales (Michael Chen, Jennifer Davis)**: Can see their region and all subordinates
- **Regional Manager (Robert Wilson, Lisa Anderson, etc.)**: Can see their territory and direct reports
- **Sales Manager (Patricia Kim, William Zhang, etc.)**: Can see their team members' data including compensation
- **Sales Rep (Addison Wells, Alex Thompson, etc.)**: Can only see their own performance and compensation data

### Access Control Examples by Role
- **Sales Reps**: Access level `SELF_ONLY` - can view only personal performance, compensation, and assigned customers
- **Sales Managers**: Access level `DIRECT_REPORTS` - can see direct reports' data including compensation and performance metrics
- **Regional Managers**: Access level `REGION` - can see all data within their geographic region
- **VP Sales**: Access level `ALL` - can see company-wide data with full compensation visibility
- **CRO**: Access level `ALL` - complete organizational visibility and administrative access

## Data Quality and Characteristics

### Realistic Data Patterns
- **Seasonal Variations**: Sales performance varies by month and quarter
- **Hierarchy Consistency**: Proper manager-subordinate relationships
- **Performance Distribution**: Realistic quota attainment patterns
- **Territory Balance**: Appropriate geographic distribution

### Data Relationships
- **One-to-Many**: Employee to Performance records (monthly)
- **One-to-Many**: Employee to Transactions (multiple per month)
- **Many-to-One**: Employees to Managers (hierarchy)
- **One-to-Many**: Employee to Customer Assignments

## Usage Guidelines

### Counting Employees vs Records
When analyzing "salespeople" or "employees":
- Always use `COUNT(DISTINCT employee_id)` for unique individuals
- Use `GROUP BY employee_id` first, then aggregate monthly data
- For employee-level analysis, use `AVG()` or `SUM()` on monthly records

### Common Analysis Patterns
- **Quota Analysis**: Use `quota_attainment` and `quota_performance_band`
- **Manager Rollups**: Use hierarchy fields (`manager_name`, `role_category`)
- **Product Analysis**: Use `product_category` from transaction view
- **Territory Analysis**: Use `territory` and `region` dimensions
- **Time Analysis**: Use `period_date`, `period_year`, `period_month`

### Performance Considerations
- Views are optimized for analytical queries
- Indexes support hierarchy traversal
- Monthly aggregation reduces data volume for performance analysis
- Transaction view provides detailed drill-down capability

## Example Queries

### Top Performers by Region
```sql
SELECT region, full_name, SUM(sales_amount) as total_sales
FROM sales_semantic_view 
WHERE period_year = 2024 AND role = 'Sales Rep'
GROUP BY region, full_name, employee_id
ORDER BY region, total_sales DESC;
```

### Quota Attainment by Manager
```sql
SELECT manager_name, 
       AVG(quota_attainment) as avg_quota_attainment,
       COUNT(DISTINCT employee_id) as team_size
FROM sales_semantic_view 
WHERE period_year = 2024 AND manager_name IS NOT NULL
GROUP BY manager_name
ORDER BY avg_quota_attainment DESC;
```

### Product Performance Analysis
```sql
SELECT product_category, 
       AVG(total_amount) as avg_deal_value,
       COUNT(*) as total_deals,
       SUM(total_amount) as total_revenue
FROM transaction_semantic_view 
WHERE transaction_year = 2024
GROUP BY product_category
ORDER BY total_revenue DESC;
```

## Maintenance and Updates

### Data Refresh
- Performance data is typically updated monthly
- Transaction data is updated in real-time
- Employee hierarchy changes are reflected immediately
- Entitlements are updated as roles change

### Data Validation
Use the validation scripts in `sql/91_data_validation.sql` to verify:
- Data consistency across tables
- Hierarchy integrity
- Performance calculations
- Date range coverage

## Integration with Cortex AI

The semantic model (`sales_semantic_model.yaml`) defines:
- Table relationships and joins
- Field descriptions and synonyms
- Verified queries for common use cases
- Custom instructions for AI analysis

This enables natural language queries like:
- "Who are the top sales managers this quarter?"
- "What's the average quota attainment in the West region?"
- "Show me sales trends for Cortex AI products"
- "Which territories have the highest deal values?"

The semantic model ensures accurate SQL generation and provides context for AI-powered analysis through the Slack bot interface.
