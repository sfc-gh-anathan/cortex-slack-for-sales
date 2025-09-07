# Cortex Slack for Sales

A comprehensive Slack bot application that integrates Snowflake Cortex AI with Slack to provide intelligent sales analytics through natural language queries. This application demonstrates advanced data security with entitlement-based filtering, AI-powered chart generation, and interactive data exploration.

## Features

- **Natural Language Queries**: Ask questions about sales data in plain English
- **AI-Powered Charts**: Automatically generate intelligent visualizations from your data
- **Entitlement-Based Security**: Row-level security based on organizational hierarchy
- **Interactive Data Filtering**: Apply filters and explore data dynamically
- **Real-time Analytics**: Get instant insights with streaming responses
- **Document Search**: Search through sales guides and documentation using Cortex Search
- **Multi-format Export**: Download data in various formats

## Architecture

The application consists of several key components:

- **Slack Bot Interface** (`app.py`): Main application handling Slack interactions
- **Cortex Integration** (`cortex_chat.py`): Interface with Snowflake Cortex AI services
- **Chart Generation** (`charter.py`): AI-powered visualization creation
- **Data Filtering** (`data_filter_modal.py`): Interactive data filtering capabilities
- **Security Layer**: Hierarchical access control based on user roles

## Prerequisites

- Python 3.8+
- Snowflake account with Cortex AI enabled
- Slack workspace with bot permissions
- RSA key pair for Snowflake authentication

## Quick Start

### 1. Environment Setup

1. Clone the repository:
```bash
git clone https://github.com/sfc-gh-anathan/cortex-slack-for-sales.git
cd cortex-slack-for-sales
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create your environment file by copying and editing the template:
```bash
cp fill-in-the-env.txt .env
```

4. Configure your `.env` file with your Snowflake and Slack credentials:
```bash
# Snowflake Configuration
ACCOUNT='YOUR-ACCOUNT-NAME'
HOST='YOUR-ACCOUNT.snowflakecomputing.com'
AGENT_ENDPOINT='https://YOUR-ACCOUNT.snowflakecomputing.com/api/v2/cortex/agent:run'
DEMO_USER='YOUR_USERNAME'
DEMO_USER_ROLE='YOUR_ROLE'

# Slack Configuration
SLACK_BOT_TOKEN='xoxb-your-bot-token'
SLACK_APP_TOKEN='xapp-your-app-token'

# Database Configuration (typically don't change these)
DEMO_DATABASE='SLACK_SALES_DEMO'
DEMO_SCHEMA='SLACK_SCHEMA'
WAREHOUSE='SLACK_S'
SEMANTIC_MODEL='@SLACK_SALES_DEMO.SLACK_SCHEMA.SLACK_SEMANTIC_MODELS/sales_semantic_model.yaml'
SEARCH_SERVICE='SLACK_SALES_DEMO.SLACK_SCHEMA.info_search'
RSA_PRIVATE_KEY_PATH='rsa_key.p8'
MODEL='claude-4-sonnet'
```

### 2. Database Setup

Run the SQL setup scripts in order:

```bash
# 1. Basic setup
snowsql -f sql/01_setup.sql

# 2. Sales hierarchy setup
snowsql -f sql/02_sales_hierarchy_setup.sql

# 3. Sample data
snowsql -f sql/03_sample_sales_data.sql

# 4. Cortex search service
snowsql -f sql/04_cortex_search_service.sql

# 5. Semantic views
snowsql -f sql/05_sales_semantic_views.sql

# 6. Query refinement procedure
snowsql -f sql/06_refine_query_procedure.sql
```

### 3. Slack App Configuration

1. Create a new Slack app at [api.slack.com](https://api.slack.com/apps)
2. Use the provided `manifest.json` to configure your app
3. Install the app to your workspace
4. Copy the Bot Token and App Token to your `.env` file

### 4. Upload Required Files

Upload the semantic model and documentation:

```sql
-- Upload semantic model
PUT file://sales_semantic_model.yaml @SLACK_SEMANTIC_MODELS AUTO_COMPRESS=FALSE;

-- Upload sales guide
PUT file://data/Sales\ Guide\ 2025.pdf @SLACK_PDFS AUTO_COMPRESS=FALSE;
```

### 5. Run the Application

```bash
python app.py
```

## User Roles & Access Levels

The application demonstrates hierarchical access control with the following roles:

1. **CRO** (`sarah.johnson@company.com`) - Full access to all data
2. **VP Sales** (`michael.chen@company.com`) - West region access
3. **Regional Manager** (`robert.wilson@company.com`) - West Coast region
4. **Sales Manager** (`patricia.kim@company.com`) - California North team
5. **Sales Rep** (`addison.wells@company.com`) - Individual access only

Change the `CURRENT_USER_EMAIL` in `app.py` to test different access levels.

## Usage Examples

Once the bot is running in your Slack workspace, you can ask questions like:

- "What were the sales trends in 2024?"
- "Show me top performers in the West region"
- "What's the quota attainment for Q4?"
- "Generate a chart of monthly sales by region"
- "How are commissions calculated?"

## Configuration Options

### Chart Generation
The application uses AI-powered chart generation. You can customize chart types and styling in `charter.py`.

### Data Filtering
Interactive filtering is available through the filter modal. Users can apply filters on:
- Date ranges
- Regions
- Sales representatives
- Product categories
- Performance metrics

### Security Settings
Modify entitlement rules in the SQL filtering logic within `app.py`. The current implementation uses:
- Recursive CTE for hierarchy traversal
- Employee-based access control
- Manager-subordinate relationships

## Project Structure

```
cortex-slack-for-sales/
├── app.py                      # Main Slack bot application
├── cortex_chat.py             # Cortex AI integration
├── charter.py                 # AI-powered chart generation
├── data_filter_modal.py       # Interactive data filtering
├── generate_jwt.py            # JWT token generation utility
├── manifest.json              # Slack app configuration
├── sales_semantic_model.yaml  # Semantic model definition
├── requirements.txt           # Python dependencies
├── sql/                       # Database setup scripts
│   ├── 01_setup.sql
│   ├── 02_sales_hierarchy_setup.sql
│   ├── 03_sample_sales_data.sql
│   ├── 04_cortex_search_service.sql
│   ├── 05_sales_semantic_views.sql
│   ├── 06_refine_query_procedure.sql
│   ├── 91_data_validation.sql
│   └── 99_cleanup.sql
└── data/
    └── Sales Guide 2025.pdf   # Documentation for search
```

## Key Features Explained

### Entitlement-Based Filtering
The application implements sophisticated row-level security:
- Users only see data they're entitled to based on organizational hierarchy
- Recursive queries traverse manager-subordinate relationships
- Real-time filtering applied to all queries

### AI Chart Generation
Charts are generated using:
- Intelligent data type detection
- Context-aware visualization selection
- Interactive Plotly charts
- Automatic styling and formatting

### Query Refinement
The system provides:
- Automated query analysis
- Suggestion prompts for better queries
- Context-aware refinements
- Background analysis for optimization

## Development

### Adding New Features
1. Extend the Slack event handlers in `app.py`
2. Add new chart types in `charter.py`
3. Implement additional filters in `data_filter_modal.py`
4. Update the semantic model as needed

### Testing
Use the validation script to test your setup:
```bash
python test-slack-connection.py
```

Run data validation queries:
```sql
-- Validate setup
@sql/91_data_validation.sql
```

### Debugging
Enable debug mode by setting `DEBUG = True` in `app.py` for verbose logging.

## Production Considerations

- Replace in-memory caching with Redis or database storage
- Implement proper error handling and monitoring
- Add rate limiting for API calls
- Set up proper logging and alerting
- Use secrets management for sensitive credentials
- Implement proper backup and disaster recovery

## Resources

- [Snowflake Cortex AI Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
- [Slack Bolt Framework](https://slack.dev/bolt-python/tutorial/getting-started)
- [Original QuickStart Guide](https://quickstarts.snowflake.com/guide/integrate_snowflake_cortex_agents_with_slack/index.html)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section in the documentation
2. Review the validation SQL scripts
3. Enable debug mode for detailed logging
4. Consult the Snowflake and Slack documentation

## Version History

- **v1.0.0** - Initial release with basic Cortex integration
- **v2.0.0** - Added AI-powered charting and data filtering
- **v3.0.0** - Implemented entitlement-based security and advanced features

---

**Note**: This application is designed for demonstration purposes. For production use, please implement appropriate security measures, error handling, and scalability considerations.