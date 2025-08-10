-- Update the semantic model to include email addresses
USE SLACK_SALES_DEMO.SLACK_SCHEMA;

-- Update the semantic model with the new YAML file that includes EMAIL field
ALTER CORTEX ANALYST sales_analysis 
SET SEMANTIC_MODEL = '@"SLACK_SALES_DEMO"."SLACK_SCHEMA"."SLACK_SEMANTIC_MODELS"'
FILE = 'sales_semantic_model.yaml';

-- Show the updated semantic model to confirm
SHOW CORTEX ANALYSTS;
