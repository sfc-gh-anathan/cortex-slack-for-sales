-- Create REFINE_QUERY procedure for SLACK_SALES_DEMO
-- This procedure was copied from SLACK_DEMO.SLACK_SCHEMA to SLACK_SALES_DEMO.SLACK_SCHEMA
-- It provides query refinement functionality for the Slack bot

USE SLACK_SALES_DEMO.SLACK_SCHEMA;
USE WAREHOUSE SLACK_S;

CREATE OR REPLACE PROCEDURE "REFINE_QUERY"("STAGE_PATH" VARCHAR, "FILE_NAME" VARCHAR, "USER_PROMPT" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'refine_query'
EXECUTE AS OWNER
AS '
def refine_query(session, stage_path, file_name, user_prompt):
    try:
        # Construct the full file path
        if not stage_path.endswith(''/''):
            full_path = stage_path + ''/'' + file_name
        else:
            full_path = stage_path + file_name
        
        # Use the Snowpark DataFrame API to read the file
        df = session.read.option("FIELD_DELIMITER", "NONE") \\
                        .option("RECORD_DELIMITER", "NONE") \\
                        .option("SKIP_HEADER", 0) \\
                        .csv(full_path)
        
        # Collect all rows and concatenate them
        rows = df.collect()
        file_content = ""
        
        for row in rows:
            if row[0] is not None:
                file_content += str(row[0])
        
        # Create the prompt for Cortex to analyze the user''s query against the semantic model
        cortex_prompt = f"""
User Query: "{user_prompt}"

Semantic Model: {file_content}

Check if this query can be executed against the semantic model. Look at the dimensions, facts, time_dimensions, and their descriptions and identify gaps in the user''s questions. We are trying to help them see oversights in how they are expressing the nature of their question. We are trying to help them "refine" their question with better criteria. You can help because you understand the semantic model.

Always respond in exactly this format:
- If data not available: "This data is not available in the current dataset."
- Do not itemize what data is available
- If the prompt leaves too much room for interpretation or lacks clarity provide provide helpful follow up: from 1 to 3 [suggestion]"
- If the prompt is very well constructed, don''t help if you don''t need to. Just write: "Prompt is appropriately specific."
- NEVER restate the prompt itself and do NOT indicate what you will do to help refine their query. Only provide thoughtful questions like a senior analyst might.

Be direct and concise.

"""
        
        # Call Cortex with the analysis prompt
        cortex_query = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE(''claude-4-sonnet'', ?) as result
        """
        
        cortex_result = session.sql(cortex_query, [cortex_prompt]).collect()
        
        if cortex_result and len(cortex_result) > 0:
            return cortex_result[0][0] if cortex_result[0][0] is not None else "No result from Cortex"
        else:
            return "No response from Cortex"
        
    except Exception as e:
        return f"Error in refine_query: {str(e)}"
';;
