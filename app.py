from typing import Any
import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import snowflake.connector
import pandas as pd
from snowflake.core import Root
from dotenv import load_dotenv
import cortex_chat
import time
import requests
import tempfile
import io

# Import experimental intelligent charting (unused but kept for compatibility)
from _chart_utils_experimental import create_intelligent_chart
# Import AI-powered charting
from charter import ai_plot
# Import data filter modal functionality
from data_filter_modal import (
    get_filter_data_button_element, 
    create_filter_modal, 
    apply_pandas_filters, 
    extract_filter_values_from_modal,
    create_filtered_result_message,
    FILTER_DATA_BUTTON_ACTION_ID,
    FILTER_MODAL_CALLBACK_ID
)


load_dotenv()

# =============================================================================
# CURRENT USER CONFIGURATION FOR ENTITLEMENT-BASED FILTERING
# =============================================================================
# Set this to the email of the current user to demonstrate entitlement filtering
# All query results will be filtered based on this user's access level and hierarchy

# Choose one of these users to demonstrate different access levels (PURE HIERARCHY):
CURRENT_USER_EMAIL = "sarah.johnson@company.com"    # 1. CRO (TOP) - Reports to: NULL - Can see ALL data
# CURRENT_USER_EMAIL = "michael.chen@company.com"     # 2. VP Sales - Reports to: Sarah Johnson - Can see West region  
# CURRENT_USER_EMAIL = "robert.wilson@company.com"    # 3. Regional Manager - Reports to: Michael Chen - Can see West Coast region
# CURRENT_USER_EMAIL = "patricia.kim@company.com"     # 4. Sales Manager - Reports to: Robert Wilson - Can see California North team
# CURRENT_USER_EMAIL = "addison.wells@company.com"    # 5. Sales Rep (BOTTOM) - Reports to: Patricia Kim - Can see only own data
# =============================================================================

# --- Environment Variables ---
ACCOUNT = os.getenv("ACCOUNT")
HOST = os.getenv("HOST")
USER = os.getenv("DEMO_USER")
DATABASE = os.getenv("DEMO_DATABASE")
SCHEMA = os.getenv("DEMO_SCHEMA")
ROLE = os.getenv("DEMO_USER_ROLE")
WAREHOUSE = os.getenv("WAREHOUSE")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
AGENT_ENDPOINT = os.getenv("AGENT_ENDPOINT")
SEMANTIC_MODEL = os.getenv("SEMANTIC_MODEL") # CORRECTED: Typo fixed here
SEARCH_SERVICE = os.getenv("SEARCH_SERVICE")
RSA_PRIVATE_KEY_PATH = os.getenv("RSA_PRIVATE_KEY_PATH")
MODEL = os.getenv("MODEL")

# --- Environment Variable Validation (Added for Robustness) ---
required_env_vars = ["ACCOUNT", "HOST", "DEMO_USER", "DEMO_DATABASE", "DEMO_SCHEMA", "DEMO_USER_ROLE", "WAREHOUSE", "SLACK_APP_TOKEN", "SLACK_BOT_TOKEN", "AGENT_ENDPOINT", "SEMANTIC_MODEL", "SEARCH_SERVICE", "RSA_PRIVATE_KEY_PATH", "MODEL"]
for var in required_env_vars:
    if not os.getenv(var):
        print(f"Error: Required environment variable '{var}' is not set. Please check your .env file.")
        exit(1) # Exit if essential environment variables are missing

DEBUG = True # Set to True for more verbose console output to see debug prints

# --- Initializes Slack App ---
app = App(token=SLACK_BOT_TOKEN)

# --- Global In-Memory Cache for SQL Queries and DataFrames ---
# WARNING: In a production environment, this should be replaced with a more robust,
# persistent, and thread-safe caching mechanism (e.g., Redis, database)
# to handle multiple concurrent users and bot restarts.
global_sql_cache = {}
global_dataframe_cache = {}  # Cache for DataFrames used in filtering
global_original_dataframe_cache = {}  # Cache for original unfiltered DataFrames
global_current_filters_cache = {}  # Cache for current active filters per message
SQL_SHOW_BUTTON_ACTION_ID = "show_full_sql_query_button"
REFINE_QUERY_BUTTON_ACTION_ID = "refine_query_button"
REFINE_PROMPT_MODAL_ACTION_ID = "refine_prompt_modal"
RENDER_CHART_BUTTON_ACTION_ID = "ai_chart_button"  # Now uses AI-powered charting
DOWNLOAD_DATA_BUTTON_ACTION_ID = "download_data_button"
ROW_LIMIT_DROPDOWN_ACTION_ID = "row_limit_select"

# Global variable to store the last user prompt
last_user_prompt_global = ""

# Global variable to store refinement information per message
global_refinement_cache = {}  # {message_ts: {"needs_refinement": bool, "suggestions": str}}

# Global variable to preserve row limit during prompt refinement
preserved_row_limit_for_refinement = None



# Constants for Snowflake stored procedure parameters
SNOWFLAKE_STAGE_PATH = '@"SLACK_SALES_DEMO"."SLACK_SCHEMA"."SLACK_SEMANTIC_MODELS"'
SNOWFLAKE_FILE_NAME = 'sales_semantic_model.yaml'


# --- Entitlement-Based Security Functions ---

def apply_entitlement_filter(sql_query):
    """
    Apply comprehensive entitlement filtering to ALL SQL queries for ALL users.
    Every query is filtered based on the user's position in the hierarchy:
    - CRO (Sarah): Can see all employees and their data
    - VP Sales: Can see their region + all subordinates 
    - Regional Managers: Can see their region + direct reports
    - Sales Managers: Can see their team + direct reports
    - Sales Reps: Can see only their own data
    """
    if not CURRENT_USER_EMAIL:
        return sql_query
    
    # Replace the original table reference with our filtered view
    modified_query = sql_query.replace(
        'FROM slack_sales_demo.slack_schema.sales_semantic_view', 
        'FROM filtered_semantic_view'
    ).replace(
        'FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_SEMANTIC_VIEW', 
        'FROM filtered_semantic_view'
    )
    
    # Check if the query already has a WITH clause
    if modified_query.strip().upper().startswith('WITH'):
        # Insert our CTEs at the beginning of the existing WITH clause
        # Find the position after "WITH" and insert our CTEs there
        with_pos = modified_query.upper().find('WITH') + 4
        
        entitlement_ctes = f"""RECURSIVE accessible_employees AS (
    -- Start with current user
    SELECT 
        se.EMPLOYEE_ID,
        se.EMAIL,
        se.ROLE,
        0 as HIERARCHY_DEPTH
    FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_EMPLOYEES se
    WHERE se.EMAIL = '{CURRENT_USER_EMAIL}' AND se.ACTIVE = TRUE
    
    UNION ALL
    
    -- Recursively get all subordinates (people who report up to current user)
    SELECT 
        se.EMPLOYEE_ID,
        se.EMAIL,
        se.ROLE,
        ae.HIERARCHY_DEPTH + 1
    FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_EMPLOYEES se
    JOIN accessible_employees ae ON se.MANAGER_ID = ae.EMPLOYEE_ID
    WHERE se.ACTIVE = TRUE AND ae.HIERARCHY_DEPTH < 10  -- Prevent infinite recursion
),
filtered_semantic_view AS (
    SELECT sv.*
    FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_SEMANTIC_VIEW sv
    JOIN accessible_employees ae ON sv.EMPLOYEE_ID = ae.EMPLOYEE_ID
),
"""
        
        final_query = (modified_query[:with_pos] + " " + entitlement_ctes + 
                      modified_query[with_pos:].strip())
    else:
        # No existing WITH clause, add our own
        entitlement_ctes = f"""WITH RECURSIVE accessible_employees AS (
    -- Start with current user
    SELECT 
        se.EMPLOYEE_ID,
        se.EMAIL,
        se.ROLE,
        0 as HIERARCHY_DEPTH
    FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_EMPLOYEES se
    WHERE se.EMAIL = '{CURRENT_USER_EMAIL}' AND se.ACTIVE = TRUE
    
    UNION ALL
    
    -- Recursively get all subordinates (people who report up to current user)
    SELECT 
        se.EMPLOYEE_ID,
        se.EMAIL,
        se.ROLE,
        ae.HIERARCHY_DEPTH + 1
    FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_EMPLOYEES se
    JOIN accessible_employees ae ON se.MANAGER_ID = ae.EMPLOYEE_ID
    WHERE se.ACTIVE = TRUE AND ae.HIERARCHY_DEPTH < 10  -- Prevent infinite recursion
),
filtered_semantic_view AS (
    SELECT sv.*
    FROM SLACK_SALES_DEMO.SLACK_SCHEMA.SALES_SEMANTIC_VIEW sv
    JOIN accessible_employees ae ON sv.EMPLOYEE_ID = ae.EMPLOYEE_ID
)
"""
        final_query = entitlement_ctes + modified_query.rstrip(';')
    
    if DEBUG:
        print(f"🔒 COMPREHENSIVE ENTITLEMENT FILTER APPLIED for {CURRENT_USER_EMAIL}")
        print(f"Original SQL: {sql_query[:200]}...")
        print(f"Filtered SQL: {final_query[:300]}...")
    
    return final_query

# --- Background Refinement Functions ---

def background_refinement_analysis(user_prompt, message_ts, channel_id, app_client):
    """
    Run refinement analysis in background and add red button if query needs refinement.
    Shows red button when refine query doesn't return 'Prompt is appropriately specific.'
    """
    import time
    
    try:
        # No delay needed - run immediately
        
        if DEBUG:
            print(f"🔍 Starting background refinement analysis for: '{user_prompt}'")
        
        # Call the existing refine query procedure (optimized)
        cur = CONN.cursor()
        try:
            escaped_stage_path = SNOWFLAKE_STAGE_PATH.replace("'", "''")
            escaped_user_prompt = user_prompt.replace("'", "''")
            
            sql_call_formatted = (
                f"CALL {DATABASE}.{SCHEMA}.REFINE_QUERY("
                f"'{escaped_stage_path}', "
                f"'{SNOWFLAKE_FILE_NAME}', "
                f"'{escaped_user_prompt}')"
            )
            
            cur.execute(sql_call_formatted)
            result = cur.fetchone()
        finally:
            cur.close()  # Ensure cursor is closed immediately
        
        if result:
            refinement_message = result[0]
        else:
            refinement_message = "No refinement suggestions received."
        
        # Always log the refinement result for visibility
        print(f"📝 PROMPT WARNING RESULT: '{refinement_message}'")
        
        if DEBUG:
            print(f"🔍 Refinement result: '{refinement_message}'")
        
        # Store refinement information in global cache for later use by action buttons
        needs_refinement = "appropriately specific" not in refinement_message.lower()
        global_refinement_cache[message_ts] = {
            "needs_refinement": needs_refinement,
            "suggestions": refinement_message
        }
        
        # Check if refinement suggests improvements (NOT "appropriately specific")
        if needs_refinement:
            print("⚠️ PROMPT NEEDS REFINEMENT: Will show Refine Prompt button")
            # Add the refinement button to the existing message
            add_refinement_button_to_message(message_ts, channel_id, app_client)
        else:
            # Add green checkmark for appropriately specific queries
            add_prompt_specific_notification(message_ts, channel_id, app_client)
            print("✅ PROMPT WARNING SKIPPED: Query is appropriately specific")
            if DEBUG:
                print("✅ Query is appropriately specific - no refinement button needed")
                
    except Exception as e:
        if DEBUG:
            print(f"❌ Error in background refinement analysis: {e}")

def add_refinement_button_to_message(message_ts, channel_id, app_client):
    """Add the Refine Prompt button to an existing message"""
    try:
        # Get the current message
        message_response = app_client.conversations_history(
            channel=channel_id,
            latest=message_ts,
            limit=1,
            inclusive=True
        )
        
        if not message_response['messages']:
            print("❌ Could not find message to add refinement button")
            return
            
        current_message = message_response['messages'][0]
        updated_blocks = current_message.get('blocks', [])
        
        # Find the action buttons block and update it to include the refinement button
        for i, block in enumerate(updated_blocks):
            if block.get("type") == "actions":
                # Get the current data size from the row limit dropdown if it exists
                data_size = None
                for element in block.get("elements", []):
                    if element.get("action_id") == ROW_LIMIT_DROPDOWN_ACTION_ID:
                        # This message has data, so we can determine the size
                        # For now, we'll use a default approach
                        data_size = 100  # Default assumption
                        break
                
                # Replace the action buttons block with one that includes the refinement button
                updated_blocks[i] = get_action_buttons_block(
                    include_show_sql=True, 
                    data_size=data_size, 
                    include_row_limit=(data_size is not None),
                    include_refine_prompt=True
                )
                break
        
        # Update the message
        app_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=updated_blocks
        )
        
        if DEBUG:
            print(f"✅ Added refinement button to message {message_ts}")
            
    except Exception as e:
        if DEBUG:
            print(f"❌ Error adding refinement button: {e}")

def add_prompt_specific_notification(message_ts, channel_id, app_client):
    """Add a green checkmark notification for appropriately specific queries"""
    
    try:
        # Get the current message blocks
        message_response = app_client.conversations_history(
            channel=channel_id,
            latest=message_ts,
            limit=1,
            inclusive=True
        )
        
        if not message_response.get('ok') or not message_response.get('messages'):
            if DEBUG:
                print("❌ Could not retrieve message for prompt specific notification")
            return
            
        current_blocks = message_response['messages'][0].get('blocks', [])
        
        # Add the green checkmark notification
        specific_notification_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "✅ *Prompt satisfactory*"
            }
        }
        
        # Update the message with the new notification block
        updated_blocks = current_blocks + [specific_notification_block]
        
        app_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=updated_blocks
        )
        
        if DEBUG:
            print(f"✅ Added prompt specific notification to message {message_ts}")
            
    except Exception as e:
        if DEBUG:
            print(f"❌ Error adding prompt specific notification: {e}")

def add_smart_refinement_button(message_ts, channel_id, refinement_suggestion, app_client):
    """Add a red refinement button to existing message with specific suggestion"""
    
    try:
        # Get the current message blocks
        message_response = app_client.conversations_history(
            channel=channel_id,
            latest=message_ts,
            limit=1,
            inclusive=True
        )
        
        if not message_response.get('ok') or not message_response.get('messages'):
            if DEBUG:
                print("❌ Could not retrieve message for refinement button")
            return
            
        current_blocks = message_response['messages'][0].get('blocks', [])
        
        # Add the smart refinement section with button at the end
        refinement_text_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🛑 *Prompt Warning*\n{refinement_suggestion}"
            }
        }
        
        # Update the message with just the refinement text (no button)
        updated_blocks = current_blocks + [refinement_text_block]
        
        app_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=updated_blocks
        )
        
        if DEBUG:
            print(f"✅ Added smart refinement button to message {message_ts}")
            
    except Exception as e:
        if DEBUG:
            print(f"❌ Error adding smart refinement button: {e}")

# --- Action Button Functions ---



def get_row_limit_dropdown_element(data_size=None):
    """Create row limit dropdown element"""
    options = []
    valid_options = [10, 25, 50, 100, 250, 500]
    
    if data_size:
        valid_options = [opt for opt in valid_options if opt <= data_size * 2]
        if data_size not in valid_options:
            valid_options.append(data_size)
    
    for value in sorted(set(valid_options)):
        options.append({
            "text": {"type": "plain_text", "text": f"{value} {'Row' if value == 1 else 'Rows'}"},
            "value": str(value)
        })
    
    initial_option = {"text": {"type": "plain_text", "text": "10 Rows"}, "value": "10"}
    
    return {
        "type": "static_select",
        "placeholder": {"type": "plain_text", "text": "Rows"},
        "options": options,
        "initial_option": initial_option,
        "action_id": ROW_LIMIT_DROPDOWN_ACTION_ID
    }

# --- Slack Message Handlers ---

@app.event("message")
def handle_message_events(ack, body, say):
    global last_user_prompt_global
    try:
        ack()
        prompt = body['event']['text']
        last_user_prompt_global = prompt
        say(
            text = "Snowflake Cortex AI is generating a response",
            blocks=[
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": "❄️ Snowflake Cortex AI is generating a response. Please wait...", # Unicode Emoji
                    }
                },
                {
                    "type": "divider"
                },
            ]
        )
        response = ask_agent(prompt)
        display_agent_response(response, say, app.client, body)
    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR: {error_info}")
        say(
            text = "Request failed...",
            blocks=[
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": f"An unexpected error occurred: {type(e).__name__}. Please try again later or contact support if the issue persists.",
                    }
                },
                {
                    "type": "divider"
                },
            ]
        )

# --- Agent Interaction ---

def ask_agent(prompt):
    """
    Sends the user prompt to the Cortex Chat Agent.
    """
    resp = CORTEX_APP.chat(prompt)
    return resp

# --- Helper for SQL display blocks ---
def get_sql_code_block(sql_query):
    """
    Generates Slack rich_text block for displaying a SQL query as code.
    """
    return {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [
                    {
                        "type": "text",
                        "text": "SQL Query:",
                        "style": {
                            "bold": True
                        }
                    }
                ]
            },
            {
                "type": "rich_text_preformatted",
                "elements": [
                    {
                        "type": "text",
                        "text": sql_query
                    }
                ]
            }
        ]
    }



# Helper for Show SQL Query button element
def get_show_sql_query_button_element():
    """
    Returns the Slack Block Kit element for the "Show SQL Query" button.
    """
    return {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Show SQL Query",
            "emoji": True
        },
        # MODIFIED: Removed style property to allow default (white) color
        "action_id": SQL_SHOW_BUTTON_ACTION_ID
    }

# Helper for Row Limit dropdown element
def get_row_limit_dropdown_element(data_size=None, selected_value=None):
    """
    Returns the Slack Block Kit element for the row limit dropdown.
    Always defaults to 10 rows and never shows more options than total rows.
    """
    # Use selected_value if provided, otherwise preserved row limit, otherwise default to 10 rows
    default_value = str(selected_value or preserved_row_limit_for_refinement or 10)
    
    # Base options (always include 10)
    base_options = [10, 25, 50, 100, 200]
    
    # Filter options to never exceed data size, but include appropriate larger options
    if data_size is not None:
        valid_options = [opt for opt in base_options if opt <= data_size]
        # Always include at least 10 (or data_size if smaller)
        if not valid_options:
            valid_options = [min(10, data_size)]
        elif 10 not in valid_options and data_size >= 10:
            valid_options.insert(0, 10)
        
        # Add the actual data size as an option if it's not already covered
        if data_size > max(valid_options) and data_size <= 200:
            valid_options.append(data_size)
    else:
        valid_options = base_options
    
    # Create option objects
    options = []
    for value in sorted(set(valid_options)):
        options.append({
            "text": {"type": "plain_text", "text": f"{value} {'Row' if value == 1 else 'Rows'}"}, 
            "value": str(value)
        })
    
    # Ensure default exists in options
    if default_value not in [opt["value"] for opt in options]:
        default_value = options[0]["value"]
    
    # Find the default option
    initial_option = next((opt for opt in options if opt["value"] == default_value), options[0])
    
    return {
        "type": "static_select",
        "placeholder": {
            "type": "plain_text",
            "text": "Rows"
        },
        "options": options,
        "initial_option": initial_option,
        "action_id": ROW_LIMIT_DROPDOWN_ACTION_ID
    }



# Helper for Render Chart button element (AI-powered)
def get_render_chart_button_element():
    """
    Returns the Slack Block Kit element for the "Render Chart" button (AI-powered).
    """
    return {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "AI Chart",
            "emoji": True
        },
        "style": "primary",
        "action_id": RENDER_CHART_BUTTON_ACTION_ID
    }

# NEW: Helper for Download Data button element
def get_download_data_button_element():
    """
    Returns the Slack Block Kit element for the "Download Data" button.
    """
    return {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Download Data",
            "emoji": True
        },
        "style": "primary",
        "action_id": DOWNLOAD_DATA_BUTTON_ACTION_ID
    }

def get_refine_prompt_button_element():
    """Returns the 'Refine Prompt' button element for modal-based refinement"""
    return {
        "type": "button",
        "text": {"type": "plain_text", "text": "Refine Prompt"},
        "style": "danger",  # Red button to indicate needs attention
        "action_id": REFINE_PROMPT_MODAL_ACTION_ID
    }

def create_refine_prompt_modal(original_prompt, refinement_suggestions):
    """Creates the modal view for prompt refinement with AI suggestions"""
    return {
        "type": "modal",
        "callback_id": "refine_prompt_modal_submit",
        "title": {
            "type": "plain_text",
            "text": "Refine Your Prompt"
        },
        "submit": {
            "type": "plain_text",
            "text": "Submit Refined Prompt"
        },
        "close": {
            "type": "plain_text",
            "text": "Cancel"
        },
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Refinement Feedback:* \n{refinement_suggestions}"
                }
            },
            {
                "type": "input",
                "block_id": "refined_prompt_input",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "refined_prompt_text",
                    "initial_value": original_prompt,
                    "multiline": True,
                    "placeholder": {
                        "type": "plain_text",
                        "text": "Edit your prompt based on the suggestions above..."
                    }
                },
                "label": {
                    "type": "plain_text",
                    "text": "Modify Your Prompt and Resubmit:"
                }
            }
        ]
    }

# NEW: Combined actions block for all four buttons
def should_include_refine_prompt(message_ts):
    """Helper function to determine if the Refine Prompt button should be included"""
    refinement_info = global_refinement_cache.get(message_ts)
    return refinement_info and refinement_info.get("needs_refinement", False)

def get_action_buttons_block(include_show_sql=True, data_size=None, include_row_limit=True, include_refine_prompt=False, selected_row_limit=None): # MODIFIED: Added include_refine_prompt parameter and selected_row_limit
    """
    Returns a Slack Block Kit 'actions' block containing desired buttons on the same row.
    """
    elements = []
    
    # Add row limit dropdown first (left-most position) - only for filtered results, not charts
    if include_row_limit:
        elements.append(get_row_limit_dropdown_element(data_size, selected_row_limit))
    
    if include_show_sql: # Only add if requested
        elements.append(get_show_sql_query_button_element())

    elements.extend([
        get_filter_data_button_element(),
        get_render_chart_button_element(),
        get_download_data_button_element()
    ])
    
    # Add "Refine Prompt" button if refinement is needed
    if include_refine_prompt:
        elements.append(get_refine_prompt_button_element())

    return {
        "type": "actions",
        "elements": elements
    }

# --- Response Display and Charting Logic ---

def _format_dataframe_for_display(df):
    """
    Format DataFrame columns for better display with commas and currency symbols
    """
    formatted_df = df.copy()
    
    for col in formatted_df.columns:
        if pd.api.types.is_numeric_dtype(formatted_df[col]):
            # Check if this looks like a currency/sales column
            is_currency = any(keyword in col.upper() for keyword in 
                            ['SALES', 'AMOUNT', 'REVENUE', 'TOTAL', 'COST', 'PRICE', 'VALUE'])
            
            # Format numeric columns
            if is_currency:
                # Currency formatting with commas
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"${x:,.0f}" if pd.notna(x) else ""
                )
            else:
                # Regular number formatting with commas for large numbers
                formatted_df[col] = formatted_df[col].apply(
                    lambda x: f"{x:,.0f}" if pd.notna(x) and abs(x) >= 1000 else 
                             (f"{x:.2f}" if pd.notna(x) and x != int(x) else 
                              f"{int(x)}" if pd.notna(x) else "")
                )
    
    return formatted_df


def _get_safe_table_text(df, truncated_message="", requested_rows=None):
    """
    Get table text that's safe for Slack's character limits.
    Prioritizes showing the requested number of rows, only reduces if absolutely necessary.
    Returns tuple: (table_text, actual_rows_displayed)
    """
    # Start with requested rows, or default to 10 if not specified
    if requested_rows is not None:
        max_rows = min(len(df), requested_rows)
    else:
        max_rows = min(len(df), 10)
    
    # First, try the exact requested number of rows
    display_df = df.head(max_rows).copy()
    
    # Format numeric columns with commas and currency symbols
    display_df = _format_dataframe_for_display(display_df)
    
    base_table_text = display_df.to_string(index=False)
    
    # Calculate space needed for row counter message
    if max_rows < len(df):
        row_message = f"\n\n(Showing {max_rows:,} of {len(df):,} rows. Use dropdown to see more.)"
    else:
        row_message = ""
    
    full_text = base_table_text + truncated_message + row_message
    
    # If the requested rows fit within the limit, return them!
    if len(full_text) <= 2800:
        return full_text, max_rows
    
    # Only reduce if absolutely necessary and the request was large
    if requested_rows and requested_rows > 25:
        # Try progressively smaller fallback amounts: half, then quarter, then reasonable minimums
        fallback_attempts = [
            max(25, requested_rows // 2),  # Half, but at least 25
            max(20, requested_rows // 4),  # Quarter, but at least 20
            max(15, requested_rows // 8),  # Eighth, but at least 15
            10  # Final fallback
        ]
        
        for fallback_rows in fallback_attempts:
            if fallback_rows >= requested_rows:  # Skip if not actually smaller
                continue
                
            display_df = df.head(fallback_rows).copy()
            display_df = _format_dataframe_for_display(display_df)
            base_table_text = display_df.to_string(index=False)
            row_message = f"\n\n(Showing {fallback_rows:,} of {len(df):,} rows - reduced from {requested_rows:,} due to Slack size limits.)"
            full_text = base_table_text + truncated_message + row_message
            
            if len(full_text) <= 2800:
                return full_text, fallback_rows
    
    # Last resort: reduce to a safe minimum
    safe_rows = 10
    while safe_rows > 3:
        display_df = df.head(safe_rows).copy()
        display_df = _format_dataframe_for_display(display_df)
        base_table_text = display_df.to_string(index=False)
        row_message = f"\n\n(Showing {safe_rows:,} of {len(df):,} rows - reduced due to Slack size limits.)"
        full_text = base_table_text + truncated_message + row_message
        
        if len(full_text) <= 2800:
            return full_text, safe_rows
        
        safe_rows -= 2
    
    # If even 3 rows is too long (very wide table), truncate the text
    display_df = df.head(3).copy()
    display_df = _format_dataframe_for_display(display_df)
    table_text = display_df.to_string(index=False)
    if len(table_text) > 2700:
        table_text = table_text[:2700] + "..."
    table_text += f"\n\n(Table truncated for Slack display. Use dropdown to adjust.)"
    
    return table_text, 3

def display_agent_response(content, say, app_client, original_body):
    """
    Displays the agent's response, handling both SQL results (with charts)
    and unstructured text responses.
    """
    channel_id = original_body['event']['channel']

    final_blocks = []

    if content['sql']:
        sql = content['sql']
        
        # Apply entitlement-based filtering to ALL queries
        filtered_sql = apply_entitlement_filter(sql)

        df = pd.read_sql(filtered_sql, CONN)

        if DEBUG:
            print("Original DataFrame info:")
            df.info()

        # --- Robust Type Conversion for Plotting ---
        if len(df.columns) >= 2:
            try:
                if pd.api.types.is_object_dtype(df.iloc[:, 0]) or pd.api.types.is_string_dtype(df.iloc[:, 0]):
                    temp_col = pd.to_datetime(df.iloc[:, 0], errors='coerce')
                    if not temp_col.isna().all():
                        df[df.columns[0]] = temp_col
                        if DEBUG:
                            print(f"Converted column '{df.columns[0]}' to datetime where possible.")
            except Exception as e:
                if DEBUG:
                    print(f"Could not convert column '{df.columns[0]}' to datetime: {e}")

            for i in range(len(df.columns)):
                try:
                    if pd.api.types.is_object_dtype(df.iloc[:, i]) or pd.api.types.is_string_dtype(df.iloc[:, i]):
                        temp_col = pd.to_numeric(df.iloc[:, i], errors='coerce')
                        if not temp_col.isna().all() and (temp_col.notna().sum() / len(temp_col) > 0.5):
                            df[df.columns[i]] = temp_col
                            if DEBUG:
                                print(f"Converted column '{df.columns[i]}' to numeric where possible.")
                    elif pd.api.types.is_numeric_dtype(df.iloc[:, i]):
                        # Only convert to float if the column contains decimal values
                        if not df[df.columns[i]].apply(lambda x: x == int(x) if pd.notna(x) else True).all():
                            df[df.columns[i]] = df[df.columns[i]].astype(float)
                            if DEBUG:
                                print(f"Kept column '{df.columns[i]}' as float (contains decimals)")
                        else:
                            df[df.columns[i]] = df[df.columns[i]].astype(int)
                            if DEBUG:
                                print(f"Converted column '{df.columns[i]}' to int (whole numbers only)")
                except Exception as e:
                    if DEBUG:
                        print(f"Could not convert column '{df.columns[i]}' to numeric: {e}")

        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                if df[col].isnull().any():
                    df.dropna(subset=[col], inplace=True)
                    if DEBUG:
                        print(f"Dropped rows with NaN in numeric column '{col}'.")

        if DEBUG:
            print("\nDataFrame after type conversion info:")
            df.info()

            print("\nDataFrame head after conversion:")
            print(df.head())

        # Check for empty DataFrame (potentially due to entitlement filtering)
        if len(df) == 0:
            final_blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "🚫 *There were no results for your query. This may be a permissions issue.*"
                }
            })
            
            # Post initial message to get message_ts
            message_ts = say(text="No results found", blocks=final_blocks)['ts']
            
            # Add action buttons without refinement (background thread will add refinement button if needed)
            final_blocks.append(get_action_buttons_block(include_show_sql=True, data_size=0, include_row_limit=False, include_refine_prompt=False))
            app_client.chat_update(
                channel=channel_id,
                ts=message_ts,
                blocks=final_blocks,
                text="No results found"
            )
            
            # Cache the empty DataFrame and SQL for potential button interactions
            global_dataframe_cache[message_ts] = df
            global_sql_cache[message_ts] = sql
            global_original_dataframe_cache[message_ts] = df.copy()
            
            return

        # Handle Single-Row Answers Specifically
        if len(df) == 1:
            formatted_answer = ""
            for col in df.columns:
                formatted_answer += f"{col.replace('_', ' ').title()}: {df[col].iloc[0]}\n"

            final_blocks.append({
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": "Here's the specific information you requested:",
                                "style": {
                                    "bold": True
                                }
                            }
                        ]
                    },
                    {
                        "type": "rich_text_preformatted",
                        "elements": [
                            {
                                "type": "text",
                                "text": formatted_answer
                            }
                        ]
                    }
                ]
            })
        else:
            # Let the safe table text function handle row limiting
            display_df = df  # Pass full dataframe

            # Display DataFrame as Text Table
            final_blocks.append({
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_quote",
                        "elements": [
                            {
                                "type": "text",
                                "text": "Answer:",
                                "style": {
                                    "bold": True
                                }
                            }
                        ]
                    },
                    {
                        "type": "rich_text_preformatted",
                        "elements": [
                            {
                                "type": "text",
                                "text": _get_safe_table_text(display_df, "", preserved_row_limit_for_refinement or min(len(df), 10))[0]
                            }
                        ]
                    }
                ]
            })

        # Add the combined action buttons block initially without refinement button
        # Use preserved row limit if available (for refined prompts), otherwise use data size for smart default
        display_limit = preserved_row_limit_for_refinement or len(df)
        final_blocks.append(get_action_buttons_block(include_show_sql=True, data_size=display_limit, include_refine_prompt=False))

        # Send the initial message and capture its timestamp (ts)
        try:
            post_response = app_client.chat_postMessage(
                channel=channel_id,
                blocks=final_blocks,
                text="Your query results are ready."
            )
            message_ts = post_response['ts']
            
            # Background thread will add refinement button if needed - no need to check immediately

            # Store the full SQL query and DataFrame in the global cache, keyed by message_ts
            global_sql_cache[message_ts] = sql
            global_dataframe_cache[message_ts] = df
            global_original_dataframe_cache[message_ts] = df.copy()  # Store original unfiltered data
            
            # Start background refinement analysis
            import threading
            threading.Thread(
                target=background_refinement_analysis,
                args=(last_user_prompt_global, message_ts, channel_id, app_client),
                daemon=True
            ).start()

        except Exception as e:
            print(f"Error posting initial message to Slack: {e}")
            say(f"An error occurred while posting results: {e}")
            return

    else:
        # --- Handle Unstructured Text Responses ---
        text_response_blocks = [
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_quote",
                        "elements": [
                            {
                                "type": "text",
                                "text": f"Answer: {content['text']}",
                                "style": {
                                    "bold": True
                                }
                            }
                        ]
                    },
                    {
                        "type": "rich_text_quote",
                        "elements": [
                            {
                                "type": "text",
                                "text": f"* Citation: {content['citations']}",
                                "style": {
                                    "italic": True
                                }
                            }
                        ]
                    }
                ]
            },
            # No buttons are added here as the response is unstructured
        ]

        say(
            text="Answer:",
            blocks=text_response_blocks
        )

# --- Action handler for "Show SQL Query" button ---
@app.action(SQL_SHOW_BUTTON_ACTION_ID)
def handle_show_sql_query(ack, body, client):
    ack()

    message_ts = body['message']['ts']
    channel_id = body['channel']['id']

    # Retrieve the SQL query from the cache using the message's timestamp
    sql_query = global_sql_cache.get(message_ts)

    current_blocks = body['message']['blocks']

    blocks_to_rebuild = []
    sql_already_displayed = False

    for block in current_blocks:
        # Filter out the existing action buttons block
        if block.get("type") == "actions" and any(e.get('action_id') in [REFINE_QUERY_BUTTON_ACTION_ID, REFINE_PROMPT_MODAL_ACTION_ID, SQL_SHOW_BUTTON_ACTION_ID, RENDER_CHART_BUTTON_ACTION_ID, DOWNLOAD_DATA_BUTTON_ACTION_ID, ROW_LIMIT_DROPDOWN_ACTION_ID] for e in block['elements']):
            # This is our action buttons block, we will re-add a modified version later
            continue
        # Check if the SQL is already in the message (e.g., if button was clicked twice)
        if block.get("type") == "rich_text" and any(el.get("text") == "SQL Query:" for el in block.get("elements", []) if el.get("type") == "rich_text_section"):
            sql_already_displayed = True
            # We don't continue here directly because we might want to keep other blocks before and after SQL
        blocks_to_rebuild.append(block)

    updated_blocks = blocks_to_rebuild[:]

    if sql_query and not sql_already_displayed:
        # Insert the full SQL query blocks
        updated_blocks.append(get_sql_code_block(sql_query))
    elif sql_already_displayed:
        # If SQL was already there, provide ephemeral feedback and don't update the message
        client.chat_postMessage(
            channel=channel_id,
            text="The SQL query is already displayed in the message above.",

            ephemeral=True
        )
        return

    # Re-add the action buttons, but this time, EXCLUDE the "Show SQL Query" button
    updated_blocks.append(get_action_buttons_block(include_show_sql=False, data_size=None)) # Pass False to exclude Show SQL

    try:
        client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=updated_blocks,
            text="Your query results and SQL."
        )
    except Exception as e:
        print(f"Error updating message with SQL: {e}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while displaying the query: {e}",

        )

# Action handler for Row Limit dropdown
@app.action(ROW_LIMIT_DROPDOWN_ACTION_ID)
def handle_row_limit_change(ack, body, client):
    """
    Handles the row limit dropdown selection change.
    Updates the displayed table with the new row limit.
    """
    ack()
    
    message_ts = body['message']['ts']
    channel_id = body['channel']['id']
    selected_limit = int(body['actions'][0]['selected_option']['value'])
    

    # Try to get DataFrame from cache first, then fall back to SQL
    df = global_dataframe_cache.get(message_ts)
    
    if df is not None:
        # We have a cached DataFrame (probably filtered results)
        if DEBUG:
            print(f"Row limit change: Using cached DataFrame with {len(df)} rows")
    else:
        # Fall back to SQL query for original results
        sql_query = global_sql_cache.get(message_ts)
        if not sql_query:
            client.chat_postMessage(
                channel=channel_id,
                text="Sorry, the query data is no longer available. Please run your query again.",
    
                ephemeral=True
            )
            return
        
        try:
            # Re-execute the SQL query with entitlement filtering
            filtered_sql = apply_entitlement_filter(sql_query)
            df = pd.read_sql(filtered_sql, CONN)
            if DEBUG:
                print(f"Row limit change: Re-executed SQL query, got {len(df)} rows")
            
            # Apply the same type conversion logic as the initial display
            if len(df.columns) >= 2:
                for i in range(len(df.columns)):
                    try:
                        if pd.api.types.is_object_dtype(df.iloc[:, i]) or pd.api.types.is_string_dtype(df.iloc[:, i]):
                            temp_col = pd.to_numeric(df.iloc[:, i], errors='coerce')
                            if not temp_col.isna().all() and (temp_col.notna().sum() / len(temp_col) > 0.5):
                                df[df.columns[i]] = temp_col
                        elif pd.api.types.is_numeric_dtype(df.iloc[:, i]):
                            # Only convert to float if the column contains decimal values
                            if not df[df.columns[i]].apply(lambda x: x == int(x) if pd.notna(x) else True).all():
                                df[df.columns[i]] = df[df.columns[i]].astype(float)
                            else:
                                df[df.columns[i]] = df[df.columns[i]].astype(int)
                    except Exception as e:
                        pass  # Silently continue if conversion fails
        except Exception as e:
            client.chat_postMessage(
                channel=channel_id,
                text=f"Error re-executing query: {e}",
    
                ephemeral=True
            )
            return
    
    try:
        
        # Check for empty DataFrame first
        if len(df) == 0:
            # Get current blocks and rebuild with empty message
            current_blocks = body['message']['blocks']
            updated_blocks = []
            
            for block in current_blocks:
                # Skip the existing table/section blocks and action buttons
                if block.get("type") == "section" and (block.get("text", {}).get("text", "").startswith("```") or "There were no results" in block.get("text", {}).get("text", "")):
                    continue
                elif block.get("type") == "actions":
                    continue
                else:
                    updated_blocks.append(block)
            
            # Add the no results message
            updated_blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "🚫 *There were no results for your query. This may be a permissions issue.*"
                }
            })
            
            # Add action buttons without row limit dropdown
            # Add action buttons (refinement button will be added by background thread if needed)
            updated_blocks.append(get_action_buttons_block(include_show_sql=True, data_size=0, include_row_limit=False, include_refine_prompt=False))
            
            # Update the message
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                blocks=updated_blocks,
                text="No results found."
            )
            return
        
        # Get current blocks and rebuild with new data
        # Note: _get_safe_table_text will handle the row limiting internally
        current_blocks = body['message']['blocks']
        updated_blocks = []
        
        for block in current_blocks:
            # Skip the existing table block
            if block.get("type") == "section" and block.get("text", {}).get("text", "").startswith("```"):
                continue
            # Skip the existing action buttons block
            elif block.get("type") == "actions":
                continue
            else:
                updated_blocks.append(block)
        
        # Add the new table block with limited rows using safe text function
        # Note: _get_safe_table_text handles all row count messages internally and limiting
        safe_table_content, actual_rows_displayed = _get_safe_table_text(df, "", selected_limit)
        table_text = f"```\n{safe_table_content}\n```"
        
        updated_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": table_text
            }
        })
        
        # Re-add the action buttons with updated dropdown selection
        # Use actual_rows_displayed (what was actually shown) instead of selected_limit (what was requested)
        updated_blocks.append(get_action_buttons_block(include_show_sql=True, data_size=len(df), include_refine_prompt=False, selected_row_limit=actual_rows_displayed))
        
        # Update the message
        client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=updated_blocks,
            text="Query results updated."
        )
        
    except Exception as e:
        print(f"Error updating row limit: {e}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while updating the row limit: {e}",

            ephemeral=True
        )

# Action handler for "Refine Query" button
@app.action(REFINE_QUERY_BUTTON_ACTION_ID)
def handle_refine_query_button_click(ack, body, client):
    """
    Handles the click event for the "Refine Prompt" button.
    Calls the Snowflake stored procedure REFINE_QUERY with the last user prompt.
    """
    ack()

    message_ts = body['message']['ts']
    channel_id = body['channel']['id']

    if not last_user_prompt_global:
        client.chat_postMessage(
            channel=channel_id,
            text="Sorry, I couldn't retrieve the last prompt to refine. Please try inputing another prompt.",

        )
        return

    try:
        # MODIFIED: Used rich_text blocks for reliable bolding and emoji
        client.chat_postMessage(
            channel=channel_id,
            blocks=[
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "❄️ ", # Unicode emoji
                                },
                                {
                                    "type": "text",
                                    "text": f"Refining prompt for: '{last_user_prompt_global}'...",
                                    "style": {
                                        "bold": True
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],

        )

        cur = CONN.cursor()

        escaped_stage_path = SNOWFLAKE_STAGE_PATH.replace("'", "''")
        escaped_file_name = SNOWFLAKE_FILE_NAME.replace("'", "''")
        escaped_user_prompt = last_user_prompt_global.replace("'", "''")

        # Using DATABASE and SCHEMA environment variables for consistency
        sql_call_formatted = (
            f"CALL {DATABASE}.{SCHEMA}.REFINE_QUERY("
            f"'{escaped_stage_path}', "
            f"'{escaped_file_name}', "
            f"'{escaped_user_prompt}')"
        )

        if DEBUG:
            print(f"DEBUG: Attempting to call with formatted SQL: {sql_call_formatted}")

        cur.execute(sql_call_formatted)

        result = cur.fetchone()

        if result:
            refinement_message = result[0]
        else:
            refinement_message = "No refinement suggestions received from Cortex."

        # Post the refinement result with action buttons
        refine_blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Bottom Line: {refinement_message}"
                }
            },
            get_action_buttons_block(include_show_sql=True, data_size=None, include_row_limit=False)
        ]
        
        refine_response = client.chat_postMessage(
            channel=channel_id,
            text=f"Bottom Line: {refinement_message}",
            blocks=refine_blocks
        )
        
        # Cache the original data for the refine message so buttons work
        refine_message_ts = refine_response['ts']
        if message_ts in global_dataframe_cache:
            global_dataframe_cache[refine_message_ts] = global_dataframe_cache[message_ts]
        if message_ts in global_sql_cache:
            global_sql_cache[refine_message_ts] = global_sql_cache[message_ts]
        if message_ts in global_original_dataframe_cache:
            global_original_dataframe_cache[refine_message_ts] = global_original_dataframe_cache[message_ts]

    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR calling Snowflake REFINE_QUERY: {error_info}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while trying to refine the prompt: {e}",

        )
    finally:
        if 'cur' in locals() and cur:
            cur.close()

# Action handler for "Refine Prompt" modal button
@app.action(REFINE_PROMPT_MODAL_ACTION_ID)
def handle_refine_prompt_modal_click(ack, body, client):
    """
    Handles the click event for the "Refine Prompt" button.
    Opens a modal with refinement suggestions and editable prompt.
    """
    ack()

    message_ts = body['message']['ts']
    channel_id = body['channel']['id']
    user_id = body['user']['id']
    trigger_id = body['trigger_id']

    if not last_user_prompt_global:
        client.chat_postMessage(
            channel=channel_id,
            text="Sorry, I couldn't retrieve the last prompt to refine. Please try another prompt."
        )
        return

    try:
        # Get refinement suggestions from cache (already computed in background)
        refinement_info = global_refinement_cache.get(message_ts)
        if refinement_info and refinement_info.get("suggestions"):
            refinement_suggestions = refinement_info["suggestions"]
        else:
            # Fallback: call Snowflake if cache is missing
            cur = CONN.cursor()
            escaped_stage_path = SNOWFLAKE_STAGE_PATH.replace("'", "''")
            escaped_file_name = SNOWFLAKE_FILE_NAME.replace("'", "''")
            escaped_user_prompt = last_user_prompt_global.replace("'", "''")

            sql_call_formatted = (
                f"CALL {DATABASE}.{SCHEMA}.REFINE_QUERY("
                f"'{escaped_stage_path}', "
                f"'{escaped_file_name}', "
                f"'{escaped_user_prompt}')"
            )

            cur.execute(sql_call_formatted)
            result = cur.fetchone()
            cur.close()

            if result:
                refinement_suggestions = result[0]
            else:
                refinement_suggestions = "No specific suggestions available. Consider being more specific about time periods, metrics, or filters."

        # Create and open the modal
        modal_view = create_refine_prompt_modal(last_user_prompt_global, refinement_suggestions)
        
        # Store the original message context for the modal submission
        modal_view["private_metadata"] = f"{message_ts}|{channel_id}"
        
        client.views_open(
            trigger_id=trigger_id,
            view=modal_view
        )

    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR opening refine prompt modal: {error_info}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while opening the refinement modal: {e}"
        )

# Action handler for "Render Chart" button (AI-powered)
@app.action(RENDER_CHART_BUTTON_ACTION_ID)
def handle_render_chart_button_click(ack, body, client):
    ack()
    message_ts = body['message']['ts']
    channel_id = body['channel']['id']

    sql_query = global_sql_cache.get(message_ts)

    if not sql_query:
        client.chat_postMessage(
            channel=channel_id,
            text="Sorry, I couldn't retrieve the data for AI charting. The query might have expired or been cleared.",
        )
        return

    try:
        # Get the current DataFrame (the data the user is looking at)
        df = global_dataframe_cache.get(message_ts)
        
        if df is None:
            client.chat_postMessage(
                channel=channel_id,
                text="❌ No data available for AI charting. The data may have expired."
            )
            return
        
        # Show progress message
        analyzing_response = client.chat_postMessage(
            channel=channel_id,
            blocks=[
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "🤖 ", 
                                },
                                {
                                    "type": "text",
                                    "text": "AI is analyzing your data and creating an intelligent chart...",
                                    "style": {
                                        "bold": True
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],
        )
        analyzing_ts = analyzing_response['ts']
        
        if DEBUG:
            print(f"AI Chart: Using DataFrame with {len(df)} rows")
            print(f"AI Chart: DataFrame shape: {df.shape}")
            print(f"AI Chart: DataFrame columns: {list(df.columns)}")
            print(f"AI Chart: User prompt: {last_user_prompt_global}")

        # Create Snowpark session from existing connection
        from snowflake.snowpark import Session
        session = Session.builder.configs({"connection": CONN}).create()
        
        # Use AI-powered charting with the original user prompt
        fig = ai_plot(session, last_user_prompt_global, df)
        
        if fig:
            # Convert to image and upload to Slack
            import plotly.io as pio
            import tempfile
            import os
            
            try:
                # Save as PNG
                with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_file:
                    pio.write_image(fig, tmp_file.name, format='png', width=1200, height=800)
                
                # Upload to Slack without initial comment (we'll update the analyzing message instead)
                with open(tmp_file.name, 'rb') as f:
                    upload_response = client.files_upload_v2(
                        channel=channel_id,
                        file=f.read(),
                        filename="ai_chart.png",
                        title="AI-Generated Chart"
                    )
                
                # Clean up temp file
                os.unlink(tmp_file.name)
                
            except Exception as render_error:
                print(f"⚠️ Chart rendering failed: {str(render_error)}")
                # Post friendly error message to Slack using exact same rich_text structure as original message
                client.chat_update(
                    channel=channel_id,
                    ts=analyzing_ts,
                    text="❌ Chart Rendering Error",
                    blocks=[
                        {
                            "type": "rich_text",
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {
                                            "type": "text",
                                            "text": "📊 ",
                                        },
                                        {
                                            "type": "text",
                                            "text": f"Chart rendering failed\n\nDataset: {len(df):,} rows",
                                            "style": {
                                                "bold": True
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                )
                return  # Exit early, don't try to process upload
            
            if upload_response.get('ok'):
                # Update the original "analyzing" message with completion status (using same rich_text structure)
                chart_blocks = [
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_section",
                                "elements": [
                                    {
                                        "type": "text",
                                        "text": "✅ ", 
                                    },
                                    {
                                        "type": "text",
                                        "text": "AI Chart Complete! The chart below was intelligently selected based on your data and question.",
                                        "style": {
                                            "bold": True
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
                
                chart_response = client.chat_update(
                    channel=channel_id,
                    ts=analyzing_ts,
                    text="AI Chart Complete",
                    blocks=chart_blocks
                )
                
                # Cache the DataFrame for the chart message so buttons work
                chart_message_ts = chart_response['ts']
                global_dataframe_cache[chart_message_ts] = df
                global_sql_cache[chart_message_ts] = global_sql_cache.get(message_ts)
                global_original_dataframe_cache[chart_message_ts] = global_original_dataframe_cache.get(message_ts)
                
                if DEBUG:
                    print("AI Chart posted successfully to main channel with action buttons")
            else:
                client.chat_postMessage(
                    channel=channel_id,
                    text="❌ Could not upload AI-generated chart."
                )
        else:
            client.chat_postMessage(
                channel=channel_id,
                text="❌ Could not generate AI chart for this data."
            )

    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR rendering AI chart: {error_info}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"❌ AI Chart generation failed: {str(e)}"
        )

# Action handler for "Download Data" button
@app.action(DOWNLOAD_DATA_BUTTON_ACTION_ID)
def handle_download_data_button_click(ack, body, client):
    ack()
    message_ts = body['message']['ts']
    channel_id = body['channel']['id']

    sql_query = global_sql_cache.get(message_ts)

    if not sql_query:
        client.chat_postMessage(
            channel=channel_id,
            text="Sorry, I couldn't retrieve the data for download. The query might have expired or been cleared.",

        )
        return

    try:
        # MODIFIED: Used rich_text blocks for reliable bolding and emoji
        client.chat_postMessage(
            channel=channel_id,
            blocks=[
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "📥 ", # Unicode emoji
                                },
                                {
                                    "type": "text",
                                    "text": "Preparing data for download...",
                                    "style": {
                                        "bold": True
                                    }
                                }
                            ]
                        }
                    ]
                }
            ],

        )

        # Re-execute the SQL query to get the data with entitlement filtering
        filtered_sql = apply_entitlement_filter(sql_query)
        df = pd.read_sql(filtered_sql, CONN)

        if DEBUG:
            print(f"DEBUG: DataFrame shape for download: {df.shape}")
            if not df.empty:
                print(f"DEBUG: First few rows of DataFrame for download:\n{df.head().to_string()}")
            else:
                print("DEBUG: DataFrame is empty for download.")

        if df.empty:
            client.chat_postMessage(
                channel=channel_id,
                text="The query returned no data to download.",
    
            )
            return

        # Use BytesIO to create an in-memory file for CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        file_name = f"query_results_{int(time.time())}.csv"

        if DEBUG:
            print(f"DEBUG: Attempting to upload file '{file_name}' to channel '{channel_id}'")

        # Capture the response from Slack API for more detailed debugging
        upload_response = client.files_upload_v2(
            channel=channel_id,
            file=csv_buffer.getvalue().encode('utf-8'), # Encode to bytes for upload
            filename=file_name,
            title="Query Results Data",

            initial_comment=f"Here is the data generated by your query: `{file_name}`"
        )
        if DEBUG:
            print(f"DEBUG: Slack upload response: {upload_response}")
            if upload_response.get('ok'):
                print(f"DEBUG: File uploaded successfully: {upload_response.get('file', {}).get('permalink')}")
            else:
                print(f"DEBUG: File upload failed: {upload_response.get('error')}")
        
        # Post a follow-up message with action buttons after successful download
        if upload_response.get('ok'):
            download_blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Data download complete!* Your file `{file_name}` has been uploaded above."
                    }
                },
                get_action_buttons_block(include_show_sql=True, data_size=len(df), include_row_limit=True)
            ]
            
            download_response = client.chat_postMessage(
                channel=channel_id,
                text="Data download complete",
                blocks=download_blocks
            )
            
            # Cache the DataFrame for the download message so buttons work
            download_message_ts = download_response['ts']
            global_dataframe_cache[download_message_ts] = df
            global_sql_cache[download_message_ts] = sql_query
            global_original_dataframe_cache[download_message_ts] = global_original_dataframe_cache.get(message_ts, df)
            
            if DEBUG:
                print(f"DEBUG: Posted download completion message with buttons")


    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR downloading data: {error_info}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while trying to download the data: {e}",

        )


# Action handler for "Clear All Filters" button in modal
@app.action("clear_all_filters_button")
def handle_clear_all_filters_button_click(ack, body, client):
    ack()
    
    # Get the original message timestamp and channel from the modal's private_metadata
    try:
        private_metadata = body['view']['private_metadata']
        if "|" in private_metadata:
            message_ts, channel_id = private_metadata.split("|", 1)
        else:
            message_ts = private_metadata
            channel_id = body['channel']['id']  # fallback
        
        # Get the ORIGINAL unfiltered DataFrame from cache
        df = global_original_dataframe_cache.get(message_ts)
        if df is None:
            client.chat_postMessage(
                channel=channel_id,
                text="Sorry, I couldn't retrieve the original data. The query might have expired or been cleared.",
    
            )
            return
        
        # Create filtered result message with original (unfiltered) data
        result_blocks = create_filtered_result_message(df, [], len(df))  # Empty filters list means "no filters applied"
        
        # Post the original results as a new message in main channel
        response = client.chat_postMessage(
            channel=channel_id,
            text="Here are your original results (all filters cleared):",
            blocks=result_blocks
        )
        
        # Cache the original DataFrame with the new message timestamp
        new_message_ts = response['ts']
        global_dataframe_cache[new_message_ts] = df.copy()
        global_original_dataframe_cache[new_message_ts] = df.copy()  # Also cache as original
        
        # Clear any current filters for the new message (since all filters are cleared)
        global_current_filters_cache[new_message_ts] = {}
        
        # Also cache the original SQL query so other buttons work
        original_sql = global_sql_cache.get(message_ts)
        if original_sql:
            global_sql_cache[new_message_ts] = original_sql
        
        if DEBUG:
            print(f"Cleared all filters, cached original DataFrame with new message_ts: {new_message_ts}")
            
    except Exception as e:
        print(f"Error handling clear all filters: {e}")
        client.chat_postMessage(
            channel=body['channel']['id'],
            text=f"An error occurred while clearing filters: {e}",
            ephemeral=True
        )

# View submission handler for filter modal
@app.view("data_filter_modal")
def handle_filter_modal_submission(ack, body, client, view):
    ack()
    
    # Get the original message timestamp and channel from the modal's private_metadata
    try:
        private_metadata = view['private_metadata']
        if "|" in private_metadata:
            message_ts, channel_id = private_metadata.split("|", 1)
        else:
            message_ts = private_metadata
            channel_id = body['user']['id']  # fallback to DM
        
        # Extract filter values from the modal state
        filter_values = extract_filter_values_from_modal(view['state']['values'])
        
        # Get the original DataFrame from cache
        df = global_original_dataframe_cache.get(message_ts)
        if df is None:
            client.chat_postMessage(
                channel=channel_id,
                text="Sorry, I couldn't retrieve the data for filtering. The query might have expired or been cleared.",
                ephemeral=True
            )
            return
        
        # Apply filters
        filtered_df, applied_filters = apply_pandas_filters(df, filter_values)
        
        # Create filtered result message
        result_blocks = create_filtered_result_message(filtered_df, applied_filters, len(df))
        
        # Post the filtered results as a new message in main channel
        response = client.chat_postMessage(
            channel=channel_id,
            text="Here are your filtered results:",
            blocks=result_blocks
        )
        
        # Cache both the filtered and original DataFrames for the new message
        new_message_ts = response['ts']
        global_dataframe_cache[new_message_ts] = filtered_df
        global_original_dataframe_cache[new_message_ts] = df
        
        # Cache the current filter values for the new message
        global_current_filters_cache[new_message_ts] = filter_values
        
        # Also cache the original SQL query for the new message
        original_sql = global_sql_cache.get(message_ts)
        if original_sql:
            global_sql_cache[new_message_ts] = original_sql
        
        if DEBUG:
            print(f"Applied filters via modal submission, cached filtered DataFrame with new message_ts: {new_message_ts}")
            
    except Exception as e:
        print(f"Error handling filter modal submission: {e}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while applying filters: {e}",
            ephemeral=True
        )

# Action handler for "Filter Query" button
@app.action(FILTER_DATA_BUTTON_ACTION_ID)
def handle_filter_data_button_click(ack, body, client):
    ack()
    message_ts = body['message']['ts']
    channel_id = body['channel']['id']
    
    # Get the ORIGINAL unfiltered DataFrame from cache for modal creation
    original_df = global_original_dataframe_cache.get(message_ts)
    
    if original_df is None:
        client.chat_postMessage(
            channel=channel_id,
            text="Sorry, I couldn't retrieve the original data for filtering. The query might have expired or been cleared.",

        )
        return
    
    try:
        # Get current filters for this message (if any)
        current_filters = global_current_filters_cache.get(message_ts, {})
        
        # Create and open the filter modal using the original DataFrame and current filters
        modal = create_filter_modal(original_df, message_ts, channel_id, current_filters)
        client.views_open(
            trigger_id=body["trigger_id"],
            view=modal
        )
    except Exception as e:
        print(f"Error opening filter modal: {e}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"An error occurred while opening the filter dialog: {e}",

        )


# Modal submission handler for refine prompt modal
@app.view("refine_prompt_modal_submit")
def handle_refine_prompt_modal_submission(ack, body, client, view):
    """
    Handles the submission of the refine prompt modal.
    Processes the refined prompt and re-runs the analysis.
    """
    ack()
    
    global last_user_prompt_global
    
    try:
        # Get the original message timestamp and channel from private_metadata
        private_metadata = view.get("private_metadata", "")
        if not private_metadata:
            print("ERROR: No private_metadata found in modal submission")
            return
            
        message_ts, channel_id = private_metadata.split("|")
        
        # Get the refined prompt from the modal
        values = view["state"]["values"]
        refined_prompt_input = values["refined_prompt_input"]["refined_prompt_text"]
        refined_prompt = refined_prompt_input["value"]
        
        if not refined_prompt or not refined_prompt.strip():
            client.chat_postMessage(
                channel=channel_id,
                text="❌ Please provide a refined prompt before submitting."
            )
            return
        
        # Update the global prompt variable
        global last_user_prompt_global
        last_user_prompt_global = refined_prompt.strip()
        
        # Get the current row limit from the original message to preserve it
        current_row_limit = None
        try:
            # Fetch the original message to get current row limit setting
            original_message = client.conversations_history(
                channel=channel_id,
                latest=message_ts,
                limit=1,
                inclusive=True
            )
            
            if original_message["ok"] and original_message["messages"]:
                blocks = original_message["messages"][0].get("blocks", [])
                for block in blocks:
                    if block.get("type") == "actions":
                        for element in block.get("elements", []):
                            if element.get("action_id") == ROW_LIMIT_DROPDOWN_ACTION_ID:
                                # Get selected value from the dropdown
                                if "initial_option" in element:
                                    current_row_limit = int(element["initial_option"]["value"])
                                    print(f"🔄 Preserving row limit: {current_row_limit}")
                                break
        except Exception as e:
            print(f"Warning: Could not retrieve row limit from original message: {e}")
        
        # Post a message indicating we're processing the refined prompt
        client.chat_postMessage(
            channel=channel_id,
            blocks=[
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "✨ ",
                                },
                                {
                                    "type": "text",
                                    "text": f"Processing your refined prompt: '{refined_prompt}'",
                                    "style": {
                                        "bold": True
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        )
        
        # Instead of copy/paste hack, directly process the refined prompt
        # This gives the user the same experience as if they typed and sent the refined prompt
        
        # Update the global prompt variable for the background analysis
        last_user_prompt_global = refined_prompt
        
        # Create a fake message event to trigger normal processing
        fake_body = {
            "event": {
                "text": refined_prompt,
                "channel": channel_id,
                "ts": str(time.time()),  # Generate new timestamp
                "user": body["user"]["id"]
            },
            "team_id": body.get("team", {}).get("id", ""),
            "api_app_id": body.get("api_app_id", "")
        }
        
        # Create a fake say function that posts to the same channel
        def fake_say(text=None, blocks=None, **kwargs):
            return client.chat_postMessage(
                channel=channel_id,
                text=text,
                blocks=blocks,
                **kwargs
            )
        
        # Process the refined prompt through the normal message handler
        print(f"🔄 Processing refined prompt: {refined_prompt}")
        
        # Store the preserved row limit globally for the fake message processing
        global preserved_row_limit_for_refinement
        preserved_row_limit_for_refinement = current_row_limit
        
        # Create a fake ack function
        def fake_ack():
            pass
        
        handle_message_events(fake_ack, fake_body, fake_say)
        
        # Clear the preserved row limit after processing
        preserved_row_limit_for_refinement = None
        
        print(f"✅ Refined prompt processed successfully")
        
    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR handling refine prompt modal submission: {error_info}")
        
        # Try to get channel_id from the error context
        try:
            private_metadata = view.get("private_metadata", "")
            if private_metadata:
                _, channel_id = private_metadata.split("|")
                client.chat_postMessage(
                    channel=channel_id,
                    text=f"An error occurred while processing your refined prompt: {e}"
                )
        except:
            pass

# Modal submission handler for filter modal
@app.view(FILTER_MODAL_CALLBACK_ID)
def handle_filter_modal_submission(ack, body, client, view):
    ack()
    
    try:
        # Get the original message timestamp and channel from private_metadata
        private_metadata = view.get("private_metadata", "")
        if not private_metadata:
            print("Error: No private_metadata found in modal")
            return
        
        # Parse message_ts and channel_id from private_metadata
        if "|" in private_metadata:
            message_ts, channel_id = private_metadata.split("|", 1)
        else:
            message_ts = private_metadata
            channel_id = None  # Fallback - will need to handle this case
        
        # Get the ORIGINAL unfiltered DataFrame from cache
        df = global_original_dataframe_cache.get(message_ts)
        if df is None:
            print(f"Error: No original DataFrame found in cache for message_ts: {message_ts}")
            return
        
        # Extract filter values from modal
        filter_values = extract_filter_values_from_modal(view["state"]["values"])
        
        if DEBUG:
            print(f"Filter values extracted: {filter_values}")
        
        # Apply pandas filters
        filtered_df, applied_filters = apply_pandas_filters(df, filter_values)
        
        if DEBUG:
            print(f"Original DataFrame shape: {df.shape}")
            print(f"Filtered DataFrame shape: {filtered_df.shape}")
            print(f"Applied filters: {applied_filters}")
        
        # Create filtered result message
        result_blocks = create_filtered_result_message(filtered_df, applied_filters, len(df))
        
        if DEBUG:
            print(f"Filter Modal: Created result blocks, length: {len(result_blocks) if result_blocks else 'None'}")
            print(f"Filter Modal: Channel ID: {channel_id}")
        
        # Post the filtered results as a new message in main channel
        if channel_id:
            if DEBUG:
                print("Filter Modal: About to post filtered results message")
            
            response = client.chat_postMessage(
                channel=channel_id,
                text="Here are your filtered results:" if applied_filters else "Here are your original results (no filters applied):",
                blocks=result_blocks
            )
            
            if DEBUG:
                print(f"Filter Modal: Posted message with timestamp: {response.get('ts')}")
            
            # Cache the filtered DataFrame and original SQL with the new message timestamp
            new_message_ts = response['ts']
            global_dataframe_cache[new_message_ts] = filtered_df
            
            # Also cache the original SQL query so other buttons (like Show SQL) work
            original_sql = global_sql_cache.get(message_ts)
            if original_sql:
                global_sql_cache[new_message_ts] = original_sql
            
            # IMPORTANT: Propagate the original unfiltered DataFrame reference
            # Always trace back to the very first original DataFrame from SQL
            original_df = global_original_dataframe_cache.get(message_ts)
            if original_df is not None:
                global_original_dataframe_cache[new_message_ts] = original_df.copy()
                if DEBUG:
                    print(f"Propagated original DataFrame ({len(original_df)} rows) to new message")
            
            if DEBUG:
                print(f"Cached filtered DataFrame with new message_ts: {new_message_ts}")
                print(f"Also cached original SQL query for new message")
        else:
            print("Error: No channel_id available to post filtered results")
        
    except Exception as e:
        print(f"Error processing filter modal submission: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")


# --- Initialization and App Start ---

def init():
    """
    Initializes Snowflake connection and Cortex Chat Agent.
    """
    conn, cortex_app = None, None

    try:
        conn = snowflake.connector.connect(
            user=USER,
            authenticator="SNOWFLAKE_JWT",
            private_key_file=RSA_PRIVATE_KEY_PATH,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA,
            role=ROLE,
            host=HOST
        )
        if not conn.rest.token:
            raise Exception("Snowflake connection unsuccessful: No token received.")
        print(">>>>>>>>>> Snowflake connection successful.")
    except Exception as e:
        print(f"ERROR: Failed to connect to Snowflake: {e}")
        exit(1) # Exit if Snowflake connection fails

    try:
        cortex_app = cortex_chat.CortexChat(
            AGENT_ENDPOINT,
            SEARCH_SERVICE,
            SEMANTIC_MODEL,
            MODEL,
            ACCOUNT,
            USER,
            RSA_PRIVATE_KEY_PATH
        )
        print(">>>>>>>>>> Cortex Chat Agent initialized.")
    except Exception as e:
        print(f"ERROR: Failed to initialize Cortex Chat Agent: {e}")
        exit(1) # Exit if Cortex Chat Agent initialization fails

    print(">>>>>>>>>> Init complete")
    return conn, cortex_app

if __name__ == "__main__":
    CONN, CORTEX_APP = init()
    Root = Root(CONN) # Assuming Root is used elsewhere or for Snowpark Session
    print("Starting SocketModeHandler...")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()
    SocketModeHandler(app, SLACK_APP_TOKEN).start()