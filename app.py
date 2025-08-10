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

# Import charting functions from the new file
from chart_utils import select_and_plot_chart, upload_chart_to_slack
# Import experimental intelligent charting
from chart_utils_experimental import create_intelligent_chart
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
SQL_SHOW_BUTTON_ACTION_ID = "show_full_sql_query_button"
REFINE_QUERY_BUTTON_ACTION_ID = "refine_query_button"
RENDER_CHART_BUTTON_ACTION_ID = "render_chart_button"
DOWNLOAD_DATA_BUTTON_ACTION_ID = "download_data_button"
ROW_LIMIT_DROPDOWN_ACTION_ID = "row_limit_select"

# Global variable to store the last user prompt
last_user_prompt_global = ""

# Constants for Snowflake stored procedure parameters
SNOWFLAKE_STAGE_PATH = '@"SLACK_SALES_DEMO"."SLACK_SCHEMA"."SLACK_SEMANTIC_MODELS"'
SNOWFLAKE_FILE_NAME = 'sales_semantic_model.yaml'


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

# Helper for Refine Query button element
def get_refine_query_button_element():
    """
    Returns the Slack Block Kit element for the "Refine Prompt" button.
    """
    return {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Refine Prompt",
            "emoji": True
        },
        "style": "primary",
        "action_id": REFINE_QUERY_BUTTON_ACTION_ID
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
def get_row_limit_dropdown_element(data_size=None):
    """
    Returns the Slack Block Kit element for the row limit dropdown.
    Always defaults to 10 rows and never shows more options than total rows.
    """
    # Always default to 10 rows
    default_value = "10"
    
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
            "text": {"type": "plain_text", "text": str(value)}, 
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

# Helper for Render Chart button element
def get_render_chart_button_element():
    """
    Returns the Slack Block Kit element for the "Render Chart" button.
    """
    return {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Render Chart",
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

# NEW: Combined actions block for all four buttons
def get_action_buttons_block(include_show_sql=True, data_size=None, include_row_limit=True): # MODIFIED: Added include_row_limit parameter
    """
    Returns a Slack Block Kit 'actions' block containing desired buttons on the same row.
    """
    elements = []
    
    # Add row limit dropdown first (left-most position) - only for filtered results, not charts
    if include_row_limit:
        elements.append(get_row_limit_dropdown_element(data_size))
    
    if include_show_sql: # Only add if requested
        elements.append(get_show_sql_query_button_element())

    elements.extend([
        get_filter_data_button_element(),
        get_refine_query_button_element(),
        get_render_chart_button_element(),
        get_download_data_button_element()
    ])

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
        return full_text
    
    # Only reduce if absolutely necessary and the request was large
    if requested_rows and requested_rows > 25:
        # For large requests that don't fit, try half the requested amount
        fallback_rows = max(10, requested_rows // 2)
        display_df = df.head(fallback_rows).copy()
        display_df = _format_dataframe_for_display(display_df)
        base_table_text = display_df.to_string(index=False)
        row_message = f"\n\n(Showing {fallback_rows:,} of {len(df):,} rows - table too wide for {requested_rows:,} rows.)"
        full_text = base_table_text + truncated_message + row_message
        
        if len(full_text) <= 2800:
            return full_text
    
    # Last resort: reduce to a safe minimum
    safe_rows = 10
    while safe_rows > 3:
        display_df = df.head(safe_rows).copy()
        display_df = _format_dataframe_for_display(display_df)
        base_table_text = display_df.to_string(index=False)
        row_message = f"\n\n(Showing {safe_rows:,} of {len(df):,} rows - table too wide for full display.)"
        full_text = base_table_text + truncated_message + row_message
        
        if len(full_text) <= 2800:
            return full_text
        
        safe_rows -= 2
    
    # If even 3 rows is too long (very wide table), truncate the text
    display_df = df.head(3).copy()
    display_df = _format_dataframe_for_display(display_df)
    table_text = display_df.to_string(index=False)
    if len(table_text) > 2700:
        table_text = table_text[:2700] + "..."
    table_text += f"\n\n(Table truncated for Slack display. Use dropdown to adjust.)"
    
    return table_text

def display_agent_response(content, say, app_client, original_body):
    """
    Displays the agent's response, handling both SQL results (with charts)
    and unstructured text responses.
    """
    channel_id = original_body['event']['channel']

    final_blocks = []

    if content['sql']:
        sql = content['sql']

        df = pd.read_sql(sql, CONN)

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

        # Handle Single-Row Answers Specifically
        if len(df) == 1:
            formatted_answer = ""
            for col in df.columns:
                formatted_answer += f"*{col.replace('_', ' ').title()}*: {df[col].iloc[0]}\n"

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
                                "text": _get_safe_table_text(display_df, "", 10)
                            }
                        ]
                    }
                ]
            })

        # Add the combined action buttons block, including Show SQL Query initially.
        final_blocks.append(get_action_buttons_block(include_show_sql=True, data_size=len(df))) # Pass data size for smart default

        # Send the initial message and capture its timestamp (ts)
        try:
            post_response = app_client.chat_postMessage(
                channel=channel_id,
                blocks=final_blocks,
                text="Your query results are ready."
            )
            message_ts = post_response['ts']

            # Store the full SQL query and DataFrame in the global cache, keyed by message_ts
            global_sql_cache[message_ts] = sql
            global_dataframe_cache[message_ts] = df
            global_original_dataframe_cache[message_ts] = df.copy()  # Store original unfiltered data

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
        if block.get("type") == "actions" and any(e.get('action_id') in [REFINE_QUERY_BUTTON_ACTION_ID, SQL_SHOW_BUTTON_ACTION_ID, RENDER_CHART_BUTTON_ACTION_ID, DOWNLOAD_DATA_BUTTON_ACTION_ID, ROW_LIMIT_DROPDOWN_ACTION_ID] for e in block['elements']):
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
            # Re-execute the SQL query
            df = pd.read_sql(sql_query, CONN)
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
        
        # Apply the selected row limit
        df_limited = df.head(selected_limit)
        
        # Get current blocks and rebuild with new data
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
        if len(df) > selected_limit:
            truncated_message = f"\n(Showing {selected_limit} of {len(df)} total rows)"
        else:
            truncated_message = ""
        
        safe_table_content = _get_safe_table_text(df_limited, truncated_message, selected_limit)
        table_text = f"```\n{safe_table_content}\n```"
        
        updated_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": table_text
            }
        })
        
        # Re-add the action buttons with updated dropdown selection
        updated_blocks.append(get_action_buttons_block(include_show_sql=True, data_size=len(df)))
        
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
                    "text": f"**Bottom Line:** {refinement_message}"
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

# Action handler for "Render Chart" button
@app.action(RENDER_CHART_BUTTON_ACTION_ID)
def handle_render_chart_button_click(ack, body, client):
    ack()
    message_ts = body['message']['ts']
    channel_id = body['channel']['id']

    sql_query = global_sql_cache.get(message_ts)

    # Debug cache status
    if DEBUG:
        print(f"Chart button clicked for message_ts: {message_ts}")
        print(f"Cache has {len(global_sql_cache)} entries")
        print(f"Cache keys: {list(global_sql_cache.keys())}")
        print(f"SQL query found: {sql_query is not None}")

    if not sql_query:
        client.chat_postMessage(
            channel=channel_id,
            text="Sorry, I couldn't retrieve the data for charting. The query might have expired or been cleared.",

        )
        return

    try:
        # Get the current DataFrame (the data the user is looking at)
        df = global_dataframe_cache.get(message_ts)
        
        if df is None:
            client.chat_postMessage(
                channel=channel_id,
                text="❌ No data available for charting. The data may have expired."
            )
            return
        
        if DEBUG:
            print(f"Chart: Using DataFrame with {len(df)} rows")
            print(f"Chart: DataFrame shape: {df.shape}")
            print(f"Chart: DataFrame columns: {list(df.columns)}")
            print(f"Chart: First few rows:\n{df.head()}")

        # Simple chart generation - use fallback method only for consistency
        chart_img_url = select_and_plot_chart(df, client)
        
        if chart_img_url:
            # Post chart with action buttons (but no record count dropdown)
            chart_blocks = [
                {
                    "type": "image",
                    "image_url": chart_img_url,
                    "alt_text": "Generated Chart"
                },
                get_action_buttons_block(include_show_sql=True, data_size=None, include_row_limit=False)
            ]
            
            chart_response = client.chat_postMessage(
                channel=channel_id,
                text="Chart generated",
                blocks=chart_blocks
            )
            
            # Cache the DataFrame for the chart message so buttons work
            chart_message_ts = chart_response['ts']
            global_dataframe_cache[chart_message_ts] = df
            global_sql_cache[chart_message_ts] = global_sql_cache.get(message_ts)
            global_original_dataframe_cache[chart_message_ts] = global_original_dataframe_cache.get(message_ts)
            
            if DEBUG:
                print("Chart posted successfully to main channel with action buttons (no record count)")
                print(f"Chart message response: {chart_response.get('ok', False)}")
        else:
            client.chat_postMessage(
                channel=channel_id,
                text="❌ Could not generate chart for this data."
            )

    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"ERROR rendering chart: {error_info}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"❌ Chart generation failed: {str(e)}"
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

        # Re-execute the SQL query to get the data
        df = pd.read_sql(sql_query, CONN)

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
        # Create and open the filter modal using the original DataFrame
        modal = create_filter_modal(original_df, message_ts, channel_id)
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