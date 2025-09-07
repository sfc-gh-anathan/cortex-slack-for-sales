"""
Data Filter Modal for Slack Sales Bot

This module provides modal-based filtering functionality for query results.
Users can click "Filter Query" to open a modal with various filter options,
then apply filters using pandas operations for fast in-memory filtering.
"""

import pandas as pd
from datetime import datetime, timedelta


# Constants for filter modal
FILTER_DATA_BUTTON_ACTION_ID = "filter_data_button"
FILTER_MODAL_CALLBACK_ID = "data_filter_modal"


def get_filter_data_button_element():
    """Create the Filter Query button element"""
    return {
        "type": "button",
        "text": {"type": "plain_text", "text": "Filter Query"},
        "action_id": FILTER_DATA_BUTTON_ACTION_ID,
        "style": "primary"
    }


def analyze_dataframe_for_filters(df):
    """
    Analyze the DataFrame to determine what filter options are available
    Returns a dict with available filter types and their options
    """
    filters_available = {
        "date_columns": [],
        "categorical_columns": {},
        "numeric_columns": [],
        "has_sales_data": False,
        "has_role_data": False,
        "has_region_data": False
    }
    
    # Check for date columns
    for col in df.columns:
        if 'DATE' in col.upper() or 'PERIOD' in col.upper():
            # Check if it's already a datetime type
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                filters_available["date_columns"].append(col)
            elif df[col].dtype == 'object':
                # Try to convert to datetime to verify it's a date column
                try:
                    pd.to_datetime(df[col].head(), errors='raise')
                    filters_available["date_columns"].append(col)
                except:
                    pass
    
    # Check for categorical columns with reasonable number of unique values
    for col in df.columns:
        if df[col].dtype == 'object' and col not in filters_available["date_columns"]:
            unique_vals = df[col].nunique()
            if 2 <= unique_vals <= 50:  # Expanded range for filter options (was 20, now 50)
                # Filter out None values before sorting to avoid comparison errors
                unique_values = [val for val in df[col].unique().tolist() if val is not None]
                filters_available["categorical_columns"][col] = sorted(unique_values)
    
    # Check for numeric columns that could be filtered by threshold
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            filters_available["numeric_columns"].append(col)
    
    # Check for specific business columns
    for col in df.columns:
        col_upper = col.upper()
        if 'SALES' in col_upper or 'AMOUNT' in col_upper or 'REVENUE' in col_upper:
            filters_available["has_sales_data"] = True
        if 'ROLE' in col_upper:
            filters_available["has_role_data"] = True
        if 'REGION' in col_upper:
            filters_available["has_region_data"] = True
    
    return filters_available


def create_filter_modal(df, message_ts, channel_id=None, current_filters=None):
    """
    Create a dynamic filter modal based on the DataFrame structure
    
    Args:
        df: DataFrame to analyze for filter options
        message_ts: Message timestamp for caching
        channel_id: Slack channel ID
        current_filters: Dict of current filter values to pre-populate the modal
    """
    filters_available = analyze_dataframe_for_filters(df)
    
    # Initialize current_filters if not provided
    if current_filters is None:
        current_filters = {}
    
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Filter your query results:*"}
        }
    ]
    
    # Date range filter (if date columns available)
    if filters_available["date_columns"]:
        date_col = filters_available["date_columns"][0]  # Use first date column
        blocks.extend([
            {
                "type": "input",
                "block_id": "date_start_block",
                "label": {"type": "plain_text", "text": f"Start Date ({date_col})"},
                "element": {
                    "type": "datepicker",
                    "action_id": "start_date",
                    "placeholder": {"type": "plain_text", "text": "Select start date"}
                },
                "optional": True
            },
            {
                "type": "input",
                "block_id": "date_end_block",
                "label": {"type": "plain_text", "text": f"End Date ({date_col})"},
                "element": {
                    "type": "datepicker",
                    "action_id": "end_date",
                    "placeholder": {"type": "plain_text", "text": "Select end date"}
                },
                "optional": True
            }
        ])
    
    # Categorical filters (for columns with reasonable number of options)
    for col_name, options in filters_available["categorical_columns"].items():
        if len(options) <= 10:  # Only show if not too many options
            slack_options = [
                {"text": {"type": "plain_text", "text": str(opt)}, "value": str(opt)} 
                for opt in options
            ]
            
            # Create the multi-select element
            element = {
                "type": "multi_static_select",
                "action_id": f"{col_name.lower()}_select",
                "placeholder": {"type": "plain_text", "text": f"Select {col_name.lower()}"},
                "options": slack_options
            }
            
            # Add initial values if current filters exist for this column
            current_filter_key = f"{col_name.lower()}_select"
            if current_filter_key in current_filters and current_filters[current_filter_key]:
                initial_options = []
                for filter_value in current_filters[current_filter_key]:
                    # Handle both dict format (from Slack) and string format
                    if isinstance(filter_value, dict) and 'value' in filter_value:
                        value = filter_value['value']
                    else:
                        value = str(filter_value)
                    
                    # Only add if the value exists in available options
                    if value in [str(opt) for opt in options]:
                        initial_options.append({
                            "text": {"type": "plain_text", "text": value},
                            "value": value
                        })
                
                if initial_options:
                    element["initial_options"] = initial_options
            
            blocks.append({
                "type": "input",
                "block_id": f"{col_name.lower()}_filter_block",
                "label": {"type": "plain_text", "text": col_name.replace('_', ' ').title()},
                "element": element,
                "optional": True
            })
    
    # Numeric threshold filters (for financial/monetary columns)
    sales_columns = [col for col in filters_available["numeric_columns"] 
                    if any(keyword in col.upper() for keyword in ['SALES', 'AMOUNT', 'REVENUE', 'COMMISSION', 'TOTAL', 'EARNINGS', 'QUOTA', 'COMPENSATION', 'PRICE', 'VALUE', 'DEAL'])]
    
    for sales_col in sales_columns[:2]:  # Limit to first 2 sales columns
        # Add minimum threshold
        min_element = {
            "type": "plain_text_input",
            "action_id": f"{sales_col.lower()}_min_threshold",
            "placeholder": {"type": "plain_text", "text": f"e.g., 100,000"}
        }
        
        # Add initial value if current filters exist
        min_filter_key = f"{sales_col.lower()}_min_threshold"
        if min_filter_key in current_filters and current_filters[min_filter_key]:
            min_element["initial_value"] = str(current_filters[min_filter_key])
        
        blocks.append({
            "type": "input",
            "block_id": f"{sales_col.lower()}_min_threshold_block",
            "label": {"type": "plain_text", "text": f"Minimum {sales_col.replace('_', ' ').title()}"},
            "element": min_element,
            "optional": True
        })
        
        # Add maximum threshold
        max_element = {
            "type": "plain_text_input",
            "action_id": f"{sales_col.lower()}_max_threshold",
            "placeholder": {"type": "plain_text", "text": f"e.g., 1,000,000"}
        }
        
        # Add initial value if current filters exist
        max_filter_key = f"{sales_col.lower()}_max_threshold"
        if max_filter_key in current_filters and current_filters[max_filter_key]:
            max_element["initial_value"] = str(current_filters[max_filter_key])
        
        blocks.append({
            "type": "input",
            "block_id": f"{sales_col.lower()}_max_threshold_block",
            "label": {"type": "plain_text", "text": f"Maximum {sales_col.replace('_', ' ').title()}"},
            "element": max_element,
            "optional": True
        })
    
    # Order By filter for sorting results
    # Create options for all columns plus ascending/descending
    order_by_options = []
    for col in df.columns:
        col_display = col.replace('_', ' ').title()
        order_by_options.extend([
            {"text": {"type": "plain_text", "text": f"{col_display} ↑"}, "value": f"{col}_asc"},
            {"text": {"type": "plain_text", "text": f"{col_display} ↓"}, "value": f"{col}_desc"}
        ])
    
    if order_by_options:
        blocks.append({
            "type": "input",
            "block_id": "order_by_block",
            "label": {"type": "plain_text", "text": "Order By"},
            "element": {
                "type": "static_select",
                "action_id": "order_by_select",
                "placeholder": {"type": "plain_text", "text": "Select sort order"},
                "options": order_by_options
            },
            "optional": True
        })
    
    # Top N filter for limiting results
    blocks.append({
        "type": "input",
        "block_id": "top_n_block",
        "label": {"type": "plain_text", "text": "Limit Results (Top N)"},
        "element": {
            "type": "plain_text_input",
            "action_id": "top_n",
            "placeholder": {"type": "plain_text", "text": "e.g., 50"}
        },
        "optional": True
    })
    
    # Add Clear All Filters button
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn", 
            "text": " "
        }
    })
    
    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Clear All Filters"
                },
                "style": "danger",
                "action_id": "clear_all_filters_button"
            }
        ]
    })
    
    # Add instruction text for clearing filters
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "💡 *Tip:* Leave all fields empty and click 'Apply Filters' to clear all filters and return to original results."
            }
        ]
    })
    
    modal = {
        "type": "modal",
        "callback_id": FILTER_MODAL_CALLBACK_ID,
        "title": {"type": "plain_text", "text": "Filter Query Results"},
        "submit": {"type": "plain_text", "text": "Apply Filters"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "private_metadata": f"{message_ts}|{channel_id}" if channel_id else message_ts,  # Store both message_ts and channel_id
        "blocks": blocks
    }
    
    print(f"DEBUG: Modal created with {len(blocks)} blocks")
    for i, block in enumerate(blocks):
        print(f"DEBUG: Block {i}: {block.get('type', 'unknown')}")
    
    return modal


def apply_pandas_filters(df, filter_values):
    """
    Apply filters to DataFrame using pandas operations
    Returns filtered DataFrame and a description of applied filters
    """
    filtered_df = df.copy()
    applied_filters = []
    
    # Apply date range filters
    date_columns = [col for col in df.columns if 'DATE' in col.upper() or 'PERIOD' in col.upper()]
    if date_columns:
        date_col = date_columns[0]
        
        start_date = filter_values.get('start_date')
        end_date = filter_values.get('end_date')
        
        if start_date or end_date:
            # Convert date column to datetime if it's not already
            if filtered_df[date_col].dtype == 'object':
                filtered_df[date_col] = pd.to_datetime(filtered_df[date_col])
            
            if start_date:
                start_datetime = pd.to_datetime(start_date)
                filtered_df = filtered_df[filtered_df[date_col] >= start_datetime]
                applied_filters.append(f"{date_col} >= {start_date}")
            
            if end_date:
                end_datetime = pd.to_datetime(end_date)
                filtered_df = filtered_df[filtered_df[date_col] <= end_datetime]
                applied_filters.append(f"{date_col} <= {end_date}")
    
    # Apply categorical filters
    for key, values in filter_values.items():
        if key.endswith('_select') and values:
            # Extract column name from key
            col_name = key.replace('_select', '').upper()
            
            # Find matching column (case-insensitive)
            matching_cols = [col for col in df.columns if col.upper() == col_name]
            if matching_cols:
                col = matching_cols[0]
                selected_values = [opt['value'] for opt in values]
                filtered_df = filtered_df[filtered_df[col].isin(selected_values)]
                applied_filters.append(f"{col} in {selected_values}")
    
    # Apply numeric threshold filters (min/max ranges)
    # Group min/max thresholds by column
    threshold_filters = {}
    for key, value in filter_values.items():
        if (key.endswith('_min_threshold') or key.endswith('_max_threshold') or key.endswith('_threshold')) and value:
            try:
                # Remove commas from the value before converting to float
                clean_value = str(value).replace(',', '')
                threshold = float(clean_value)
                
                # Extract column name and threshold type
                if key.endswith('_min_threshold'):
                    col_name = key.replace('_min_threshold', '').upper()
                    threshold_type = 'min'
                elif key.endswith('_max_threshold'):
                    col_name = key.replace('_max_threshold', '').upper()
                    threshold_type = 'max'
                else:  # backward compatibility for '_threshold'
                    col_name = key.replace('_threshold', '').upper()
                    threshold_type = 'min'  # treat old format as minimum
                
                if col_name not in threshold_filters:
                    threshold_filters[col_name] = {}
                threshold_filters[col_name][threshold_type] = threshold
            except ValueError:
                pass  # Skip invalid numeric values
    
    # Apply the threshold filters
    for col_name, thresholds in threshold_filters.items():
        # Find matching column (case-insensitive)
        matching_cols = [col for col in df.columns if col.upper() == col_name]
        if matching_cols:
            col = matching_cols[0]
            
            # Apply minimum threshold
            if 'min' in thresholds:
                min_threshold = thresholds['min']
                filtered_df = filtered_df[filtered_df[col] >= min_threshold]
                applied_filters.append(f"{col} >= {min_threshold:,.0f}")
            
            # Apply maximum threshold
            if 'max' in thresholds:
                max_threshold = thresholds['max']
                filtered_df = filtered_df[filtered_df[col] <= max_threshold]
                applied_filters.append(f"{col} <= {max_threshold:,.0f}")
    
    # Apply order by sorting
    order_by = filter_values.get('order_by_select')
    if order_by and 'value' in order_by:
        order_value = order_by['value']
        if '_asc' in order_value:
            column = order_value.replace('_asc', '')
            if column in filtered_df.columns:
                filtered_df = filtered_df.sort_values(by=column, ascending=True)
                col_display = column.replace('_', ' ').title()
                applied_filters.append(f"Ordered by {col_display} (ascending)")
        elif '_desc' in order_value:
            column = order_value.replace('_desc', '')
            if column in filtered_df.columns:
                filtered_df = filtered_df.sort_values(by=column, ascending=False)
                col_display = column.replace('_', ' ').title()
                applied_filters.append(f"Ordered by {col_display} (descending)")
    
    # Apply top N limit
    top_n = filter_values.get('top_n')
    if top_n:
        try:
            n = int(top_n)
            if n > 0 and n < len(filtered_df):
                filtered_df = filtered_df.head(n)
                applied_filters.append(f"Limited to top {n} rows")
        except ValueError:
            pass  # Skip invalid numeric values
    
    return filtered_df, applied_filters


def extract_filter_values_from_modal(view_state):
    """
    Extract filter values from the modal submission
    """
    filter_values = {}
    
    for block_id, block_data in view_state.items():
        for action_id, action_data in block_data.items():
            if action_id == 'start_date':
                filter_values['start_date'] = action_data.get('selected_date')
            elif action_id == 'end_date':
                filter_values['end_date'] = action_data.get('selected_date')
            elif action_id.endswith('_select'):
                if action_id == 'order_by_select':
                    # Single select for order by
                    filter_values[action_id] = action_data.get('selected_option')
                else:
                    # Multi select for other filters
                    filter_values[action_id] = action_data.get('selected_options', [])
            elif action_id.endswith('_min_threshold') or action_id.endswith('_max_threshold'):
                filter_values[action_id] = action_data.get('value')
            elif action_id.endswith('_threshold'):  # Keep for backward compatibility
                filter_values[action_id] = action_data.get('value')
            elif action_id == 'top_n':
                filter_values['top_n'] = action_data.get('value')
    
    return filter_values


def _convert_filter_to_friendly_format(filter_desc):
    """
    Convert technical filter descriptions to user-friendly format
    Examples:
    - "ROLE in ['CRO', 'Sales Manager']" -> "Role: CRO, Sales Manager"
    - "TOTAL_SALES >= 1000000" -> "Total Sales: >= $1,000,000"
    - "START_DATE >= 2024-01-01" -> "Start Date: >= 2024-01-01"
    """
    import re
    
    # Handle "in" filters (categorical)
    in_match = re.match(r"(\w+) in \[(.+)\]", filter_desc)
    if in_match:
        column = in_match.group(1).replace('_', ' ').title()
        values_str = in_match.group(2)
        # Remove quotes and split by comma
        values = [v.strip().strip("'\"") for v in values_str.split(',')]
        return f"{column}: {', '.join(values)}"
    
    # Handle comparison filters (>=, <=, >, <, =)
    comp_match = re.match(r"(\w+) ([><=]+) (.+)", filter_desc)
    if comp_match:
        column = comp_match.group(1).replace('_', ' ').title()
        operator = comp_match.group(2)
        value = comp_match.group(3)
        
        # Format currency values
        if ('SALES' in comp_match.group(1) or 'AMOUNT' in comp_match.group(1) or 
            'REVENUE' in comp_match.group(1) or 'TOTAL' in comp_match.group(1) or
            'COST' in comp_match.group(1) or 'PRICE' in comp_match.group(1) or 
            'VALUE' in comp_match.group(1)):
            try:
                numeric_value = float(value.replace(',', ''))  # Remove existing commas first
                formatted_value = f"${numeric_value:,.0f}"
            except ValueError:
                formatted_value = value
        else:
            try:
                # For non-currency numeric values, still add commas for readability
                numeric_value = float(value.replace(',', ''))
                if numeric_value >= 1000:
                    formatted_value = f"{numeric_value:,.0f}"
                else:
                    formatted_value = value
            except ValueError:
                formatted_value = value
        
        # Use mathematical symbols
        friendly_operator = operator
        
        return f"{column}: {friendly_operator} {formatted_value}"
    
    # Fallback - return original with some cleanup
    return filter_desc.replace('_', ' ').title()


def create_filtered_result_message(filtered_df, applied_filters, original_count):
    """
    Create the message blocks for displaying filtered results
    """
    from app import _get_safe_table_text, get_action_buttons_block
    
    blocks = []
    
    # Add filter summary with user-friendly formatting
    if applied_filters:
        # Convert technical filter descriptions to user-friendly format
        friendly_filters = []
        for filter_desc in applied_filters:
            friendly_filter = _convert_filter_to_friendly_format(filter_desc)
            friendly_filters.append(friendly_filter)
        
        filter_summary = "🔍 Applied Filters:\n" + "\n".join(friendly_filters)
        filter_summary += f"\n\nResults: {len(filtered_df):,} of {original_count:,} rows"
    else:
        filter_summary = "📊 All Data - no filters applied"
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": filter_summary
        }
    })
    
    # Add filtered data table
    if len(filtered_df) > 0:
        # Get the actual table text and number of rows displayed
        table_text, actual_rows_displayed = _get_safe_table_text(filtered_df, "", min(len(filtered_df), 25))
        
        blocks.append({
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_quote",
                    "elements": [
                        {
                            "type": "text",
                            "text": "Filtered Results:"
                        }
                    ]
                },
                {
                    "type": "rich_text_preformatted",
                    "elements": [
                        {
                            "type": "text",
                            "text": table_text
                        }
                    ]
                }
            ]
        })
        
        # Add action buttons for the filtered results using the actual rows displayed
        blocks.append(get_action_buttons_block(include_show_sql=False, data_size=len(filtered_df), selected_row_limit=actual_rows_displayed))
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🚫 *There were no results for your query. This may be a permissions issue.*"
            }
        })
        
        # Add action buttons even for empty filtered results
        blocks.append(get_action_buttons_block(include_show_sql=False, data_size=0, include_row_limit=False))
    
    return blocks


def get_sample_data_for_filters(df, max_samples=5):
    """
    Get sample data to show users what values are available for filtering
    """
    samples = {}
    
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].nunique() <= 20:
            unique_vals = df[col].unique()
            samples[col] = unique_vals[:max_samples].tolist()
        elif pd.api.types.is_numeric_dtype(df[col]):
            samples[col] = {
                'min': float(df[col].min()),
                'max': float(df[col].max()),
                'mean': float(df[col].mean())
            }
    
    return samples
