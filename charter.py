import plotly.express as px
import pandas as pd
import json
import ast
import os
from snowflake.snowpark import Session

# Dictionary mapping string keys to corresponding Plotly Express functions.
PX_FUNCTIONS = {
    "scatter": px.scatter,
    "line": px.line,
    "area": px.area,
    "bar": px.bar,
    "histogram": px.histogram,
    "violin": px.violin,
    "box": px.box,
    "strip": px.strip,
    "funnel": px.funnel,
    "funnel_area": px.funnel_area,
    "scatter_3d": px.scatter_3d,
    "line_3d": px.line_3d,
    "scatter_ternary": px.scatter_ternary,
    "line_ternary": px.line_ternary,
    "scatter_mapbox": px.scatter_mapbox,
    "line_mapbox": px.line_mapbox,
    "density_mapbox": px.density_mapbox,
    "choropleth_mapbox": px.choropleth_mapbox,
    "scatter_geo": px.scatter_geo,
    "line_geo": px.line_geo,
    "choropleth": px.choropleth,
    "scatter_polar": px.scatter_polar,
    "line_polar": px.line_polar,
    "bar_polar": px.bar_polar,
    "scatter_matrix": px.scatter_matrix,
    "imshow": px.imshow,
    "density_contour": px.density_contour,
    "density_heatmap": px.density_heatmap,
    "pie": px.pie,
    "treemap": px.treemap,
    "sunburst": px.sunburst,
    "parallel_coordinates": px.parallel_coordinates,
    "parallel_categories": px.parallel_categories,
    "timeline": px.timeline
}

def _get_kwargs(function_arguments) -> dict:
    """
    Processes a list of function argument dictionaries and converts them into
    a dictionary of keyword arguments suitable for Plotly Express functions.

    Args:
        function_arguments (list of dict): A list where each dictionary contains:
            - 'argument_name' (str): The name of the argument.
            - 'argument_type' (str): The expected type of the argument (e.g., "NUMBER", "BOOLEAN").
            - 'argument_value' (str): The string representation of the argument's value.

    Returns:
        dict: A dictionary of processed keyword arguments.
    """
    kwargs = {}
    for arg in function_arguments:
        arg_name = arg.get('argument_name')  # Extract the argument name
        arg_type = arg.get('argument_type')  # Extract the expected type
        arg_value = arg.get('argument_value')  # Extract the value as string

        # Convert the argument value based on its expected type
        if arg_type == 'NUMBER':
            try:
                new_arg_value = float(arg_value)  # Convert numeric values to float
            except (ValueError, TypeError):
                new_arg_value = arg_value  # Keep as string if conversion fails
        elif arg_type == 'BOOLEAN':
            new_arg_value = arg_value.lower() == 'true'  # Convert to boolean
        elif arg_type == 'LIST':
            try:
                new_arg_value = ast.literal_eval(arg_value)
            except (ValueError, SyntaxError, TypeError):
                # If ast.literal_eval fails, try to parse as a simple list
                try:
                    new_arg_value = json.loads(arg_value)
                except (json.JSONDecodeError, TypeError):
                    # Fallback: treat as string and split by comma
                    if isinstance(arg_value, str):
                        # Clean up malformed brackets and df references
                        cleaned_value = arg_value.strip()
                        # Remove malformed df[] references and brackets
                        if "df['" in cleaned_value:
                            # Extract column names from df['column'] patterns
                            import re
                            matches = re.findall(r"df\['([^']+)'\]", cleaned_value)
                            if matches:
                                new_arg_value = matches
                            else:
                                new_arg_value = [cleaned_value]
                        elif ',' in cleaned_value:
                            new_arg_value = [item.strip().strip('"\'[]') for item in cleaned_value.split(',')]
                        else:
                            # Single value, clean it up
                            new_arg_value = cleaned_value.strip('"\'[]')
                    else:
                        new_arg_value = arg_value
        elif arg_type == 'DICT':
            try:
                new_arg_value = ast.literal_eval(arg_value)
            except (ValueError, SyntaxError, TypeError):
                # If ast.literal_eval fails, try JSON parsing
                try:
                    new_arg_value = json.loads(arg_value)
                except (json.JSONDecodeError, TypeError):
                    # Fallback: keep as string
                    new_arg_value = arg_value
        else:
            new_arg_value = arg_value
        kwargs[arg_name] = new_arg_value  # Store the processed argument
    
    # Fix common parameter issues
    # If x or y is a list, use only the first element (most common case)
    if 'x' in kwargs and isinstance(kwargs['x'], list):
        print(f"⚠️ FIXING: x parameter was a list {kwargs['x']}, using first element")
        kwargs['x'] = kwargs['x'][0] if kwargs['x'] else None
    
    if 'y' in kwargs and isinstance(kwargs['y'], list):
        print(f"⚠️ FIXING: y parameter was a list {kwargs['y']}, using first element")
        kwargs['y'] = kwargs['y'][0] if kwargs['y'] else None
    
    # Fix facet_col_wrap - must be an integer, not float
    if 'facet_col_wrap' in kwargs and isinstance(kwargs['facet_col_wrap'], float):
        print(f"⚠️ FIXING: facet_col_wrap parameter was a float {kwargs['facet_col_wrap']}, converting to int")
        kwargs['facet_col_wrap'] = int(kwargs['facet_col_wrap'])
    
    # Fix facet spacing for charts with many subplots
    if 'facet_col' in kwargs and 'facet_col_wrap' in kwargs:
        # Estimate number of rows based on unique values in facet column
        try:
            if hasattr(processed_data, 'columns') and kwargs['facet_col'] in processed_data.columns:
                unique_facets = processed_data[kwargs['facet_col']].nunique()
                facet_wrap = kwargs.get('facet_col_wrap', 3)
                estimated_rows = (unique_facets + facet_wrap - 1) // facet_wrap  # Ceiling division
                
                # If we have many rows, adjust spacing to prevent Plotly error
                if estimated_rows > 10:
                    max_spacing = 1.0 / (estimated_rows - 1) if estimated_rows > 1 else 0.02
                    safe_spacing = min(0.02, max_spacing * 0.8)  # Use 80% of max to be safe
                    kwargs['facet_row_spacing'] = safe_spacing
                    print(f"⚠️ FIXING: Many facets detected ({unique_facets} facets, ~{estimated_rows} rows), setting facet_row_spacing to {safe_spacing:.4f}")
        except Exception as e:
            print(f"⚠️ WARNING: Could not estimate facet spacing: {e}")
            # Set a conservative default
            kwargs['facet_row_spacing'] = 0.01
    
    # Fix invalid column references (AI trying to create computed columns)
    if 'x' in kwargs and isinstance(kwargs['x'], str):
        if 'str(' in kwargs['x'] or 'for row in' in kwargs['x'] or '+' in kwargs['x']:
            print(f"⚠️ FIXING: Invalid x column reference '{kwargs['x']}', defaulting to PERIOD_QUARTER")
            kwargs['x'] = 'PERIOD_QUARTER'
    
    if 'y' in kwargs and isinstance(kwargs['y'], str):
        if 'str(' in kwargs['y'] or 'for row in' in kwargs['y'] or '+' in kwargs['y']:
            print(f"⚠️ FIXING: Invalid y column reference '{kwargs['y']}', defaulting to AVG_QUOTA_ATTAINMENT")
            kwargs['y'] = 'AVG_QUOTA_ATTAINMENT'
    
    # For time-series data, if we have separate year/quarter columns, prefer quarter for x-axis
    if 'x' in kwargs and kwargs['x'] == 'PERIOD_YEAR' and 'PERIOD_QUARTER' in [kwargs.get('color'), kwargs.get('facet_col')]:
        print("⚠️ FIXING: Swapping PERIOD_YEAR to PERIOD_QUARTER for better time-series visualization")
        kwargs['x'] = 'PERIOD_QUARTER'
    
    return kwargs


def _plot_with_px(name: str, data_frame, **kwargs):
    """
    Generates a Plotly Express chart dynamically based on a string key.

    Args:
        name (str): The string key representing the desired Plotly Express function.
                    If prefixed with 'px.', it will be stripped.
        data_frame (pandas.DataFrame): The dataset to visualize.
        **kwargs: Additional keyword arguments to be passed to the selected Plotly Express function.

    Returns:
        plotly.graph_objects.Figure: A Plotly figure object.

    Raises:
        ValueError: If the provided name does not match any available Plotly Express function.
    """
    if name.startswith('px.'):
        name = name[3:]  # Remove 'px.' prefix if present

    func = PX_FUNCTIONS.get(name)  # Retrieve the corresponding Plotly function
    if func is None:
        raise ValueError(f"No Plotly Express function found for key '{name}'")

    # Pre-process datetime columns to avoid Plotly datetime bugs
    processed_data = data_frame.copy()
    if 'x' in kwargs:
        x_col = kwargs['x']
        if x_col in processed_data.columns and processed_data[x_col].dtype.name.startswith('datetime'):
            # Convert datetime to string format for plotting
            processed_data[x_col] = processed_data[x_col].dt.strftime('%Y-%m')
            print(f"Converted datetime column '{x_col}' to string format for plotting")
    
    fig = func(processed_data, **kwargs)  # Generate the Plotly figure
    
    # Add dataset description and enhance chart information
    data_description = f"Dataset: {processed_data.shape[0]} records across {processed_data.shape[1]} columns"
    
    # Get the original prompt and sampling info from the ai_plot function call context
    prompt_info = getattr(_plot_with_px, '_current_prompt', None)
    was_sampled = getattr(_plot_with_px, '_was_sampled', False)
    original_size = getattr(_plot_with_px, '_original_size', len(data_frame))
    
    # Add sampling info to data description if applicable
    final_data_description = data_description
    if was_sampled:
        final_data_description += f" (Showing {len(data_frame):,} of {original_size:,} total records)"
    
    if 'title' in kwargs:
        # Enhance the title with prompt and dataset info (keep short to avoid filename issues)
        title_parts = [kwargs['title']]
        if prompt_info:
            # Truncate long prompts to prevent filename issues
            short_prompt = prompt_info[:50] + "..." if len(prompt_info) > 50 else prompt_info
            title_parts.append(f"<i>Question: {short_prompt}</i>")
        title_parts.append(f"<sub>{final_data_description}</sub>")
        enhanced_title = "<br>".join(title_parts)
        fig.update_layout(title=enhanced_title)
    else:
        base_title = prompt_info[:50] + "..." if prompt_info and len(prompt_info) > 50 else (prompt_info if prompt_info else "Data Analysis")
        enhanced_title = f"{base_title}<br><sub>{final_data_description}</sub>"
        fig.update_layout(title=enhanced_title)
    
    # Debug: Print actual data values before chart generation
    print("=== CHART DEBUG INFO ===")
    print(f"DataFrame shape: {data_frame.shape}")
    print(f"DataFrame dtypes: {data_frame.dtypes.to_dict()}")
    if 'x' in kwargs:
        x_col = kwargs['x']
        if x_col in data_frame.columns:
            print(f"X-axis column '{x_col}' sample values:")
            print(data_frame[x_col].head().tolist())
            print(f"X-axis column '{x_col}' data type: {data_frame[x_col].dtype}")
            print(f"X-axis column '{x_col}' min/max: {data_frame[x_col].min()} / {data_frame[x_col].max()}")
    if 'y' in kwargs:
        y_col = kwargs['y']
        if y_col in data_frame.columns:
            print(f"Y-axis column '{y_col}' sample values:")
            print(data_frame[y_col].head().tolist())
            print(f"Y-axis column '{y_col}' data type: {data_frame[y_col].dtype}")
            print(f"Y-axis column '{y_col}' min/max: {data_frame[y_col].min()} / {data_frame[y_col].max()}")
    print("========================")
    
    # Format large numbers to avoid scientific notation
    # Helper function for Y-axis currency formatting (same as used for labels)
    def format_currency_axis(value):
        if abs(value) >= 1_000_000_000:
            return f'${value/1_000_000_000:.1f}B'
        elif abs(value) >= 1_000_000:
            return f'${value/1_000_000:.1f}M'
        elif abs(value) >= 1_000:
            return f'${value/1_000:.0f}K'
        else:
            return f'${value:.0f}'
    
    # Apply custom abbreviated currency formatting to Y-axis ticks
    if 'y' in kwargs:
        y_col = kwargs['y']
        if y_col in processed_data.columns:
            # Get the range of Y values to determine appropriate tick values
            y_min = processed_data[y_col].min()
            y_max = processed_data[y_col].max()
            
            # Create custom tick values and labels
            import numpy as np
            tick_count = 6  # Number of ticks on Y-axis
            tick_values = np.linspace(y_min, y_max, tick_count)
            tick_labels = [format_currency_axis(val) for val in tick_values]
            
            fig.update_layout(
                yaxis=dict(
                    tickmode='array',
                    tickvals=tick_values,
                    ticktext=tick_labels,
                    hoverformat=',.2f',
                    exponentformat='none'
                )
            )
    else:
        # Fallback formatting for non-currency data
        fig.update_layout(
            yaxis=dict(
                tickformat=',.0f',  # Format numbers with commas, no decimals for y-axis
                hoverformat=',.2f',  # Format hover text with commas and 2 decimals
                exponentformat='none'  # Disable scientific notation
            )
        )
    
    # Handle X-axis formatting - check if it's numeric (datetime is now converted to strings)
    if 'x' in kwargs:
        x_col = kwargs['x']
        if x_col in data_frame.columns and data_frame[x_col].dtype in ['int64', 'float64']:
            # For numeric columns, use comma formatting
            fig.update_layout(
                xaxis=dict(
                    tickformat=',.0f',  # Format x-axis numbers with commas
                    exponentformat='none'  # Disable scientific notation on x-axis too
                )
            )
    
    # Enhanced hover template for better user experience
    if 'y' in kwargs:
        y_col = kwargs['y']
        if y_col in processed_data.columns:
            # Update traces with enhanced hover information only
            trace_updates = {
                'hovertemplate': f'<b>%{{x}}</b><br>{y_col}: %{{y:,.0f}}<br><extra></extra>'  # Enhanced hover with column name
            }
            
            # Apply the updates to all traces
            fig.update_traces(**trace_updates)
    
    # Fallback hover formatting for traces without specific formatting
    fig.update_traces(
        hovertemplate='%{y:,.0f}<extra></extra>'  # Custom hover format to avoid scientific notation
    )
    
    # Enhance overall chart appearance
    fig.update_layout(
        showlegend=True,  # Show legend if multiple series
        plot_bgcolor='white',  # Clean white background
        paper_bgcolor='white',
        font=dict(size=12),  # Readable font size
        margin=dict(t=100, b=80, l=80, r=80),  # Better margins for labels
        height=500  # Consistent height for better readability
    )
    
    # Add grid lines for better readability
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    try:
        # Test if the figure can be rendered by converting to JSON
        # This catches most rendering issues before they hit Kaleido
        fig.to_json()
        return fig
    except Exception as e:
        print(f"⚠️ Chart rendering validation failed: {str(e)}")
        # Return a simple fallback chart
        import plotly.graph_objects as go
        fallback_fig = go.Figure()
        # Get sampling info if available
        was_sampled = getattr(_plot_with_px, '_was_sampled', False)
        original_size = getattr(_plot_with_px, '_original_size', len(data_frame))
        
        if was_sampled:
            size_text = f"Dataset: {original_size:,} rows (sampled to {len(data_frame):,})"
        else:
            size_text = f"Dataset: {len(data_frame):,} rows"
        
        fallback_fig.add_annotation(
            text=f"📊 Chart rendering failed<br><br>{size_text}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="darkblue"),
            bgcolor="lightblue",
            bordercolor="blue",
            borderwidth=2
        )
        fallback_fig.update_layout(
            title="Chart Rendering Error",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor='white',
            paper_bgcolor='white',
            height=400
        )
        return fallback_fig


def ai_plot(session: Session, original_prompt: str, data: pd.DataFrame):
    """
    Generates a Plotly Express visualization based on a dataset and user query using an AI model.

    This function communicates with an AI model (Claude-4-Sonnet) to determine the most 
    suitable Plotly Express visualization for the given dataset and user question.
    
    Args:
        original_prompt (str): The user's query or question regarding the dataset.
        data (pd.DataFrame): The dataset to visualize.

    Returns:
        plotly.graph_objects.Figure: A Plotly figure generated dynamically based on the AI's recommendation.

    Raises:
        ValueError: If the response from the AI does not contain valid function or arguments.
    """
    
    # Sample large datasets to prevent rendering issues
    MAX_VISUALIZATION_ROWS = 5000
    original_size = len(data)
    
    if len(data) > MAX_VISUALIZATION_ROWS:
        print(f"Your dataset has over 5000 rows, which is too large for visualization.")
        # Use random sampling to get representative data
        data = data.sample(n=MAX_VISUALIZATION_ROWS, random_state=42).reset_index(drop=True)
        print(f"📊 Sampled DataFrame shape: {data.shape}")

    # System prompt providing context for the AI's role in visualization selection.
    system_prompt = '''
    You choose Plotly Express charts. Keep it simple.
    
    Rules:
    1. Time trends = px.line (time on x-axis, metric on y-axis, categories as color)
    2. Category comparison = px.bar 
    3. Distribution = px.histogram
    4. Correlation = px.scatter
    
    DO NOT include 'data_frame' or 'df' in your arguments.
    
    For the data you see, pick the simplest chart that answers the question.
    '''
    
    # User-specific prompt, including the original question and a preview of the dataset.
    user_prompt = f"""
    Question: {original_prompt}
    
    Data Preview:
    {data.iloc[:100]}  # Sending the first 100 rows as a sample
    """
    
    # Defines the expected AI response format as JSON, ensuring a structured response.
    response_format = {
        'type': 'json',
        'schema': {
            'type': 'object',
            'properties': {
                'plotly_function': {
                    'type': 'string',
                    'description': 'Name of the Plotly Express function. '
                                   'Assume we imported the Plotly Express library as px.'
                },
                'arguments': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'argument_name': {
                                'type': 'string',
                                'description': 'The name of the argument required in the plotly_function. '
                                               'If there is no name, respond with "POSITIONAL".'
                            },
                            'argument_type': {
                                'type': 'string',
                                'description': 'One of NUMBER, STRING, LIST, DICT, or BOOLEAN.'
                            },
                            'argument_value': {
                                'type': 'string',
                                'description': 'The value of the argument. '
                                               'If it is not a string, convert it to a string '
                                               'and handle it according to argument_type. '
                                               'For boolean values, return strictly TRUE or FALSE.'
                            }
                        }, 
                        'required': ['argument_name', 'argument_type', 'argument_value']
                    }
                }
            },
            'required': ['plotly_function', 'arguments']
        }
    }
    
    # AI model configuration options, including response format constraints.
    options = {
        'temperature': 0,  # Setting temperature to 0 ensures deterministic responses.
        'max_tokens': 2000,  # Limits response size.
        'response_format': response_format  # Ensures AI returns structured output.
    }
    
    model = os.getenv('MODEL', 'claude-4-sonnet')  # Use model from environment variable
    
    # SQL query to invoke the AI model via Snowflake Cortex.
    query = f"""
    SELECT snowflake.cortex.complete(
        '{model}',
        [
            {{
                'role': 'system',
                'content': $${system_prompt}$$
            }},
            {{
                'role': 'user',
                'content': $${user_prompt}$$
            }}
        ],
        {options}
    )
    """

    # Execute the SQL query and collect the response.
    res = session.sql(query).collect()

    # Parse the AI response and extract structured output.
    try:
        response_data = json.loads(res[0][0])
        structured_output = response_data.get('structured_output')
        if not structured_output or len(structured_output) == 0:
            raise ValueError("No structured output found in AI response")
        output = structured_output[0].get('raw_message')
        if not output:
            raise ValueError("No raw_message found in structured output")
    except (IndexError, KeyError, TypeError, json.JSONDecodeError) as e:
        print(f"Error parsing AI response: {e}")
        print(f"Raw response: {res[0][0] if res and len(res) > 0 else 'No response'}")
        return _create_fallback_chart(data, original_prompt)

    # Extract function name and arguments from the response.
    function_args = output.get('arguments')
    function_name = output.get('plotly_function')

    # Convert function arguments to the correct types.
    kwargs = _get_kwargs(function_args)
    
    # Force line chart for time-series data patterns
    if ('PERIOD_QUARTER' in str(kwargs.get('x', '')) or 'PERIOD_YEAR' in str(kwargs.get('x', ''))) and 'MANAGER_NAME' in str(kwargs.get('color', '')):
        print("⚠️ FIXING: Detected time-series pattern, forcing px.line chart")
        function_name = 'line'

    print('Plotting with Kwargs:')
    print(json.dumps(kwargs, indent=4))

    # Pass the original prompt and sampling info to the plotting function for title enhancement
    _plot_with_px._current_prompt = original_prompt
    _plot_with_px._original_size = original_size
    _plot_with_px._was_sampled = len(data) < original_size
    
    # Generate and return the visualization.
    result = _plot_with_px(function_name, data, **kwargs)
    
    # Clean up the references
    for attr in ['_current_prompt', '_original_size', '_was_sampled']:
        if hasattr(_plot_with_px, attr):
            delattr(_plot_with_px, attr)
    
    return result