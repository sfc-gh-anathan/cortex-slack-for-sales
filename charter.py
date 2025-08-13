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
    
    # Get the original prompt from the ai_plot function call context
    # We'll pass this through the _plot_with_px call
    prompt_info = getattr(_plot_with_px, '_current_prompt', None)
    
    if 'title' in kwargs:
        # Enhance the title with prompt and dataset info
        title_parts = [kwargs['title']]
        if prompt_info:
            title_parts.append(f"<i>Question: {prompt_info}</i>")
        title_parts.append(f"<sub>{data_description}</sub>")
        enhanced_title = "<br>".join(title_parts)
        fig.update_layout(title=enhanced_title)
    else:
        base_title = prompt_info if prompt_info else "Data Analysis"
        enhanced_title = f"{base_title}<br><sub>{data_description}</sub>"
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
    
    # Add value labels only for high/low data points to avoid clutter
    if 'y' in kwargs:
        y_col = kwargs['y']
        if y_col in processed_data.columns:
            # Find min and max values and their indices
            min_idx = processed_data[y_col].idxmin()
            max_idx = processed_data[y_col].idxmax()
            min_val = processed_data[y_col].min()
            max_val = processed_data[y_col].max()
            
            # Helper function to format currency with abbreviations
            def format_currency(value):
                if abs(value) >= 1_000_000_000:
                    return f'${value/1_000_000_000:.1f}B'
                elif abs(value) >= 1_000_000:
                    return f'${value/1_000_000:.1f}M'
                elif abs(value) >= 1_000:
                    return f'${value/1_000:.0f}K'
                else:
                    return f'${value:.0f}'
            
            # Create text labels - only show for min/max points
            text_labels = [''] * len(processed_data)  # Empty strings for all points
            text_labels[min_idx] = f'LOW: {format_currency(min_val)}'
            text_labels[max_idx] = f'HIGH: {format_currency(max_val)}'
            
            # Update traces with selective labeling
            fig.update_traces(
                text=text_labels,
                textposition='top center',
                textfont=dict(size=11, color='darkblue', family='Arial Black'),
                mode='lines+markers+text' if 'markers' in str(kwargs) else 'lines+text',
                hovertemplate=f'<b>%{{x}}</b><br>{y_col}: %{{y:,.0f}}<br><extra></extra>'  # Enhanced hover with column name
            )
    
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
    
    return fig


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

    # System prompt providing context for the AI's role in visualization selection.
    system_prompt = '''
    Your role is to decide how best to visualize a dataset in Python
    for business intelligence analysts and stakeholders.
    
    You are to receive a representative sample of the dataset, 
    with all columns and some rows.
    
    You will also receive the question the user asked the "Analyst Agent".
    
    Your goal is, based on the content present, to decide which Plotly Express 
    visualization to use and return the appropriate function name and arguments.
    Assume the dataframe is always called 'df' and you do not need to supply this argument.
    Assume we have imported the Plotly Express library as px.

    You should try to make the visualization "Scrollable" if possible (for example, in the case of
    bar charts), so that the user is able to easily consume the information. This may mean reducing the
    number of immediately visible elements.

    You are one agent as part of a larger collection of agents, 
    which you will communicate with, and it is integral that you provide
    only the requested output such that the "plotly agent" can take your
    function and arguments and build the visualization for the user.
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
    
    model = os.getenv('MODEL', 'claude-3-5-sonnet')  # Use model from environment variable
    
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
    output = json.loads(res[0][0]).get('structured_output')[0].get('raw_message')

    # Extract function name and arguments from the response.
    function_args = output.get('arguments')
    function_name = output.get('plotly_function')

    # Convert function arguments to the correct types.
    kwargs = _get_kwargs(function_args)

    print('Plotting with Kwargs:')
    print(json.dumps(kwargs, indent=4))

    # Pass the original prompt to the plotting function for title enhancement
    _plot_with_px._current_prompt = original_prompt
    
    # Generate and return the visualization.
    result = _plot_with_px(function_name, data, **kwargs)
    
    # Clean up the prompt reference
    if hasattr(_plot_with_px, '_current_prompt'):
        delattr(_plot_with_px, '_current_prompt')
    
    return result