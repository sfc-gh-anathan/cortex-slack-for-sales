"""
Experimental Chart Utils with LLM-Generated Plotting Code
This module uses an LLM to generate matplotlib/seaborn code for intelligent chart creation.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import io
import base64
from typing import Dict, Any, Optional, Tuple
import re
import traceback
from dotenv import load_dotenv
import os

# For LLM integration - you can use your existing Cortex setup or OpenAI
try:
    import snowflake.cortex as cortex
except ImportError:
    cortex = None

load_dotenv()

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

class IntelligentChartGenerator:
    """
    Uses LLM to generate matplotlib/seaborn code for data visualization
    """
    
    def __init__(self, snowflake_connection=None):
        self.conn = snowflake_connection
        
    def analyze_data_structure(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze DataFrame structure for LLM context"""
        
        analysis = {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.to_dict(),
            "sample_data": df.head(3).to_dict(),
            "numeric_columns": df.select_dtypes(include=[np.number]).columns.tolist(),
            "categorical_columns": df.select_dtypes(include=['object', 'category']).columns.tolist(),
            "date_columns": df.select_dtypes(include=['datetime64']).columns.tolist(),
            "null_counts": df.isnull().sum().to_dict(),
            "unique_counts": {col: df[col].nunique() for col in df.columns},
        }
        
        # Detect potential business context
        analysis["business_context"] = self._detect_business_context(df)
        
        return analysis
    
    def _detect_business_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect business context from column names and data patterns"""
        
        context = {
            "domain": "unknown",
            "metrics": [],
            "dimensions": [],
            "time_dimension": None,
            "geographic_dimension": None
        }
        
        # Sales-specific detection
        sales_indicators = ['sales', 'revenue', 'quota', 'commission', 'deal', 'target']
        if any(indicator in ' '.join(df.columns).lower() for indicator in sales_indicators):
            context["domain"] = "sales"
            
        # Identify common patterns
        for col in df.columns:
            col_lower = col.lower()
            
            # Metrics (usually numeric)
            if any(word in col_lower for word in ['amount', 'sales', 'revenue', 'quota', 'commission', 'total']):
                context["metrics"].append(col)
                
            # Dimensions (usually categorical)
            elif any(word in col_lower for word in ['name', 'region', 'territory', 'category', 'type', 'role']):
                context["dimensions"].append(col)
                
            # Time dimensions
            elif any(word in col_lower for word in ['date', 'time', 'period', 'month', 'year']):
                context["time_dimension"] = col
                
            # Geographic dimensions
            elif any(word in col_lower for word in ['region', 'territory', 'country', 'state', 'city']):
                context["geographic_dimension"] = col
                
        return context
    
    def generate_chart_code(self, df: pd.DataFrame, user_question: str = "") -> Tuple[str, Dict]:
        """
        Generate matplotlib/seaborn code using LLM
        Returns: (python_code, metadata)
        """
        
        # Analyze the data
        analysis = self.analyze_data_structure(df)
        
        # Create prompt for LLM
        prompt = self._create_chart_prompt(analysis, user_question)
        
        # Get code from LLM
        try:
            if self.conn:
                # Use Cortex if available
                response = self._get_cortex_response(prompt)
            else:
                # Fallback to a simple rule-based approach for testing
                response = self._fallback_chart_code(analysis)
                
            # Extract Python code from response
            python_code = self._extract_python_code(response)
            
            # Validate and clean the code
            cleaned_code = self._validate_and_clean_code(python_code)
            
            metadata = {
                "analysis": analysis,
                "user_question": user_question,
                "raw_response": response,
                "code_extracted": True
            }
            
            return cleaned_code, metadata
            
        except Exception as e:
            # Fallback to simple chart
            fallback_code = self._fallback_chart_code(analysis)
            metadata = {
                "error": str(e),
                "fallback_used": True,
                "analysis": analysis
            }
            return fallback_code, metadata
    
    def _create_chart_prompt(self, analysis: Dict, user_question: str) -> str:
        """Create a prompt asking Cortex for chart recommendations"""
        
        prompt = f"""
Based on this data analysis, recommend the best chart type and configuration:

DATA ANALYSIS:
- Columns: {analysis['columns']}
- Data types: {analysis['dtypes']}
- Sample data: {analysis['sample_data']}
- Numeric columns: {analysis['numeric_columns']}
- Categorical columns: {analysis['categorical_columns']}
- Business context: {analysis['business_context']['domain']}

USER QUESTION: "{user_question}"

Please recommend:
1. Chart type (bar, line, scatter, pie, etc.)
2. X-axis column
3. Y-axis column (if applicable)
4. Color/grouping column (if applicable)
5. Title for the chart
6. Any special formatting or business context to include

Respond with a clear recommendation in this format:
Chart Type: [type]
X-axis: [column]
Y-axis: [column]
Title: [title]
Notes: [any special considerations]
"""
        
        return prompt
    
    def _get_cortex_response(self, prompt: str) -> str:
        """Get response from Cortex using your existing chat setup"""
        try:
            # Import your existing cortex_chat module
            import cortex_chat
            
            # Use your existing Cortex Chat setup
            cortex_app = cortex_chat.CortexChat(
                os.getenv("AGENT_ENDPOINT"),
                os.getenv("SEARCH_SERVICE"), 
                os.getenv("SEMANTIC_MODEL"),
                os.getenv("MODEL"),
                os.getenv("ACCOUNT"),
                os.getenv("DEMO_USER"),
                os.getenv("RSA_PRIVATE_KEY_PATH")
            )
            
            # Get chart recommendation from Cortex
            response = cortex_app.chat(prompt)
            return response.get('text', str(response))
            
        except Exception as e:
            raise Exception(f"Cortex chat call failed: {e}")
    
    def _extract_python_code(self, response: str) -> str:
        """Extract Python code from LLM response"""
        
        # Look for code blocks
        code_patterns = [
            r'```python\n(.*?)```',
            r'```\n(.*?)```',
            r'```python(.*?)```'
        ]
        
        for pattern in code_patterns:
            match = re.search(pattern, response, re.DOTALL)
            if match:
                return match.group(1).strip()
        
        # If no code blocks found, assume entire response is code
        return response.strip()
    
    def _validate_and_clean_code(self, code: str) -> str:
        """Validate and clean the generated code"""
        
        # Basic safety checks
        dangerous_patterns = ['import os', 'import subprocess', 'exec(', 'eval(', '__import__']
        for pattern in dangerous_patterns:
            if pattern in code:
                raise Exception(f"Potentially dangerous code detected: {pattern}")
        
        # Ensure required imports
        required_imports = [
            "import matplotlib.pyplot as plt",
            "import seaborn as sns", 
            "import pandas as pd",
            "import numpy as np"
        ]
        
        # Add missing imports
        for imp in required_imports:
            if imp.split()[-1] not in code and imp not in code:
                code = imp + "\n" + code
        
        # Ensure figure creation and fig variable
        if "plt.figure(" not in code:
            code = "plt.figure(figsize=(12, 8))\n" + code
            
        if "fig = plt.gcf()" not in code and "fig =" not in code:
            code += "\nfig = plt.gcf()"
            
        return code
    
    def _fallback_chart_code(self, analysis: Dict) -> str:
        """Generate simple fallback chart code"""
        
        numeric_cols = analysis['numeric_columns']
        categorical_cols = analysis['categorical_columns']
        
        if len(numeric_cols) >= 1 and len(categorical_cols) >= 1:
            # Bar chart
            return f"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

plt.figure(figsize=(12, 8))
df_plot = df.head(10)  # Limit to top 10 for readability
sns.barplot(data=df_plot, x='{categorical_cols[0]}', y='{numeric_cols[0]}')
plt.title('Bar Chart: {numeric_cols[0]} by {categorical_cols[0]}')
plt.xticks(rotation=45)
plt.tight_layout()
fig = plt.gcf()
"""
        elif len(numeric_cols) >= 2:
            # Scatter plot
            return f"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

plt.figure(figsize=(12, 8))
sns.scatterplot(data=df, x='{numeric_cols[0]}', y='{numeric_cols[1]}')
plt.title('Scatter Plot: {numeric_cols[1]} vs {numeric_cols[0]}')
plt.tight_layout()
fig = plt.gcf()
"""
        else:
            # Simple line plot
            return f"""
import matplotlib.pyplot as plt
import pandas as pd

plt.figure(figsize=(12, 8))
df.plot(kind='line')
plt.title('Data Overview')
plt.tight_layout()
fig = plt.gcf()
"""
    
    def _parse_cortex_recommendation(self, response: str) -> Dict[str, str]:
        """Parse Cortex chart recommendation response"""
        
        recommendation = {
            'chart_type': 'bar',
            'x_axis': '',
            'y_axis': '',
            'title': 'Data Visualization',
            'notes': ''
        }
        
        lines = response.split('\n')
        for line in lines:
            line = line.strip()
            # Handle both regular and markdown formatting
            if 'Chart Type:' in line:
                chart_type = line.split(':', 1)[1].strip().lower().replace('*', '')
                # Normalize chart type names
                if 'line' in chart_type:
                    chart_type = 'line'
                elif 'bar' in chart_type or 'column' in chart_type:
                    chart_type = 'bar'
                elif 'scatter' in chart_type:
                    chart_type = 'scatter'
                elif 'pie' in chart_type:
                    chart_type = 'pie'
                recommendation['chart_type'] = chart_type
            elif 'X-axis:' in line:
                x_axis_raw = line.split(':', 1)[1].strip().replace('*', '').strip()
                # Clean up complex column recommendations
                x_axis_clean = x_axis_raw.split('(')[0].strip()  # Remove parenthetical explanations
                x_axis_clean = x_axis_clean.split(' or ')[0].strip()  # Take first option if multiple
                recommendation['x_axis'] = x_axis_clean
            elif 'Y-axis:' in line:
                y_axis_raw = line.split(':', 1)[1].strip().replace('*', '').strip()
                # Clean up complex column recommendations
                y_axis_clean = y_axis_raw.split('(')[0].strip()  # Remove parenthetical explanations
                y_axis_clean = y_axis_clean.split(' or ')[0].strip()  # Take first option if multiple
                recommendation['y_axis'] = y_axis_clean
            elif 'Title:' in line:
                recommendation['title'] = line.split(':', 1)[1].strip().replace('*', '').strip()
            elif 'Notes:' in line:
                recommendation['notes'] = line.split(':', 1)[1].strip().replace('*', '').strip()
                
        return recommendation
    
    def _create_safe_chart(self, df: pd.DataFrame, recommendation: Dict[str, str]) -> plt.Figure:
        """Create chart using safe, predefined functions"""
        
        try:
            fig, ax = plt.subplots(figsize=(12, 10))  # Increased height for description
            
            chart_type = recommendation['chart_type']
            x_col = recommendation['x_axis']
            y_col = recommendation['y_axis']
            title = recommendation['title']
            notes = recommendation.get('notes', '')
            
            # Use full dataset (no limiting)
            df_plot = df.copy()
            
            if chart_type in ['bar', 'column'] and x_col and y_col:
                # Bar chart with tooltips
                bars = sns.barplot(data=df_plot, x=x_col, y=y_col, ax=ax)
                ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
                
                # Add value labels on bars (simple tooltips)
                for i, bar in enumerate(bars.patches):
                    height = bar.get_height()
                    if not np.isnan(height):
                        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                               f'{height:,.0f}', ha='center', va='bottom', fontsize=9)
                
            elif chart_type == 'line' and x_col and y_col:
                # Check if we have a good grouping column for multi-line chart
                categorical_cols = df_plot.select_dtypes(include=['object', 'category']).columns
                grouping_col = None
                
                # Look for good grouping columns
                for col in categorical_cols:
                    if col != x_col and col != y_col and df_plot[col].nunique() <= 10:  # Max 10 groups
                        grouping_col = col
                        break
                
                if grouping_col:
                    # Multi-line chart with grouping
                    sns.lineplot(data=df_plot, x=x_col, y=y_col, hue=grouping_col, 
                               ax=ax, marker='o', markersize=6)
                    ax.legend(title=grouping_col, bbox_to_anchor=(1.05, 1), loc='upper left')
                else:
                    # Single line chart with markers
                    sns.lineplot(data=df_plot, x=x_col, y=y_col, ax=ax, marker='o', markersize=6)
                    
                    # Add value labels at data points (only for single line to avoid clutter)
                    for i, (x_val, y_val) in enumerate(zip(df_plot[x_col], df_plot[y_col])):
                        if i % max(1, len(df_plot) // 10) == 0:  # Show every 10th label to avoid clutter
                            ax.annotate(f'{y_val:,.0f}', (x_val, y_val), 
                                      textcoords="offset points", xytext=(0,10), ha='center', fontsize=8)
                
            elif chart_type == 'scatter' and x_col and y_col:
                # Scatter plot with enhanced styling
                scatter = sns.scatterplot(data=df_plot, x=x_col, y=y_col, ax=ax, 
                                        alpha=0.7, s=60)  # Semi-transparent, larger points
                
                # Add trend line
                try:
                    z = np.polyfit(df_plot[x_col], df_plot[y_col], 1)
                    p = np.poly1d(z)
                    ax.plot(df_plot[x_col], p(df_plot[x_col]), "r--", alpha=0.8, linewidth=2)
                except:
                    pass  # Skip trend line if it fails
                
            elif chart_type == 'pie' and len(df_plot.columns) >= 2:
                # Pie chart
                if y_col and y_col in df_plot.columns:
                    wedges, texts, autotexts = ax.pie(df_plot[y_col], labels=df_plot[x_col] if x_col else None, 
                                                     autopct='%1.1f%%', startangle=90)
                    # Enhance text readability
                    for autotext in autotexts:
                        autotext.set_color('white')
                        autotext.set_fontweight('bold')
                else:
                    # Use first numeric column
                    numeric_cols = df_plot.select_dtypes(include=[np.number]).columns
                    if len(numeric_cols) > 0:
                        wedges, texts, autotexts = ax.pie(df_plot[numeric_cols[0]], 
                                                         labels=df_plot[x_col] if x_col else None, 
                                                         autopct='%1.1f%%', startangle=90)
                        for autotext in autotexts:
                            autotext.set_color('white')
                            autotext.set_fontweight('bold')
                        
            else:
                # Default to bar chart with first categorical and numeric columns
                numeric_cols = df_plot.select_dtypes(include=[np.number]).columns
                categorical_cols = df_plot.select_dtypes(include=['object', 'category']).columns
                
                if len(numeric_cols) > 0 and len(categorical_cols) > 0:
                    bars = sns.barplot(data=df_plot, x=categorical_cols[0], y=numeric_cols[0], ax=ax)
                    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
                    
                    # Add value labels
                    for bar in bars.patches:
                        height = bar.get_height()
                        if not np.isnan(height):
                            ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                                   f'{height:,.0f}', ha='center', va='bottom', fontsize=9)
                else:
                    # Simple line plot of all numeric data
                    df_plot.select_dtypes(include=[np.number]).plot(ax=ax, marker='o')
            
            # Enhanced title and formatting
            ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
            
            # Tailored Y-axis scaling
            if chart_type != 'pie' and y_col and y_col in df_plot.columns:
                y_data = df_plot[y_col].dropna()
                if len(y_data) > 0:
                    y_min, y_max = y_data.min(), y_data.max()
                    y_range = y_max - y_min
                    # Add 5% padding on top and bottom
                    ax.set_ylim(y_min - y_range * 0.05, y_max + y_range * 0.05)
                    
                    # Format Y-axis with appropriate scale
                    if y_max > 1000000:
                        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000000:.1f}M'))
                    elif y_max > 1000:
                        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1000:.0f}K'))
                    else:
                        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:.0f}'))
            
            # Add description below the chart
            if notes:
                # Truncate notes if too long
                description = notes[:200] + "..." if len(notes) > 200 else notes
                fig.text(0.1, 0.02, f"📊 {description}", fontsize=10, style='italic', 
                        wrap=True, ha='left', va='bottom')
            
            # Add data info
            data_info = f"Data points: {len(df_plot):,}"
            fig.text(0.9, 0.02, data_info, fontsize=9, ha='right', va='bottom', alpha=0.7)
            
            plt.tight_layout()
            plt.subplots_adjust(bottom=0.15)  # Make room for description
            
            # Adjust layout if we have a legend outside the plot
            if hasattr(ax, 'legend_') and ax.legend_:
                plt.subplots_adjust(right=0.75)  # Make room for legend
            
            return fig
            
        except Exception as e:
            print(f"Error creating safe chart: {e}")
            
            # Create error chart
            fig, ax = plt.subplots(figsize=(12, 8))
            ax.text(0.5, 0.5, f'Chart Generation Error:\n{str(e)}', 
                   ha='center', va='center', transform=ax.transAxes,
                   fontsize=12, bbox=dict(boxstyle="round", facecolor='lightcoral'))
            ax.set_title('Chart Generation Failed')
            return fig
    
    def create_intelligent_chart(self, df: pd.DataFrame, user_question: str = "") -> Tuple[plt.Figure, Dict]:
        """
        Main method: Generate and execute intelligent chart using Cortex recommendations
        Returns: (figure, metadata)
        """
        
        try:
            # Analyze the data
            analysis = self.analyze_data_structure(df)
            
            # Create prompt for Cortex
            prompt = self._create_chart_prompt(analysis, user_question)
            
            # Get recommendation from Cortex
            if self.conn:
                cortex_response = self._get_cortex_response(prompt)
                recommendation = self._parse_cortex_recommendation(cortex_response)
            else:
                # Fallback to simple recommendation
                recommendation = self._get_fallback_recommendation(analysis)
                cortex_response = "Fallback recommendation used"
            
            # Create the chart safely
            fig = self._create_safe_chart(df, recommendation)
            
            metadata = {
                "analysis": analysis,
                "user_question": user_question,
                "cortex_response": cortex_response,
                "recommendation": recommendation,
                "method": "cortex_safe_generation"
            }
            
            return fig, metadata
            
        except Exception as e:
            print(f"Error in intelligent chart creation: {e}")
            
            # Create fallback chart
            fig = self._create_fallback_chart(df)
            
            metadata = {
                "error": str(e),
                "fallback_used": True,
                "method": "fallback_chart"
            }
            
            return fig, metadata
    
    def _get_fallback_recommendation(self, analysis: Dict) -> Dict[str, str]:
        """Generate fallback recommendation when Cortex is not available"""
        
        numeric_cols = analysis['numeric_columns']
        categorical_cols = analysis['categorical_columns']
        
        if len(numeric_cols) >= 1 and len(categorical_cols) >= 1:
            return {
                'chart_type': 'bar',
                'x_axis': categorical_cols[0],
                'y_axis': numeric_cols[0],
                'title': f'{numeric_cols[0]} by {categorical_cols[0]}',
                'notes': 'Fallback bar chart'
            }
        elif len(numeric_cols) >= 2:
            return {
                'chart_type': 'scatter',
                'x_axis': numeric_cols[0],
                'y_axis': numeric_cols[1],
                'title': f'{numeric_cols[1]} vs {numeric_cols[0]}',
                'notes': 'Fallback scatter plot'
            }
        else:
            return {
                'chart_type': 'line',
                'x_axis': '',
                'y_axis': '',
                'title': 'Data Overview',
                'notes': 'Fallback line chart'
            }
    
    def _create_fallback_chart(self, df: pd.DataFrame) -> plt.Figure:
        """Create simple fallback chart when everything else fails"""
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                df[numeric_cols].head(10).plot(kind='bar', ax=ax)
            else:
                ax.text(0.5, 0.5, 'No numeric data to visualize', 
                       ha='center', va='center', transform=ax.transAxes)
        except:
            ax.text(0.5, 0.5, 'Unable to create visualization', 
                   ha='center', va='center', transform=ax.transAxes)
        
        ax.set_title('Data Visualization')
        plt.tight_layout()
        
        return fig


def create_intelligent_chart(df: pd.DataFrame, user_question: str = "", snowflake_conn=None) -> Tuple[plt.Figure, Dict]:
    """
    Convenience function to create intelligent charts
    """
    generator = IntelligentChartGenerator(snowflake_conn)
    return generator.create_intelligent_chart(df, user_question)


# Test function
def test_chart_generation():
    """Test the chart generation with sample data"""
    
    # Create sample sales data
    data = {
        'full_name': ['Justice Henry', 'Aiden Freeman', 'Bay Hughes', 'Cameron Flores', 'Ellis Butler'],
        'total_sales': [6198257.65, 6161444.15, 6095185.55, 6036723.05, 5987234.22],
        'region': ['West', 'West', 'West', 'West', 'West'],
        'quota_attainment': [1.45, 1.32, 1.34, 1.28, 1.41]
    }
    
    df = pd.DataFrame(data)
    
    # Test chart generation
    fig, metadata = create_intelligent_chart(df, "Who are the top sales reps by total sales?")
    
    print("Generated code:")
    print(metadata.get('generated_code', 'No code found'))
    
    # Show the chart
    plt.show()
    
    return fig, metadata


if __name__ == "__main__":
    test_chart_generation()
