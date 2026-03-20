import datetime

import numpy as np
import pandas as pd


def _format_sql_value(val):
    """Internal helper function to format a single Python value for SQL."""
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, bool):
        return str(val).upper()  # TRUE or FALSE
    elif isinstance(val, (int, float, np.integer, np.floating)):
        return str(val)  # Numbers don't need quotes
    elif isinstance(val, (datetime.date, datetime.datetime, pd.Timestamp)):
        return f"'{val.isoformat()}'"  # Dates/timestamps as ISO strings
    else:
        # Default to string, escaping single quotes
        return f"'{str(val).replace("'", "''")}'"


def df_to_snowflake_cte(df: pd.DataFrame, table_name: str) -> str:
    """
    Converts a pandas DataFrame into a Snowflake SQL CTE.

    Args:
        df: The pandas DataFrame to convert.
        table_name: The desired name for the CTE (e.g., "my_data").

    Returns:
        A string containing the complete SQL CTE.

    Raises:
        ValueError: If the DataFrame has no columns.
    """
    if df.columns.empty:
        raise ValueError("DataFrame has no columns.")

    # Format column names, quoting them to handle spaces or reserved words
    col_names = ", ".join([f"{c}" for c in df.columns])

    cte_parts = []
    cte_parts.append(f"WITH {table_name} ({col_names}) AS (")

    if df.empty:
        # Handle empty DataFrame by creating a valid empty table structure
        # This selects NULL of the correct type for each column where 1=0 (so it's empty)
        select_parts = []
        for col, dtype in df.dtypes.items():
            sql_type = "NULL"
            if pd.api.types.is_numeric_dtype(dtype):
                sql_type = "NULL::NUMBER"
            elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
                sql_type = "NULL::VARCHAR"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "NULL::TIMESTAMP_NTZ"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "NULL::BOOLEAN"
            select_parts.append(f'{sql_type} AS "{col}"')

        cte_parts.append(f"  SELECT {', '.join(select_parts)}")
        cte_parts.append("  WHERE 1 = 0")  # Ensures no rows are returned

    else:
        # Handle non-empty DataFrame with a VALUES clause
        cte_parts.append("  SELECT * FROM VALUES")
        rows = []
        for row_tuple in df.itertuples(index=False, name=None):
            formatted_values = [_format_sql_value(val) for val in row_tuple]
            rows.append(f"    ({', '.join(formatted_values)})")

        cte_parts.append(",\n".join(rows))

    cte_parts.append(")")
    return "\n".join(cte_parts)
