"""
Shared utility functions for data processing in the bike share pipeline.
"""
import json
import logging
import numpy as np
import pandas as pd


def convert_complex_cols_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds object-type columns and converts any dict/list/array
    values to JSON strings, passing others (str, int, nan) through.
    
    This is necessary because PostgreSQL and Snowflake cannot store
    nested Python objects directly - they must be serialized to JSON strings.
    
    Args:
        df: DataFrame that may contain complex nested columns
        
    Returns:
        DataFrame with complex columns converted to JSON strings
    """
    def convert_value_to_json(x):
        """Helper function for .apply() - converts complex types to JSON."""
        if isinstance(x, (dict, list)):
            return json.dumps(x)
        if isinstance(x, np.ndarray):
            lst = [None if pd.isna(i) else i for i in x.tolist()]
            return json.dumps(lst)
        return x

    for col in list(df.columns):
        if df[col].dtype == "object":
            is_complex = df[col].dropna().apply(
                lambda v: isinstance(v, (dict, list, np.ndarray))
            ).any()
            if is_complex:
                logging.info(f"Converting complex column '{col}' to JSON string.")
                df[col] = df[col].apply(convert_value_to_json)
    
    return df

