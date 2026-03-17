"""StatCan data parsing utilities.

Helper functions for working with Statistics Canada API responses,
CSV data, and vector-to-dataframe conversions.
"""

import io
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def parse_statcan_csv(csv_text: str) -> Optional[pd.DataFrame]:
    """Parse a Statistics Canada CSV response into a DataFrame.

    StatCan CSV files typically have a header row and use UTF-8-BOM encoding.
    Handles the common format with columns: REF_DATE, GEO, DGUID, etc.

    Args:
        csv_text: Raw CSV text from StatCan API.

    Returns:
        DataFrame with parsed data, or None on failure.
    """
    try:
        df = pd.read_csv(io.StringIO(csv_text), encoding="utf-8")
        if "REF_DATE" in df.columns:
            df["REF_DATE"] = pd.to_datetime(df["REF_DATE"], errors="coerce")
        return df
    except Exception:
        logger.exception("Failed to parse StatCan CSV")
        return None


def vectors_to_latest(df: pd.DataFrame, vector_map: Dict[str, str]) -> Dict[str, float]:
    """Extract the latest value for each vector from a DataFrame.

    Args:
        df: DataFrame from stats_can.vectors_to_df() or similar.
        vector_map: Mapping of indicator_name -> vector_id.

    Returns:
        Dict of indicator_name -> latest numeric value.
    """
    results = {}
    for name, vec_id in vector_map.items():
        matching_cols = [c for c in df.columns if vec_id in str(c)]
        if matching_cols:
            series = df[matching_cols[0]].dropna()
            if not series.empty:
                results[name] = float(series.iloc[-1])
    return results


def filter_by_geo(df: pd.DataFrame, geo: str = "Canada") -> pd.DataFrame:
    """Filter a StatCan DataFrame to a specific geography.

    Args:
        df: DataFrame with a GEO column.
        geo: Geography name to filter by (default: "Canada").

    Returns:
        Filtered DataFrame.
    """
    if "GEO" in df.columns:
        return df[df["GEO"].str.contains(geo, case=False, na=False)]
    return df


def extract_time_series(
    df: pd.DataFrame,
    value_col: str = "VALUE",
    date_col: str = "REF_DATE",
) -> Optional[pd.Series]:
    """Extract a time series from a StatCan DataFrame.

    Args:
        df: DataFrame with date and value columns.
        value_col: Name of the value column.
        date_col: Name of the date column.

    Returns:
        pandas Series indexed by date, or None if columns missing.
    """
    if date_col not in df.columns or value_col not in df.columns:
        return None
    series = df.set_index(date_col)[value_col].dropna()
    series.index = pd.to_datetime(series.index, errors="coerce")
    return series.sort_index()
