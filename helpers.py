import polars as pl


def get_distinct_crash_years(dataframe: pl.DataFrame, col: str) -> list[int] | None:
    """
    Extracts distinct years from a string column and returns them as a sorted list.
    Assumes the year is the last 4 characters of the string.

    Args:
        dataframe: The Polars DataFrame to process.
        col: The name of the column containing date strings.

    Returns:
        A sorted list of distinct years, or None if an error occurs.
    """
    try:
        # This expression defines the transformation
        year_series_expr = (
            pl.col(col)
            .str.slice(-4)  # Get the last 4 characters of the string
            .cast(pl.UInt16)  # Cast to a small integer type for sorting and numeric use
            .unique() # To figure what years of public holidays get to match
            .sort(descending=True)
        )
        # Select the column, get it as a Series, and convert to a Python list
        return dataframe.select(year_series_expr).to_series().to_list()
    except Exception as e:
        print(f"\nCould not get distinct years. Error: {e}")
        print(f"Is the '{col}' column present and in a recognizable format?")
        return None
