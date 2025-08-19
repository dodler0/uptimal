import polars as pl


def create_dataframe_from_csv(file_path: str) -> pl.DataFrame | None:
    """
    Creates a Polars DataFrame from a local CSV file path.

    Args:
        file_path: The path to the local CSV file.

    Returns:
        A Polars DataFrame, or None if parsing fails.
    """
    try:
        print(f"Reading data from {file_path} into Polars DataFrame...")
        df = pl.read_csv(file_path)
        #print(df.head())
        return df
    except Exception as e:
        print(f"An error occurred while parsing the CSV from {file_path}: {e}")
        return None
