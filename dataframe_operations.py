import polars as pl
from pathlib import Path


def create_dataframe_from_csv(file_path: str) -> pl.DataFrame | None:
    """
    Creates a Polars DataFrame from a local CSV file path.
    Checks for file existence before attempting to read.

    Args:
        file_path: The path to the local CSV file.

    Returns:
        A Polars DataFrame, or None if the file doesn't exist or parsing fails.
    """
    path = Path(file_path)
    if not path.is_file():
        print(f"Error: File not found at '{file_path}'.")
        return None

    try:
        print(f"Reading data from {file_path} into Polars DataFrame...")
        df = pl.read_csv(file_path)
        return df
    except Exception as e:
        print(f"An error occurred while parsing the CSV from {file_path}: {e}")
        return None

def create_dataframe_from_json(file_path: str | list[str]) -> pl.DataFrame | None:
    """
    Creates a Polars DataFrame from a local JSON file path.
    Checks for file existence before attempting to read.

    Args:
        file_path: The path to the local JSON file.

    Returns:
        A Polars DataFrame, or None if the file doesn't exist or parsing fails.
    """

    path = Path(file_path)
    if not path.is_file():
        print(f"Error: File not found at '{file_path}'.")
        return None

    try:
        print(f"Reading data from {file_path} into Polars DataFrame...")
        df = pl.read_json(file_path)
        return df
    except Exception as e:
        print(f"An error occurred while parsing the JSON from {file_path}: {e}")
        return None

def create_union_of_dataframes(file_path: list[str]) -> pl.DataFrame | None:

    dfs = []
    for path in file_path:
        df = create_dataframe_from_json(path)
        if df is not None:
            dfs.append(df)
    if dfs:
        print(pl.concat(dfs))        
        return pl.concat(dfs)
    else:
        print("No valid JSON files found.")
        return None
