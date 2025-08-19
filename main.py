from get_static_content import download_csv_file, download_json_file
from pathlib import Path
from get_crash_years import get_distinct_crash_years
from dataframe_operations import create_dataframe_from_csv
import polars as pl



if __name__ == "__main__":

    years_list = []
    country = "US"
    dataframe = pl.DataFrame()
    
    csv_url = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv"
    csv_output_filename = "nyc_motor_vehicle_collisions.csv"
    
    # Define the output directory and the full path using the idiomatic '/' operator
    csv_output_dir = Path("collisions")
    path = csv_output_dir / csv_output_filename
    
    if not path.is_file():
        # Ensure the directory exists before attempting to download the file into it
        csv_output_dir.mkdir(parents=True, exist_ok=True)
        csv_file_path = download_csv_file(csv_url, str(path))
    else:
        print(f"File '{path}' already exists. Skipping download.")
        csv_file_path = str(path)

    if csv_file_path:
        dataframe = create_dataframe_from_csv(csv_file_path)
        if dataframe is not None:
            years_list = get_distinct_crash_years(dataframe, col="CRASH DATE")

    for year in years_list:
        
        json_url = f"https://date.nager.at/api/v3/publicholidays/{year}/{country}"
        json_output_filename = f"publicholidays_{year}_{country}.json"
        json_output_dir = Path("holidays")
        path = json_output_dir / json_output_filename

        if not path.is_file():
            # Ensure the directory exists before downloading
            json_output_dir.mkdir(parents=True, exist_ok=True)
            download_json_file(json_url, str(path))
        else:
            print(f"File '{path}' already exists. Skipping download.")
