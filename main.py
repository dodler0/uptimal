from get_static_content import download_csv_file, download_json_file
from pathlib import Path
from get_crash_years import get_distinct_crash_years
from dataframe_operations import create_dataframe_from_csv, create_union_of_dataframes
import polars as pl



if __name__ == "__main__":

    years_list = []
    country = "US"
    dataframe = pl.DataFrame()
    
    csv_url = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv"
    csv_output_filename = "nyc_motor_vehicle_collisions.csv"
    csv_output_folder = "collisions"
    
    csv_file_path = download_csv_file(
        csv_url=csv_url, 
        output_filename=csv_output_filename, 
        output_dir=csv_output_folder
    )

    crashes_df = dataframe = create_dataframe_from_csv(csv_file_path)
    
    if crashes_df is not None:
        years_list = get_distinct_crash_years(crashes_df, col="CRASH DATE")

    for year in years_list:
        json_url = f"https://date.nager.at/api/v3/publicholidays/{year}/{country}"
        json_output_filename = f"publicholidays_{year}_{country}.json"
        json_output_dir = Path("holidays")
        
        download_json_file(json_url=json_url, output_filename=json_output_filename, output_dir=json_output_dir)

    json_holiday_files = [str(p) for p in json_output_dir.glob("*.json")]
    
    
    holidays_df = create_union_of_dataframes(json_holiday_files)
