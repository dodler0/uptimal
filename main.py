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
    holidays_df = holidays_df.with_columns(
            pl.col("date").str.to_date(format="%Y-%m-%d", strict=True).dt.to_string("%m/%d/%Y")
        )
    
    if crashes_df is not None and holidays_df is not None:
        print("\nEnriching crashes data with holiday information...")

        # To perform a clean join, we'll rename the 'date' column in holidays_df
        # to match the 'CRASH DATE' column in crashes_df.
        holidays_df = holidays_df.rename({"date": "CRASH DATE"})

        # We perform a 'left' join to keep all crash records and add holiday
        # information where the dates match.
        enriched_df = crashes_df.join(holidays_df, on="CRASH DATE", how="left")
        
        # Add a boolean column 'IS_HOLIDAY' to easily identify crashes on holidays.
        # This is True if the 'name' column (from holidays_df) is not null.
        enriched_df = enriched_df.with_columns(
            pl.col("name").is_not_null().alias("IS_HOLIDAY")
        )
        
        print("Enrichment complete.")
        
        # Let's verify by counting crashes on holidays vs. non-holidays.
        print("\nCrash distribution (Holiday vs. Non-Holiday):")
        print(enriched_df.get_column("IS_HOLIDAY").value_counts())

        # Show the first 5 crashes that occurred on holidays using the new column.
        crashes_on_holidays = enriched_df.filter(pl.col("IS_HOLIDAY"))
        print(f"\nFound {len(crashes_on_holidays)} crashes that occurred on a public holiday.")
        print("Showing the first 5 crashes that occurred on holidays (with the new 'IS_HOLIDAY' column):")
        print(crashes_on_holidays.head())
