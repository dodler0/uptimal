# Uptimal - Data Enrichment Pipeline

This project demonstrates a data pipeline that fetches motor vehicle collision data for New York City and enriches it with US public holiday information. It identifies which collisions occurred on a holiday, providing a dataset ready for further analysis.

## Features

- **Data Fetching**: Downloads collision data from NYC Open Data and public holiday data from the Nager.Date API.
- **Dynamic Year Detection**: Automatically detects the years present in the collision data to fetch the correct holiday information.
- **Efficient Caching**: Caches downloaded files locally to avoid redundant network requests on subsequent runs.
- **High-Performance Processing**: Uses the [Polars](https://pola.rs/) DataFrame library for fast and memory-efficient data manipulation.
- **Data Enrichment**: Joins the collision and holiday datasets, adding a boolean `IS_HOLIDAY` flag to each crash record for easy filtering and analysis.

## Requirements

- Python 3.9+
- PDM (Python Dependency Manager)

The project dependencies are managed by PDM and are listed in `pyproject.toml`. The key libraries are:
- `polars`
- `requests`

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <this-repo-url>
    cd uptimal
    ```

2.  **Install dependencies using PDM:**
    (If you don't have PDM, install it first: `pip install pdm` or `brew install pdm` for macOS`)
    ```bash
    pdm install
    ```

## Usage

To run the entire data pipeline, execute the main script from the root of the project:

```bash
pdm run python main.py
```

The script will:
1.  Create `collisions/` and `holidays/` directories if they don't exist.
2.  Download the necessary CSV and JSON files into these directories.
3.  Process the data using Polars.
4.  Print a summary of the enrichment process, including the number of crashes that occurred on a holiday.

## Output

The script generates a single output file in the project's root directory:

- **`crashes.parquet`**: A Parquet file containing all the original collision data, enriched with holiday information. It includes an `IS_HOLIDAY` boolean column for easy analysis.

## Data Analysis

The project includes a Jupyter Notebook, `analysis.ipynb`, for exploratory data analysis of the final `crashes.parquet` file.

### Running the Analysis

1.  Ensure you have installed the project dependencies by running `pdm install`. This includes libraries for analysis and visualization like `matplotlib`, `seaborn`, and `pyarrow`.
2.  Launch Jupyter Lab from your terminal:
    ```bash
    pdm run jupyter lab
    ```
3.  Open `analysis.ipynb` and run the cells to see the analysis.

### Notebook Content
- A comparison of total crashes on holidays versus non-holidays.
- A breakdown of the holidays with the highest number of crashes, aggregated across all years.

## Data Sources

- **Motor Vehicle Collisions**: Provided by NYC Open Data.
- **Public Holidays**: Provided by the Nager.Date API.
