from pathlib import Path
import requests

def download_csv_file(csv_url: str, output_filename: str, output_dir: str) -> str | None:
    """
    Downloads a file from a URL and saves it to a local path.

    Args:
        csv_url: The URL of the file to download.
        output_path: The local path to save the file to.

    Returns:
        The output path if successful, or None if an error occurred.
    """
    csv_output_dir = Path(output_dir)
    output_path = csv_output_dir / output_filename

    try:
        if not output_path.is_file():
        # Ensure the directory exists before attempting to download the file into it
            csv_output_dir.mkdir(parents=True, exist_ok=True)
        
            print(f"Downloading data from {csv_url} to {output_path}...")
            # Use streaming to handle large files efficiently
            with requests.get(csv_url, stream=True) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print("Download complete.")
            return output_path
        else:
            print(f"File '{output_path}' already exists. Skipping download.")
            return str(output_path)
    except (requests.exceptions.RequestException, IOError) as e:
        print(f"An error occurred during processing of a csv file: {e}")
        return None

def download_json_file(json_url: str, output_filename: str, output_dir: str) -> str | None:
    """
    Downloads a JSON file from a URL and saves it to a local path.

    Args:
        json_url: The URL of the JSON file to download.
        output_path: The local path to save the file to.

    Returns:
        The output path if successful, or None if an error occurred.
    """

    output_dir = Path(output_dir)
    output_path = output_dir / output_filename

    try:
        if not output_path.is_file():
        # Ensure the directory exists before attempting to download the file into it
            output_dir.mkdir(parents=True, exist_ok=True)
            print(f"Downloading data from {json_url} to {output_path}...")
            with requests.get(json_url, stream=True) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print("Download complete.")
            return output_path
        else:
            print(f"File '{output_path}' already exists. Skipping download.")
            return str(output_path)        
    except (requests.exceptions.RequestException, IOError) as e:
        print(f"An error occurred during processing of a json file: {e}")
        return None
