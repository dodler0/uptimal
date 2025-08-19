
import requests

def download_csv_file(csv_url: str, output_path: str) -> str | None:
    """
    Downloads a file from a URL and saves it to a local path.

    Args:
        csv_url: The URL of the file to download.
        output_path: The local path to save the file to.

    Returns:
        The output path if successful, or None if an error occurred.
    """
    try:
        print(f"Downloading data from {csv_url} to {output_path}...")
        # Use streaming to handle large files efficiently
        with requests.get(csv_url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")
        return output_path
    except (requests.exceptions.RequestException, IOError) as e:
        print(f"An error occurred during processing of a csv file: {e}")
        return None

def download_json_file(json_url: str, output_path: str) -> str | None:
    """
    Downloads a JSON file from a URL and saves it to a local path.

    Args:
        json_url: The URL of the JSON file to download.
        output_path: The local path to save the file to.

    Returns:
        The output path if successful, or None if an error occurred.
    """
    try:
        print(f"Downloading data from {json_url} to {output_path}...")
        with requests.get(json_url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")
        return output_path
    except (requests.exceptions.RequestException, IOError) as e:
        print(f"An error occurred during processing of a json file: {e}")
        return None
