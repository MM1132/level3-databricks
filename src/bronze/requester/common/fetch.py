# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests>=2.32.5",
# ]
# ///

from pathlib import Path
import json
import os.path
from datetime import datetime

def fetch_and_save_local(fetch_function, get_count_from_data, get_data_length, prefix):
    date_today = f"{datetime.now().date()}"

    save_dir = Path(__file__).resolve().parent.parent/"bronze"/date_today/prefix

    total_count_printed = False
    total_count = 0
    offset = 0
    while not total_count_printed or offset < total_count:
        filename = f"{prefix}_{offset}.json"
        output_path = (
            save_dir/filename
        )
        if os.path.exists(output_path):
            print(f"{filename} with offset {offset} already exists. Skipping...")
            offset += 100
            continue

        data = fetch_function(offset)
        if "ProcessingErrors" in data:
            if "ProcessingError" in data["ProcessingErrors"]:
                if "Type" in data["ProcessingErrors"]["ProcessingError"]:
                    print(f"!!! Error: {data["ProcessingErrors"]["ProcessingError"]["Type"]}")
                    print()

            return
        if "error" in data:
            print(f"!!! Error from request: {data["error"]}")
            return
        if data is None:
            print("!!! REQUEST FAILED")
            return

        total_count = int(get_count_from_data(data))
        if not total_count_printed:
            print(f"total_count: {total_count}")
        total_count_printed = True

        fetched_elements_length = get_data_length(data)
        print(f"Fetched: {fetched_elements_length}")
        print()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f)

        offset += 100

def fetch_and_save_databricks(fetch_function, get_count_from_data, get_data_length, prefix):
    output_dir = "/Volumes/main/lufthansa/bronze"
    date_today = f"{datetime.now().date()}"

    total_count_printed = False
    total_count = 0
    offset = 0
    while not total_count_printed or offset < total_count:

        filename = f"{prefix}_{offset}.json"
        output_path = f"{output_dir}/{date_today}_{filename}"

        if os.path.exists(output_path):
            print(f"{filename} with offset {offset} already exists. Skipping...")
            offset += 100
            continue

        data = fetch_function(offset)
        if "ProcessingErrors" in data:
            if "ProcessingError" in data["ProcessingErrors"]:
                if "Type" in data["ProcessingErrors"]["ProcessingError"]:
                    print(f"!!! Error: {data["ProcessingErrors"]["ProcessingError"]["Type"]}")

            break
        if "error" in data:
            print(f"!!! Error from request: {data["error"]}")
            break
        if data is None:
            print("!!! REQUEST FAILED")
            break

        total_count = int(get_count_from_data(data))
        if not total_count_printed:
            print(f"total_count: {total_count}")
        total_count_printed = True

        fetched_elements_length = get_data_length(data)
        print(f"Fetched: {fetched_elements_length}")
        print()

        with open(output_path, "w") as f:
        # Create the path if it does not exist
        # Path(output_path).resolve().parent.mkdir(parents=True, exist_ok=True)
            json.dump(data, f)

        offset += 100
