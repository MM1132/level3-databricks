from pathlib import Path
import json

def get_saved_airport_codes():
    # 1. The path where the save file directories are located,
    # each directory name is a date when the data was fetched
    load_dir = Path(__file__).resolve().parent.parent.parent.parent/"bronze_data"/"Airports"

    # 2. Get the directory with the latest possible date
    # Put it into variable called `date_directory`
    date_directories = [p for p in load_dir.iterdir() if p.is_dir()]
    if not date_directories:
        return []

    # Date folder names use ISO format, so lexical max is the latest date.
    airports_directory = max(date_directories, key=lambda p: p.name)

    if not airports_directory.exists() or not airports_directory.is_dir():
        return []

    # 4. In the Airports directory, there are lots of Airport files
    # ordered by the offset with which they were fetched
    # examples: `Airports_0.json`, `Airports_100.json`
    # Loop through those files and convert each of them to a Python dictionary from JSON
    # Extract the airport codes from the airports in the list, and add them to the list of airport codes here

    # The list of airports is located in the created Dictionary: 
    """
    airports = airports_dict["AirportResource"]["Airports"]["Airport"]
    # And airport code can be extracted this way:
    airport_code = airports[airport_index]["AirportCode"]
    """

    airport_codes = []
    seen_codes = set()

    airport_files = sorted(
        [p for p in airports_directory.glob("*.json") if p.is_file()],
        key=lambda p: int(p.stem.rsplit("_", 1)[-1]) if "_" in p.stem and p.stem.rsplit("_", 1)[-1].isdigit() else p.stem,
    )

    for airport_file in airport_files:
        with open(airport_file, "r") as f:
            airports_dict = json.load(f)

        airports = (
            airports_dict
            .get("AirportResource", {})
            .get("Airports", {})
            .get("Airport", [])
        )

        if isinstance(airports, dict):
            airports = [airports]

        for airport in airports:
            airport_code = airport.get("AirportCode")
            if airport_code and airport_code not in seen_codes:
                seen_codes.add(airport_code)
                airport_codes.append(airport_code)

    # Finally return the finalized array
    return airport_codes
