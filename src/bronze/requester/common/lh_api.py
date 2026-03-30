import requests
import time
from utils.utils import deep_get
from requester.Worker import Worker

class LHApi:
    def __init__(self):
        pass

def make_request(url, token, worker: Worker, params = ""):
    MAX_DELAY = 100

    headers = {
        "Authorization": f"Bearer {token}"
    }
    # return requests.get(f"https://lh-proxy.onrender.com{url}", headers=headers)

    full_url = f"https://api.lufthansa.com{url}?limit=100&lang=EN{params}"

    retry_delay = 0.7
    while retry_delay < MAX_DELAY:
        worker.started_time = time.perf_counter()
        request = requests.get(full_url, headers=headers)
        # print(f"API: Request status: {request.status_code}")

        if request.status_code == 401:
            json = request.json()
            print(f"401: {json}")
            return None

        if request.status_code == 404:
            json = request.json()

            exists_broken = deep_get(json, "ProcessingErrors", "ProcessingError", "Description")
            if exists_broken:
                if "semantically incorrect" in json["ProcessingErrors"]["ProcessingError"]["Description"]:
                    print(f"404 Error: Semantically Incorrect")
                    return
            print(request.json())
            return None

        if request.status_code == 429:
            print(f"API: Rate limited. Waiting {retry_delay} seconds")
            time.sleep(retry_delay)
            retry_delay = retry_delay * 2
            continue

        try:
            json = request.json()
            if "Error" in json:
                if "Account Over Queries Per Second Limit" in json["Error"] or "Gateway Timeout" in json["Error"]:
                    print(f"API: Rate limited. Waiting {retry_delay} seconds")
                    worker.started_time = time.perf_counter()
                    time.sleep(retry_delay)
                    retry_delay = retry_delay * 2
                    continue
        except:
            return request

        return request
    return None

class LHApi:
    def get_airports(token: str, worker, offset: str = 0):
        url = "/v1/references/airports"
        extra_params = f"&LHoperated=1&offset={offset}"

        request = make_request(url, token, worker, extra_params)
        try:
            json = request.json()
        except:
            return None
        return json

    def get_countries(token: str, worker, offset: str = 0):
        url = "/v1/mds-references/countries"
        extra_params = f"&offset={offset}"

        request = make_request(url, token, worker, extra_params)
        try:
            json = request.json()
        except:
            print("Could not get JSON out of request :c")
            return None
        return json

    def get_cities(token: str, worker, offset: str = 0):
        url = "/v1/mds-references/cities"
        extra_params = f"&offset={offset}"

        request = make_request(url, token, worker, extra_params)
        try:
            json = request.json()
        except:
            return None
        return json
    
    def get_airlines(token: str, worker, offset: str = 0):
        url = "/v1/mds-references/airlines"
        extra_params = f"&offset={offset}"

        request = make_request(url, token, worker, extra_params)
        try:
            json = request.json()
        except:
            return None
        return json
    
    def get_aircraft(token: str, worker, offset: str = 0):
        url = "/v1/mds-references/aircraft"
        extra_params = f"&offset={offset}"

        request = make_request(url, token, worker, extra_params)
        try:
            json = request.json()
        except:
            return None
        return json

    # Get flights form the city 
    def get_flights(token: str, offset: str, airport_from: str, date_time_block: str, worker: Worker):
        url = f"/v1/operations/flightstatus/departures/{airport_from}/{date_time_block}"
        extra_params = f"&offset={offset}"

        request = make_request(url, token, worker, extra_params)
        try:
            json = request.json()
        except:
            return None
        return json
