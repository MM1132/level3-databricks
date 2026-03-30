# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests>=2.32.5",
# ]
# ///

import requests
import time
import threading
from .request_config import RequestConfig
from .common.lh_api import LHApi
from utils.save import save_flights_databricks
from requester.Worker import Worker

class Client:
    MAX_WORKERS = 5
    MAX_REQUESTS_PER_SECOND = 5

    access_token_url = "https://developer.lufthansa.com/io-docs/getoauth2accesstoken"

    access_token = None
    workers = []
    def __init__(self, save_function, spark, name: str, client_id: str, client_secret: str):
        self.spark = spark
        self.save_function = save_function

        self.name = name
        self.client_id = client_id
        self.client_secret = client_secret

        self.t = threading.Thread(target=self.retrieve_access_token)
        self.t.start()

        self.total_flight_count = 0

    def retrieve_access_token(self):
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "apiId": "3166",
            "auth_flow": "client_cred"
        }

        request = requests.post(Client.access_token_url, data=payload)
        if request.status_code != 200:
            print(f"ERROR: client {self.client_id} could not retrieve access_token")
            self.access_token = None
            return
        # print(f"Request status: {request.status_code}")

        json = request.json()
        if "result" in json and "access_token" in json["result"]:
            self.access_token = json["result"]["access_token"]
        
        print(f"{self.name}: created with token {self.access_token}")

    def can_start_new_request(self):
        return len([w for w in self.workers if (time.perf_counter() - w.started_time) < 1.0]) < Client.MAX_WORKERS

    def active_worker_count(self):
        alive_workers = [w for w in self.workers if w.t.is_alive()]

        workers_started_within_last_second = [w for w in alive_workers if (time.perf_counter() - w.started_time) < 1.0]

        return len(workers_started_within_last_second)

    def get_requests_count_started_in_last_second(self):
        # They can also be not alive workers, which is completely fine
        workers_started_within_last_second = [w for w in self.workers if (time.perf_counter() - w.started_time) < 1.0]

        return len(workers_started_within_last_second)

    # Filter out all the workers that have stopped their work (thread is not alive anymore)
    def clean_old_workers(self):
        # self.workers = [worker for worker in self.workers if worker.t.is_alive() or (time.perf_counter() - worker.started_time) < 1.0]
        self.workers = [worker for worker in self.workers if worker.t.is_alive()]

    def request_list(self, config: RequestConfig, offset: int):
        # Clean old workers
        self.clean_old_workers()

        # Cannot start more workers
        if len(self.workers) >= Client.MAX_WORKERS:
            return None

        # Cannot start more requests
        if self.get_requests_count_started_in_last_second() >= Client.MAX_REQUESTS_PER_SECOND:
            return None

        # Create and start a new worker
        def worker_function(worker: Worker):
            jsonData = config.fetch_fn(self.access_token, worker, offset)
            print(f"{self.name}: finished fetching offset {offset}")

            # save_local(config, offset, jsonData)
            try:
                if jsonData is not None:
                    self.save_function(self.spark, config, offset, jsonData)
                else:
                    print(f"!!! jsonData was None, from request_list")
            except Exception as e:
                print("Saving failed!")
                print(e)
                raise e

            return jsonData

        new_worker = Worker(worker_function)
        self.workers.append(new_worker)
        return new_worker
    
    def start_flights_request(self, airport_code, date_time_block):
        # Clean old workers
        self.clean_old_workers()

        # Cannot start more workers
        if len(self.workers) >= Client.MAX_WORKERS:
            return None
    
        # Cannot start more requests
        if self.get_requests_count_started_in_last_second() >= Client.MAX_REQUESTS_PER_SECOND:
            return None

        # Create and start a new worker
        # Also when the worker finisher and there are more requests to make for the particular airport,
        # instantly start the new request without ending the worker thread
        def worker_function(worker: Worker, offset = 0):
            # Make sure that the new request does not get made too fast
            while self.get_requests_count_started_in_last_second() >= Client.MAX_REQUESTS_PER_SECOND:
                time.sleep(0.01)

            json_data = LHApi.get_flights(self.access_token, offset, airport_code, date_time_block, worker)
            if json_data is None:
                print("json_data is None in Client.py")

            if json_data is not None:
                try:
                    save_flights_databricks(self.spark, json_data, airport_code, offset, date_time_block)
                except Exception as e:
                    print(f"Error with saving: {e}")
                    raise e

            offset += 50 # Because flights are fetched by 50, not 100

            elapsed_s = (time.perf_counter() - worker.started_time)

            if json_data is not None:
                flight_count = len(json_data["FlightStatusResource"]["Flights"]["Flight"])
                print(f"Fetched {airport_code}, got {flight_count} flights, took {elapsed_s:.2f} seconds")
                self.total_flight_count += flight_count

                total_count = json_data["FlightStatusResource"]["Meta"]["TotalCount"]
                if offset < total_count:
                    return worker_function(worker, offset)

            return json_data

        new_worker = Worker(worker_function)
        self.workers.append(new_worker)
        return new_worker
