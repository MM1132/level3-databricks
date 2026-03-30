from concurrent.futures import ThreadPoolExecutor, as_completed
from .Client import Client
import time
from. request_config import REQUEST_CONFIGS, RequestType
from utils.get_time import get_yesterday_date_time_block
from utils.load import get_saved_airport_codes

def _load_client_credentials(spark):
    client_credentials = [
        { "name": "markus0", "client_id": "", "client_secret": "" },
        { "name": "markus1", "client_id": "", "client_secret": "" },
        { "name": "markus2", "client_id": "", "client_secret": "" },
        { "name": "markus3", "client_id": "", "client_secret": "" },
        { "name": "markus4", "client_id": "", "client_secret": "" },
        { "name": "markus5", "client_id": "", "client_secret": "" },
        { "name": "markus6", "client_id": "", "client_secret": "" },
        { "name": "markus7", "client_id": "", "client_secret": "" },
        { "name": "markus8", "client_id": "", "client_secret": "" },
        { "name": "markus9", "client_id": "", "client_secret": "" },
    ]

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

        for c in client_credentials:
            c["client_id"] = dbutils.secrets.get(scope="lufthansa_level3", key=f"{c['name']}_client_id")
            c["client_secret"] = dbutils.secrets.get(scope="lufthansa_level3", key=f"{c['name']}_client_secret")
    except Exception as e:
        raise RuntimeError(f"Failed to load Databricks secrets for Lufthansa clients: {e}") from e

    return client_credentials

# The cluster of many requesting clients
class LHRequester:
    def __init__(self, save_function, spark):
        self.spark = spark
        self.clients = []
        self.client_credentials = _load_client_credentials(spark)

        for credential in self.client_credentials:
            self.clients.append(Client(save_function, spark, **credential))
        
        # Wait for all the client access tokens to be created
        clients_to_keep = []
        for client in self.clients:
            client.t.join()
            if client.access_token is not None:
                clients_to_keep.append(client)
        self.clients = clients_to_keep
        print()

    def _get_request_config(self, request_type: RequestType):
        if request_type in REQUEST_CONFIGS:
            return REQUEST_CONFIGS[request_type]
        raise ValueError(f"Unsupported request type: {request_type}")

    # Get the client that has started the least amount of requests
    # If all share the same number of workers started, return self.clients[0]
    def _get_least_workers_client(self):
        return min(self.clients, key=lambda c: c.get_requests_count_started_in_last_second())

    def wait_for_all_workers_to_finish(self):
        for client in self.clients:
            for worker in client.workers:
                worker.t.join()
            client.workers = []

    def get_requests_count_started_last_second(self):
        return sum([c.get_requests_count_started_in_last_second() for c in self.clients])

    def fetch_all(self, request_type: RequestType):
        print(f"Passed request type: {request_type}")

        config = self._get_request_config(request_type)

        # Wait for a worker to be available first
        # For simplicity we just wait for all workers to finish
        self.wait_for_all_workers_to_finish()

        least_workers_client = self._get_least_workers_client()
        new_worker = least_workers_client.request_list(config, 0)
        if new_worker.error:
            print(f"Worker error: {new_worker.error}")

        print(f"Worker count on least_workers_client: {len(least_workers_client.workers)}")
        new_worker.t.join()
        total_count = new_worker.result[config.resource_key]["Meta"]["TotalCount"]
        print(f"Fetching {total_count} {config.container_key}:")

        start = time.perf_counter()
        offset = 100
        while offset < total_count:
            # Find the client with the least requests in progress
            least_workers_client = self._get_least_workers_client()

            started = least_workers_client.request_list(config, offset)
            if started is not None:
                offset += 100
                if offset % 1000 == 0:
                    print(f"Requests within last second: {self.get_requests_count_started_last_second()}")
            time.sleep(0.01)

        # Wait for all Clients to finish
        self.wait_for_all_workers_to_finish()
        end = time.perf_counter()

        elapsed_s = (end - start)
        print(f"Finished in {elapsed_s:.2f} seconds with {len(self.clients)} clients")

    def fetch_flights(self):
        # Read in all the city codes
        airport_codes = get_saved_airport_codes()
        
        print(f"Fetching departures for {len(airport_codes)} airports")

        # Get our date and time block
        date_time_block = get_yesterday_date_time_block()

        start_time = time.perf_counter()

        # Start fetching, possibly start all requests at once
        code_i = 0
        while code_i < len(airport_codes):
            # Fetch all flights leaving that airport
            client = self._get_least_workers_client()

            worker = client.start_flights_request(airport_codes[code_i], date_time_block)
            # If the worker didn't start (there was no free workers available)
            # then just wait a little and try again
            if worker is None:
                time.sleep(0.01)
                continue

            if code_i % 50 == 0:
                print(f"Requests within last second: {self.get_requests_count_started_last_second()}")
            code_i += 1
        
        self.wait_for_all_workers_to_finish()
        end_time = time.perf_counter()
        elapsed_s = (end_time - start_time)
        print(f"Finished all departures in {elapsed_s:.2f} seconds with {len(self.clients)} clients")

        total_fetched_flights = sum([c.total_flight_count for c in self.clients])
        print(f"Fetched total {total_fetched_flights} flights")
