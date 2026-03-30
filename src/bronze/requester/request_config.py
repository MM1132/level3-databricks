from dataclasses import dataclass
from typing import Any
from collections.abc import Callable
from requester.common.lh_api import LHApi
from enum import StrEnum, auto

class RequestType(StrEnum):
    AIRPORTS = auto()
    CITIES = auto()
    COUNTRIES = auto()
    AIRLINES = auto()
    AIRCRAFT = auto()
    FLIGHTS = auto()

@dataclass(frozen=True)
class RequestConfig:
    fetch_fn: Callable[[str, int], dict[str, Any] | None]
    resource_key: str
    container_key: str
    list_key: str

REQUEST_CONFIGS: dict[RequestType, RequestConfig] = {
    RequestType.AIRPORTS: RequestConfig(
        fetch_fn=LHApi.get_airports,
        resource_key="AirportResource",
        container_key="Airports",
        list_key="Airport",
    ),
    RequestType.COUNTRIES: RequestConfig(
        fetch_fn=LHApi.get_countries,
        resource_key="CountryResource",
        container_key="Countries",
        list_key="Country",
    ),
    RequestType.CITIES: RequestConfig(
        fetch_fn=LHApi.get_cities,
        resource_key="CityResource",
        container_key="Cities",
        list_key="City",
    ),
    RequestType.AIRLINES: RequestConfig(
        fetch_fn=LHApi.get_airlines,
        resource_key="AirlineResource",
        container_key="Airlines",
        list_key="Airline",
    ),
    RequestType.AIRCRAFT: RequestConfig(
        fetch_fn=LHApi.get_aircraft,
        resource_key="AircraftResource",
        container_key="AircraftSummaries",
        list_key="AircraftSummary",
    ),
    RequestType.FLIGHTS: RequestConfig(
        fetch_fn=LHApi.get_flights,
        resource_key="FlightStatusResource",
        container_key="Flights",
        list_key="Flight",
    ),
}
