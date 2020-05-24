import json
import requests
from requests.exceptions import HTTPError
import sys


class APIClient:

    def __init__(self):
        cities_config_data = open("config/cities_config_data.json")
        self.cities = json.load(cities_config_data)
        api_config = open("config/api_config.json")
        api = json.load(api_config)
        self.redirect = api["URL"]
        self.upcoming_timestamps_amount = api["passes"]

    def connect(self, city):
        city_lat = self.cities[city]["latitude"]
        city_lon = self.cities[city]["longitude"]
        PARAMS = {"lat": city_lat, "lon": city_lon, "n": self.upcoming_timestamps_amount}
        try:
            response = requests.get(url=self.redirect, params=PARAMS)
            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except requests.exceptions.Timeout as timeout_err:
            print(timeout_err)
        except requests.exceptions.TooManyRedirects as badurl_err:
            print(badurl_err, "Domain wasn't found. Try a different one")
        except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
            print(e)
            sys.exit(1)
        else:
            return response

    def parse_json(self):
        res = {}
        for city in self.cities:
            try:
                response = self.connect(city)
                if response:
                    data = response.json()
                    city_data = data["response"]
                    res[city] = city_data

            except Exception as exc:
                print(exc)

        return res
