from dataclasses import dataclass
from settings import landbot_env
from typing import Optional
import json
import requests


@dataclass
class ExtractorLandbot:
    """
    get: repository where you'd like get data.
    """

    get: str

    def __post_init__(self):
        allowed_types = ["channels", "customers"]
        if self.get not in allowed_types:
            raise ValueError(
                f"Invalid param 'get'. Allowed types are: {', '.join(allowed_types)}"
            )

    def __initialize_connection__(self, offset: int = 0, limit: int = 200) -> dict:
        url = f"https://api.landbot.io/v1/{self.get}/"

        headers = {
            "Authorization": f"Token {landbot_env.USER}",
            "Content-Type": "application/json",
        }

        params = {"limit": limit, "offset": offset}

        r = requests.get(url=url, headers=headers, params=params)
        data = r.json()

        return data

    def extract_data(self):
        offset = 0
        limit = 200
        all_data = []

        while True:
            data = self.__initialize_connection__(offset)
            print(f"el offset es {offset}")
            customers = data.get("customers", [])
            if not customers:
                break  # Si no hay más registros, salir del bucle

            all_data.append(data)
            print(f"Total de datos en all_data: {len(all_data)}")
            offset += limit


test = ExtractorLandbot(get="customers")
test.extract_data()
