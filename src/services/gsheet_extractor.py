"""
This code is used to extract data from a Google Sheet and be called from other codes.
"""

import os
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


class GsheetConfig:
    def __init__(self) -> None:
        self.credentials = {
            "type": os.getenv("GSHEET_TYPE"),
            "project_id": os.getenv("GSHEET_PROJECT_ID"),
            "private_key_id": os.getenv("GSHEET_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GSHEET_PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("GSHEET_CLIENT_EMAIL"),
            "client_id": os.getenv("GSHEET_CLIENT_ID"),
            "auth_uri": os.getenv("GSHEET_AUTH_URI"),
            "token_uri": os.getenv("GSHEET_TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv(
                "GSHEET_AUTH_PROVIDER_X509_CERT_URL"
            ),
            "client_x509_cert_url": os.getenv("GSHEET_CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("GSHEET_UNIVERSE_DOMAIN"),
        }

    def __initialize_connection__(self, spreadsheet: str, worksheet: int):
        gc = gspread.service_account_from_dict(self.credentials)
        spreadsheet = gc.open_by_key(spreadsheet)
        sheet = spreadsheet.get_worksheet_by_id(worksheet)

        return sheet

    def from_spreadsheet_to_df(self, spreadsheet: str, worksheet: int) -> pd.DataFrame:
        sheet = self.__initialize_connection__(spreadsheet, worksheet)
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        # df2 = pd.DataFrame(sheet.get_all_records()) falla porque las columnas no tienen nombre unico
        return df

    def from_df_to_worksheet(
        self, df: pd.DataFrame, spreadsheet: str, worksheet: int, method: str
    ) -> None:
        if method not in ["append", "overwrite"]:
            raise ValueError("El modo debe ser 'append' o 'overwrite'.")

        sheet = self.__initialize_connection__(spreadsheet, worksheet)

        if method == "overwrite":
            sheet.clear()
            values = df.values.tolist()
            sheet.insert_rows(values)

        else:
            values = df.values.tolist()
            sheet.insert_rows(values)


# Llamada a la api
g = GsheetConfig()
a = g.from_spreadsheet_to_df(
    spreadsheet="1ZK3dnbF98a1B97ioJWJI1_Zye5NC7MXQZvgLksbxsLs", worksheet=0
)
g.from_df_to_worksheet(
    a.head(),
    spreadsheet="1_MEQ7SN0YaiQJcPHpgdvcrUhUbLSwfaBddyNhgKdL2c",
    worksheet=2119812905,
    method="append",
)
