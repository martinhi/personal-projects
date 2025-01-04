from dataclasses import dataclass
from dotenv import load_dotenv
from pyairtable import metadata
import boto3
import json
import os
import pandas as pd
import pyairtable
import datetime

load_dotenv()


@dataclass
class ExtractorAirtable:
    token: str = os.getenv("AIRTABLE_TOKEN")
    base_id: str = os.getenv("AIRTABLE_BASE_ID")
    aws_access_key: str = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    api: pyairtable.Api = None

    def __post_init__(self) -> None:
        self.api = pyairtable.Api(self.token)

    def __extract_data__(self, table_id: str) -> str:
        if self.api is None:
            self.__post_init__()
        table = self.api.table(self.base_id, table_id)
        data = table.all()

        return data

    def get_id_tables(
        self, list_tables: list = None, tables_to_exclude: list = None
    ) -> pd.DataFrame:
        if self.api is None:
            self.__post_init__()
        base = self.api.base(self.base_id)
        tables = metadata.get_base_schema(base)["tables"]
        df = pd.DataFrame(
            [{"id": table["id"], "name": table["name"]} for table in tables]
        )

        if tables_to_exclude:
            df = df[~df["name"].isin(tables_to_exclude)]

        if list_tables:
            df = df[df["name"].isin(list_tables)]

        return df

    def upload_to_s3(self, name_bucket: str = "testmartinwaznews"):
        exclude = ["CANAL PRINCIPAL", "CANALES", "GRILLA"]
        df = self.get_id_tables(tables_to_exclude=exclude)
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
        )
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        for index, row in df.iterrows():
            data = self.__extract_data__(table_id=row["id"])
            data_json = json.dumps(data)
            name_object_s3 = f'{today}/{row["name"]}.json'
            s3.put_object(
                Bucket=name_bucket,
                Key=name_object_s3,
                Body=data_json,
                ContentType="application/json",
            )

            print(
                f"El objeto JSON se ha cargado en S3: s3://{name_bucket}/{name_object_s3}"
            )
