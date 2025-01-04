import pandas as pd
import datetime
import numpy as np


class UsersPush:
    def __init__(
        self,
    ) -> None:
        self.df = pd.DataFrame

    def __read_bbdd_users__(self, columns: list) -> pd.DataFrame:
        df = pd.read_csv(
            "Usuarios-Grid view.csv", parse_dates=["PRIMER INGRESO"], dayfirst=True
        )
        df_filtered = df[columns]
        df_filtered.rename(columns={"Phone": "Usuarios"}, inplace=True)
        df_filtered.drop_duplicates("Usuarios", inplace=True)

        return df_filtered

    def __historical_list_push__(
        self,
    ) -> pd.DataFrame:
        df = pd.read_excel("Historico Push.xlsx")

        return df

    def __strategic_users__(
        self,
    ) -> pd.DataFrame:
        df = pd.read_excel("Historico Push.xlsx", sheet_name="Usuarios Claves")
        df.drop_duplicates("Usuarios", inplace=True)
        df["category"] = "strategic"

        return df[["Usuarios", "category"]]

    def __get_interactions_push__(
        self,
    ) -> pd.DataFrame:
        df = pd.read_excel("4.detail_users_push.xlsx")

        return df

    def category_users(
        self,
    ) -> pd.DataFrame:
        df = self.__historical_list_push__()
        df2 = self.__read_bbdd_users__(
            columns=[
                "Phone",
                "ELIMINAR",
                "NombreWP",
                "PRIMER INGRESO",
                "CONTEO INGRESOS",
                "ULTIMO",
            ]
        )
        df_strategic = self.__strategic_users__()
        df_use_push = self.__get_interactions_push__()

        df = df[~df["Usuarios"].isin(df_strategic["Usuarios"])]
        df_use_push = df_use_push[
            ~df_use_push["Usuarios"].isin(df_strategic["Usuarios"])
        ]

        df = df.groupby("Usuarios", as_index=False)["Fecha"].count()
        df["Fecha"] = df["Fecha"].astype(str)
        df["category"] = "push_" + df["Fecha"]
        df.drop(columns="Fecha", inplace=True)

        df2 = df2.merge(df_strategic, how="left", on="Usuarios")
        df2 = df2.append(df_strategic[~df_strategic["Usuarios"].isin(df2["Usuarios"])])
        df2 = df2.merge(df, how="left", on="Usuarios")
        df2["category"] = np.where(
            df2["category_x"].isna(), df2["category_y"], df2["category_x"]
        )
        df2.drop(columns=["category_x", "category_y"], inplace=True)
        df2["category"] = np.where(df2["category"].isna(), "organic", df2["category"])
        df2["category"] = np.where(
            df2["Usuarios"].isin(df_use_push["Usuarios"]),
            "from_last_list",
            df2["category"],
        )

        return df2

    def algorithm_push_list(
        self, threshold: int = 5, len_list: int = 1000
    ) -> pd.DataFrame:
        df_category = self.category_users()
        df_category = df_category[df_category["ELIMINAR"].isna()]
        new_list = df_category[df_category["category"] == "strategic"]

        fecha_corte = pd.to_datetime(
            datetime.date.today() - datetime.timedelta(days=threshold)
        )
        fecha_corte = pd.to_datetime(datetime.date.today())
        while threshold > 0:
            fecha_corte -= pd.DateOffset(days=1)
            if fecha_corte.weekday() < 5:
                threshold -= 1

        df_use_push = df_category[df_category["category"] == "from_last_list"]
        df_use_push = df_use_push[pd.to_datetime(df_use_push["ULTIMO"]) >= fecha_corte]
        new_list = new_list.append(df_use_push)

        df_organic = df_category[df_category["category"] == "organic"]
        df_organic = df_organic.dropna(
            subset=["PRIMER INGRESO", "CONTEO INGRESOS", "ULTIMO"], how="any"
        )
        df_organic.sort_values(
            ["ULTIMO", "CONTEO INGRESOS", "PRIMER INGRESO"],
            ascending=[False, False, False],
            inplace=True,
        )

        i = 0
        while len(new_list) < len_list and i < len(df_organic):
            new_list = new_list.append(df_organic.iloc[i])
            i += 1
        # TODO: Agregar que haga update automatico al masivo.
        return new_list


test = UsersPush()
df = test.algorithm_push_list(threshold=7)
df.to_excel("new_list_push.xlsx", index=False)
