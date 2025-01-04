import pandas as pd
import numpy as np
# from ...config.gsheet_extractor import GsheetConfig


class DailyUsers:
    def __init__(
        self,
    ) -> None:
        self.df = pd.DataFrame

    def __read_bbdd_users__(self, columns: list) -> pd.DataFrame:
        df = pd.read_csv(
            "Usuarios-Grid view.csv", parse_dates=["PRIMER INGRESO"], dayfirst=True
        )
        df_filtered = df[columns]
        df.drop_duplicates("Phone", keep="first", inplace=True)
        df_filtered.rename(columns={"Phone": "Usuarios"}, inplace=True)

        return df_filtered

    def __read_sheets_users_interaction__(
        self,
    ) -> pd.DataFrame:
        df = pd.read_csv("Ingresos-Grid view.csv")

        df = df.drop(["Name", "Inputs"], axis=1)
        df["DetalleIngreso"] = pd.to_datetime(
            df["DetalleIngreso"], format="%Y-%m-%d %H:%M:%S"
        )
        df["Fecha"] = df["DetalleIngreso"].dt.date
        df["Hora"] = df["DetalleIngreso"].dt.hour
        df["Mes"] = df["DetalleIngreso"].dt.strftime("%Y-%m")
        df["Semana"] = (
            df["DetalleIngreso"]
            - pd.to_timedelta(df["DetalleIngreso"].dt.dayofweek, unit="d")
        ).dt.date

        return df

    def __read_list_users_push__(
        self,
    ) -> pd.DataFrame:
        df = pd.read_excel("list_users.xlsx")
        df.drop_duplicates("phone", keep="first", inplace=True)
        df = df[["phone"]]
        df.rename(columns={"phone": "Usuarios"}, inplace=True)

        return df

    def __interaction_users_with_category__(
        self, type_user: str = None
    ) -> pd.DataFrame:
        df = self.__read_sheets_users_interaction__()
        df2 = self.__read_list_users_push__()

        all_users = df.merge(df2, how="left", on="Usuarios", indicator=True)
        all_users["_merge"] = all_users["_merge"].replace(
            {"both": "push", "left_only": "organic"}
        )
        if type_user != None:
            df3 = all_users[all_users["_merge"] == type_user]
            return df3
        else:
            return all_users

    def __historic_users__(
        self,
    ) -> pd.DataFrame:
        df = pd.read_excel("Historico Push.xlsx")

        return df

    # TODO: Modificar para que se pueda agrupar por semana, mes o dia
    def total_new_users_per_date(
        self,
    ) -> pd.DataFrame:
        columns = ["Phone", "NombreWP", "PRIMER INGRESO"]
        df = self.__read_bbdd_users__(columns)

        df["PRIMER INGRESO"] = pd.to_datetime(df["PRIMER INGRESO"], format="%d-%m-%Y")
        df["Fecha"] = df["PRIMER INGRESO"].dt.date
        df["Mes"] = df["PRIMER INGRESO"].dt.month
        df["Semana"] = df["PRIMER INGRESO"].dt.weekday

        df = df.groupby("Fecha", as_index=False)["Usuarios"].count()

        df.sort_values("Fecha", ascending=True)
        df.rename(columns={"Usuarios": "Usuarios_Nuevos"}, inplace=True)

        return df

    def group_users_by_date(self, time_to_group: str = "Fecha") -> pd.DataFrame:
        df = self.__interaction_users_with_category__()

        tot_users_daily = df.groupby(time_to_group, as_index=False).agg(
            {"Usuarios": "nunique"}
        )
        tot_users_daily.rename(columns={"Usuarios": "Total_Ingresos"}, inplace=True)

        users_push = df[df["_merge"] == "push"]
        users_organic = df[df["_merge"] == "organic"]

        users_push = users_push.groupby(time_to_group, as_index=False).agg(
            {"Usuarios": "nunique"}
        )
        users_push.rename(columns={"Usuarios": "Ingresos_Push"}, inplace=True)

        users_organic = users_organic.groupby(time_to_group, as_index=False).agg(
            {"Usuarios": "nunique"}
        )
        users_organic.rename(columns={"Usuarios": "Ingresos_Organicos"}, inplace=True)

        df2 = tot_users_daily.merge(users_push, on=time_to_group, how="outer")
        df2 = df2.merge(users_organic, on=time_to_group, how="outer")

        df2 = df2.sort_values(by=[time_to_group])
        df2[f"Variacion_{time_to_group}"] = df2["Total_Ingresos"].pct_change()

        if time_to_group != "Fecha":
            df_historico = self.total_users_push_per_date(date=time_to_group)
            df2 = df2.merge(df_historico, on=time_to_group, how="left")
            df2["Tasa_Apertura_Push"] = df2["Ingresos_Push"] / df2["Usuarios_Push"]

            columns = [
                f"{time_to_group}",
                "Total_Ingresos",
                f"Variacion_{time_to_group}",
                "Ingresos_Push",
                "Ingresos_Organicos",
                "Usuarios_Push",
                "Tasa_Apertura_Push",
            ]

            return df2[columns]

        else:
            return df2

    def verbosity_daily_users(
        self,
    ) -> pd.DataFrame:
        df = self.group_users_by_date()
        df2 = self.total_new_users_per_date()

        combined_data = df.merge(df2, on="Fecha", how="outer")

        combined_data["Fecha"].fillna(combined_data["Fecha"], inplace=True)
        combined_data["Usuarios_Nuevos"].fillna(0, inplace=True)

        combined_data["Fecha"] = pd.to_datetime(combined_data["Fecha"])
        combined_data = combined_data.sort_values(by="Fecha", ascending=True)
        combined_data["Total_Usuarios"] = combined_data["Usuarios_Nuevos"].cumsum()
        combined_data["Usuarios_Push"] = 250

        combined_data["Usuarios_Organicos"] = (
            combined_data["Total_Usuarios"] - combined_data["Usuarios_Push"]
        )
        combined_data["Usuarios_Organicos"] = np.where(
            combined_data["Usuarios_Organicos"] < 0,
            0,
            combined_data["Usuarios_Organicos"],
        )

        combined_data["Ratio_Organico"] = (
            combined_data["Usuarios_Organicos"] / combined_data["Total_Usuarios"]
        )
        combined_data.insert(
            4,
            "Ratio_Push",
            combined_data["Usuarios_Push"] / combined_data["Total_Usuarios"],
        )

        combined_data["Tasa_Ingresos"] = (
            combined_data["Total_Ingresos"] / combined_data["Total_Usuarios"]
        )
        combined_data["Tasa_Push"] = (
            combined_data["Ingresos_Push"] / combined_data["Total_Ingresos"]
        )
        combined_data["Tasa_Organico"] = (
            combined_data["Ingresos_Organicos"] / combined_data["Total_Ingresos"]
        )
        combined_data["Tasa_Apertura_Push"] = (
            combined_data["Ingresos_Push"] / combined_data["Usuarios_Push"]
        )
        combined_data = combined_data.sort_values(by="Fecha", ascending=False)

        columns = [
            "Fecha",
            "Total_Ingresos",
            "Tasa_Ingresos",
            "Ingresos_Push",
            "Tasa_Push",
            "Ingresos_Organicos",
            "Tasa_Organico",
            "Usuarios_Nuevos",
            "Total_Usuarios",
            "Usuarios_Push",
            "Ratio_Push",
            "Tasa_Apertura_Push",
            "Usuarios_Organicos",
            "Ratio_Organico",
        ]

        return combined_data[columns]

    def total_users_push_per_date(self, date: str = "Semana") -> pd.DataFrame:
        df = self.__historic_users__()
        df["Fecha"] = pd.to_datetime(df["Fecha"])
        df["Mes"] = df["Fecha"].dt.strftime("%Y-%m")
        df["Semana"] = (
            df["Fecha"] - pd.to_timedelta(df["Fecha"].dt.dayofweek, unit="d")
        ).dt.date

        df = df.groupby(date, as_index=False)["Usuarios"].nunique()
        df.rename(columns={"Usuarios": "Usuarios_Push"}, inplace=True)

        return df

    def details_users_by_category(self, user_type: str = "both") -> pd.DataFrame:
        columns = [
            "Phone",
            "NombreWP",
            "CONTEO INGRESOS",
            "ULTIMO",
            "ULTIMA HORA",
            "ELIMINAR",
        ]
        df = self.__read_list_users_push__()
        df2 = self.__read_bbdd_users__(columns=columns)

        df3 = df.merge(df2, how="outer", on="Usuarios", indicator=True)

        columns = ["Usuarios", "NombreWP", "CONTEO INGRESOS", "ULTIMO"]

        df3 = df3[df3["_merge"] == user_type]
        df3 = df3[columns].sort_values(["CONTEO INGRESOS"], ascending=False)
        return df3[columns]

    def users_per_hour(self, filter: bool = True) -> pd.DataFrame:
        df = self.__read_sheets_users_interaction__()

        hours = range(0, 24)
        resultados = pd.DataFrame(
            columns=["dia", "hora", "total", "acumulado", "usuarios_unicos", "usuarios"]
        )

        for date in df["Fecha"].unique():
            df_day = df[df["Fecha"] == date]
            usuarios_unicos_globales = set()

            for hora in hours:
                df_hora = df_day[df_day["Hora"] == hora]
                total_en_hora = len(df_hora)
                usuarios_unicos_hora = set()
                usuarios_hora = df_hora["Usuarios"].nunique()

                for index, row in df_hora.iterrows():
                    usuario = row["Usuarios"]
                    if usuario not in usuarios_unicos_globales:
                        usuarios_unicos_globales.add(usuario)
                        usuarios_unicos_hora.add(usuario)

                if resultados.empty:
                    acumulado = total_en_hora
                else:
                    acumulado = (
                        resultados.loc[resultados["hora"] < hora, "total"].sum()
                        + total_en_hora
                    )

                resultados = resultados.append(
                    {
                        "dia": date,
                        "hora": hora,
                        "total": total_en_hora,
                        "acumulado": acumulado,
                        "usuarios_unicos": len(usuarios_unicos_hora),
                        "usuarios_hora": usuarios_hora,
                    },
                    ignore_index=True,
                )

        return resultados

    def details_usage_per_date(
        self,
    ) -> pd.DataFrame:
        df = self.__read_sheets_users_interaction__()
        columns = ["Phone", "NombreWP", "PRIMER INGRESO"]
        df2 = self.__read_bbdd_users__(columns=columns)
        df2.drop_duplicates("Usuarios", keep="first", inplace=True)

        df_resumen = df.groupby(["Usuarios", "Fecha"]).size().unstack(fill_value=0)
        df_resumen["Total_Interacciones"] = (df_resumen > 0).sum(axis=1)
        df_resumen = df_resumen.fillna(0).astype(int)
        df_resumen["ultima_vez"] = df.groupby("Usuarios")["DetalleIngreso"].max()
        df_resumen.reset_index(inplace=True)
        cols = (
            ["Usuarios"]
            + ["ultima_vez"]
            + ["Total_Interacciones"]
            + [
                col
                for col in df_resumen.columns
                if col not in ["Usuarios", "ultima_vez", "Total_Interacciones"]
            ]
        )
        df_resumen = df_resumen[cols]

        df3 = self.__read_list_users_push__()

        df_resumen = df2.merge(df_resumen, how="inner", on="Usuarios")
        df_resumen = df_resumen.merge(df3, how="left", on="Usuarios", indicator=True)

        df_resumen["_merge"] = df_resumen["_merge"].replace(
            {"both": "push", "left_only": "organic"}
        )

        df_resumen.rename(columns={"_merge": "tipo_usuario"}, inplace=True)

        df_resumen = df_resumen.sort_values(by="ultima_vez", ascending=False)

        return df_resumen


test = DailyUsers()
df = test.verbosity_daily_users()
df2 = test.group_users_by_date(time_to_group="Semana")
df3 = test.group_users_by_date(time_to_group="Mes")
df4 = test.details_users_by_category(user_type="both")
df5 = test.details_users_by_category(user_type="right_only")

df.to_excel("1.overview daily.xlsx", index=False)
df2.to_excel("2.weekly_users.xlsx", index=False)
df3.to_excel("3.monthly_users.xlsx", index=False)
df4.to_excel("4.detail_users_push.xlsx", index=False)
df5.to_excel("5.detail_users_no_push.xlsx", index=False)
