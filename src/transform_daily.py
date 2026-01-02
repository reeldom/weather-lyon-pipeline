"""
Transformation RAW (horaire UTC) -> MART (journalier Europe/Paris)

- Convertit ts_utc -> ts_local (Europe/Paris)
- Calcule date_local
- Agrège en KPI journaliers
- Calcule complétude (hours_observed / hours_expected) en tenant compte des jours à 23/24/25h
"""

import pandas as pd
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Europe/Paris")


def hours_expected_for_date(date_local: pd.Timestamp) -> int:
    """
    Calcule le nombre d'heures attendues dans une journée locale Europe/Paris.
    Important pour les jours de changement d'heure (23h/25h).
    """
    start = pd.Timestamp(date_local).tz_localize(LOCAL_TZ)
    end = start + pd.Timedelta(days=1)
    return int((end - start) / pd.Timedelta(hours=1))


def build_weather_daily(df_hourly_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_hourly_raw.copy()

    # 1) Convertir UTC -> heure locale
    df["ts_local"] = df["ts_utc"].dt.tz_convert(LOCAL_TZ)

    # 2) Extraire la date locale (jour "métier" pour Lyon)
    df["date_local"] = df["ts_local"].dt.date  # objet date (sans timezone)

    # 3) Agrégation journalière
    agg = (
        df.groupby(["city", "date_local"], as_index=False)
        .agg(
            latitude=("latitude", "first"),
            longitude=("longitude", "first"),
            avg_temperature_c=("temperature_c", "mean"),
            min_temperature_c=("temperature_c", "min"),
            max_temperature_c=("temperature_c", "max"),
            total_precipitation_mm=("precipitation_mm", "sum"),
            avg_wind_speed_kmh=("wind_speed_kmh", "mean"),
            hours_observed=("ts_utc", "count"),
        )
    )

    # 4) hours_expected + complétude
    # On calcule hours_expected par date (Europe/Paris)
    agg["hours_expected"] = agg["date_local"].apply(
        lambda d: hours_expected_for_date(pd.Timestamp(d))
    )
    agg["completeness_rate"] = agg["hours_observed"] / agg["hours_expected"]
    agg["is_complete"] = agg["hours_observed"] == agg["hours_expected"]

    # 5) arrondir pour lecture
    agg["avg_temperature_c"] = agg["avg_temperature_c"].round(2)
    agg["avg_wind_speed_kmh"] = agg["avg_wind_speed_kmh"].round(2)
    agg["completeness_rate"] = agg["completeness_rate"].round(3)

    return agg
