"""
Ingestion des données météo horaires pour la ville de Lyon
depuis l'API Open-Meteo.

- Récupère les dernières 48h
- Transforme la réponse JSON en format tabulaire
- Filtre les données futures
- Prépare les données pour un stockage RAW
"""


import requests
import pandas as pd
from datetime import datetime, timezone

from config import CITY, LAT, LON, OPEN_METEO_URL, OPEN_METEO_PARAMS


def fetch_hourly_observed() -> pd.DataFrame:
    r = requests.get(OPEN_METEO_URL, params=OPEN_METEO_PARAMS)
    r.raise_for_status()
    data = r.json()
    
    hourly = data['hourly']
    df = pd.DataFrame({
        'ts_utc': pd.to_datetime(hourly['time'], utc=True),
        'temperature_c': hourly['temperature_2m'],
        'precipitation_mm': hourly['precipitation'],
        'wind_speed_kmh': hourly['wind_speed_10m']
    })

    # Filtre: on garde uniquement l'observé (pas le futur)
    now_utc = datetime.now(timezone.utc)
    df = df[df["ts_utc"] <= now_utc].copy()

    # Ajout métadonnées RAW
    df["city"] = CITY
    df["latitude"] = LAT
    df["longitude"] = LON
    df["ingested_at_utc"] = now_utc

    return df


def main() -> None:
    df = fetch_hourly_observed()
    print("Rows: ", len(df))
    print(df.tail(5))


if __name__ == "__main__":
    main()

