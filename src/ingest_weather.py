import requests
import pandas as pd
from datetime import datetime, timezone


url = "https://api.open-meteo.com/v1/forecast"

CITY = "Lyon"
LAT = 45.7485
LON = 4.8467

PARAMS = {
    "latitude" : LAT,
    "longitude" : LON,
    "hourly" : "temperature_2m,precipitation,wind_speed_10m",
    "past_days" : 2,
    "timezone" : "UTC"
}

def main():
    r = requests.get(url, params=PARAMS)
    r.raise_for_status()
    data = r.json()
    
    hourly = data['hourly']
    df = pd.DataFrame({
        'ts_utc': pd.to_datetime(hourly['time'], utc=True),
        'temperature_c': hourly['temperature_2m'],
        'precipitation_mm': hourly['precipitation'],
        'wind_speed_10m': hourly['wind_speed_10m']
    })

    # Filtre: on garde uniquement l'observé (pas le futur)
    now_utc = datetime.now(timezone.utc)
    df = df[df["ts_utc"] <= now_utc].copy()

    # Ajout métadonnées RAW
    df["city"] = CITY
    df["latitude"] = LAT
    df["longitude"] = LON
    df["ingested_at_utc"] = now_utc

    print("Rows after filtering future:", len(df))
    print(df.tail(5))

if __name__ == "__main__":
    main()

