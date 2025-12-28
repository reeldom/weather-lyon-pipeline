from ingest_weather import fetch_hourly_observed
from db import get_conn

INSERT_SQL = """
INSERT INTO raw.weather_hourly (
    city, latitude, longitude, ts_utc,
    temperature_c, precipitation_mm, wind_speed_kmh,
    ingested_at_utc
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, ts_utc)
DO UPDATE SET 
    temperature_c = EXCLUDED.temperature_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
    ingested_at_utc = EXCLUDED.ingested_at_utc;
"""

def main():
    df = fetch_hourly_observed()

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(
                    INSERT_SQL,
                    (
                        row["city"],
                        row["latitude"],
                        row["longitude"],
                        row["ts_utc"],
                        row["temperature_c"],
                        row["precipitation_mm"],
                        row["wind_speed_kmh"],
                        row["ingested_at_utc"],
                    )
                )
        conn.commit()
        print(f"Upserted {len(df)} rows into raw.weather_hourly")
    finally:
        conn.close()

if __name__ == "__main__":
    main()