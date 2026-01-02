from transform_daily import build_weather_daily, hours_expected_for_date
from db import get_conn
import pandas as pd

INSERT_SQL = """
INSERT INTO mart.weather_daily (
    city, latitude, longitude, date_local,
    avg_temperature_c, min_temperature_c, max_temperature_c,
    total_precipitation_mm, avg_wind_speed_kmh,
    hours_observed, hours_expected, completeness_rate, is_complete
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, date_local)
DO UPDATE SET 
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    avg_temperature_c = EXCLUDED.avg_temperature_c,
    min_temperature_c = EXCLUDED.min_temperature_c,
    max_temperature_c = EXCLUDED.max_temperature_c,
    total_precipitation_mm = EXCLUDED.total_precipitation_mm,
    avg_wind_speed_kmh = EXCLUDED.avg_wind_speed_kmh,
    hours_observed = EXCLUDED.hours_observed,
    hours_expected = EXCLUDED.hours_expected,
    completeness_rate = EXCLUDED.completeness_rate,
    is_complete = EXCLUDED.is_complete,
    updated_at_utc = now();
"""

SQL_RAW = """
SELECT city, latitude, longitude, ts_utc,
    temperature_c, precipitation_mm, wind_speed_kmh
FROM raw.weather_hourly
WHERE ts_utc >= %s AND ts_utc < %s
ORDER BY ts_utc;
"""

def main():
    conn = None
    try:
        conn = get_conn()

        start_ts = pd.Timestamp.utcnow().floor("D") - pd.Timedelta(days=2)
        end_ts = pd.Timestamp.utcnow() + pd.Timedelta(days=1)

        # Lecture RAW depuis Postgres
        with conn.cursor() as cur:
            cur.execute(SQL_RAW, (start_ts, end_ts))
            rows = cur.fetchall()
            
            df_hourly_raw = pd.DataFrame(rows, columns=[
                "city", "latitude", "longitude", "ts_utc",
                "temperature_c", "precipitation_mm", "wind_speed_kmh"
            ])

        if df_hourly_raw.empty:
            print("No raw data found for the specified period.")
            return
        
        # Transformation vers MART
        df_daily = build_weather_daily(df_hourly_raw)

        print(df_daily.columns.tolist())

        df_daily["hours_expected"] = df_daily["date_local"].apply(
            lambda d: hours_expected_for_date(pd.Timestamp(d))
        )
        
        # Recalcul des métriques de qualité (sécurisation)
        df_daily["completeness_rate"] = (
            df_daily["hours_observed"] / df_daily["hours_expected"]
        )

        df_daily["is_complete"] = (
            df_daily["hours_observed"] == df_daily["hours_expected"]
        )

        # Upsert vers MART
        with conn.cursor() as cur:
            for _, row in df_daily.iterrows():
                cur.execute(
                    INSERT_SQL,
                    (
                        row["city"],
                        row["latitude"],
                        row["longitude"],
                        row["date_local"],
                        row["avg_temperature_c"],
                        row["min_temperature_c"],
                        row["max_temperature_c"],
                        row["total_precipitation_mm"],
                        row["avg_wind_speed_kmh"],
                        int(row["hours_observed"]),
                        int(row["hours_expected"]),
                        float(row["completeness_rate"]),
                        bool(row["is_complete"]),
                    )
                )
        conn.commit()
        print(f"Upserted {len(df_daily)} rows into mart.weather_daily")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()