from ingest_weather import fetch_hourly_observed
from transform_daily import build_weather_daily

def main():
    hourly = fetch_hourly_observed()
    daily = build_weather_daily(hourly)

    print("Hourly rows:", len(hourly))
    print("Daily rows:", len(daily))
    print(daily.sort_values(["date_local"]).tail(10))
    print(
    daily[["city", "date_local", "hours_observed", "hours_expected", "completeness_rate", "is_complete"]]
    .sort_values("date_local")
)


if __name__ == "__main__":
    main()
