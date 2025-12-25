
CITY = "Lyon"
LAT = 45.7485
LON = 4.8467

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Contrat d'entrée : dernières 48h + UTC + variables horaires
OPEN_METEO_PARAMS = {
    "latitude": LAT,
    "longitude": LON,
    "hourly": "temperature_2m,precipitation,wind_speed_10m",
    "past_days": 2,
    "timezone": "UTC",
}