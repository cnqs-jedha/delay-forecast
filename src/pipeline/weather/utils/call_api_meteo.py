import os
import json
import requests
from datetime import datetime, timedelta

# Configuration des chemins relatifs
# On remonte de deux niveaux (src, pipeline) pour atteindre la racine du projet
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Création du dossier data s'il n'existe pas
os.makedirs(DATA_DIR, exist_ok=True)

def get_dates(mode="archive"):
    """Calcule les dates de début et de fin selon le mode choisi."""
    today = datetime.now()
    if mode == "archive":
        # Historique : de janvier 2025 jusque hier
        start = "2025-01-01"
        end = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # Prévisions : de J+0 à J+7
        start = today.strftime('%Y-%m-%d')
        end = (today + timedelta(days=7)).strftime('%Y-%m-%d')
    return start, end

def fetch_weather_data(latitude, longitude, mode="archive"):
    """Récupère les données météo et les enregistre en JSON localement."""
    
    start_d, end_d = get_dates(mode)
    
    if mode == "archive":
        url = "https://archive-api.open-meteo.com/v1/archive"
    else:
        url = "https://api.open-meteo.com/v1/forecast"
    
    # Paramètres incluant les variables prédictives et les variables de contrôle
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_d,
        "end_date": end_d,
        "hourly": [
            # Variables principales d'impact
            "temperature_2m",
            "precipitation",
            "rain",
            "snowfall",
            "wind_speed_10m",
            "wind_gusts_10m",
            "weather_code",
            "cloud_cover",
            # Variables de contrôle / Interprétabilité
            "shortwave_radiation",
            "dew_point_2m",
            "wind_direction_10m",
            "uv_index"
        ],
        "daily": ["sunrise", "sunset"],
        "timezone": "Europe/Stockholm"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_data = response.json()

        return raw_data

    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête API : {e}")
        return None

if __name__ == "__main__":
    # Coordonnées Stockholm
    LAT, LON = 59.3251172, 18.0710935

    print("--- Démarrage de la collecte météo ---")

    # 1. Collecte de l'historique
    fetch_weather_data(
        latitude=LAT,
        longitude=LON,
        mode="archive",
        filename="weather_stockholm_archive.json"
    )

    # 2. Collecte des prévisions
    fetch_weather_data(
        latitude=LAT,
        longitude=LON,
        mode="forecast",
        filename="weather_stockholm_forecast.json"
    )