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


# ═══════════════════════════════════════════════════════════════════════════
# MODE MANUEL - Décommenter pour initialisation de la base historique
# ═══════════════════════════════════════════════════════════════════════════
# MANUAL_START_DATE = "2024-01-01"  # Date de début historique souhaitée
# MANUAL_END_DATE = "2025-12-31"    # Date de fin historique souhaitée
# ═══════════════════════════════════════════════════════════════════════════


def get_dates(mode="archive", start_date=None, end_date=None):
    """
    Calcule les dates de début et de fin selon le mode choisi.
    
    Args:
        mode: "archive" (historique) ou "forecast" (prévisions)
        start_date: Date de début manuelle (format YYYY-MM-DD) - optionnel
        end_date: Date de fin manuelle (format YYYY-MM-DD) - optionnel
    
    Returns:
        tuple: (start_date, end_date) au format string YYYY-MM-DD
    
    Notes:
        - Si start_date et end_date sont fournis, ils sont utilisés directement
        - Sinon, calcul automatique :
            - archive: dernière semaine (J-7 à J-1)
            - forecast: prochains 7 jours (J à J+7)
    """
    # Mode manuel : si les dates sont fournies explicitement
    if start_date and end_date:
        return start_date, end_date
    
    today = datetime.now()
    
    if mode == "archive":
        # Mode automatique : dernière semaine complète
        end = (today - timedelta(days=1)).strftime('%Y-%m-%d')  # Hier
        start = (today - timedelta(days=7)).strftime('%Y-%m-%d')  # J-7
    else:
        # Prévisions : de J+0 à J+7
        start = today.strftime('%Y-%m-%d')
        end = (today + timedelta(days=7)).strftime('%Y-%m-%d')
    
    return start, end

def fetch_weather_data(latitude, longitude, filename, mode="archive", start_date=None, end_date=None):
    """
    Récupère les données météo et les enregistre en JSON localement.
    
    Args:
        latitude: Latitude du lieu
        longitude: Longitude du lieu
        filename: Nom du fichier de sortie
        mode: "archive" ou "forecast"
        start_date: Date de début (YYYY-MM-DD) - optionnel, sinon calculé automatiquement
        end_date: Date de fin (YYYY-MM-DD) - optionnel, sinon calculé automatiquement
    """
    start_d, end_d = get_dates(mode, start_date, end_date)
    print(f"Période : {start_d} → {end_d}")
    
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

        # Construction du chemin de sortie
        output_path = os.path.join(DATA_DIR, filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
            
        print(f"Données {mode} enregistrées avec succès : {output_path}")
        return output_path

    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête API : {e}")
        return None

if __name__ == "__main__":
    # Coordonnées Stockholm
    LAT, LON = 59.3251172, 18.0710935

    print("--- Démarrage de la collecte météo ---")

    # ═══════════════════════════════════════════════════════════════════════
    # OPTION 1 : Mode automatique (production) - dernière semaine
    # ═══════════════════════════════════════════════════════════════════════
    fetch_weather_data(
        latitude=LAT,
        longitude=LON,
        mode="archive",
        filename="weather_stockholm_archive.json"
    )

    # ═══════════════════════════════════════════════════════════════════════
    # OPTION 2 : Mode manuel (initialisation) - Décommenter pour utiliser
    # ═══════════════════════════════════════════════════════════════════════
    # fetch_weather_data(
    #     latitude=LAT,
    #     longitude=LON,
    #     mode="archive",
    #     filename="weather_stockholm_archive_init.json",
    #     start_date="2024-01-01",  # Date de début souhaitée
    #     end_date="2025-12-31"     # Date de fin souhaitée
    # )

    # Collecte des prévisions (toujours automatique J à J+7)
    fetch_weather_data(
        latitude=LAT,
        longitude=LON,
        mode="forecast",
        filename="weather_stockholm_forecast.json"
    )