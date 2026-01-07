import requests
from datetime import datetime, timedelta
import holidays
import os

def get_weather_features(month: int, day: int, hour: int):
    """
    Récupère les données météo pour une date donnée à Stockholm via Open-Meteo.
    On utilise l'année en cours par défaut.
    """
    year = datetime.now().year
    target_date = datetime(year, month, day, hour)
    now = datetime.now()
    
    # Coordonnées Stockholm
    LAT, LON = 59.3251172, 18.0710935
    
    # Choix de l'API (Archive vs Forecast)
    # Open-Meteo Forecast API couvre J-2 à J+7 (ou plus selon paramètres)
    # Archive API couvre jusqu'à J-2 environ
    is_archive = target_date < (now - timedelta(days=2))
    
    if is_archive:
        url = "https://archive-api.open-meteo.com/v1/archive"
    else:
        url = "https://api.open-meteo.com/v1/forecast"
        
    date_str = target_date.strftime("%Y-%m-%d")
    
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": [
            "temperature_2m", "precipitation", "rain", "snowfall",
            "weather_code", "cloud_cover", "dew_point_2m", "wind_speed_10m", 
            "wind_gusts_10m", "wind_direction_10m"
        ],
        "daily": ["sunrise", "sunset"],
        "timezone": "Europe/Stockholm"
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # On récupère l'index correspondant à l'heure
        # Les données horaires commencent à 00:00
        h_idx = hour
        
        hourly = data.get("hourly", {})
        res = {
            "temperature_2m": hourly.get("temperature_2m")[h_idx],
            "precipitation": hourly.get("precipitation")[h_idx],
            "rain": hourly.get("rain")[h_idx],
            "snowfall": hourly.get("snowfall")[h_idx],
            "weather_code": hourly.get("weather_code")[h_idx],
            "cloud_cover": hourly.get("cloud_cover")[h_idx],
            "dew_point_2m": hourly.get("dew_point_2m")[h_idx],
            "wind_speed_10m": hourly.get("wind_speed_10m")[h_idx],
            "wind_gusts_10m": hourly.get("wind_gusts_10m")[h_idx],
            "wind_direction_10m": hourly.get("wind_direction_10m")[h_idx],
        }
        
        # Soleil levé ?
        sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
        sunset = datetime.fromisoformat(data["daily"]["sunset"][0])
        res["soleil_leve"] = 1 if sunrise <= target_date <= sunset else 0
        
        # Logique Risques
        res["risque_gel_pluie"] = 1 if res["temperature_2m"] <= 0 and res["weather_code"] in [61, 63, 65] else 0
        res["risque_gel_neige"] = 1 if res["temperature_2m"] <= 0 and res["weather_code"] in [71, 73, 75] else 0
        res["neige_fondue"] = 1 if res["temperature_2m"] > 0 and res["weather_code"] in [71, 73, 75] else 0
        
        return res
    except Exception as e:
        print(f"Erreur API Météo: {e}")
        # On propage l'erreur pour que l'API principale la gère
        raise Exception(f"Impossible de récupérer les données météo : {str(e)}")

def get_calendar_features(month: int, day: int, day_of_week: int):
    """
    Calcule les features calendaires pour Stockholm.
    """
    year = datetime.now().year
    dt = datetime(year, month, day)
    
    se_holidays = holidays.CountryHoliday('SE')
    
    is_weekend = 1 if day_of_week in [5, 6] else 0
    is_holiday = 1 if dt in se_holidays else 0
    
    # Vacances scolaires (simplifié comme dans le pipeline original)
    def est_vacances_scolaires(date):
        week = date.isocalendar()[1]
        m, d = date.month, date.day
        if week == 9: return 1 # Sportlov
        if week == 15: return 1 # Pasklov
        if 24 <= week <= 33: return 1 # Sommarlov
        if week == 44: return 1 # Höstlov
        if (m == 12 and d >= 21) or (m == 1 and d <= 8): return 1 # Jullov
        return 0
        
    return {
        "est_weekend": is_weekend,
        "est_jour_ferie": is_holiday,
        "vacances_scolaires": est_vacances_scolaires(dt)
    }
