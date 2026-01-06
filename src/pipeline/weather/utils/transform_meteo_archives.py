import pandas as pd
import json
import holidays
from datetime import datetime
import os
import uuid


# Configuration du chemin vers le dossier data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

# FONCTION 1 - TRANSFORMATION MÉTÉO
def etl_weather_transformation(json_file):
    """
    Transforme le JSON brut Open-Meteo en DataFrame aplati 
    avec logique Sunrise/Sunset et risques météo.
    """
# Construction du chemin complet vers le fichier dans /data
    # json_path = os.path.join(DATA_DIR, json_filename)
    
    # if not os.path.exists(json_path):
    #     raise FileNotFoundError(f"Le fichier {json_path} est introuvable.")

    # with open(json_path, 'r', encoding='utf-8') as f:
    #     data = json.load(f)

    data = json_file

    # Aplatissement horaire
    df_h = pd.DataFrame(data['hourly'])
    df_h['timestamp'] = pd.to_datetime(df_h['time'])
    
    # Arrondi à l'heure la plus proche pour jointure transport
    #df_h['timestamp_rounded'] = df_h['timestamp'].dt.round('H')

    # Aplatissement journalier (sunrise/sunset)
    df_d = pd.DataFrame(data['daily'])
    df_d['date'] = pd.to_datetime(df_d['time']).dt.date
    df_d['sunrise'] = pd.to_datetime(df_d['sunrise'])
    df_d['sunset'] = pd.to_datetime(df_d['sunset'])

    # Merge Daily/Hourly pour ajouter sunrise/sunset à chaque ligne horaire
    df_h['date'] = df_h['timestamp'].dt.date
    df = df_h.merge(df_d[['date', 'sunrise', 'sunset']], on='date', how='left')

    # --- Feature Engineering Météo ---
    # Logique Jour/Nuit
    df['soleil_leve'] = ((df['timestamp'] >= df['sunrise']) & (df['timestamp'] <= df['sunset'])).astype(int)
    
    # Logique Risques (Précipitations + Température)
    # Codes 61,63,65 (pluie) | Codes 71,73,75 (neige)
    df['risque_gel_pluie'] = ((df['temperature_2m'] <= 0) & (df['weather_code'].isin([61, 63, 65]))).astype(int)
    df['risque_gel_neige'] = ((df['temperature_2m'] <= 0) & (df['weather_code'].isin([71, 73, 75]))).astype(int)
    df['neige_fondue'] = ((df['temperature_2m'] > 0) & (df['weather_code'].isin([71, 73, 75]))).astype(int)
    
    return df

# FONCTION 2 - ENRICHISSEMENT CALENDRIER SUÉDOIS
def enrich_calendar_features(df):
    """Ajoute les spécificités du calendrier suédois."""
    sweden_holidays = holidays.CountryHoliday('SE')

    # Bases (année, mois, jour de la semaine, weekend, jour férié)
    df["observation_uuid"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    # df['day'] = df['timestamp'].dt.day
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['est_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['est_jour_ferie'] = df['timestamp'].dt.date.apply(lambda x: 1 if x in sweden_holidays else 0)
    
    # Vacances scolaires suédoises (silmplification)
    def est_vacances_scolaires(date):
        week = date.isocalendar()[1]
        month, day = date.month, date.day
        if week == 9: return 1 # Sportlov
        if week == 15: return 1 # Pasklov
        if 24 <= week <= 33: return 1 # Sommarlov
        if week == 44: return 1 # Höstlov
        if (month == 12 and day >= 21) or (month == 1 and day <= 8): return 1 # Jullov
        return 0

    df['vacances_scolaires'] = df['timestamp'].apply(est_vacances_scolaires)


    return df

# FONCTION 3 - PIPELINE COMPLÈTE
def process_etl_meteo(input_path):
    """Fonction appelée par le run.py
    Execute la pipeline ETL complète : 
    - transformation météo 
    - enrichissement calendrier 
    - sauvegarde
    Sauvegarde au format Parquet dans /data."
    """
    # 1. Transformations
    df = etl_weather_transformation(input_path)
    df = enrich_calendar_features(df)
    
    # 2. Nettoyage des colonnes temporaires et rennomage
    cols_to_drop = ['date', 'sunrise', 'sunset', 'time']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    # Pour renommer proprement une colonne existante :
    df = df.rename(columns={'timestamp': 'timestamp_rounded'})

    return df

# --- EXÉCUTION ---
if __name__ == "__main__":
    # Test manuel si on lance le script directement
    process_etl_meteo("weather_stockholm_archive.json")