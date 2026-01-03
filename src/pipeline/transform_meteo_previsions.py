import os
from transform_meteo_archives import process_etl_meteo

# Configuration du chemin vers le dossier data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

def process_etl_previsions(input_filename):
    """
    Spécifiquement pour les prévisions :
    Appelle la logique commune et peut ajouter des filtres spécifiques.
    """
    print(f"Traitement des PRÉVISIONS : {input_filename}")
    
    # 1. Utilisation de la fonction définie dans transform_archives.py
    df = process_etl_meteo(input_filename)
    
    return df

if __name__ == "__main__":
    # Test manuel
    process_etl_previsions("weather_stockholm_forecast.json")