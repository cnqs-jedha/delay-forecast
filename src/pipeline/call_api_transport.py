import os
import requests
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
load_dotenv()


def fetch_transport_koda(date_str):
    """Récupère l'archive historique KoDa (.7z) pour une date donnée."""
    api_key = os.getenv("API_KODA_KEY")
    url = "https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/TripUpdates"
    
    params = {"date": date_str, "key": api_key}
    filename = f"transport_koda_{date_str}.7z"
    output_path = os.path.join(DATA_DIR, filename)

    print(f"Téléchargement KoDa pour le {date_str}...")
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(r.content)
    
    print(f"Archive sauvegardée : {output_path}")
    return filename

def fetch_transport_realtime():
    """Récupère le flux temps réel actuel (.pb)."""
    api_key = os.getenv("API_GTFS_RT_KEY")
    url = "https://opendata.samtrafiken.se/gtfs-rt/sl/TripUpdates.pb"
    
    params = {"key": api_key}
    filename = f"transport_rt_{datetime.now().strftime('%Y%m%d_%H%M')}.pb"
    output_path = os.path.join(DATA_DIR, filename)

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(r.content)
    
    return filename

# --- BLOC D'EXÉCUTION ---
if __name__ == "__main__":
    # Téléchargement de l'archive historique
    print("Démarrage du téléchargement historique...")
    try:
        fichier_archive = fetch_transport_koda("2025-03-04")
        print(f"Terminé : {fichier_archive}")
    except Exception as e:
        print(f"Erreur historique : {e}")

    print("-" * 30)

    # Téléchargement du temps réel actuel
    print("Démarrage du téléchargement temps réel...")
    try:
        fichier_rt = fetch_transport_realtime()
        print(f"Terminé : {fichier_rt}")
    except Exception as e:
        print(f"Erreur temps réel : {e}")