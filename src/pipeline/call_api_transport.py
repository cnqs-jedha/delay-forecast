import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
load_dotenv()


# ═══════════════════════════════════════════════════════════════════════════
# MODE MANUEL - Décommenter pour initialisation de la base historique
# ═══════════════════════════════════════════════════════════════════════════
# MANUAL_START_DATE = "2024-01-01"  # Date de début historique souhaitée
# MANUAL_END_DATE = "2025-12-31"    # Date de fin historique souhaitée
# ═══════════════════════════════════════════════════════════════════════════


def get_archive_dates(start_date=None, end_date=None):
    """
    Calcule les dates pour l'archive transport.
    
    Args:
        start_date: Date de début manuelle (YYYY-MM-DD) - optionnel
        end_date: Date de fin manuelle (YYYY-MM-DD) - optionnel
    
    Returns:
        tuple: (start_date, end_date) au format string YYYY-MM-DD
    
    Notes:
        - Si les dates sont fournies, elles sont utilisées directement
        - Sinon, retourne la dernière semaine (J-7 à J-1)
    """
    if start_date and end_date:
        return start_date, end_date
    
    today = datetime.now()
    end = (today - timedelta(days=1)).strftime('%Y-%m-%d')  # Hier
    start = (today - timedelta(days=7)).strftime('%Y-%m-%d')  # J-7
    return start, end


def fetch_transport_koda(date_str):
    """
    Récupère l'archive historique KoDa (.7z) pour une date donnée.
    
    Args:
        date_str: Date au format YYYY-MM-DD
    
    Returns:
        str: Nom du fichier téléchargé
    """
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


def fetch_transport_koda_range(start_date=None, end_date=None):
    """
    Récupère les archives historiques KoDa pour une plage de dates.
    
    Args:
        start_date: Date de début (YYYY-MM-DD) - optionnel
        end_date: Date de fin (YYYY-MM-DD) - optionnel
    
    Returns:
        list: Liste des fichiers téléchargés
    """
    start, end = get_archive_dates(start_date, end_date)
    print(f"Période : {start} → {end}")
    
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    
    files = []
    current = start_dt
    while current <= end_dt:
        date_str = current.strftime('%Y-%m-%d')
        try:
            filename = fetch_transport_koda(date_str)
            files.append(filename)
        except Exception as e:
            print(f"⚠️ Échec pour {date_str} : {e}")
        current += timedelta(days=1)
    
    return files

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
    print("--- Démarrage de la collecte transport ---")

    # ═══════════════════════════════════════════════════════════════════════
    # OPTION 1 : Mode automatique (production) - dernière semaine
    # ═══════════════════════════════════════════════════════════════════════
    print("\n[Mode automatique - dernière semaine]")
    try:
        fichiers = fetch_transport_koda_range()  # Utilise J-7 à J-1
        print(f"✅ {len(fichiers)} fichiers téléchargés")
    except Exception as e:
        print(f"❌ Erreur : {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # OPTION 2 : Mode manuel (initialisation) - Décommenter pour utiliser
    # ═══════════════════════════════════════════════════════════════════════
    # print("\n[Mode manuel - période personnalisée]")
    # try:
    #     fichiers = fetch_transport_koda_range(
    #         start_date="2024-01-01",  # Date de début souhaitée
    #         end_date="2024-01-31"     # Date de fin souhaitée
    #     )
    #     print(f"✅ {len(fichiers)} fichiers téléchargés")
    # except Exception as e:
    #     print(f"❌ Erreur : {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # OPTION 3 : Une seule date (debug)
    # ═══════════════════════════════════════════════════════════════════════
    # fichier = fetch_transport_koda("2025-03-04")

    print("\n" + "-" * 50)

    # Téléchargement du temps réel actuel
    print("\n[Temps réel]")
    try:
        fichier_rt = fetch_transport_realtime()
        print(f"✅ Terminé : {fichier_rt}")
    except Exception as e:
        print(f"❌ Erreur temps réel : {e}")