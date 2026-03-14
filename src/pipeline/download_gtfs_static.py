"""
Téléchargement des fichiers GTFS statiques pour Stockholm (SL)

Ces fichiers sont nécessaires pour :
- Mapper les route_id vers les numéros de bus (routes.txt)
- Identifier les arrêts (stops.txt)
- Connaître les horaires planifiés (trips.txt, stop_times.txt)

Usage:
    python download_gtfs_static.py

Prérequis:
    - Variable d'environnement API_GTFS_STATIC définie dans .env
    - Clé API obtenue sur https://www.trafiklab.se/

Note:
    Les fichiers statiques changent rarement (quelques fois par an).
    Un seul téléchargement suffit pour un POC ou une démo.
"""

import os
import sys
import zipfile
import requests
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration
GTFS_STATIC_URL = "https://opendata.samtrafiken.se/gtfs/sl/sl.zip"
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sweden_data"


def download_gtfs_static(api_key: str = None, output_dir: Path = None) -> bool:
    """
    Télécharge et extrait les fichiers GTFS statiques pour Stockholm (SL).
    
    Args:
        api_key: Clé API Trafiklab (utilise API_GTFS_STATIC si non fournie)
        output_dir: Dossier de destination (utilise data/sweden_data/ par défaut)
    
    Returns:
        bool: True si succès, False sinon
    """
    # Récupérer la clé API (essaie plusieurs noms de variables)
    api_key = api_key or os.getenv("GTFS_REGIONAL_STATIC_KEY") or os.getenv("API_GTFS_STATIC")
    if not api_key:
        print("[ERREUR] Variable GTFS_REGIONAL_STATIC_KEY ou API_GTFS_STATIC non definie dans .env")
        print("   Obtenez une cle sur https://www.trafiklab.se/")
        return False
    
    # Dossier de sortie
    output_dir = output_dir or OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    zip_path = output_dir / "sl_gtfs.zip"
    
    print("=" * 60)
    print("[DOWNLOAD] Telechargement des fichiers GTFS statiques (Stockholm SL)")
    print("=" * 60)
    
    # Télécharger le fichier ZIP
    url = f"{GTFS_STATIC_URL}?key={api_key}"
    print(f"\n[API] Telechargement depuis Trafiklab...")
    print(f"   URL: {GTFS_STATIC_URL}")
    
    try:
        # ici pour stopper le téléchargement sur GitHub
        if os.getenv("GITHUB_ACTIONS"):
            print("\n[SKIP] GitHub Actions détecté : bypass du téléchargement GTFS pour la CI.")
            return True

        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        # Sauvegarder le ZIP
        total_size = int(response.headers.get('content-length', 0))

        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = (downloaded / total_size) * 100
                    print(f"\r   Progression: {pct:.1f}%", end="", flush=True)
        
        print(f"\n[OK] Fichier telecharge ({downloaded / 1024 / 1024:.1f} MB)")
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("[ERREUR] 401 : Cle API invalide ou expiree")
        elif e.response.status_code == 403:
            print("[ERREUR] 403 : Acces refuse (verifiez les permissions de votre cle)")
        else:
            print(f"[ERREUR] HTTP {e.response.status_code}: {e}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"[ERREUR] Connexion: {e}")
        return False
    
    # Extraire le ZIP
    print(f"\n[EXTRACT] Extraction vers {output_dir}...")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            zip_ref.extractall(output_dir)
        
        # Supprimer le ZIP
        zip_path.unlink()
        
        print(f"[OK] {len(file_list)} fichiers extraits :")
        for f in sorted(file_list)[:10]:
            print(f"   - {f}")
        if len(file_list) > 10:
            print(f"   ... et {len(file_list) - 10} autres fichiers")
            
    except zipfile.BadZipFile:
        print("[ERREUR] Le fichier telecharge n'est pas un ZIP valide")
        return False
    
    # Vérifier les fichiers essentiels
    print("\n[CHECK] Verification des fichiers essentiels...")
    essential_files = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]
    missing = [f for f in essential_files if not (output_dir / f).exists()]
    
    if missing:
        print(f"[WARN] Fichiers manquants : {missing}")
    else:
        print("[OK] Tous les fichiers essentiels sont presents")
    
    print("\n" + "=" * 60)
    print("[OK] GTFS statiques prets a l'emploi !")
    print(f"   Dossier : {output_dir}")
    print("=" * 60)
    
    return True


def check_gtfs_exists(output_dir: Path = None) -> bool:
    """Vérifie si les fichiers GTFS statiques existent déjà."""
    output_dir = output_dir or OUTPUT_DIR
    routes_file = output_dir / "routes.txt"
    return routes_file.exists()


if __name__ == "__main__":
    # Vérifier si déjà téléchargé
    if check_gtfs_exists():
        print("[INFO] Les fichiers GTFS statiques existent deja.")
        response = input("   Voulez-vous les re-telecharger ? (o/N) : ")
        if response.lower() not in ['o', 'oui', 'y', 'yes']:
            print("   Annule.")
            sys.exit(0)
    
    # Télécharger
    success = download_gtfs_static()
    sys.exit(0 if success else 1)
