import requests
import random
from datetime import datetime

# Configuration
API_URL = "http://localhost:8000/predict" # Vérifie bien ton port API
NB_PREDICTIONS = 50

def simulate():
    print(f"Lancement de {NB_PREDICTIONS} prédictions pour le bus 541...")
    
    success_count = 0
    for i in range(NB_PREDICTIONS):
        # On reste sur le 7 Mars pour que ton archive puisse "matcher"
        payload = {
            "direction_id": random.choice([0, 1]),
            "month": 3,
            "day": 7,
            "hour": random.randint(8, 20), # On varie les heures de la journée
            "day_of_week": 7,
            "bus_nbr": "541",
            "stop_sequence": random.randint(1, 10)
        }
        
        try:
            response = requests.post(API_URL, json=payload)
            if response.status_code == 200:
                success_count += 1
            if i % 10 == 0:
                print(f"Progression : {i}/{NB_PREDICTIONS}...")
        except Exception as e:
            print(f"Erreur à l'appel {i}: {e}")

    print(f"{success_count} prédictions ajoutées dans prediction_logs.")

if __name__ == "__main__":
    simulate()