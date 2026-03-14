"""
Peuplement de prediction_logs via l'API, puis synchronisation de ground_truth.

Usage (depuis la machine hote) :
    python src/pipeline/seed_predictions.py
    python src/pipeline/seed_predictions.py --count 200
    python src/pipeline/seed_predictions.py --sync-only
"""

import argparse
import itertools
import random
import subprocess
import sys
import time

import requests

API_URL = "http://localhost:8000"
PREDICT_ENDPOINT = f"{API_URL}/predict"

BUS_NBR = "541"
DIRECTIONS = [0, 1]
HOURS = list(range(6, 23))
STOP_SEQUENCES = [1, 5, 10, 15]
MONTHS = [2, 3]
DAYS_OF_WEEK = list(range(7))
DAYS = list(range(1, 29))


def build_payloads(count: int) -> list[dict]:
    """Genere un ensemble de payloads varies pour l'API /predict."""
    combos = list(itertools.product(DIRECTIONS, MONTHS, DAYS_OF_WEEK, HOURS, STOP_SEQUENCES))
    random.shuffle(combos)
    combos = combos[:count]

    payloads = []
    for direction_id, month, dow, hour, stop_seq in combos:
        day = random.choice(DAYS)
        payloads.append({
            "direction_id": direction_id,
            "month": month,
            "day": day,
            "hour": hour,
            "day_of_week": dow,
            "bus_nbr": BUS_NBR,
            "stop_sequence": stop_seq,
        })
    return payloads


def send_predictions(payloads: list[dict]) -> tuple[int, int]:
    ok, fail = 0, 0
    for i, payload in enumerate(payloads, 1):
        try:
            r = requests.post(PREDICT_ENDPOINT, json=payload, timeout=30)
            if r.status_code == 200:
                ok += 1
            else:
                fail += 1
                print(f"  [{i}] HTTP {r.status_code} : {r.text[:120]}")
        except requests.RequestException as e:
            fail += 1
            print(f"  [{i}] Erreur connexion : {e}")

        if i % 20 == 0:
            print(f"  ... {i}/{len(payloads)} ({ok} OK, {fail} echecs)")

    return ok, fail


def sync_ground_truth():
    """Synchronise ground_truth via docker compose exec."""
    print("\n--- Synchronisation ground_truth ---")
    cmd = [
        "docker", "compose", "exec", "-T",
        "-w", "/opt/project/pipeline",
        "airflow-webserver",
        "python", "-c",
        "from prediction_vs_ground_truth import sync_ground_truth_optimized; sync_ground_truth_optimized()",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Seed prediction_logs & ground_truth")
    parser.add_argument("--count", type=int, default=100, help="Nombre de predictions (defaut: 100)")
    parser.add_argument("--sync-only", action="store_true", help="Synchroniser ground_truth uniquement")
    parser.add_argument("--no-sync", action="store_true", help="Ne pas synchroniser ground_truth")
    args = parser.parse_args()

    if not args.sync_only:
        # Verifier que l'API est up
        try:
            r = requests.get(API_URL, timeout=5)
            print(f"API accessible ({API_URL})")
        except requests.RequestException:
            print(f"ERREUR : API inaccessible sur {API_URL}")
            print("Verifiez que les conteneurs tournent (make up)")
            sys.exit(1)

        payloads = build_payloads(args.count)
        print(f"\nEnvoi de {len(payloads)} predictions a l'API...")
        t0 = time.time()
        ok, fail = send_predictions(payloads)
        elapsed = time.time() - t0
        print(f"\n=== Bilan predictions : {ok} OK / {fail} echecs en {elapsed:.1f}s ===")

    if not args.no_sync:
        sync_ground_truth()

    print("\nTermine. Prochaines etapes :")
    print("  1. Verifier prediction_logs dans Neon")
    print("  2. Verifier ground_truth dans Neon")
    print("  3. Lancer le DAG monitoring_weekly depuis Airflow UI")


if __name__ == "__main__":
    main()
