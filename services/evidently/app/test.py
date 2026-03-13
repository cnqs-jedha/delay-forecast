import requests
import pandas as pd

# 1. On définit la référence (ex: données du 5 mars)
ref_data = {"data": [{"bus_nbr": "541", "stop_sequence": 1, "prediction_P50": 10, "actual_delay": 12}]}
requests.post("http://localhost:8001/reference", json=ref_data)

# 2. On teste le drift avec une donnée aberrante
current_data = {"data": [{"bus_nbr": "541", "stop_sequence": 1, "prediction_P50": 10, "actual_delay": 500}]}
response = requests.post("http://localhost:8001/drift/report", json=current_data)

print(response.json())