from pydantic import BaseModel
from typing import Optional

# Structure pour les données d'entrée - Simplifiée pour l'utilisateur
class PredictionInput(BaseModel):
    # Ce que l'utilisateur doit fournir
    direction_id: int
    month: int
    day: int
    hour: int
    day_of_week: int
    
    # Valeurs par défaut
    bus_nbr: str = "541"
    stop_sequence: int = 1
    
# Structure pour les données de sortie
class PredictionOutput(BaseModel):
    prediction_P50: float
    prediction_P80: float
    prediction_P90: float
