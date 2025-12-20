from pydantic import BaseModel
from typing import List

# Structure pour les données d'entrée
class PredictionInput(BaseModel):
    line: int
    weather: str

# Structure pour les données de sortie
class PredictionOutput(BaseModel):
    prediction: float
    probability: float = None
