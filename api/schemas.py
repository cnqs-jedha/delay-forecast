from pydantic import BaseModel
from typing import List

# Structure pour les données d'entrée
class PredictionInput(BaseModel):
    # Transport & Calendaire
    direction_id: int
    month: int
    day: int
    hour: int
    day_of_week: int
    
    # Météo
    weather_code: int
    temperature_2m: float
    precipitation: float
    rain: float
    snowfall: float
    wind_speed_10m: float
    wind_gusts_10m: float
    cloud_cover: int
    dew_point_2m: float
    wind_direction_10m: int
    
    # Contexte
    soleil_leve: int
    risque_gel_pluie: int
    risque_gel_neige: int
    neige_fondue: int
    est_weekend: int
    est_jour_ferie: int
    vacances_scolaires: int

# Structure pour les données de sortie
class PredictionOutput(BaseModel):
    prediction: float
    probability: float = None
