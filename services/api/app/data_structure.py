from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from .database import Base

class PredictionLog(Base):
    __tablename__ = "prediction_logs"

    id = Column(Integer, primary_key=True, index=True)

    # Transport et Calendaire
    bus_nbr = Column(String)
    direction_id = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    hour = Column(Integer)
    day_of_week = Column(Integer)
    
    # Météo
    weather_code = Column(Integer)
    temperature_2m = Column(Float)
    precipitation = Column(Float)
    rain = Column(Float)
    snowfall = Column(Float)
    wind_speed_10m = Column(Float)
    wind_gusts_10m = Column(Float)
    cloud_cover = Column(Integer)
    dew_point_2m = Column(Float)
    wind_direction_10m = Column(Integer)
    
    # Contexte
    soleil_leve = Column(Integer)
    risque_gel_pluie = Column(Integer)
    risque_gel_neige = Column(Integer)
    neige_fondue = Column(Integer)
    est_weekend = Column(Integer)
    est_jour_ferie = Column(Integer)
    vacances_scolaires = Column(Integer)
    
    prediction = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
