from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from .database import Base

class PredictionLog(Base):
    __tablename__ = "prediction_logs"

    id = Column(Integer, primary_key=True, index=True)
    line = Column(Integer)
    weather = Column(String)
    prediction = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
