from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Delay Forecast API", version="0.1.0")


class PredictIn(BaseModel):
    station_id: str
    ts: str  # ISO datetime string (ex: 2025-12-20T12:00:00Z)

    # POC: features optionnelles passées directement (simple pour démo)
    temperature: float | None = None
    rain_mm: float | None = None
    wind_kmh: float | None = None
    is_holiday: int | None = None
    traffic_level: float | None = None


class PredictOut(BaseModel):
    predicted_delay_minutes: float
    model_version: str | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict", response_model=PredictOut)
def predict(payload: PredictIn):
    # Placeholder POC (à brancher ensuite sur modèle MLflow/S3)
    return PredictOut(predicted_delay_minutes=3.5, model_version="poc")
