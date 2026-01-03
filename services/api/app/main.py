from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from .schemas import PredictionInput, PredictionOutput
from .model import model_instance
from .database import SessionLocal, engine, get_db
from . import data_structure
from .weather_utils import get_weather_features, get_calendar_features

app = FastAPI(
    title="API Delay Forecast",
    description="Interface FastAPI pour un modèle de Machine Learning de prévision de retard des transports de Stockholm en fonction de la météo et des conditions de circulation",
    version="0.1.0"
)

# On crée les tables au démarrage
data_structure.Base.metadata.create_all(bind=engine)

@app.get("/")
async def root():
    return {"message": "Bienvenue sur l'API de prédiction de retard des transports Stockholm Delay Forecast"}

@app.post("/predict", response_model=PredictionOutput)
async def predict(data: PredictionInput, db: Session = Depends(get_db)):
    
    # On transforme l'objet Pydantic en dictionnaire
    features = data.model_dump()
    
    print(f"--- Nouvelle requête reçue ---")
    print(f"Données utilisateur: month={data.month}, day={data.day}, hour={data.hour}, direction={data.direction_id}")
    
    # Complétion automatique des features manquantes
    # 1. Calendrier
    cal_feats = get_calendar_features(data.month, data.day, data.day_of_week)
    features.update(cal_feats)
        
    # 2. Météo - Récupération systématique
    print("Récupération des données météo...")
    meteo_feats = get_weather_features(data.month, data.day, data.hour)
    features.update(meteo_feats)
    
    # 3. Prédiction
    try:
        prediction = model_instance.predict(features)
        print(f"Prédiction calculée: {prediction}")
    except Exception as e:
        print(f"Erreur lors de la prédiction : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    # 4. Log en DB
    db_log = data_structure.PredictionLog(**features, prediction=prediction)
    db.add(db_log)
    db.commit()
    db.refresh(db_log)
    print(f"Log enregistré en base de données (ID: {db_log.id})")
    print(f"-------------------------------")
    
    return PredictionOutput(prediction=prediction)

if __name__ == "__main__":
    import uvicorn
    # On crée les tables uniquement quand on lance l'app directement
    data_structure.Base.metadata.create_all(bind=engine)
    uvicorn.run(app, host="0.0.0.0", port=8000)