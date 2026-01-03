from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from .schemas import PredictionInput, PredictionOutput
from .model import model_instance
from .database import SessionLocal, engine, get_db
from . import data_structure
from .weather_utils import get_weather_features, get_calendar_features

app = FastAPI(
    title="API Delay Forecast",
    description="Interface FastAPI pour un modèle de Machine Learning de prévision de retard des transports parisiens en fonction de la météo et des conditions de circulation",
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
    if features.get("est_weekend") is None:
        cal_feats = get_calendar_features(data.month, data.day, data.day_of_week)
        features.update(cal_feats)
        
    # 2. Météo
    # On vérifie si au moins une variable météo est absente
    meteo_needed = [
        "temperature_2m", "precipitation", "rain", "snowfall", "weather_code",
        "cloud_cover", "dew_point_2m", "wind_speed_10m", "wind_gusts_10m",
        "wind_direction_10m", "soleil_leve", "risque_gel_pluie", 
        "risque_gel_neige", "neige_fondue"
    ]
    
    if any(features.get(k) is None for k in meteo_needed):
        print("Récupération automatique des données météo...")
        meteo_feats = get_weather_features(data.month, data.day, data.hour)
        # On ne remplace que les valeurs qui sont None
        for k, v in meteo_feats.items():
            if features.get(k) is None:
                features[k] = v
    
    print(f"Features finales envoyées au modèle: {features}")
    
    # 3. Prédiction
    prediction = model_instance.predict(features)
    print(f"Prédiction calculée: {prediction}")
    
    # 4. Log en DB
    # On crée une copie pour le log en DB car PredictionLog pourrait avoir des colonnes en plus/moins
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