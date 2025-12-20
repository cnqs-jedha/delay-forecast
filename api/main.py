from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from api.schemas import PredictionInput, PredictionOutput
from api.model import model_instance
from api.database import SessionLocal, engine, get_db
from api import data_structure

# Création des tables dans la base de données
data_structure.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="API Delay Forecast",
    description="Interface FastAPI pour un modèle de Machine Learning de prévision de retard des transports parisiens en fonction de la météo et des conditions de circulation",
    version="0.1.0"
)

@app.get("/")
async def root():
    return {"message": "Bienvenue sur l'API de prédiction de retard des transports parisiens Delay Forecast"}

@app.post("/predict", response_model=PredictionOutput)
async def predict(data: PredictionInput, db: Session = Depends(get_db)):
    
    # On transforme l'objet Pydantic en dictionnaire d'un coup
    features = data.dict()
    print(f"--- Nouvelle requête reçue ---")
    print(f"Données d'entrée: {features}")
    
    # 1. Prédiction
    prediction = model_instance.predict(features)
    print(f"Prédiction calculée: {prediction}")
    
    # 2. Log en DB (plus propre avec les kwargs)
    db_log = data_structure.PredictionLog(**features, prediction=prediction)
    db.add(db_log)
    db.commit()
    db.refresh(db_log)
    print(f"Log enregistré en base de données (ID: {db_log.id})")
    print(f"-------------------------------")
    
    return PredictionOutput(prediction=prediction)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
