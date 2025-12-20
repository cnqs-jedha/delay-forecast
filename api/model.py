import numpy as np
import joblib 
import pandas as pd

class MLModel:
    def __init__(self):
        # self.model = joblib.load('../model/model.joblib')
        print("Model chargé")
    
    def pre_process(self, features_dict: dict):
        # On prépare ici les données avant de les envoyer au modèle
        features = pd.DataFrame([features_dict])
        return features

    def predict(self, features_dict: dict):
        # On extrait les valeurs dans l'ordre attendu par le modèle ML
        features = self.pre_process(features_dict)
        
        # On effectue la prédiction
        prediction = np.random.random()
        
        # En production: prediction = self.model.predict(features)[0]
        return prediction

# On initialise le modèle ici, pas à chaque requête API
model_instance = MLModel()
