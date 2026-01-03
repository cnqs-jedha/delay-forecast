import os
import joblib
import pandas as pd
import numpy as np

class MLModel:
    def __init__(self):
        self.clf = None
        self.reg = None
        
        # Chemins vers les modèles sauvegardés localement via joblib
        # Le dossier models doit être monté dans le conteneur à l'emplacement /app/models
        # ou relatif au dossier d'exécution.
        clf_path = "models/classifier_model.joblib"
        reg_path = "models/regressor_model.joblib"
        
        try:
            print(f"Tentative de chargement des modèles depuis {clf_path} et {reg_path}...")
            
            if os.path.exists(clf_path):
                self.clf = joblib.load(clf_path)
                print(f"Classifieur chargé depuis {clf_path}")
            else:
                print(f"ATTENTION: Fichier {clf_path} introuvable.")

            if os.path.exists(reg_path):
                self.reg = joblib.load(reg_path)
                print(f"Régresseur chargé depuis {reg_path}")
            else:
                print(f"ATTENTION: Fichier {reg_path} introuvable.")

        except Exception as e:
            print(f"Erreur critique lors du chargement des modèles via joblib: {e}")

    def predict(self, features_dict: dict):
        # Conversion du dictionnaire en DataFrame car sklearn/pandas pipeline l'exige
        df_features = pd.DataFrame([features_dict])
        
        # SÉCURITÉ TYPES : 
        # 1. Colonnes OneHot (doivent être string pour être sûrs)
        cols_onehot = ['bus_nbr', 'direction_id', 'day_of_week', 'weather_code']
        for col in cols_onehot:
            if col in df_features.columns:
                df_features[col] = df_features[col].astype(str)
        
        # 2. Colonnes Numériques -> float
        exclude_cols = cols_onehot + ['month', 'hour']
        num_cols = [c for c in df_features.columns if c not in exclude_cols]
        for col in num_cols:
            df_features[col] = pd.to_numeric(df_features[col], errors='coerce').fillna(0)

        # PRÉDICTION DIRECTE (REGRESSION)
        if self.reg is not None:
            try:
                # On utilise directement le régresseur
                delay_seconds = self.reg.predict(df_features)[0]
                return float(delay_seconds)
            except Exception as e:
                print(f"Erreur pendant la prédiction (Reg): {e}")
                raise e
        
        raise ValueError("Erreur: Le modèle de régression n'est pas chargé.")

# On initialise le modèle ici
model_instance = MLModel()
