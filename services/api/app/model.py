import os
import joblib
import pandas as pd
import numpy as np

class MLModel:
    def __init__(self):
        self.models = None
        
        # Chemin vers le pack de modèles quantiles dans le dossier models à la racine du projet
        from pathlib import Path
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        model_path = str(project_root / "models" / "50_80_90_models_quantiles.pkl")
        
        try:
            print(f"Tentative de chargement des modèles depuis {model_path}...")
            
            if os.path.exists(model_path):
                self.models = joblib.load(model_path)
                print(f"Modèles quantiles chargés depuis {model_path} ({list(self.models.keys())})")
            else:
                print(f"ATTENTION: Fichier {model_path} introuvable.")

        except Exception as e:
            print(f"Erreur critique lors du chargement des modèles : {e}")

    def predict(self, features_dict: dict):
        if self.models is None:
            raise ValueError("Erreur: Les modèles ne sont pas chargés.")

        # Préparation des données pour correspondre exactement au pipeline d'entraînement
        df = pd.DataFrame([features_dict])
        
        # 1. Feature Engineering Cyclique (Heure, Jour, Mois)
        if 'hour' in df.columns:
            df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
            df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        
        if 'day_of_week' in df.columns:
            df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
            df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
            
        if 'month' in df.columns:
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

        # 2. One-Hot Encoding manuel (pour correspondre aux colonnes du modèle)
        # On récupère les colonnes utilisées pendant l'entraînement depuis le premier modèle
        first_model = list(self.models.values())[0]
        model_features = first_model.feature_names_in_

        # Colonnes catégorielles à dummifier (comme dans train_model.py)
        # On pré-remplit avec 0 les colonnes One-Hot
        for col in model_features:
            if col not in df.columns:
                df[col] = 0

        # Mapping direction_id -> direction_id_1 (dans l'entraînement, direction_id était str, dummified)
        if 'direction_id' in df.columns and 'direction_id_1' in model_features:
             if str(df['direction_id'].iloc[0]) == '1':
                 df['direction_id_1'] = 1

        # Mapping weather_code -> weather_code_X
        if 'weather_code' in df.columns:
            wc = str(int(df['weather_code'].iloc[0]))
            wc_col = f"weather_code_{wc}"
            if wc_col in model_features:
                df[wc_col] = 1

        # 3. Sélection et réordonnancement des colonnes
        df_final = df[model_features]

        # 4. Prédictions
        results = {}
        try:
            results['prediction_P50'] = float(self.models['P50_Median'].predict(df_final)[0])
            results['prediction_P80'] = float(self.models['P80_Pessimist'].predict(df_final)[0])
            results['prediction_P90'] = float(self.models['P90_Extreme'].predict(df_final)[0])
            return results
        except Exception as e:
            print(f"Erreur pendant la prédiction : {e}")
            raise e

# On initialise le modèle ici
model_instance = MLModel()
