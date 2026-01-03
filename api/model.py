import os
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn

class MLModel:
    def __init__(self):
        # Configuration de l'URI MLflow via variable d'environnement ou défaut
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        mlflow.set_tracking_uri(tracking_uri)
        
        self.clf = None
        self.reg = None
        
        # On essaie de récupérer le dernier run de l'expérience pour charger les modèles
        try:
            experiment_name = "Retards_transports_Stockholm"
            experiment = mlflow.get_experiment_by_name(experiment_name)
            
            if experiment:
                runs = mlflow.search_runs(
                    experiment_ids=[experiment.experiment_id],
                    order_by=["start_time DESC"],
                    max_results=1
                )
                
                if not runs.empty:
                    run_id = runs.iloc[0].run_id
                    # Chargement des artefacts par défaut du script train_model.py
                    self.clf = mlflow.sklearn.load_model(f"runs:/{run_id}/classifier_model")
                    self.reg = mlflow.sklearn.load_model(f"runs:/{run_id}/regressor_model")
                    print(f"Modèles chargés avec succès depuis le run {run_id}")
                else:
                    print(f"Aucun run trouvé pour l'expérience {experiment_name}")
            else:
                print(f"Expérience {experiment_name} introuvable")
                
        except Exception as e:
            print(f"Erreur lors du chargement des modèles MLflow: {e}")
            print("L'API tournera en mode dégradé (prédictions aléatoires)")

    def predict(self, features_dict: dict):
        # Conversion du dictionnaire en DataFrame car sklearn/pandas pipeline l'exige
        df_features = pd.DataFrame([features_dict])
        
        if self.clf is not None and self.reg is not None:
            try:
                # 1. Classification : Y a-t-il un retard (> 2min)?
                is_delayed = self.clf.predict(df_features)[0]
                
                if is_delayed == 0:
                    return 0.0
                
                # 2. Régression : Durée du retard
                delay_seconds = self.reg.predict(df_features)[0]
                return float(delay_seconds)
            except Exception as e:
                print(f"Erreur pendant la prédiction: {e}")
                return np.random.random() * 10 
        
        # Fallback si les modèles ne sont pas chargés
        return np.random.random() * 10

# On initialise le modèle ici, pas à chaque requête API
model_instance = MLModel()
