import os
import joblib
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
db_url = os.getenv("DATABASE_URL")

# --- CONFIGURATION MLFLOW ---
import tempfile
mlflow.set_tracking_uri("http://mlflow:5000")
experiment_name = "Retards_transports_Stockholm_v8"

# Assurer un dossier local pour les artifacts si le serveur est mal configuré
# ou si l'on veut éviter d'écrire à la racine /mlflow
artifact_location = str(Path.cwd() / "mlruns_artifacts")
Path(artifact_location).mkdir(exist_ok=True)

try:
    mlflow.create_experiment(experiment_name, artifact_location=f"file://{artifact_location}")
except Exception:
    pass
mlflow.set_experiment(experiment_name)

def load_data():
    """Charger les données depuis la DB et effectuer le merge initial"""
    engine = create_engine(db_url, pool_pre_ping=True)

    with engine.connect() as conn:
        result_transport = conn.execute(text("SELECT * FROM stg_transport_archive"))
        df_transport = pd.DataFrame(result_transport.mappings().all())

        result_weather = conn.execute(text("SELECT * FROM stg_weather_archive"))
        df_weather = pd.DataFrame(result_weather.mappings().all())

    # Homogénéisation du nom de la colonne de jointure
    if 'datetime_rounded' in df_transport.columns:
        df_transport = df_transport.rename(columns={'datetime_rounded': 'timestamp_rounded'})

    # Conversion en datetime pour assurer la jointure
    df_transport['timestamp_rounded'] = pd.to_datetime(df_transport['timestamp_rounded'], utc=True)
    df_weather['timestamp_rounded'] = pd.to_datetime(df_weather['timestamp_rounded'], utc=True)

    df = pd.merge(df_transport, df_weather, on="timestamp_rounded", how="left")

    # Suppression immédiate des IDs et colonnes inutiles
    cols_to_drop = ["id", "observation_uuid", "entity_id", "route_id_static", "timestamp", 
                    "timestamp_dt", "trip_id", "route_id", "year", "uv_index", 
                    "shortwave_radiation", "timestamp_rounded", 'arrival_delay']
    
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
    
    print(f"\nDonnées chargées : {len(df)} lignes")
    print(f"Colonnes disponibles : {df.columns.tolist()}")
    
    return df

def preprocess(df):
    """Prépare les features numériques et catégorielles"""
    
    # 1. Vérifier les colonnes disponibles
    print("\n ANALYSE DES COLONNES")
    print("=" * 60)
    
    # 2. Définition de la cible (Y)
    if 'departure_delay' not in df.columns:
        raise ValueError(" Colonne 'departure_delay' introuvable !")
    
    # NETTOYER LES NaN DANS Y AVANT clip()
    print(f"NaN dans departure_delay AVANT nettoyage : {df['departure_delay'].isna().sum()}")
    
    # Supprimer les lignes avec NaN dans la cible
    df = df[df['departure_delay'].notna()].copy()
    
    y = df['departure_delay'].clip(lower=0)
    print(f"Cible (y) : {len(y)} observations, moyenne = {y.mean():.1f}s")
    
    # 3. Préparation de X
    X = df.drop(columns=['departure_delay', 'arrival_delay'], errors='ignore')
    
    # 4. IDENTIFICATION CORRECTE DES CATÉGORIELLES
    # Liste des colonnes qui DOIVENT être catégorielles (même si int)
    categorical_cols = []
    
    # Vérifier bus_nbr
    if 'bus_nbr' in X.columns:
        categorical_cols.append('bus_nbr')
        X['bus_nbr'] = X['bus_nbr'].astype(str)
        print(f"bus_nbr trouvé : {X['bus_nbr'].nunique()} valeurs uniques")
    
    #  Vérifier direction_id  
    if 'direction_id' in X.columns:
        categorical_cols.append('direction_id')
        X['direction_id'] = X['direction_id'].astype(str)    
        print(f"direction_id trouvé : {X['direction_id'].nunique()} valeurs uniques")
    
    # Weather code : le forcer en catégoriel
    if 'weather_code' in X.columns:
        categorical_cols.append('weather_code')
        # Convertir explicitement en string pour forcer le traitement catégoriel
        X['weather_code'] = X['weather_code'].astype(str)
        print(f"weather_code trouvé : {X['weather_code'].nunique()} codes uniques")
        print(f"Codes présents : {sorted(X['weather_code'].unique())}")
    
    print(f"\nColonnes catégorielles : {categorical_cols}")   
    print(f"  Type : {X['weather_code'].dtype}")
    
    # Feature Engineering Cyclique pour l'heure
    if 'hour' in X.columns:
        X['hour_sin'] = np.sin(2 * np.pi * X['hour'] / 24)
        X['hour_cos'] = np.cos(2 * np.pi * X['hour'] / 24)
        print(f"Features cycliques créées pour 'hour'")
    
    # Jour de la semaine cyclique
    if 'day_of_week' in X.columns:
        X['day_sin'] = np.sin(2 * np.pi * X['day_of_week'] / 7)
        X['day_cos'] = np.cos(2 * np.pi * X['day_of_week'] / 7)
        print(f"Features cycliques créées pour 'day_of_week'")
    
    # Mois cyclique
    if 'month' in X.columns:
        X['month_sin'] = np.sin(2 * np.pi * X['month'] / 12)
        X['month_cos'] = np.cos(2 * np.pi * X['month'] / 12)
        print(f"Features cycliques créées pour 'month'")
    
    #  One-Hot Encoding des catégorielles
    if categorical_cols:
        print(f"\n One-Hot Encoding de : {categorical_cols}")
        print(f" Colonnes avant : {X.shape[1]}")
        X = pd.get_dummies(X, columns=categorical_cols, drop_first=True, dtype=int)
        print(f"Encoding terminé. Colonnes après : {X.shape[1]}")
    else:
        print("\n Aucune colonne catégorielle détectée !")
    
    # 9. Nettoyage final : ne garder QUE les numériques
    before_cleanup = X.shape[1]
    X = X.select_dtypes(include=[np.number])
    after_cleanup = X.shape[1]
    


    if before_cleanup > after_cleanup:
        print(f"\n{before_cleanup - after_cleanup} colonnes non-numériques supprimées")
    
    # 10. Vérification de NaN
    nan_cols = X.columns[X.isna().any()].tolist()
    if nan_cols:
        print(f"\nColonnes avec NaN : {nan_cols}")
        print(f"   → Remplissage avec 0")
        X = X.fillna(0)
    
    print(f"\nFeatures finales : {X.shape[1]} colonnes")
    print(f"   {X.columns.tolist()}")
    
    return X, y

def train_quantile_models():

    # --- ETAPE 1 : CHARGEMENT ET PREPROCESS ---
    print("\n" + "="*80)
    print("ENTRAÎNEMENT DES MODÈLES DE PRÉDICTION DE RETARDS")
    print("="*80)
    
    df_raw = load_data()
    X, y = preprocess(df_raw)

    # Split temporel (shuffle=False pour respecter la chronologie)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False, random_state=42
    )
    
    # --- DÉMONSTRATION DU RESPECT DE LA CHRONOLOGIE ---
    # On récupère les index (qui correspondent à l'ordre d'origine dans la DB)
    train_indices = X_train.index
    test_indices = X_test.index

    print("\n--- PREUVE DU SPLIT TEMPOREL ---")
    print(f"Dernier index du bloc TRAIN : {train_indices.max()}")
    print(f"Premier index du bloc TEST  : {test_indices.min()}")

    if test_indices.min() > train_indices.max():
        print(" DÉMONSTRATION RÉUSSIE : Le bloc Test commence strictement après la fin du bloc Train.")
        print(f"Aucune donnée du futur (index > {train_indices.max()}) n'est présente dans le Train.")
    else:
        print("ALERTE : Il y a un chevauchement ou les données n'étaient pas triées !")
    print("-" * 30) 

    print(f"\n Split des données :")
    print(f"   Train : {len(X_train)} observations")
    print(f"   Test  : {len(X_test)} observations")
    print(f"   Ratio : {len(X_test)/len(X)*100:.1f}%")

    quantiles = [0.5, 0.8, 0.9]
    names = ["P50_Median", "P80_Pessimist", "P90_Extreme"]
    
    print(f"\nEntraînement de {len(quantiles)} modèles quantiles")
    print("-" * 80)
    
    # Dictionnaire pour stocker les modèles
    all_trained_models = {}
    input_example = X_train.head(1)
    
    # === ENTRAINEMENT DES MODELES (sans dépendance MLflow) ===
    for alpha, name in zip(quantiles, names):
        print(f"\n Entrainement du modele {name} (alpha={alpha})...")
        
        model = GradientBoostingRegressor(
            loss="quantile",
            alpha=alpha,
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        all_trained_models[name] = model
        preds = model.predict(X_test)

        # Calcul des métriques
        mae = mean_absolute_error(y_test, preds)
        rmse = root_mean_squared_error(y_test, preds)
        r2 = r2_score(y_test, preds)
        reliability = (y_test <= preds).mean()
        
        mean_pred = preds.mean()
        mean_actual = y_test.mean()

        print(f" MAE       : {mae:.2f}s ({mae/60:.2f} min)")
        print(f" RMSE      : {rmse:.2f}s ({rmse/60:.2f} min)")
        print(f" R2        : {r2:.3f}")
        print(f" Fiabilite : {reliability:.1%}")
        print(f" Prediction moyenne : {mean_pred:.1f}s")
        print(f" Retard moyen reel  : {mean_actual:.1f}s")
    
    # Feature importance du dernier modèle (P90)
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\nTop 10 features les plus importantes (P90) :")
    print(feature_importance.head(10).to_string(index=False))
    
    # === SAUVEGARDE LOCALE DU MODELE (AVANT MLflow) ===
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent 
    model_dir = project_root / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = model_dir / "50_80_90_models_quantiles.pkl"
    joblib.dump(all_trained_models, model_path)
    
    print(f"\n[OK] Pack de {len(all_trained_models)} modeles sauvegarde.")
    print(f"Chemin : {model_path}")
    
    # === LOG MLFLOW (optionnel - si serveur disponible) ===
    try:
        with mlflow.start_run(run_name="quantile_bundle_v2_fixed"):
            mlflow.log_param("n_estimators", 300)
            mlflow.log_param("max_depth", 5)
            mlflow.log_param("learning_rate", 0.05)
            mlflow.log_param("n_features", X.shape[1])
            mlflow.log_param("n_train", len(X_train))
            mlflow.log_param("n_test", len(X_test))
            
            for name, m in all_trained_models.items():
                preds = m.predict(X_test)
                mlflow.log_metric(f"{name}_mae", mean_absolute_error(y_test, preds))
                mlflow.log_metric(f"{name}_r2", r2_score(y_test, preds))
            
            feature_importance.to_csv("feature_importance.csv", index=False)
            mlflow.log_artifact("feature_importance.csv")
            
        print("[OK] Metriques enregistrees dans MLflow")
    except Exception as e:
        print(f"[INFO] MLflow non disponible (normal en local) : {type(e).__name__}")

    print("\n" + "="*80)
    print("ENTRAINEMENT TERMINE")
    print("="*80)

if __name__ == "__main__":
    train_quantile_models()