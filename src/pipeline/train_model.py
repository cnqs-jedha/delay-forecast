import os
import json
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, f1_score, precision_score, recall_score
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv("DATABASE_URL")

# --- CONFIGURATION MLFLOW ---
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Retards_transports_Stockholm")

def load_data():
    """Charger les données depuis la DB"""
    engine = create_engine(
    db_url,
    pool_pre_ping=True,
        )

    with engine.connect() as conn:
        result_transport = conn.execute(text("SELECT * FROM stg_transport_archive"))
        transport_data = result_transport.mappings().all()  # liste de dictionnaires

        result_weather = conn.execute(text("SELECT * FROM stg_weather_archive"))
        weather_data = result_weather.mappings().all()  # liste de dictionnaires

    """    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 

    PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    json_path = os.path.join(DATA_DIR, "transport_koda_one_bus_3_days.json")
    #json_path = os.path.join(DATA_DIR, "transport_koda_one_bus_any_days.json")

    if not os.path.exists(json_path):
        # Petit conseil : affiche BASE_DIR pour déboguer le chemin
        print(f"Dossier actuel de recherche : {DATA_DIR}")
        raise FileNotFoundError(f"Le fichier {json_path} est introuvable.")

    with open(json_path, 'r', encoding='utf-8') as f:
        transport_data = json.load(f)"""

    df_transport = pd.DataFrame(transport_data)
    df_weather = pd.DataFrame(weather_data)

    #df_transport = df_transport.rename(columns={'datetime_rounded': 'timestamp_rounded'})

    # Pour vérifier
    print(df_transport.columns)
    df_transport.describe(include='all')

    # 1. Convertir en datetime ET forcer l'UTC pour les deux --> à corriger dans le process etl /!\
    df_transport['timestamp_rounded'] = pd.to_datetime(df_transport['timestamp_rounded'], utc=True)
    df_weather['timestamp_rounded'] = pd.to_datetime(df_weather['timestamp_rounded'], utc=True)

    # 2. Maintenant le merge fonctionnera sans erreur
    df_merged = pd.merge(
        df_transport,
        df_weather,
        on="timestamp_rounded",
        how="left"
    )

    # 3. Suppression des colonnes
    cols_to_drop = ["entity_id", "route_id_static", "timestamp", "timestamp_dt", 
                    "trip_id", "route_id", "year", "uv_index", "shortwave_radiation"]

    df_merged = df_merged.drop(columns=cols_to_drop, errors='ignore')

    df_merged.sample(20)

    return df_merged

def build_preprocessor(numeric_cols, categorical_cols):
    """Crée le pipeline de nettoyage des données"""
    return ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_cols),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
        ]
    )

def train_model():
    df = load_data()
    
    # --- PRÉPARATION DES CIBLES ---
    # Seuil de retard : 120 secondes (2 min)
    df['is_delayed'] = (df['arrival_delay'] > 120).astype(int)
    
    target_class = 'is_delayed'
    target_reg = 'arrival_delay'
    
    # Définition des colonnes
    categorical_cols = ['bus_nbr', 'direction_id', 'day_of_week', 'month', 'hour', 'weather_code']
    # On exclut les targets et les délais de départ (pour la prédiction pure)
    exclude = [target_class, target_reg, 'departure_delay', 'timestamp_rounded']
    numeric_cols = [c for c in df.select_dtypes(include=['number']).columns if c not in categorical_cols + exclude]

    X = df.drop(columns=exclude)
    y_class = df[target_class]
    y_reg = df[target_reg]

    # Split
    X_train, X_test, y_class_train, y_class_test = train_test_split(X, y_class, test_size=0.2, random_state=42)
    _, _, y_reg_train, y_reg_test = train_test_split(X, y_reg, test_size=0.2, random_state=42)

    with mlflow.start_run(run_name="modele_combine_clf_reg_v1"):
        # 1. LOG DES PARAMÈTRES
        mlflow.log_param("n_days", len(df['timestamp_rounded'].dt.date.unique()))
        mlflow.log_param("features", X.columns.tolist())
        
        # 2. CONSTRUCTION DU PRÉPROCESSEUR
        preprocessor = build_preprocessor(numeric_cols, categorical_cols)
        
        # --- ÉTAPE 1 : CLASSIFICATION (Y aura-t-il un retard ?) ---
        clf = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
        ])
        
        clf.fit(X_train, y_class_train)
        y_class_pred = clf.predict(X_test)
        
        # Log métriques Classif
        mlflow.log_metric("clf_precision", precision_score(y_class_test, y_class_pred))
        mlflow.log_metric("clf_recall", recall_score(y_class_test, y_class_pred))
        mlflow.log_metric("clf_f1", f1_score(y_class_test, y_class_pred))
        
        # --- ÉTAPE 2 : RÉGRESSION (Combien ?) ---
        # On n'entraîne que sur les lignes où il y a réellement un retard
        mask_train = y_class_train == 1
        X_reg_train = X_train[mask_train]
        y_reg_train_filtered = y_reg_train[mask_train]
        
        reg = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
        ])
        
        reg.fit(X_reg_train, y_reg_train_filtered)
        
        # Evaluation Régression (sur les vrais positifs du test)
        mask_test = y_class_test == 1
        X_reg_test = X_test[mask_test]
        y_reg_test_filtered = y_reg_test[mask_test]
        
        if len(X_reg_test) > 0:
            y_reg_pred = reg.predict(X_reg_test)
            mlflow.log_metric("reg_rmse", np.sqrt(mean_squared_error(y_reg_test_filtered, y_reg_pred)))
            mlflow.log_metric("reg_r2", r2_score(y_reg_test_filtered, y_reg_pred))

        # 3. SAUVEGARDE DES MODÈLES
        mlflow.sklearn.log_model(clf, "classifier_model")
        mlflow.sklearn.log_model(reg, "regressor_model")
        
        print("Run MLflow complétée avec succès.")

if __name__ == "__main__":
    train_model()