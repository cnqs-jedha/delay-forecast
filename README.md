# Delay Forecast — POC MLOps

POC de prédiction des retards de train en fonction de la météo (et contexte : trafic, jours fériés).

## Stack
Airflow • MLflow • FastAPI • Docker Compose • GitHub Actions • Neon Postgres • S3

---

## 🚀 Run local

```bash
# Première installation uniquement (si .env n'existe pas)
cp .env.example .env
# Puis renseigner les variables (voir section Configuration)
make up
```

### URLs des services

| Service | URL | Description |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | Orchestration des pipelines (admin/admin) |
| MLflow | http://localhost:5000 | Tracking des expériences ML |
| API | http://localhost:8000 | API de prédiction |
| API Docs | http://localhost:8000/docs | Documentation Swagger |

### Commandes utiles

```bash
make up       # Démarrer tous les services
make down     # Arrêter et supprimer les conteneurs
make logs     # Voir les logs en temps réel
make ps       # État des conteneurs
make rebuild  # Reconstruire les images sans cache
```

---

## 🔧 Configuration

### Générer les clés Airflow

```bash
# AIRFLOW_FERNET_KEY (chiffrement des secrets)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# AIRFLOW_WEBSERVER_SECRET_KEY (sessions web)
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

---

## 📊 MLflow — Guide d'utilisation

### Accès

- **Interface Web** : http://localhost:5000
- **Tracking URI (depuis les conteneurs)** : `http://mlflow:5000`
- **Tracking URI (depuis l'hôte)** : `http://localhost:5000`

### Connexion depuis Python

```python
import mlflow

# Configurer la connexion
mlflow.set_tracking_uri("http://mlflow:5000")  # Depuis un conteneur Docker
# mlflow.set_tracking_uri("http://localhost:5000")  # Depuis l'hôte

# Créer ou sélectionner une expérience
mlflow.set_experiment("delay-forecast")
```

### Logger un entraînement

```python
import mlflow
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import numpy as np

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("delay-forecast")

with mlflow.start_run(run_name="linear-regression-v1"):
    # Paramètres
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("features", ["temperature", "rain_mm", "wind_kmh"])
    
    # Entraînement
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Métriques
    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", model.score(X_test, y_test))
    
    # Sauvegarder le modèle
    mlflow.sklearn.log_model(
        model, 
        "model",
        registered_model_name="delay-forecast-model"
    )
```

### Charger un modèle (dans l'API)

```python
import mlflow

# Charger la dernière version en Production
model = mlflow.sklearn.load_model("models:/delay-forecast-model/Production")

# Ou une version spécifique
model = mlflow.sklearn.load_model("models:/delay-forecast-model/1")

# Faire une prédiction
prediction = model.predict([[15.0, 2.5, 20.0, 0, 0.7]])
```

### Promouvoir un modèle en Production

Via l'interface MLflow (http://localhost:5000) :
1. Aller dans **Models**
2. Sélectionner le modèle `delay-forecast-model`
3. Cliquer sur une version
4. Cliquer sur **Stage** → **Transition to Production**

Ou via Python :
```python
from mlflow import MlflowClient

client = MlflowClient(tracking_uri="http://mlflow:5000")
client.transition_model_version_stage(
    name="delay-forecast-model",
    version=1,
    stage="Production"
)
```

---

## 📁 Structure du projet

```
delay-forecast/
├── docker-compose.yml      # Orchestration des services
├── Makefile                # Commandes raccourcies
├── .env.example            # Template des variables d'environnement
├── libs/                   # Bibliothèques partagées
│   ├── db/neon.py         # Connexion Neon DB
│   └── storage/s3.py      # Client S3
├── services/
│   ├── airflow/
│   │   ├── dags/          # DAGs Airflow
│   │   ├── tasks/         # Scripts Python (ingestion, ETL, training)
│   │   └── scripts/       # Scripts d'initialisation
│   ├── api/               # API FastAPI
│   └── mlflow/            # (Configuration MLflow)
└── .github/workflows/     # CI/CD
```

---

## 👥 Équipe

| Responsabilité | Composants |
|----------------|------------|
| Infrastructure | Docker, Airflow, MLflow |
| Data Pipeline | Ingestion, ETL, Neon DB |
| ML | Training, Evidently |
| API | FastAPI, Prédictions |
