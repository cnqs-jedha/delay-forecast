# :bus: Delay Forecast — POC MLOps

Ce projet est un POC MLOps de prédiction de retards de transports en commun en fonction de contextes externes (météo, jours fériés, temporalité, etc.).

Il combine ingestion de données, transformation, entraînement de modèles, suivi des performances et exposition via une API de prédiction.

📍 Les données utilisées concernent les transports en commun de la ville de Stockholm.

## :rocket: Objectif du projet

Le projet couvre l’ensemble du cycle de vie Machine Learning :

1. Ingestion de données via appels API
2. Stockage des données brutes
3. Transformation et feature engineering
4. Entraînement et suivi des modèles
5. Versioning des modèles
6. Exposition via une API REST
7. Automatisation du pipeline MLOps

## :brain: Flux global du projet

```text
         Appels API
            ↓
   Stockage données brutes
            (S3)
            ↓
        Airflow DAG
     (ingestion / ETL)
            ↓
     PostgreSQL (Neon)
            ↓
     Entraînement ML
      (MLflow tracking)
            ↓
      Modèles + métriques
            ↓
     API FastAPI → Prédictions
            ↓
      Monitoring / logs

```

## :building_construction: Architecture globale

```bash
delay-forecast/
├── docker-compose.yml
├── Makefile
├── .env.example
├── libs/
│   ├── db/              # Connexion base de données (Neon)
│   └── storage/         # Connexion stockage S3
├── services/
│   ├── airflow/
│   │   ├── dags/        # Définition des DAGs
│   │   ├── tasks/       # Tâches ETL et training
│   │   └── scripts/
│   ├── api/             # API FastAPI
│   └── mlflow/          # Service MLflow
├── src/                 # Code partagé (features, utils)
├── tests/               # Tests unitaires et ETL
├── mlruns/              # Expériences MLflow
└── mlruns_artifacts/    # Artefacts modèles

```

### Composants

| Technologie       | Rôle                                              |
| ----------------- | ------------------------------------------------- |
| Airflow           | Orchestration des pipelines ETL et d’entraînement |
| MLflow            | Suivi des expériences, métriques et modèles       |
| FastAPI           | API REST de prédiction                            |
| PostgreSQL (Neon) | Stockage des données transformées                 |
| S3                | Stockage des données brutes et artefacts ML       |
| Docker Compose    | Environnement reproductible                       |
| GitHub Actions    | Intégration continue (tests & qualité)            |

### Interactions entre les modules

| Étape          | Module source | Module cible | Description               | Fichier clé                  |
| -------------- | ------------- | ------------ | ------------------------- | ---------------------------- |
| Configuration  | `.env`        | Services     | Variables d’environnement | `.env`                       |
| Orchestration  | Airflow DAG   | Tasks        | Définition du pipeline    | `services/airflow/dags/*.py` |
| Ingestion      | Task ETL      | S3           | Stockage données brutes   | `tasks/ingest_etl.py`        |
| Transformation | Task ETL      | PostgreSQL   | Nettoyage & features      | `tasks/transform.py`         |
| Entraînement   | Task ML       | MLflow       | Training & métriques      | `tasks/train.py`             |
| Registry       | MLflow        | MLflow       | Versioning modèle         | MLflow UI                    |
| Serving        | FastAPI       | MLflow       | Chargement modèle prod    | `services/api/main.py`       |

### Flux API de prédiction

1. Démarrage de l’API FastAPI
2. Chargement automatique du modèle en Production depuis MLflow
3. Appel client POST /predict
4. Validation des données
5. Prédiction retournée en JSON

Documentation interactive :

```bash
http://localhost:8000/docs
```

### MLFlow

- **Interface Web** : http://localhost:5000
- **Tracking URI (depuis les conteneurs)** : `http://mlflow:5000`
- **Tracking URI (depuis l'hôte)** : `http://localhost:5000`

MLFlow permet de :

- comparer les runs,
- analyser les métriques,
- gérer les versions,
- promouvoir un modèle de production

## :atom_symbol: Installation

Prérequis

- Docker
- Docker Compose
- Make

Etapes

```bash
git clone https://github.com/cnqs-jedha/delay-forecast.git
cd delay-forecast
cp .env.example .env
```

Renseigner les variables nécessaires dans `.env`

### Générer les clés Airflow

```bash
# AIRFLOW_FERNET_KEY (chiffrement des secrets)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# AIRFLOW_WEBSERVER_SECRET_KEY (sessions web)
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### Exécution

```bash
make up       # Démarrer tous les services
make down     # Arrêter et supprimer les conteneurs
make logs     # Voir les logs en temps réel
make ps       # État des conteneurs
make rebuild  # Reconstruire les images sans cache
```

### URLs des services

| Service | URL | Description |
| ------- | --- | ----------- |
| Airflow | http://localhost:8080 | Orchestration des pipelines (admin/admin) |
| MLflow | http://localhost:5000 | Tracking des expériences ML |
| API | http://localhost:8000 | API de prédiction |
| API Docs | http://localhost:8000/docs | Documentation Swagger |

### Utilisation

1. Lancer le DAG dans Airflow (Call API + Stockage + ETL + training)
2. Vérifier les runs et métriques dans MLflow
3. Mettre un modèle en stage Production
4. Appeler l’API `/predict`

### Exemple de prédiction

Requête

```http
POST/predict
```

```json
{
    "direction_id" : 1,
    "month": 1,
    "day": 8,
    "hour": 20,
    "day_of_week": 4,
    "bus_nbr": "541",
    "stop_sequence": 1
}
```

Réponse

```json
{
    "prediction_P50": 37.77369710002,
    "prediction_P80": 78.81960627238,
    "prediction_P90": 123.9659931845
}
```

### Promouvoir un modèle en Production

Via l'interface MLflow (http://localhost:5000) :

1. Aller dans **Models**
2. Sélectionner le modèle `delay-forecast-model`
3. Cliquer sur une version
4. Cliquer sur **Stage** → **Transition to Production**

## :test_tube: Tests & qualité

- Tests unitaires dans `tests/`
- Validation des pipelines ETL
- Intégration continue via GitHub Actions

## :mag: Dépannage

Airflow

- DAGs non visibles → vérifier `AIRFLOW_HOME` et que `.env` est chargé
- Tâches bloquées → logs dans Airflow UI (task instance logs)
- Scheduler pas démarré → vérifier service `airflow-scheduler`

MLflow

- Runs absents → vérifier que MLflow tracking URI est bien défini (`mlflow.set_tracking_uri`)
GitHub
- Modèle non trouvé → vérifier que tu as bien enregistré le modèle dans le registry et qu’une version est en Production.

FastAPI

- Erreur de prédiction → logs API `docker-compose logs api`
- Routes non disponibles → vérifier sur `/docs`

Docker

- Rebuild nécessaire → `make rebuild` pour forcer la reconstruction sans cache
- Conteneurs qui plantent → `make logs` pour surveiller en continu

## :compass: Roadmap

* [ ] Retraining automatique
* [ ] Monitoring du drift
* [ ] Inclusion nouvelles API
* [ ] Authentification API
* [ ] Déploiement cloud

## :busts_in_silhouette: Auteurs

rojet développé par [Stéphane Durig](https://github.com/StephaneDurig), [Quentin Haentjens](https://github.com/Quentin-qha), [Nadège Lefort](https://github.com/nlefort), [Mathis Genton](https://github.com/matt-GTN)

Sous la supervision de [Jedha](https://www.jedha.co/)

*La réalisation de ce projet s'inscrit dans le cadre de la [formation Data Scientist](https://www.jedha.co/formations/formation-data-engineer) développé par [Jedha](https://www.jedha.co/), en vue de l'obtention de la certification professionnelle de niveau 7 (bac+7) enregistrée au RNCP : [Architecte en intelligence artificielle](https://www.francecompetences.fr/recherche/rncp/38777/).*
