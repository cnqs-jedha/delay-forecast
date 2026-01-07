import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

def test_root(client):
    """Teste la route racine."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Bienvenue sur l'API de prédiction de retard des transports Stockholm Delay Forecast"}

@patch("app.main.model_instance.predict")
@patch("app.main.get_weather_features")
@patch("app.main.get_calendar_features")
def test_predict_success(mock_calendar, mock_weather, mock_predict, client):
    """
    Teste une prédiction complète réussie.
    On mocke:
    1. get_calendar_features -> renvoie des features fictives
    2. get_weather_features -> renvoie des features météo fictives
    3. model_instance.predict -> renvoie une valeur de retard
    """
    
    # Mock des retours
    mock_calendar.return_value = {
        "est_weekend": 0,
        "est_jour_ferie": 0,
        "vacances_scolaires": 0
    }
    
    mock_weather.return_value = {
        "temperature_2m": 10.5,
        "precipitation": 0.0,
        "rain": 0.0,
        "snowfall": 0.0,
        "weather_code": 1,
        "cloud_cover": 20,
        "dew_point_2m": 5.0,
        "wind_speed_10m": 12.0,
        "wind_gusts_10m": 25.0,
        "wind_direction_10m": 180,
        "soleil_leve": 1,
        "risque_gel_pluie": 0,
        "risque_gel_neige": 0,
        "neige_fondue": 0
    }
    
    mock_predict.return_value = 180.5  # Retard prédit en secondes
    
    payload = {
        "direction_id": 0,
        "month": 5,
        "day": 15,
        "hour": 8,
        "day_of_week": 3
    }
    
    response = client.post("/predict", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["prediction"] == 180.5
    
    # Vérifications des appels
    mock_calendar.assert_called_once_with(5, 15, 3)
    mock_weather.assert_called_once_with(5, 15, 8)
    mock_predict.assert_called_once()
    
    # Vérifier que le modèle reçoit bien les features fusionnées
    args, _ = mock_predict.call_args
    # args[0] est le dictionnaire de features passé à predict
    features_passed = args[0]
    assert features_passed["bus_nbr"] == "541"  # Valeur par défaut
    assert features_passed["stop_sequence"] == 1  # Valeur par défaut
    assert features_passed["temperature_2m"] == 10.5  # Venant du mock météo
    assert features_passed["est_weekend"] == 0  # Venant du mock calendrier

@patch("app.main.model_instance.predict")
@patch("app.main.get_weather_features")
@patch("app.main.get_calendar_features")
def test_predict_model_failure(mock_calendar, mock_weather, mock_predict, client):
    """
    Teste le cas où le modèle lève une exception (ex: modèle non chargé).
    L'API doit renvoyer une erreur 500.
    """
    mock_calendar.return_value = {}
    mock_weather.return_value = {}
    
    # Simulation d'une erreur du modèle
    mock_predict.side_effect = Exception("Modèle non chargé")
    
    payload = {
        "direction_id": 0,
        "month": 5,
        "day": 15,
        "hour": 8,
        "day_of_week": 3
    }
    
    response = client.post("/predict", json=payload)
    
    assert response.status_code == 500
    assert response.json()["detail"] == "Modèle non chargé"

def test_predict_validation_error(client):
    """
    Teste que FastAPI renvoie bien une 422 si des champs obligatoires manquent.
    """
    # Payload incomplet (manque hour, day...)
    payload = {
        "direction_id": 0
    }
    
    response = client.post("/predict", json=payload)
    
    assert response.status_code == 422
    data = response.json()
    # On vérifie que c'est bien une erreur de validation
    assert data["detail"][0]["type"] == "missing"

@patch("app.main.model_instance.predict")
@patch("app.main.get_weather_features")
@patch("app.main.get_calendar_features")
def test_predict_saves_log_to_db(mock_calendar, mock_weather, mock_predict, client, db_session):
    """
    Vérifie qu'une prédiction est bien sauvegardée en base de données.
    """
    # Mocks minimaux
    mock_calendar.return_value = {"est_weekend": 0}
    mock_weather.return_value = {"temperature_2m": 12.0}
    mock_predict.return_value = 42.0
    
    payload = {
        "direction_id": 1,
        "month": 6,
        "day": 20,
        "hour": 14,
        "day_of_week": 4
    }
    
    # Appel API
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    
    # Vérification DB
    from app.data_structure import PredictionLog
    log = db_session.query(PredictionLog).order_by(PredictionLog.id.desc()).first()
    
    assert log is not None
    assert log.prediction == 42.0
    assert log.direction_id == 1
    # Vérifie qu'on a bien sauvegardé les features enrichies
    assert log.temperature_2m == 12.0 # Venant du mock météo

@patch("app.main.get_weather_features")
@patch("app.main.get_calendar_features")
def test_predict_weather_api_failure(mock_calendar, mock_weather, client):
    """
    Teste que l'API renvoie une 503 si l'API Météo échoue.
    """
    mock_calendar.return_value = {}
    # On simule une erreur de l'API météo
    mock_weather.side_effect = Exception("Service Unavailable")
    
    payload = {
        "direction_id": 1,
        "month": 6,
        "day": 20,
        "hour": 14,
        "day_of_week": 4
    }
    
    response = client.post("/predict", json=payload)
    assert response.status_code == 503
    assert "Service Unavailable" in response.json()["detail"]
