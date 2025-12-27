import pytest

def test_root(client):
    """Teste la route racine."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Bienvenue sur l'API de prédiction de retard des transports parisiens Delay Forecast"}

def test_predict_success(client):
    """Teste une requête de prédiction avec des données valides."""
    payload = {
        "line": 1,
        "weather": "Sunny"
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "prediction" in data
    assert isinstance(data["prediction"], float)

def test_predict_invalid_data(client):
    """Teste une requête de prédiction avec des données invalides."""
    payload = {
        "line": 1
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 422 # Erreur de validation via model.py

def test_predict_wrong_type(client):
    """Teste une requête de prédiction avec des données de mauvais type."""
    payload = {
        "line": "not-a-number",
        "weather": "Rainy"
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 422 # Erreur de validation via model.py
