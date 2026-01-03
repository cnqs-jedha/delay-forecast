"""
Configuration du service Evidently.
Charge les variables d'environnement pour la connexion à la base de données.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Configuration centralisée du service de monitoring."""
    
    # Base de données NeonDB (pour lire les logs de prédiction)
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/delay_forecast"
    )
    
    # Chemin pour stocker les rapports HTML générés
    REPORTS_PATH: str = os.getenv("REPORTS_PATH", "/app/reports")
    
    # Seuils d'alerte pour le Data Drift
    DRIFT_THRESHOLD: float = float(os.getenv("DRIFT_THRESHOLD", "0.1"))
    
    # Nombre de prédictions à analyser pour le monitoring
    MONITORING_WINDOW: int = int(os.getenv("MONITORING_WINDOW", "1000"))
    
    # URL de l'API pour les webhooks d'alerte (optionnel)
    ALERT_WEBHOOK_URL: str | None = os.getenv("ALERT_WEBHOOK_URL")


settings = Settings()
