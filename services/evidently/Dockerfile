# Image Python légère
FROM python:3.11-slim

# Métadonnées
LABEL maintainer="delay-forecast-team"
LABEL description="Evidently AI - ML Monitoring Service"

# Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Répertoire de travail
WORKDIR /app

# Dépendances système (pour psycopg2 et compilation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copier et installer les dépendances Python
COPY services/evidently/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY services/evidently/app ./app

# Créer le dossier pour stocker les rapports générés
RUN mkdir -p /app/reports

# Exposer le port
EXPOSE 8001

# Commande de démarrage
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
