# Utiliser une image Python légère
FROM python:3.11-slim

# Définir le répertoire de travail
WORKDIR /app

# Empêcher Python de générer des fichiers .pyc et forcer l'affichage des logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installer les dépendances système nécessaires (si besoin)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copier le fichier requirements.txt
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code de l'application
COPY . .

# Exposer le port sur lequel l'API va tourner
EXPOSE 8000

# Commande pour lancer l'application avec uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
