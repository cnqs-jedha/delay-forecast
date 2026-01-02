# MLFlow

Lancer `mlflow ui` à la racine du projet `delay_forecast`

Lancer le script `train_model.py`

1. Le dossier mlruns (Les métadonnées)
C'est le "cerveau" des expériences. Il contient des petits fichiers texte et JSON qui stockent tout ce qui est court et rapide à lire :

- Les Paramètres : (ex: n_estimators=100, learning_rate=0.01).
- Les Métriques : Les valeurs de tes scores au fil du temps (ex: RMSE, Accuracy).
- Les Tags : Les notes que tu as mises (ex: user="NLefort", version="1.0").
- L'état du run : Est-ce que le script a réussi ou crashé ?

2. Le dossier mlartifacts (Les fichiers lourds)
C'est le "garage" ou l'entrepôt. Il contient les objets volumineux générés par le script que MLflow ne peut pas stocker comme du simple texte :

- Les Modèles sauvegardés : Tes fichiers .pkl ou modèles XGBoost compressés (ceux créés par mlflow.sklearn.log_model).
- Les Graphiques : Si j'enregistres ta matrice de corrélation ou des courbes de perte en .png ou .html.
- Les Datasets : Si je décide de sauvegarder un échantillon de tes données (.csv).

Pourquoi cette séparation ?
Dans un contexte professionnel (entreprise) :

- La base de données (mlruns) est souvent stockée sur un serveur SQL (PostgreSQL/MySQL) pour que les recherches et comparaisons soient ultra-rapides.
- Le stockage d'artéfacts (mlartifacts) est déporté sur un service de stockage "Cloud" (comme AWS S3 ou Google Cloud Storage) car les modèles peuvent peser plusieurs Go et ne doivent pas ralentir la base de données.