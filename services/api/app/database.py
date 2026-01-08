import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# URL de connexion NeonDB
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")

# On retire les guillemets si présents
if SQLALCHEMY_DATABASE_URL:
    
    SQLALCHEMY_DATABASE_URL = SQLALCHEMY_DATABASE_URL.strip("'\"")

if SQLALCHEMY_DATABASE_URL is None:
    raise ValueError("DATABASE_URL n'est pas renseigné. Veuillez renseigner l'URL dans le fichier .env")

# Création de l'engine et de la session
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Schéma de base de BDD
Base = declarative_base()

# Dépendance pour obtenir la session de DB dans les routes FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
