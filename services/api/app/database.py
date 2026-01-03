import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# URL de connexion NeonDB
SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URI")

# On retire les guillemets si présents
if SQLALCHEMY_DATABASE_URI:
    
    SQLALCHEMY_DATABASE_URI = SQLALCHEMY_DATABASE_URI.strip("'\"")

if SQLALCHEMY_DATABASE_URI is None:
    raise ValueError("DATABASE_URI n'est pas renseigné. Veuillez renseigner l'URI dans le fichier .env")

# Création de l'engine et de la session
engine = create_engine(SQLALCHEMY_DATABASE_URI)
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
