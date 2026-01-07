import pytest
import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

# Add services/api to sys.path to allow imports from app
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../services/api')))

from app.main import app as fastapi_app, get_db
from app.database import Base
import app.data_structure 

# Setup de la base de test
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
# check_same_thread=False est nécessaire pour SQLite avec FastAPI
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Fixture de la base de test
@pytest.fixture(scope="function")
def db_session():
    # Créé les tables de la base de test
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        # Supprime les tables de la base de test après chaque test
        Base.metadata.drop_all(bind=engine)

# Fixture de création d'un client pour les tests de l'API
@pytest.fixture(scope="function")
def client(db_session):

    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    fastapi_app.dependency_overrides[get_db] = override_get_db
    with TestClient(fastapi_app) as c:
        yield c
    fastapi_app.dependency_overrides.clear()
