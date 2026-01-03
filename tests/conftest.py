import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
from api.main import app, get_db
from api.database import Base

# Permet d'enregistrer la structure des tables pour le test
import api.data_structure 

# Setup de la base de test
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Fixture de la base de test
@pytest.fixture(scope="function")
def db():
    # Créé les tables de la base de test
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        # Supprime les tables de la base de test après chaque test pour assurer l'isolation
        Base.metadata.drop_all(bind=engine)

# Fixture de création d'un client pour les tests de l'API
@pytest.fixture(scope="function")
def client(db):

    def override_get_db():
        try:
            yield db
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
