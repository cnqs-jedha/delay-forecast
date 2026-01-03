"""
API FastAPI pour le service de monitoring Evidently.
Expose des endpoints pour générer des rapports et vérifier le drift.
"""

from typing import Optional
from datetime import datetime
import os

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
import pandas as pd

from app.config import settings
from app.monitoring import monitor, MLMonitor


# ============================================================================
# Application FastAPI
# ============================================================================

app = FastAPI(
    title="Evidently Monitoring Service",
    description="""
    Service de monitoring ML pour Delay Forecast.
    
    Fonctionnalités :
    - 📊 **Data Drift** : Détecte les changements dans la distribution des données
    - 🔍 **Data Quality** : Vérifie la qualité des données d'entrée
    - 📈 **Model Performance** : Évalue les performances du modèle
    - 🚨 **Alertes** : Tests automatisés avec seuils configurables
    """,
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


# ============================================================================
# Modèles Pydantic
# ============================================================================

class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class ReferenceDataRequest(BaseModel):
    """Données de référence pour la comparaison (format JSON)."""
    data: list[dict]
    

class MonitoringDataRequest(BaseModel):
    """Données à analyser pour le drift/qualité."""
    data: list[dict]
    include_prediction: bool = False
    prediction_column: str = "prediction"
    target_column: Optional[str] = None


class DriftReportResponse(BaseModel):
    status: str
    drift_detected: bool
    drift_share: float
    threshold: float
    report_filename: str
    timestamp: str


class TestResultResponse(BaseModel):
    status: str
    all_tests_passed: bool
    report_filename: str
    timestamp: str


class ReportInfo(BaseModel):
    filename: str
    path: str
    created_at: str
    size_kb: float


# ============================================================================
# Endpoints
# ============================================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Vérifie que le service est opérationnel.
    Utilisé par Docker pour le healthcheck.
    """
    return HealthResponse(
        status="healthy",
        service="evidently-monitoring",
        timestamp=datetime.now().isoformat()
    )


@app.get("/", tags=["Health"])
async def root():
    """Page d'accueil avec liens vers la documentation."""
    return {
        "service": "Evidently Monitoring Service",
        "version": "0.1.0",
        "documentation": "/docs",
        "health": "/health"
    }


# ----------------------------------------------------------------------------
# Gestion des données de référence
# ----------------------------------------------------------------------------

@app.post("/reference", tags=["Reference Data"])
async def set_reference_data(request: ReferenceDataRequest):
    """
    Définit les données de référence (baseline) pour la détection de drift.
    
    Ces données représentent le comportement "normal" du modèle.
    Elles seront comparées aux nouvelles données pour détecter les anomalies.
    
    **Exemple de payload :**
    ```json
    {
        "data": [
            {"temperature": 15.0, "rain_mm": 0.0, "wind_kmh": 10.0, "is_holiday": 0},
            {"temperature": 18.0, "rain_mm": 2.5, "wind_kmh": 15.0, "is_holiday": 0}
        ]
    }
    ```
    """
    try:
        df = pd.DataFrame(request.data)
        monitor.set_reference_data(df)
        return {
            "status": "success",
            "message": f"Données de référence définies ({len(df)} lignes, {len(df.columns)} colonnes)",
            "columns": list(df.columns),
            "rows": len(df)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/reference/status", tags=["Reference Data"])
async def get_reference_status():
    """Vérifie si les données de référence sont définies."""
    if monitor.reference_data is None:
        return {
            "status": "not_set",
            "message": "Aucune donnée de référence définie. Utilisez POST /reference"
        }
    
    return {
        "status": "set",
        "rows": len(monitor.reference_data),
        "columns": list(monitor.reference_data.columns)
    }


# ----------------------------------------------------------------------------
# Rapports de monitoring
# ----------------------------------------------------------------------------

@app.post("/drift/report", response_model=DriftReportResponse, tags=["Monitoring"])
async def generate_drift_report(request: MonitoringDataRequest):
    """
    Génère un rapport de Data Drift.
    
    Compare les données fournies aux données de référence pour détecter
    des changements dans la distribution des features.
    
    **Retourne :**
    - `drift_detected`: True si un drift significatif est détecté
    - `drift_share`: Pourcentage de colonnes avec drift
    - `report_filename`: Nom du rapport HTML généré
    """
    if monitor.reference_data is None:
        raise HTTPException(
            status_code=400,
            detail="Données de référence non définies. Utilisez POST /reference d'abord."
        )
    
    try:
        df = pd.DataFrame(request.data)
        result = monitor.generate_data_drift_report(df)
        
        return DriftReportResponse(
            status=result["status"],
            drift_detected=result["drift_detected"],
            drift_share=result["drift_share"],
            threshold=result["threshold"],
            report_filename=result["report_filename"],
            timestamp=result["timestamp"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/quality/report", tags=["Monitoring"])
async def generate_quality_report(request: MonitoringDataRequest):
    """
    Génère un rapport de qualité des données.
    
    Analyse les données pour détecter :
    - Valeurs manquantes
    - Doublons
    - Valeurs aberrantes
    - Distribution des features
    """
    try:
        df = pd.DataFrame(request.data)
        result = monitor.generate_data_quality_report(df)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/drift/test", response_model=TestResultResponse, tags=["Monitoring"])
async def run_drift_tests(request: MonitoringDataRequest):
    """
    Exécute une suite de tests automatisés pour le drift.
    
    Utile pour les alertes automatiques (intégration Airflow).
    
    **Tests exécutés :**
    - Nombre de colonnes avec drift < 3
    - Part de colonnes avec drift < seuil configuré (défaut: 10%)
    
    **Retourne :**
    - `all_tests_passed`: True si tous les tests passent
    - `status`: "pass" ou "fail"
    """
    if monitor.reference_data is None:
        raise HTTPException(
            status_code=400,
            detail="Données de référence non définies. Utilisez POST /reference d'abord."
        )
    
    try:
        df = pd.DataFrame(request.data)
        result = monitor.run_drift_tests(df)
        
        return TestResultResponse(
            status=result["status"],
            all_tests_passed=result["all_tests_passed"],
            report_filename=result["report_filename"],
            timestamp=result["timestamp"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ----------------------------------------------------------------------------
# Gestion des rapports
# ----------------------------------------------------------------------------

@app.get("/reports", response_model=list[ReportInfo], tags=["Reports"])
async def list_reports():
    """
    Liste tous les rapports générés.
    
    Les rapports sont triés par date de création (plus récent en premier).
    """
    return monitor.list_reports()


@app.get("/reports/{filename}", tags=["Reports"])
async def get_report(filename: str):
    """
    Télécharge un rapport HTML spécifique.
    
    Le rapport peut être ouvert dans un navigateur pour visualisation.
    """
    filepath = os.path.join(settings.REPORTS_PATH, filename)
    
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"Rapport '{filename}' non trouvé")
    
    return FileResponse(
        filepath,
        media_type="text/html",
        filename=filename
    )


@app.delete("/reports/{filename}", tags=["Reports"])
async def delete_report(filename: str):
    """Supprime un rapport spécifique."""
    filepath = os.path.join(settings.REPORTS_PATH, filename)
    
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"Rapport '{filename}' non trouvé")
    
    os.remove(filepath)
    return {"status": "deleted", "filename": filename}


# ----------------------------------------------------------------------------
# Dashboard (HTML simple)
# ----------------------------------------------------------------------------

@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard():
    """
    Dashboard HTML simple affichant les derniers rapports.
    """
    reports = monitor.list_reports()
    
    reports_html = ""
    for report in reports[:10]:  # Afficher les 10 derniers
        report_type = report["filename"].split("_")[0].replace("data", "Data").replace("drift", "Drift")
        reports_html += f"""
        <tr>
            <td>{report_type}</td>
            <td><a href="/reports/{report['filename']}" target="_blank">{report['filename']}</a></td>
            <td>{report['created_at'][:19]}</td>
            <td>{report['size_kb']} KB</td>
        </tr>
        """
    
    if not reports_html:
        reports_html = "<tr><td colspan='4'>Aucun rapport généré</td></tr>"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Evidently Monitoring Dashboard</title>
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
            h2 {{ color: #666; margin-top: 30px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background: #4CAF50; color: white; }}
            tr:hover {{ background: #f5f5f5; }}
            a {{ color: #2196F3; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            .status {{ padding: 5px 10px; border-radius: 4px; font-weight: bold; }}
            .status.healthy {{ background: #C8E6C9; color: #2E7D32; }}
            .links {{ margin: 20px 0; }}
            .links a {{ margin-right: 20px; padding: 10px 20px; background: #2196F3; color: white; border-radius: 4px; }}
            .links a:hover {{ background: #1976D2; text-decoration: none; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Evidently Monitoring Dashboard</h1>
            
            <p>Status: <span class="status healthy">Healthy</span></p>
            
            <div class="links">
                <a href="/docs">📚 API Documentation</a>
                <a href="/reference/status">🔍 Reference Data Status</a>
            </div>
            
            <h2>📁 Derniers Rapports</h2>
            <table>
                <tr>
                    <th>Type</th>
                    <th>Fichier</th>
                    <th>Date de création</th>
                    <th>Taille</th>
                </tr>
                {reports_html}
            </table>
            
            <h2>🚀 Utilisation rapide</h2>
            <ol>
                <li>Définir les données de référence : <code>POST /reference</code></li>
                <li>Générer un rapport de drift : <code>POST /drift/report</code></li>
                <li>Lancer les tests automatiques : <code>POST /drift/test</code></li>
            </ol>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html)
