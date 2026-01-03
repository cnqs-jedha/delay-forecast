"""
Module de monitoring ML avec Evidently.
Génère des rapports de Data Drift et de qualité des prédictions.
"""

import os
import json
from datetime import datetime
from typing import Optional

import pandas as pd
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset, RegressionPreset
from evidently.metrics import (
    DataDriftTable,
    DatasetDriftMetric,
    RegressionQualityMetric,
)
from evidently.test_suite import TestSuite
from evidently.tests import (
    TestNumberOfDriftedColumns,
    TestShareOfDriftedColumns,
    TestColumnDrift,
)

from app.config import settings


class MLMonitor:
    """
    Service de monitoring pour le modèle de prédiction de retards.
    Utilise Evidently pour détecter le Data Drift et évaluer la qualité.
    """
    
    def __init__(self):
        self.reports_path = settings.REPORTS_PATH
        self.drift_threshold = settings.DRIFT_THRESHOLD
        self.reference_data: Optional[pd.DataFrame] = None
        
        # Créer le dossier des rapports s'il n'existe pas
        os.makedirs(self.reports_path, exist_ok=True)
    
    def set_reference_data(self, data: pd.DataFrame) -> None:
        """
        Définit les données de référence (baseline) pour la comparaison.
        Ces données représentent le comportement "normal" du modèle.
        
        Args:
            data: DataFrame avec les features et prédictions de référence
        """
        self.reference_data = data
    
    def generate_data_drift_report(
        self, 
        current_data: pd.DataFrame,
        column_mapping: Optional[ColumnMapping] = None
    ) -> dict:
        """
        Génère un rapport de Data Drift comparant les données actuelles
        aux données de référence.
        
        Args:
            current_data: DataFrame avec les données récentes
            column_mapping: Mapping des colonnes (features, target, prediction)
        
        Returns:
            dict avec le statut du drift et le chemin du rapport
        """
        if self.reference_data is None:
            return {
                "status": "error",
                "message": "Données de référence non définies. Appelez set_reference_data() d'abord."
            }
        
        # Créer le rapport de drift
        report = Report(metrics=[
            DatasetDriftMetric(),
            DataDriftTable(),
        ])
        
        report.run(
            reference_data=self.reference_data,
            current_data=current_data,
            column_mapping=column_mapping
        )
        
        # Sauvegarder le rapport HTML
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"data_drift_{timestamp}.html"
        report_path = os.path.join(self.reports_path, report_filename)
        report.save_html(report_path)
        
        # Extraire les métriques pour l'API
        report_dict = report.as_dict()
        drift_detected = report_dict["metrics"][0]["result"]["dataset_drift"]
        drift_share = report_dict["metrics"][0]["result"]["share_of_drifted_columns"]
        
        return {
            "status": "drift_detected" if drift_detected else "no_drift",
            "drift_detected": drift_detected,
            "drift_share": drift_share,
            "threshold": self.drift_threshold,
            "report_path": report_path,
            "report_filename": report_filename,
            "timestamp": timestamp
        }
    
    def generate_data_quality_report(
        self,
        data: pd.DataFrame,
        column_mapping: Optional[ColumnMapping] = None
    ) -> dict:
        """
        Génère un rapport de qualité des données.
        Vérifie les valeurs manquantes, les doublons, etc.
        
        Args:
            data: DataFrame à analyser
            column_mapping: Mapping des colonnes
        
        Returns:
            dict avec les métriques de qualité et le chemin du rapport
        """
        report = Report(metrics=[DataQualityPreset()])
        
        report.run(
            reference_data=self.reference_data,
            current_data=data,
            column_mapping=column_mapping
        )
        
        # Sauvegarder le rapport
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"data_quality_{timestamp}.html"
        report_path = os.path.join(self.reports_path, report_filename)
        report.save_html(report_path)
        
        return {
            "status": "success",
            "report_path": report_path,
            "report_filename": report_filename,
            "timestamp": timestamp
        }
    
    def generate_model_performance_report(
        self,
        data: pd.DataFrame,
        column_mapping: ColumnMapping
    ) -> dict:
        """
        Génère un rapport de performance du modèle de régression.
        Compare les prédictions aux valeurs réelles.
        
        Args:
            data: DataFrame avec colonnes 'prediction' et 'target' (valeur réelle)
            column_mapping: Mapping avec target et prediction définis
        
        Returns:
            dict avec les métriques de performance
        """
        report = Report(metrics=[RegressionPreset()])
        
        report.run(
            reference_data=self.reference_data,
            current_data=data,
            column_mapping=column_mapping
        )
        
        # Sauvegarder le rapport
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"model_performance_{timestamp}.html"
        report_path = os.path.join(self.reports_path, report_filename)
        report.save_html(report_path)
        
        return {
            "status": "success",
            "report_path": report_path,
            "report_filename": report_filename,
            "timestamp": timestamp
        }
    
    def run_drift_tests(
        self,
        current_data: pd.DataFrame,
        column_mapping: Optional[ColumnMapping] = None
    ) -> dict:
        """
        Exécute une suite de tests automatisés pour détecter le drift.
        Utile pour les alertes automatiques.
        
        Args:
            current_data: DataFrame avec les données récentes
            column_mapping: Mapping des colonnes
        
        Returns:
            dict avec les résultats des tests (pass/fail)
        """
        if self.reference_data is None:
            return {
                "status": "error",
                "message": "Données de référence non définies."
            }
        
        # Définir la suite de tests
        test_suite = TestSuite(tests=[
            TestNumberOfDriftedColumns(lt=3),  # Moins de 3 colonnes avec drift
            TestShareOfDriftedColumns(lt=self.drift_threshold),  # Moins de 10% de drift
        ])
        
        test_suite.run(
            reference_data=self.reference_data,
            current_data=current_data,
            column_mapping=column_mapping
        )
        
        # Sauvegarder le rapport de tests
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"drift_tests_{timestamp}.html"
        report_path = os.path.join(self.reports_path, report_filename)
        test_suite.save_html(report_path)
        
        # Extraire les résultats
        results = test_suite.as_dict()
        all_passed = all(
            test["status"] == "SUCCESS" 
            for test in results.get("tests", [])
        )
        
        return {
            "status": "pass" if all_passed else "fail",
            "all_tests_passed": all_passed,
            "tests": results.get("tests", []),
            "report_path": report_path,
            "report_filename": report_filename,
            "timestamp": timestamp
        }
    
    def list_reports(self) -> list[dict]:
        """
        Liste tous les rapports générés.
        
        Returns:
            Liste des rapports avec leurs métadonnées
        """
        reports = []
        for filename in os.listdir(self.reports_path):
            if filename.endswith(".html"):
                filepath = os.path.join(self.reports_path, filename)
                reports.append({
                    "filename": filename,
                    "path": filepath,
                    "created_at": datetime.fromtimestamp(
                        os.path.getctime(filepath)
                    ).isoformat(),
                    "size_kb": round(os.path.getsize(filepath) / 1024, 2)
                })
        
        # Trier par date de création (plus récent en premier)
        reports.sort(key=lambda x: x["created_at"], reverse=True)
        return reports


# Instance singleton du moniteur
monitor = MLMonitor()
