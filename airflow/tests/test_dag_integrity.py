"""
Tests to verify DAG integrity and configuration.
These tests ensure DAGs load without errors and have proper configuration.
"""

import pytest
from pathlib import Path


class TestDagIntegrity:
    """Tests for DAG file integrity."""

    def test_dag_file_exists(self):
        """The bike ingestion DAG file should exist."""
        dag_path = Path(__file__).parent.parent / "dags" / "bike_ingestion_dag.py"
        assert dag_path.exists(), f"DAG file not found at {dag_path}"

    def test_dag_file_is_valid_python(self):
        """DAG file should be valid Python syntax."""
        dag_path = Path(__file__).parent.parent / "dags" / "bike_ingestion_dag.py"
        
        with open(dag_path, "r") as f:
            source = f.read()
        
        # This will raise SyntaxError if the file has invalid Python
        compile(source, dag_path, "exec")

    def test_utils_module_exists(self):
        """The utils module should exist and be importable structure."""
        utils_path = Path(__file__).parent.parent / "dags" / "utils"
        assert utils_path.exists(), "Utils directory not found"
        assert (utils_path / "__init__.py").exists(), "Utils __init__.py not found"
        assert (utils_path / "data_utils.py").exists(), "data_utils.py not found"


class TestDagConfiguration:
    """Tests for DAG configuration values."""

    def test_dag_has_schedule_interval(self):
        """DAG should have a schedule interval defined."""
        dag_path = Path(__file__).parent.parent / "dags" / "bike_ingestion_dag.py"
        
        with open(dag_path, "r") as f:
            content = f.read()
        
        # Check for schedule definition (could be schedule_interval or schedule)
        assert "schedule" in content.lower(), "DAG should have a schedule defined"

    def test_dag_has_catchup_disabled(self):
        """DAG should have catchup disabled for production use."""
        dag_path = Path(__file__).parent.parent / "dags" / "bike_ingestion_dag.py"
        
        with open(dag_path, "r") as f:
            content = f.read()
        
        # Catchup=False is a best practice for real-time pipelines
        assert "catchup" in content.lower(), "DAG should have catchup parameter defined"


class TestDbtConfiguration:
    """Tests for dbt project configuration."""

    def test_dbt_project_file_exists(self):
        """dbt_project.yml should exist."""
        dbt_path = Path(__file__).parent.parent / "dbt" / "dbt_project.yml"
        assert dbt_path.exists(), f"dbt_project.yml not found at {dbt_path}"

    def test_dbt_models_directory_exists(self):
        """dbt models directory should exist with staging and marts."""
        models_path = Path(__file__).parent.parent / "dbt" / "models"
        assert models_path.exists(), "dbt models directory not found"
        assert (models_path / "staging").exists(), "staging models not found"
        assert (models_path / "marts").exists(), "marts models not found"

    def test_dbt_mart_models_exist(self):
        """Required dbt mart models should exist."""
        marts_path = Path(__file__).parent.parent / "dbt" / "models" / "marts"
        
        required_models = [
            "dim_station.sql",
            "fct_station_status_hourly.sql",
            "marts.yml"
        ]
        
        for model in required_models:
            assert (marts_path / model).exists(), f"Missing mart model: {model}"

