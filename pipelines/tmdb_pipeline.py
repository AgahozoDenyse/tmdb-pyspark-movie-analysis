"""
Module: tmdb_pipeline.py

Description:
------------
Main pipeline orchestrator for the TMDb data pipeline.
Executes Bronze → Silver → Gold → Visualization sequentially.

Usage:
------
python -m pipelines.tmdb_pipeline
"""

from ingestion.bronze_ingestion import run_bronze
from processing.silver_transformer import run_silver
from processing.gold_aggregations import run_gold
from analytics.visualization import main as run_visualization
from utils.logger import get_logger

logger = get_logger("pipeline")


def run_pipeline():
    logger.info("Starting TMDB pipeline...")

    # -------------------------
    # Bronze Layer
    # -------------------------
    logger.info("Running Bronze layer...")
    run_bronze()

    # -------------------------
    # Silver Layer
    # -------------------------
    logger.info("Running Silver layer...")
    run_silver()

    # -------------------------
    # Gold Layer
    # -------------------------
    logger.info("Running Gold layer...")
    run_gold()

    # -------------------------
    # Visualization
    # -------------------------
    logger.info("Running Visualization...")
    run_visualization()

    logger.info("Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()