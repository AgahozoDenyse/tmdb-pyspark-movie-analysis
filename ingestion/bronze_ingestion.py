"""
Module: bronze_ingestion.py

Description:
------------
This module implements the Bronze Layer of the TMDb data pipeline.
It is responsible for fetching raw movie data and credits from the TMDb API
and storing them in their original JSON format using PySpark.

Key Features:
-------------
- Parallel API calls using ThreadPoolExecutor
- Robust error handling and logging
- Config-driven design (movie IDs externalized)
- Raw data storage (Bronze Layer) for downstream processing
- Scalable ingestion using Spark DataFrames

"""
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

from pyspark.sql import SparkSession
from ingestion.tmdb_api_client import fetch_movie, fetch_credits
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import get_logger
from config import MOVIE_IDS
import json


# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TMDB Bronze Layer")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)


# -----------------------------
# Initialize Logger
# -----------------------------
logger = get_logger("bronze_layer")


def fetch_all_movies(ids):
    """
    Fetch movie details and credits in parallel.

    Parameters:
    -----------
    ids : list
        List of movie IDs to fetch from TMDb API.

    Returns:
    --------
    tuple (movies, credits)
        movies : list of JSON strings
        credits : list of JSON strings

    Notes:
    ------
    - Uses multithreading for faster API calls
    - Handles API errors gracefully
    """

    movies = []
    credits = []

    logger.info("Starting parallel API requests...")

    # Fetch movies
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_movie, mid): mid for mid in ids}

        for future in as_completed(futures):
            try:
                movie = future.result()
                if movie:
                    movies.append(json.dumps(movie))
            except Exception as e:
                logger.error(f"Error fetching movie: {e}")

    # Fetch credits
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_credits, mid): mid for mid in ids}

        for future in as_completed(futures):
            mid = futures[future]
            try:
                credit = future.result()
                if credit:
                    credits.append(json.dumps({
                        "id": mid,
                        "cast": credit.get("cast", []),
                        "crew": credit.get("crew", [])
                    }))
            except Exception as e:
                logger.error(f"Error fetching credits for {mid}: {e}")

    return movies, credits


def run_bronze():
    """
    Execute the Bronze Layer pipeline.

    Workflow:
    ---------
    1. Fetch movie data from TMDb API (config-driven)
    2. Convert raw JSON into Spark DataFrames
    3. Save data into Bronze storage layer

    Outputs:
    --------
    - data/bronze/movies/
    - data/bronze/credits/

    Notes:
    ------
    - Data is stored in raw JSON format
    - This layer serves as input for Silver transformation
    """

    logger.info("Fetching data from API...")

    movies_json, credits_json = fetch_all_movies(MOVIE_IDS)

    logger.info(f"Fetched {len(movies_json)} movies")
    logger.info(f"Fetched {len(credits_json)} credits")

    if not movies_json:
        logger.warning("No movie data fetched. Exiting pipeline.")
        return

    logger.info("Creating Spark DataFrames...")

    movies_df = spark.read.json(
        spark.sparkContext.parallelize(movies_json, 4)
    )
    credits_df = spark.read.json(
        spark.sparkContext.parallelize(credits_json, 4)
    )

    logger.info("Saving Bronze Layer...")

    movies_df.write.mode("overwrite").json("data/bronze/movies")
    credits_df.write.mode("overwrite").json("data/bronze/credits")

    logger.info("Bronze layer completed successfully!")


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    run_bronze()