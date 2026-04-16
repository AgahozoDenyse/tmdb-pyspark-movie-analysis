"""
Module: silver_transformer.py

Description:
------------
This module implements the Silver Layer of the TMDb data pipeline.
It cleans, transforms, and enriches raw Bronze data into a structured format.

Key Features:
-------------
- Flatten nested JSON structures
- Extract meaningful features (genres, cast, director)
- Handle missing and invalid values
- Perform type conversions and feature engineering
- Save cleaned dataset for downstream analytics (Gold layer)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.logger import get_logger

# -----------------------------
# Initialize Spark (WSL-safe)
# -----------------------------
spark = (
    SparkSession.builder
    .appName("TMDB Silver Layer")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

# -----------------------------
# Logger
# -----------------------------
logger = get_logger("silver_layer")


# -----------------------------
# Utility: Extract names safely
# -----------------------------
def extract_names(column_name):
    return concat_ws("|", expr(f"transform({column_name}, x -> x.name)"))


# -----------------------------
# Main Silver Pipeline
# -----------------------------
def run_silver():

    logger.info("Reading Bronze data...")

    movies = spark.read.json("data/bronze/movies")
    credits = spark.read.json("data/bronze/credits")

    # -------------------------
    # Flatten movie columns
    # -------------------------
    movies = movies \
        .withColumn("genres", extract_names("genres")) \
        .withColumn("production_companies", extract_names("production_companies")) \
        .withColumn("production_countries", extract_names("production_countries")) \
        .withColumn("spoken_languages", extract_names("spoken_languages")) \
        .withColumn("belongs_to_collection", col("belongs_to_collection.name"))

    # -------------------------
    # Process credits
    # -------------------------
    credits = credits.withColumn(
        "director",
        expr("filter(crew, x -> x.job = 'Director')[0].name")
    )

    # Capture sizes before flattening
    credits = credits.withColumn("cast_size", size("cast")) \
                     .withColumn("crew_size", size("crew"))

    # Extract top 5 cast
    credits = credits.withColumn(
        "cast",
        concat_ws("|", expr("transform(slice(cast, 1, 5), x -> x.name)"))
    )

    # -------------------------
    # Join datasets
    # -------------------------
    df = movies.join(credits, on="id", how="left")

    logger.info("Cleaning and transforming data...")

    # -------------------------
    # Type conversion
    # -------------------------
    numeric_cols = ["budget", "revenue", "runtime", "popularity", "vote_count", "vote_average"]

    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast("double"))

    df = df.withColumn("release_date", to_date("release_date"))

    # -------------------------
    # Handle invalid values
    # -------------------------
    df = df.replace(0, None, subset=["budget", "revenue", "runtime"])

    # -------------------------
    # Feature engineering
    # -------------------------
    df = df.withColumn("budget_musd", col("budget") / 1_000_000) \
           .withColumn("revenue_musd", col("revenue") / 1_000_000)

    # -------------------------
    # Filtering
    # -------------------------
    df = df.filter(col("status") == "Released")

    # -------------------------
    # Drop unnecessary columns
    # -------------------------
    df = df.drop("adult", "imdb_id", "original_title", "video", "homepage", "status")

    # -------------------------
    # Remove duplicates
    # -------------------------
    df = df.dropDuplicates(["id"])

    logger.info("Saving Silver layer...")

    df.write.mode("overwrite").parquet("data/silver/movies")

    logger.info("Silver layer completed successfully!")


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    run_silver()