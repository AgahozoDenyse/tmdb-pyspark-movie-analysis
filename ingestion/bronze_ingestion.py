from pyspark.sql import SparkSession
from ingestion.tmdb_api_client import fetch_movie, fetch_credits
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Initialize Spark
spark = SparkSession.builder.appName("TMDB Bronze Layer").getOrCreate()

# Movie IDs (given)
movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
             420818, 24428, 168259, 99861, 284054, 12445,
             181808, 330457, 351286, 109445, 321612, 260513]


def fetch_all_movies(ids):
    movies = []
    credits = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(fetch_movie, mid): mid for mid in ids}

        for future in as_completed(future_to_id):
            movie = future.result()
            if movie:
                movies.append(json.dumps(movie))

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_id = {executor.submit(fetch_credits, mid): mid for mid in ids}

        for future in as_completed(future_to_id):
            credit = future.result()
            mid = future_to_id[future]

            if credit:
                credits.append(json.dumps({
                    "id": mid,
                    "cast": credit.get("cast", []),
                    "crew": credit.get("crew", [])
                }))

    return movies, credits


def run_bronze():
    print("Fetching data from API...")

    movies_json, credits_json = fetch_all_movies(movie_ids)

    print("Creating Spark DataFrames...")

    movies_df = spark.read.json(spark.sparkContext.parallelize(movies_json))
    credits_df = spark.read.json(spark.sparkContext.parallelize(credits_json))

    print("Saving Bronze Layer...")

    movies_df.write.mode("overwrite").json("data/bronze/movies")
    credits_df.write.mode("overwrite").json("data/bronze/credits")

    print("Bronze layer completed!")


if __name__ == "__main__":
    run_bronze()
