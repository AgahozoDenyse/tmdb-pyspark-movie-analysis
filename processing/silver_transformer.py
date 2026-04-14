from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("TMDB Silver Layer").getOrCreate()


def extract_names(column):
    return concat_ws("|", transform(column, lambda x: x["name"]))


def run_silver():

    print("Reading Bronze data...")

    movies = spark.read.json("data/bronze/movies")
    credits = spark.read.json("data/bronze/credits")

    # Flatten movie columns
    movies = movies \
        .withColumn("genres", extract_names(col("genres"))) \
        .withColumn("production_companies", extract_names(col("production_companies"))) \
        .withColumn("production_countries", extract_names(col("production_countries"))) \
        .withColumn("spoken_languages", extract_names(col("spoken_languages"))) \
        .withColumn("belongs_to_collection", col("belongs_to_collection.name"))

    # Extract director
    credits = credits.withColumn(
        "director",
        expr("filter(crew, x -> x.job = 'Director')[0].name")
    )

    # Extract top 5 cast
        # Cast size BEFORE converting to string
    credits = credits.withColumn("cast_size", size("cast")) \
                    .withColumn("crew_size", size("crew"))

    # Extract top 5 cast names
    credits = credits.withColumn(
        "cast",
        concat_ws("|", expr("transform(slice(cast, 1, 5), x -> x.name)"))
    )

    # Join datasets
    df = movies.join(credits, on="id", how="left")

    print("Cleaning data...")

    # Convert types
    numeric_cols = ["budget", "revenue", "runtime", "popularity", "vote_count", "vote_average"]

    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast("double"))

    df = df.withColumn("release_date", to_date("release_date"))

    # Replace 0 with null
    df = df.replace(0, None, subset=["budget", "revenue", "runtime"])

    # Convert to million USD
    df = df.withColumn("budget_musd", col("budget") / 1_000_000) \
           .withColumn("revenue_musd", col("revenue") / 1_000_000)

    # Filter released movies
    df = df.filter(col("status") == "Released")

    # Drop unnecessary columns
    df = df.drop("adult", "imdb_id", "original_title", "video", "homepage", "status")

    # Remove duplicates
    df = df.dropDuplicates(["id"])

    print("Saving Silver layer...")

    df.write.mode("overwrite").parquet("data/silver/movies")

    print("Silver layer completed!")


if __name__ == "__main__":
    run_silver()
