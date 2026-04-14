from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.logger import get_logger

# ---------------------------
# Initialize Spark
# ---------------------------
spark = SparkSession.builder.appName("TMDB Gold Layer").getOrCreate()

# Logger
logger = get_logger("gold_layer")


# ---------------------------
# Utility: Flatten array columns
# ---------------------------
def flatten_array_columns(df):
    for field in df.schema.fields:

        # ARRAY<STRING>
        if str(field.dataType).startswith("ArrayType(StringType"):
            df = df.withColumn(field.name, concat_ws("|", col(field.name)))

        # ARRAY<STRUCT> → extract name
        elif str(field.dataType).startswith("ArrayType(StructType"):
            df = df.withColumn(
                field.name,
                concat_ws("|", expr(f"transform({field.name}, x -> x.name)"))
            )

    return df


# ---------------------------
# Utility: Save CSV
# ---------------------------
def save_csv(df, path):
    df.write.mode("overwrite").option("header", True).csv(path)


# ---------------------------
# Main Gold Pipeline
# ---------------------------
def run_gold():

    logger.info("Reading Silver dataset...")
    df = spark.read.parquet("data/silver/movies")

    logger.info("Performing feature engineering (profit, ROI)...")
    df = df.withColumn("profit", col("revenue_musd") - col("budget_musd")) \
           .withColumn("roi", col("revenue_musd") / col("budget_musd"))

    logger.info("Dropping unnecessary complex columns (crew)...")
    df = df.drop("crew")

    logger.info("Flattening array columns for CSV compatibility...")
    df = flatten_array_columns(df)

    logger.info("Computing KPI rankings...")

    # -------------------------
    # KPI Rankings
    # -------------------------
    save_csv(df.orderBy(desc("revenue_musd")).limit(10), "data/gold/top_revenue")
    save_csv(df.orderBy(desc("budget_musd")).limit(10), "data/gold/top_budget")
    save_csv(df.orderBy(desc("profit")).limit(10), "data/gold/top_profit")
    save_csv(df.orderBy("profit").limit(10), "data/gold/lowest_profit")

    # -------------------------
    # ROI Analysis
    # -------------------------
    logger.info("Computing ROI-based rankings...")
    roi_df = df.filter(col("budget_musd") >= 10)

    save_csv(roi_df.orderBy(desc("roi")).limit(10), "data/gold/top_roi")
    save_csv(roi_df.orderBy("roi").limit(10), "data/gold/lowest_roi")

    # -------------------------
    # Popularity & Ratings
    # -------------------------
    logger.info("Computing popularity & rating metrics...")
    save_csv(df.orderBy(desc("vote_count")).limit(10), "data/gold/most_voted")

    save_csv(df.filter(col("vote_count") >= 10)
                .orderBy(desc("vote_average")).limit(10),
             "data/gold/highest_rated")

    save_csv(df.filter(col("vote_count") >= 10)
                .orderBy("vote_average").limit(10),
             "data/gold/lowest_rated")

    save_csv(df.orderBy(desc("popularity")).limit(10), "data/gold/most_popular")

    # -------------------------
    # Advanced Filters
    # -------------------------
    logger.info("Running advanced filters...")

    bruce = df.filter(
        (col("genres").contains("Science Fiction")) &
        (col("genres").contains("Action")) &
        (col("cast").contains("Bruce Willis"))
    ).orderBy(desc("vote_average"))

    save_csv(bruce, "data/gold/bruce_willis_movies")

    tarantino = df.filter(
        (col("cast").contains("Uma Thurman")) &
        (col("director") == "Quentin Tarantino")
    ).orderBy("runtime")

    save_csv(tarantino, "data/gold/tarantino_movies")

    # -------------------------
    # Franchise Analysis
    # -------------------------
    logger.info("Performing franchise analysis...")
    df.groupBy("belongs_to_collection").agg(
        count("*").alias("total_movies"),
        mean("revenue_musd").alias("avg_revenue"),
        mean("budget_musd").alias("avg_budget"),
        mean("vote_average").alias("avg_rating"),
        mean("popularity").alias("avg_popularity")
    ).write.mode("overwrite").parquet("data/gold/franchise_analysis")

    # -------------------------
    # Director Analysis
    # -------------------------
    logger.info("Performing director analysis...")
    df.groupBy("director").agg(
        count("*").alias("total_movies"),
        sum("revenue_musd").alias("total_revenue"),
        mean("vote_average").alias("avg_rating")
    ).write.mode("overwrite").parquet("data/gold/director_analysis")

    logger.info("Gold layer pipeline completed successfully!")


# ---------------------------
# Entry Point
# ---------------------------
if __name__ == "__main__":
    run_gold()