import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

# -------------------------
# Initialize Spark
# -------------------------
spark = SparkSession.builder.appName("Visualization").getOrCreate()

# -------------------------
# Load data
# -------------------------
df = spark.read.parquet("data/silver/movies")

# Convert to pandas
pdf = df.toPandas()

# Set style
sns.set(style="whitegrid")

# -------------------------
# Data Preparation
# -------------------------

# Convert release_date safely
pdf["release_date"] = pd.to_datetime(pdf["release_date"], errors="coerce")

# Robust year extraction (avoids .dt errors)
pdf["year"] = pdf["release_date"].apply(
    lambda x: x.year if pd.notnull(x) else None
)

# Compute ROI
pdf["roi"] = pdf["revenue_musd"] / pdf["budget_musd"]

# Handle division issues
pdf["roi"] = pdf["roi"].replace([float("inf"), -float("inf")], None)

# Clean dataset
pdf_clean = pdf.dropna(subset=[
    "budget_musd",
    "revenue_musd",
    "popularity",
    "vote_average",
    "roi",
    "year"
])

# -------------------------
# 1. Revenue vs Budget
# -------------------------
plt.figure(figsize=(8, 5))
sns.scatterplot(data=pdf_clean, x="budget_musd", y="revenue_musd")
plt.title("Revenue vs Budget")
plt.xlabel("Budget (Million USD)")
plt.ylabel("Revenue (Million USD)")
plt.tight_layout()
plt.savefig("outputs/revenue_vs_budget.png")
plt.close()

# -------------------------
# 2. Popularity vs Rating
# -------------------------
plt.figure(figsize=(8, 5))
sns.scatterplot(data=pdf_clean, x="popularity", y="vote_average")
plt.title("Popularity vs Rating")
plt.xlabel("Popularity")
plt.ylabel("Vote Average")
plt.tight_layout()
plt.savefig("outputs/popularity_vs_rating.png")
plt.close()

# -------------------------
# 3. ROI Distribution
# -------------------------
plt.figure(figsize=(8, 5))
sns.histplot(pdf_clean["roi"], bins=20, kde=True)
plt.title("ROI Distribution")
plt.xlabel("Return on Investment (ROI)")
plt.tight_layout()
plt.savefig("outputs/roi_distribution.png")
plt.close()

# -------------------------
# 4. Revenue Over Time
# -------------------------
plt.figure(figsize=(10, 5))
sns.boxplot(data=pdf_clean, x="year", y="revenue_musd")
plt.xticks(rotation=45)
plt.title("Revenue Distribution Over Time")
plt.xlabel("Year")
plt.ylabel("Revenue (Million USD)")
plt.tight_layout()
plt.savefig("outputs/revenue_over_time.png")
plt.close()
