# TMDb Movie Data Analysis using PySpark and APIs

---

## Project Overview

This project implements an **end-to-end data engineering and analytics pipeline** using **PySpark** and the **TMDb API**.

The pipeline extracts raw movie data, transforms it into structured datasets, computes key performance indicators (KPIs), and generates insightful visualizations.

It follows a **Bronze–Silver–Gold architecture**, a best practice in modern data engineering.

---

## Project Highlights

* End-to-end PySpark ETL pipeline
* TMDb API integration (movies + credits)
* Bronze–Silver–Gold architecture
* KPI analysis (Revenue, Profit, ROI, Ratings, Popularity)
* Advanced filtering (actors & directors insights)
* Franchise and director performance analysis
* Data visualization using Matplotlib & Seaborn

---

## Architecture

```
TMDb API
   ↓
Bronze Layer (Raw JSON)
   ↓
Silver Layer (Cleaned & Structured Data)
   ↓
Gold Layer (KPIs & Aggregations)
   ↓
Visualization (Insights)
```

---

## Project Structure

```
tmdb-pyspark-project/
│
├── ingestion/            # API data extraction (Bronze layer)
├── processing/           # Data transformation (Silver & Gold)
├── analytics/            # Visualization scripts
├── pipelines/            # Pipeline orchestration
├── utils/                # Logger and utilities
│
├── data/                 # Data storage (ignored in Git)
├── outputs/              # Generated plots
├── logs/                 # Logs
│
├── project_report.md     # Detailed report
├── README.md             # Documentation
├── requirements.txt      # Dependencies
└── test_api.py           # API testing
```

---

## Technologies Used

* Python 3.x
* PySpark
* Pandas
* Matplotlib
* Seaborn
* TMDb API

---

## How to Run the Project

### 1. Clone the repository

```bash
git clone https://github.com/AgahozoDenyse/tmdb-pyspark-movie-analysis.git
cd tmdb-pyspark-movie-analysis
```

---

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

---

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Add TMDb API Key

Create a `.env` file:

```env
TMDB_API_KEY=your_api_key_here
```

---

### 5. Run the pipeline

#### 🔹 Data Ingestion (Bronze)

```bash
python -m ingestion.bronze_ingestion
```

#### 🔹 Data Transformation (Silver)

```bash
python -m processing.silver_transformer
```

#### 🔹 KPI Analysis (Gold)

```bash
python -m processing.gold_aggregations
```

#### 🔹 Visualization

```bash
python -m analytics.visualization
```

---

## Visual Insights

### Revenue vs Budget

![Revenue vs Budget](outputs/revenue_vs_budget.png)

---

### Popularity vs Rating

![Popularity vs Rating](outputs/popularity_vs_rating.png)

---

### ROI Distribution

![ROI Distribution](outputs/roi_distribution.png)

---

### Revenue Over Time

![Revenue Over Time](outputs/revenue_over_time.png)

---

## Outputs

### Data Outputs

* `data/bronze/` → Raw API data
* `data/silver/` → Clean dataset
* `data/gold/` → KPI results

### Visual Outputs

* Revenue vs Budget
* Popularity vs Rating
* ROI Distribution
* Revenue Over Time

---

## Key Insights

* High-budget movies tend to generate higher revenue, but ROI varies significantly
* Popularity does not always correlate with higher ratings
* Franchise movies often outperform standalone films
* Certain directors consistently generate higher revenue

---

## Challenges Faced

* Handling nested JSON structures (arrays and structs)
* Managing PySpark data types
* Converting Spark data for visualization
* Configuring Java & PySpark environment

---

## Future Improvements

* Expand dataset using API pagination
* Add real-time data ingestion
* Integrate machine learning models
* Deploy pipeline using Docker or cloud platforms

---

## Report

Detailed report available in:

```
project_report.md
```

---

## Author

**Denyse AGAHOZO**

---

## Final Remark

This project demonstrates a complete **data engineering workflow**, combining API integration, big data processing with PySpark, and analytical insights — suitable for real-world applications.
