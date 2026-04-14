# TMDb Movie Data Analysis using PySpark and APIs

---

## Project Overview

This project implements an end-to-end data pipeline using **PySpark** to analyze movie data from the **TMDb API**. The goal is to extract, clean, transform, and analyze movie data to generate meaningful insights on financial performance, popularity, and trends.

The pipeline follows a **multi-layer architecture**:

* **Bronze Layer** → Raw data ingestion from API
* **Silver Layer** → Data cleaning and transformation
* **Gold Layer** → KPI analysis and aggregations
* **Visualization Layer** → Insightful plots and trends

---

## Project Objectives

* Fetch movie data from TMDb API
* Clean and structure raw JSON data
* Perform exploratory data analysis (EDA)
* Compute key performance indicators (KPIs)
* Analyze franchises and directors
* Visualize key insights

---

## Project Structure

```
tmdb-pyspark-project/
│
├── ingestion/            # API data extraction (Bronze layer)
├── processing/           # Data transformation (Silver & Gold)
├── analytics/            # Visualization scripts
├── utils/                # Logger and utilities
├── data/                 # Bronze, Silver, Gold datasets
├── outputs/              # Visualization images
├── logs/                 # Pipeline logs
│
├── project_report.md     # Final project report
├── requirements.txt      # Dependencies
└── test_api.py           # API testing script
```

---

## Technologies Used

* Python 3.12
* PySpark
* Pandas
* Matplotlib
* Seaborn
* TMDb API

---

## How to Run the Project

### 1. Clone or navigate to the project folder

```bash
cd tmdb-pyspark-project
```

---

### 2. Create and activate virtual environment

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

### 4. Add your TMDb API key

Create a `.env` file:

```env
TMDB_API_KEY=your_api_key_here
```

---

### 5. Run the pipeline

#### Step 1: Data Ingestion (Bronze)

```bash
python -m ingestion.bronze_ingestion
```

#### Step 2: Data Cleaning (Silver)

```bash
python -m processing.silver_transformer
```

#### Step 3: KPI Analysis (Gold)

```bash
python -m processing.gold_aggregations
```

#### Step 4: Visualization

```bash
python -m analytics.visualization
```

---

## Outputs

### Data Outputs

* `data/bronze/` → Raw API data
* `data/silver/` → Cleaned dataset
* `data/gold/` → KPI results and aggregations

### Visual Outputs

* `revenue_vs_budget.png`
* `popularity_vs_rating.png`
* `roi_distribution.png`
* `revenue_over_time.png`

---

## Key Features

* KPI rankings (Revenue, Profit, ROI, Ratings)
* Advanced filtering (actors & directors)
* Franchise performance analysis
* Director performance insights
* Data visualization for trends and patterns

---

##Challenges Faced

* Handling nested JSON data structures
* Managing PySpark data types
* Converting Spark data to Pandas
* Configuring Java environment for Spark

---

## Future Improvements

* Expand dataset using API pagination
* Add real-time data processing
* Integrate machine learning models
* Deploy pipeline using Docker or cloud platforms

---

## Report

A detailed explanation of the project is available in:

```
project_report.md
```

---

## Author

**Denyse AGAHOZO**


---

## Final Remark

This project demonstrates a complete **data engineering and analytics pipeline**, showcasing practical skills in PySpark, API integration, and data visualization.
