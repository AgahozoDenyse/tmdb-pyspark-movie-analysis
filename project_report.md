# TMDb Movie Data Analysis using PySpark and APIs

---

## 1. Executive Summary

This project presents an end-to-end data analysis pipeline built using PySpark and the TMDb API. The objective was to extract, clean, transform, and analyze movie data to generate meaningful insights about financial performance, popularity, and industry trends.

A multi-layer architecture (Bronze → Silver → Gold) was implemented to structure the workflow efficiently. Key performance indicators (KPIs) such as revenue, profit, return on investment (ROI), popularity, and ratings were computed. Additionally, advanced filtering and aggregation techniques were applied to extract deeper insights.

The final outputs include structured datasets and visualizations that support data-driven decision-making.

---

## 2. Methodology

### 2.1 Data Extraction (Bronze Layer)

* Movie data was fetched from the TMDb API using predefined movie IDs.
* Two API endpoints were used:

  * Movie details
  * Credits (cast and crew)
* Data was stored in raw JSON format using PySpark DataFrames.
* Error handling was implemented to manage invalid API responses (e.g., missing IDs).

---

### 2.2 Data Cleaning & Transformation (Silver Layer)

The raw data was processed and transformed into an analysis-ready dataset:

* Removed irrelevant columns:

  * adult, imdb_id, original_title, video, homepage
* Flattened nested JSON fields:

  * genres, production companies, production countries, spoken languages
* Extracted important features:

  * Director from crew data
  * Top cast members
* Converted data types:

  * Numeric columns (budget, revenue, popularity, etc.)
  * release_date converted to datetime
* Created new features:

  * budget_musd and revenue_musd
* Filtered only released movies
* Removed duplicates and rows with missing values

---

### 2.3 KPI Implementation & Analysis (Gold Layer)

Key performance indicators were implemented using PySpark transformations:

#### Ranking KPIs

* Highest Revenue
* Highest Budget
* Highest Profit
* Lowest Profit
* Highest ROI (budget ≥ 10M)
* Lowest ROI
* Most Voted
* Highest Rated (vote_count ≥ 10)
* Lowest Rated (vote_count ≥ 10)
* Most Popular

#### Advanced Filtering

* Sci-Fi and Action movies featuring Bruce Willis
* Movies with Uma Thurman directed by Quentin Tarantino

#### Aggregations

* Franchise Analysis:

  * Total movies
  * Average revenue, budget, rating, and popularity
* Director Analysis:

  * Total movies
  * Total revenue
  * Average rating

---

### 2.4 Data Visualization

Four visualizations were created to highlight key insights:

1. Revenue vs Budget
   Shows the relationship between production cost and earnings.

2. Popularity vs Rating
   Compares audience engagement with movie quality.

3. ROI Distribution
   Displays profitability patterns across movies.

4. Revenue Over Time
   Highlights box office trends across release years.

---

## 3. Key Findings

* Higher budget movies tend to generate higher revenue, but not always proportionally.
* ROI varies significantly, showing that high investment does not guarantee high returns.
* Popularity does not always correlate with higher ratings.
* Franchise movies often perform better than standalone films.
* Directors with multiple films tend to accumulate higher total revenue.
* Actor and director combinations significantly influence movie success.

---

## 4. Challenges and Solutions

### Challenges

* Handling nested JSON data structures (arrays and structs)
* Managing Spark data types and schema transformations
* Converting Spark DataFrames to Pandas for visualization
* Configuring Java and PySpark environment

### Solutions

* Used PySpark functions such as transform and concat_ws to flatten data
* Applied robust data type conversions and null handling
* Implemented safe datetime parsing in Pandas
* Configured Java (JDK 17) to ensure compatibility with PySpark

---

## 5. Limitations

* Limited dataset based on selected movie IDs
* API rate limits restrict large-scale data extraction
* Some missing or incomplete fields in the dataset
* Visualizations rely on Pandas (not fully scalable for big data)

---

## 6. Conclusion

This project demonstrates how PySpark can be used to build a scalable data pipeline for real-world analytics tasks. The layered architecture ensures clear separation of concerns, from raw data ingestion to advanced analytics.

The results provide valuable insights into movie performance, audience behavior, and industry trends. The project also highlights best practices in data engineering, including modular design, logging, and efficient data transformations.

---

## 7. Future Improvements

* Expand dataset using API pagination
* Integrate real-time data ingestion
* Use Spark-based visualization tools
* Deploy pipeline using Docker or cloud platforms
* Apply machine learning models for prediction

---

## 8. Tools and Technologies

* Python 3.12
* PySpark
* Pandas
* Matplotlib and Seaborn
* TMDb API

---

## Final Remark

This project successfully combines data engineering and data analysis, demonstrating the ability to handle real-world datasets and generate actionable insights using PySpark.
