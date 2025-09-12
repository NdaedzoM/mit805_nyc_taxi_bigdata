# NYC Yellow Taxi Data Analysis

This project downloads, processes, and analyzes the **NYC Yellow Taxi Trip Data** (2009–2025) using Python and PySpark.  

---

## Project Overview

- **Data Source:** NYC Taxi and Limousine Commission (TLC)  
- **Format:** Parquet files  
- **Purpose:**  
  - Explore schema evolution across three periods  
  - Count monthly trips  
  - Compare schemas and datatypes  
  - Prepare data for visualization  
  - Extract insights for business and urban planning  

---
## Environment

- **Google Colab:** Used for running notebooks with code for downloading, processing, and analyzing data  
- **Libraries Used:**  
  - Python 3.x  
  - PySpark  
  - requests  
  - tqdm  
  - glob, os, json  
  
---

## Steps

1. **Download Data**  
   Downloads monthly taxi trip data in parallel using `requests` and `ThreadPoolExecutor`.

2. **Initialize PySpark**  
   Set up Spark session to read and process large Parquet files.

3. **Verify Files**  
   Check all downloaded files and compute total size.

4. **Schema Selection**  
   Select representative files for:
   - Schema 1 (2009–2016)  
   - Schema 2 (2016–2020)  
   - Schema 3 (2021–2025)

5. **Schema Analysis**  
   - Read Parquet files  
   - Print schemas and sample rows  
   - Count trips by year and month  

6. **Schema Comparison**  
   Compare columns and datatypes across schema periods.

7. **Visualization**  
   Prepare data for line charts showing monthly trip counts.  
   Plan to create at least 2–3 visualizations (graphs, charts, dashboards) to reveal trends, correlations, or clusters.

8. **Data Processing & Analysis Goals**  
   - Process dataset using Hadoop (MapReduce) or Spark  
   - Extract meaningful information (correlations, clustering, predictions)  
   - Interpret results both technically and in terms of business value  

---

## Requirements

- Python 3.x  
- PySpark  
- requests  
- tqdm  

---

## Usage

1. Run the data download script.  
2. Start PySpark and run the analysis script.  
3. View trip counts and schema differences.  
4. Generate visualization data for charting.  
5. Extend analysis to Hadoop/Spark MapReduce for deeper insights.  

---

## Notes

- Parquet format allows efficient storage and processing.  
- The dataset contains millions of taxi trips per month (~30 GB total).  
- Schema changes over time require careful handling for consistent analysis.  
- Insights can support urban planning, fleet optimization, and business strategies.
