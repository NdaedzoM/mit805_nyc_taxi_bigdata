# NYC Yellow Taxi Data Analysis

This project downloads, processes, and analyzes the **NYC Yellow Taxi Trip Data** (2009–2025) using Python and PySpark.  

---
## Dataset
- **Source:** [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Years Covered:** 2009 – 2025
- **Format:** Parquet files
- **Size:** ~29 GB (199 files)

## Project Workflow
1. **Data Download**
   - Downloaded Parquet files using Python scripts with parallelized requests.
2. **Data Processing**
   - Used **PySpark** in Google Colab to inspect schemas, sample data, and compute monthly trip counts.
   - Applied schema harmonization across three schema periods (2009–2016, 2017–2020, 2021–2025).
3. **Analysis**
   - Explored the dataset using the **7 Vs of Big Data** (Volume, Velocity, Variety, Value, Veracity, Variability, Visualization).
   - Generated insights for business and city planning.
4. **Visualizations**
   - Created **line charts** showing monthly trip counts.
   - Schema comparison tables and sample data previews.
5. **Future Steps**
   - Process data using **Hadoop MapReduce**.
   - Perform further analyses such as **correlations, clustering, and predictions**.
   - Develop **additional visualizations** and interpret results technically and in business terms.

---
## Environment

- **Google Colab:** Used for running notebooks with code for downloading, processing, and analyzing data  
- **Libraries Used:**  
  - Python 3.x  
  - PySpark  
  - requests  
  - tqdm  
  - glob, os, json  
  - Hadoop
  - Matplotlib / Chart.js for visualizations
  
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
