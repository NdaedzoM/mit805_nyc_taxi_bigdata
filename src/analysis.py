
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year
import os
import glob
import json
import time


# Initialize Spark
spark = SparkSession.builder.appName("NYC Taxi Schema Analysis").config("spark.driver.memory", "4g").getOrCreate()


# Path to data 
data_path = "../data/nyc_taxi/yellow_tripdata_*.parquet"

# Verify files and compute size
file_list = glob.glob(data_path)
if not file_list:
    print("Error: No Parquet files found in ./nyc_taxi/. Please download from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    spark.stop()
    exit()


# Check formats and size
formats = set(os.path.splitext(f)[1] for f in file_list)
print(f"Found {len(file_list)} files. Formats: {formats}")
if '.parquet' not in formats:
    print("Warning: Expected .parquet files only.")
total_size_bytes = sum(os.path.getsize(f) for f in file_list)
total_size_gb = total_size_bytes / (1024 ** 3)
print(f"Total combined size: {total_size_gb:.2f} GB")
if 10 <= total_size_gb <= 40:
    print("Total data size from files is between 10 GB and 40 GB.")
else:
    print("Total data size from files is NOT between 10 GB and 40 GB.")

# Print file details
print("\nFile details:")
for f in sorted(file_list):
    size_mb = os.path.getsize(f) / (1024 ** 2)
    print(f"  {os.path.basename(f)}: {size_mb:.1f} MB")

# Function to select one file per schema period
def select_files_by_period(file_list):
    schema_files = {
        'Schema 1 (2009-2016)': None,
        'Schema 2 (2016-2020)': None,
        'Schema 3 (2021-2025)': None
    }
    for file in sorted(file_list):
        try:
            year = int(os.path.basename(file).split('_')[-1].split('-')[0])
            if 2009 <= year <= 2016 and not schema_files['Schema 1 (2009-2016)']:
                schema_files['Schema 1 (2009-2016)'] = file
            elif 2016 < year <= 2020 and not schema_files['Schema 2 (2016-2020)']:
                schema_files['Schema 2 (2016-2020)'] = file
            elif 2021 <= year <= 2025 and not schema_files['Schema 3 (2021-2025)']:
                schema_files['Schema 3 (2021-2025)'] = file
        except ValueError:
            continue
    return schema_files

# Select representative files
sample_files = select_files_by_period(file_list)

# Function to read Parquet with retries
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def read_parquet_with_retry(file_path):
    try:
        return spark.read.parquet(file_path)
    except Exception as e:
        if "Transport endpoint is not connected" in str(e):
            print(f"Transport endpoint error for {file_path}. Remounting Drive...")
            mount_drive()
        raise

# Collect schemas for comparison
schema_dict = {}
all_monthly_counts = []

# Process each schema period
for schema_name, file_path in sample_files.items():
    if file_path is None:
        print(f"\n{schema_name}: No file found for this period.")
        continue

    print(f"\n{'='*50}")
    print(f"{schema_name} - File: {os.path.basename(file_path)}")
    print(f"{'='*50}")

    try:
        # Load the Parquet file with retries
        df = read_parquet_with_retry(file_path)

        # Store schema
        schema_dict[schema_name] = [(field.name, field.dataType) for field in df.schema]

        # Print schema
        print("\nSchema (Columns and Datatypes):")
        df.printSchema()

        # Display sample data (first 3 rows)
        print("\nSample Data (First 3 Rows):")
        df.show(3, truncate=False)

        # Determine the correct pickup timestamp column
        columns = [col.lower() for col in df.columns]
        if 'tpep_pickup_datetime' in columns:
            pickup_col = 'tpep_pickup_datetime'
        elif 'pickup_datetime' in columns:
            pickup_col = 'pickup_datetime'
        elif 'trip_pickup_datetime' in columns:
            pickup_col = 'trip_pickup_datetime'
        else:
            print(f"Error: No recognizable pickup timestamp column in {file_path}")
            continue

        # Count trips per month
        df = df.withColumn("pickup_month", month(col(pickup_col)))
        df = df.withColumn("pickup_year", year(col(pickup_col)))
        monthly_counts = df.groupBy("pickup_year", "pickup_month").count().orderBy("pickup_year", "pickup_month")
        print("\nTrip Counts by Year and Month:")
        monthly_counts.show(5, truncate=False)

        # Collect data for visualization
        all_monthly_counts.append((schema_name, monthly_counts.collect()))

    except Exception as e:
        print(f"Error processing '{file_path}': {str(e)}")
        if "Transport endpoint is not connected" in str(e):
            print("Retrying after remounting Drive...")
            try:
                mount_drive()
                df = read_parquet_with_retry(file_path)
                schema_dict[schema_name] = [(field.name, field.dataType) for field in df.schema]
                print("\nSchema (Columns and Datatypes):")
                df.printSchema()
                print("\nSample Data (First 3 Rows):")
                df.show(3, truncate=False)
            except Exception as e2:
                print(f"Retry failed: {str(e2)}")
                continue

# Compare schemas to highlight differences
print("\nSchema Comparison:")
schema_names = list(schema_dict.keys())
if len(schema_names) > 1:
    for i in range(len(schema_names)):
        for j in range(i + 1, len(schema_names)):
            s1_name = schema_names[i]
            s2_name = schema_names[j]
            s1_cols = set(col_name for col_name, _ in schema_dict[s1_name])
            s2_cols = set(col_name for col_name, _ in schema_dict[s2_name])

            print(f"\nComparing {s1_name} vs {s2_name}:")
            print(f"Columns in {s1_name} but not in {s2_name}: {s1_cols - s2_cols}")
            print(f"Columns in {s2_name} but not in {s1_name}: {s2_cols - s1_cols}")
            common_cols = s1_cols & s2_cols
            print(f"Common columns: {common_cols}")
            # Check datatype differences for common columns
            for col_name in common_cols:
                dtype1 = next(dtype for name, dtype in schema_dict[s1_name] if name == col_name)
                dtype2 = next(dtype for name, dtype in schema_dict[s2_name] if name == col_name)
                if dtype1 != dtype2:
                    print(f"  Datatype difference for '{col_name}': {s1_name}={dtype1}, {s2_name}={dtype2}")

# Combine monthly counts for visualization (if any data was collected)
if all_monthly_counts:
    labels = []
    counts = []
    for schema_name, data in all_monthly_counts:
        for row in data:
            label = f"{row['pickup_year']}-{row['pickup_month']:02d}"
            if label not in labels:  # Avoid duplicates
                labels.append(label)
                counts.append(row['count'])

    # Sort by year-month
    sorted_data = sorted(zip(labels, counts), key=lambda x: x[0])
    labels, counts = zip(*sorted_data) if sorted_data else ([], [])

    # Create a Chart.js line plot
    print("\nChart for Trip Counts per Month:")
    print("```chartjs")
    chart_data = {
        "type": "line",
        "data": {
            "labels": list(labels),
            "datasets": [{
                "label": "Number of Taxi Trips",
                "data": list(counts),
                "borderColor": "#1e88e5",
                "backgroundColor": "rgba(30, 136, 229, 0.2)",
                "fill": True,
                "tension": 0.4
            }]
        },
        "options": {
            "scales": {
                "x": {
                    "title": {
                        "display": True,
                        "text": "Year-Month"
                    }
                },
                "y": {
                    "title": {
                        "display": True,
                        "text": "Number of Trips"
                    },
                    "beginAtZero": True
                }
            },
            "plugins": {
                "title": {
                    "display": True,
                    "text": "NYC Taxi Trips per Month (Sampled Files)"
                }
            }
        }
    }
    print(json.dumps(chart_data, indent=2))
    print("```")

# Stop Spark
spark.stop()