# Download NYC Taxi Yellow Trip Data 2023–2025
import os
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from datetime import datetime

# Folder to save data
data_dir = "../data/nyc_taxi"
os.makedirs(data_dir, exist_ok=True)

# Years and months
years = list(range(2009, 2022))

# Maximum number of parallel downloads
MAX_WORKERS = 4  # Increase if internet is fast

# Current month for 2025
current_month = datetime.now().month

# Prepare all download tasks
tasks = []
for year in years:
    for month in range(1, 13):
        if year == 2025 and month > current_month:
            break
        month_str = f"{month:02d}"
        filename = f"yellow_tripdata_{year}-{month_str}.parquet"
        # Official TLC page does not provide direct URLs; use AWS/OpenData registry
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        file_path = os.path.join(data_dir, filename)
        if not os.path.exists(file_path):
            tasks.append((url, file_path))

def download_file(url_file):
    url, file_path = url_file
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        print(f"Failed to download {url} (status code {response.status_code})")
        return None
    total_size = int(response.headers.get('content-length', 0))
    with open(file_path, 'wb') as f, tqdm(
        desc=os.path.basename(file_path),
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=1024):
            f.write(data)
            bar.update(len(data))
    return file_path

# Run downloads in parallel
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_file, task) for task in tasks]
    for future in as_completed(futures):
        result = future.result()
        if result:
            print(f"Downloaded: {result}")