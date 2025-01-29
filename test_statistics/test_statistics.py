import pyarrow.parquet as pq

try:
    print("Creating Statistics instance...")
    stats = pq.Statistics()
    print("Statistics instance created successfully!")
    print(f"Has distinct count: {stats.has_distinct_count}")
except Exception as e:
    print(f"Error: {e}")
