import pyarrow.dataset as ds
from time import perf_counter

dataset = ds.dataset("/home/raulcd/code/pyarrow-issue-isin/BIG_DATASET")
population = [i for i in range(1, 10_000_001)]
filter_expr = ds.field("unit_id").isin(population)

start = perf_counter()
table = dataset.to_table(filter=filter_expr)
end = perf_counter()
print("=== PYARROW VERSION 20 ===")
print(f"Retrieved {table.num_rows:,} rows in {end - start:.2f} seconds.\n")
