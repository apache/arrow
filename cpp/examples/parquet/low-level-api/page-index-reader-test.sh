## member queries
echo "Launching member queries.."
$ARROW_HOME/build/debug/parquet-reader-with-pageindex ~/parquet_data/parquet_cpp_example_10000000_m_sorted.parquet  1000000 &

$ARROW_HOME/build/debug/parquet-reader-with-pageindex ~/parquet_data/parquet_cpp_example_10000000_m_unsorted.parquet 1000000 &

## non-member queries
echo "launching non-member queries.."
$ARROW_HOME/build/debug/parquet-reader-with-pageindex ~/parquet_data/parquet_cpp_example_10000000_n_sorted.parquet  10000000 &

$ARROW_HOME/build/debug/parquet-reader-with-pageindex ~/parquet_data/parquet_cpp_example_10000000_n_unsorted.parquet 10000000 &

#perf record -ag -e faults -p $pid

#iostat -k 1 -p sda > ~/parquet_data/debug_read_writes