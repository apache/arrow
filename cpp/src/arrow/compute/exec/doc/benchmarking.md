# Benchmarking

Please see the [C++ Developer Documentation](https://github.com/apache/arrow/blob/master/docs/source/developers/cpp/building.rst) for how to build benchmarks.

This document will detail the supporting scripts under `exec/benchmark_scripts` included to support a few of Arrow's benchmarks.

## table_generation

To support our table join benchmarks like asofjoin and hashjoin, we include a utility to generate `.feather` tables of varying properties.

These files require the following dependencies:
```
pyarrow
pandas
numpy
```
and appending a path to `benchmark_scripts/table_generation` in your `$PYTHONPATH` environmental variable.

`generate_benchmark_files.py` can be called with a target directory to generate a basic suite of feather files that contain tables that vary in width, key density, and time frequency.

`batch_process.py` can be called directly with command-line arguments to generate specific tables with specific properties.

`datagen.py` specifies the tables that can be generated and their properties.

