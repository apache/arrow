## Benchmark Requirements

The benchmarks are run using [asv][1] which is also their only requirement.

## Running the benchmarks

To run the benchmarks, call `asv run --python=same`. You cannot use the
plain `asv run` command at the moment as asv cannot handle python packages
in subdirectories of a repository.

[1]: https://asv.readthedocs.org/
