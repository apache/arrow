# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from itertools import filterfalse, groupby, tee
import json
import subprocess
from tempfile import NamedTemporaryFile

from .core import Benchmark
from ..utils.command import Command


def partition(pred, iterable):
    # adapted from python's examples
    t1, t2 = tee(iterable)
    return list(filter(pred, t1)), list(filterfalse(pred, t2))


class GoogleBenchmarkCommand(Command):
    """ Run a google benchmark binary.

    This assumes the binary supports the standard command line options,
    notably `--benchmark_filter`, `--benchmark_format`, etc...
    """

    def __init__(self, benchmark_bin, benchmark_filter=None):
        self.bin = benchmark_bin
        self.benchmark_filter = benchmark_filter

    def list_benchmarks(self):
        argv = ["--benchmark_list_tests"]
        if self.benchmark_filter:
            argv.append(f"--benchmark_filter={self.benchmark_filter}")
        result = self.run(*argv, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
        return str.splitlines(result.stdout.decode("utf-8"))

    def results(self):
        with NamedTemporaryFile() as out:
            argv = ["--benchmark_repetitions=20",
                    f"--benchmark_out={out.name}",
                    "--benchmark_out_format=json"]

            if self.benchmark_filter:
                argv.append(f"--benchmark_filter={self.benchmark_filter}")

            self.run(*argv, check=True)
            return json.load(out)


class GoogleBenchmarkObservation:
    """ Represents one run of a single (google c++) benchmark.

    Observations are found when running with `--benchmark_repetitions`. Sadly,
    the format mixes values and aggregates, e.g.

    RegressionSumKernel/32768/0                 1 us          1 us  25.8077GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.7066GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.1481GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.846GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.6453GB/s
    RegressionSumKernel/32768/0_mean            1 us          1 us  25.6307GB/s
    RegressionSumKernel/32768/0_median          1 us          1 us  25.7066GB/s
    RegressionSumKernel/32768/0_stddev          0 us          0 us  288.046MB/s

    As from benchmark v1.4.1 (2019-04-24), the only way to differentiate an
    actual run from the aggregates, is to match on the benchmark name. The
    aggregates will be appended with `_$agg_name`.

    This class encapsulate the logic to separate runs from aggregate . This is
    hopefully avoided in benchmark's master version with a separate json
    attribute.
    """

    def __init__(self, name, real_time, cpu_time, time_unit, size=None,
                 bytes_per_second=None, **kwargs):
        self._name = name
        self.real_time = real_time
        self.cpu_time = cpu_time
        self.time_unit = time_unit
        self.size = size
        self.bytes_per_second = bytes_per_second

    @property
    def is_agg(self):
        """ Indicate if the observation is a run or an aggregate. """
        suffixes = ["_mean", "_median", "_stddev"]
        return any(map(lambda x: self._name.endswith(x), suffixes))

    @property
    def is_realtime(self):
        """ Indicate if the preferred value is realtime instead of cputime. """
        return self.name.find("/realtime") != -1

    @property
    def name(self):
        name = self._name
        return name.rsplit("_", maxsplit=1)[0] if self.is_agg else name

    @property
    def time(self):
        return self.real_time if self.is_realtime else self.cpu_time

    @property
    def value(self):
        """ Return the benchmark value."""
        return self.bytes_per_second if self.size else self.time

    @property
    def unit(self):
        return "bytes_per_second" if self.size else self.time_unit

    def __repr__(self):
        return f"{self.value}"


class GoogleBenchmark(Benchmark):
    """ A set of GoogleBenchmarkObservations. """

    def __init__(self, name, runs):
        """ Initialize a GoogleBenchmark.

        Parameters
        ----------
        name: str
              Name of the benchmark
        runs: list(GoogleBenchmarkObservation)
              Repetitions of GoogleBenchmarkObservation run.

        """
        self.name = name
        # exclude google benchmark aggregate artifacts
        _, runs = partition(lambda b: b.is_agg, runs)
        self.runs = sorted(runs, key=lambda b: b.value)
        unit = self.runs[0].unit
        # If `size` is found in the json dict, then the benchmark is reported
        # in bytes per second
        less_is_better = self.runs[0].size is None
        values = [b.value for b in self.runs]
        super().__init__(name, unit, less_is_better, values)

    def __repr__(self):
        return f"GoogleBenchmark[name={self.name},runs={self.runs}]"

    @classmethod
    def from_json(cls, payload):
        def group_key(x):
            return x.name

        benchmarks = map(lambda x: GoogleBenchmarkObservation(**x), payload)
        groups = groupby(sorted(benchmarks, key=group_key), group_key)
        return [cls(k, list(bs)) for k, bs in groups]
