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

    def __init__(self, benchmark_bin, benchmark_filter=None, benchmark_extras=None):
        self.bin = benchmark_bin
        self.benchmark_filter = benchmark_filter
        self.benchmark_extras = benchmark_extras or []

    def list_benchmarks(self):
        argv = ["--benchmark_list_tests"]
        if self.benchmark_filter:
            argv.append("--benchmark_filter={}".format(self.benchmark_filter))
        result = self.run(*argv, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
        return str.splitlines(result.stdout.decode("utf-8"))

    def results(self, repetitions=1, repetition_min_time=None):
        with NamedTemporaryFile() as out:
            argv = [f"--benchmark_repetitions={repetitions}",
                    f"--benchmark_out={out.name}",
                    "--benchmark_out_format=json"]

            if repetition_min_time is not None:
                argv.append(f"--benchmark_min_time={repetition_min_time:.6f}s")

            if self.benchmark_filter:
                argv.append(f"--benchmark_filter={self.benchmark_filter}")

            argv += self.benchmark_extras

            self.run(*argv, check=True)
            return json.load(out)


class GoogleBenchmarkObservation:
    """ Represents one run of a single (google c++) benchmark.

    Aggregates are reported by Google Benchmark executables alongside
    other observations whenever repetitions are specified (with
    `--benchmark_repetitions` on the bare benchmark, or with the
    archery option `--repetitions`). Aggregate observations are not
    included in `GoogleBenchmark.runs`.

    RegressionSumKernel/32768/0                 1 us          1 us  25.8077GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.7066GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.1481GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.846GB/s
    RegressionSumKernel/32768/0                 1 us          1 us  25.6453GB/s
    RegressionSumKernel/32768/0_mean            1 us          1 us  25.6307GB/s
    RegressionSumKernel/32768/0_median          1 us          1 us  25.7066GB/s
    RegressionSumKernel/32768/0_stddev          0 us          0 us  288.046MB/s
    """

    def __init__(self, name, real_time, cpu_time, time_unit, run_type,
                 size=None, bytes_per_second=None, items_per_second=None,
                 **counters):
        self._name = name
        self.real_time = real_time
        self.cpu_time = cpu_time
        self.time_unit = time_unit
        self.run_type = run_type
        self.size = size
        self.bytes_per_second = bytes_per_second
        self.items_per_second = items_per_second
        self.counters = counters

    @property
    def is_aggregate(self):
        """ Indicate if the observation is a run or an aggregate. """
        return self.run_type == "aggregate"

    @property
    def is_realtime(self):
        """ Indicate if the preferred value is realtime instead of cputime. """
        return self.name.find("/real_time") != -1

    @property
    def name(self):
        name = self._name
        return name.rsplit("_", maxsplit=1)[0] if self.is_aggregate else name

    @property
    def time(self):
        return self.real_time if self.is_realtime else self.cpu_time

    @property
    def value(self):
        """ Return the benchmark value."""
        return self.bytes_per_second or self.items_per_second or self.time

    @property
    def unit(self):
        if self.bytes_per_second:
            return "bytes_per_second"
        elif self.items_per_second:
            return "items_per_second"
        else:
            return self.time_unit

    def __repr__(self):
        return str(self.value)


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
        _, runs = partition(lambda b: b.is_aggregate, runs)
        self.runs = sorted(runs, key=lambda b: b.value)
        unit = self.runs[0].unit
        time_unit = self.runs[0].time_unit
        less_is_better = not unit.endswith("per_second")
        values = [b.value for b in self.runs]
        times = [b.real_time for b in self.runs]
        # Slight kludge to extract the UserCounters for each benchmark
        counters = self.runs[0].counters
        super().__init__(name, unit, less_is_better, values, time_unit, times,
                         counters)

    def __repr__(self):
        return "GoogleBenchmark[name={},runs={}]".format(self.names, self.runs)

    @classmethod
    def from_json(cls, payload):
        def group_key(x):
            return x.name

        benchmarks = map(lambda x: GoogleBenchmarkObservation(**x), payload)
        groups = groupby(sorted(benchmarks, key=group_key), group_key)
        return [cls(k, list(bs)) for k, bs in groups]
