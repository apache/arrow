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
from ..utils.maven import Maven


def partition(pred, iterable):
    # adapted from python's examples
    t1, t2 = tee(iterable)
    return list(filter(pred, t1)), list(filterfalse(pred, t2))


class JavaMicrobenchmarkHarnessCommand(Maven):
    """ Run a Java Micro Benchmark Harness

    This assumes the binary supports the standard command line options,
    notably `--benchmark_filter`, `--benchmark_format`, etc...
    """

    def __init__(self, build, benchmark_filter=None):
        #self.performance_dir = build.binaries_dir + "/performance"
        self.benchmark_filter = benchmark_filter
        self.build = build
        self.maven = Maven()

    def list_benchmarks(self):
        argv = []
        if self.benchmark_filter:
            argv.append("-Dbenchmark.filter={}".format(self.benchmark_filter))
        #result = self.maven.run(*argv, cwd=self.performance_dir,
        #                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = self.build.list(*argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        """ Extract benchmark names from output. Assume the following output
          ...
          Benchmarks:
          org.apache.arrow.vector.IntBenchmarks.setIntDirectly
          ...
          org.apache.arrow.vector.IntBenchmarks.setWithValueHolder
          org.apache.arrow.vector.IntBenchmarks.setWithWriter
          ...
          [INFO]
        """

        lists = []
        benchmarks = False
        for line in str.splitlines(result.stdout.decode("utf-8")):
            if not benchmarks:
                if line.startswith("Benchmarks:"):
                    benchmarks = True
            else:
                if line.startswith("org.apache.arrow"):
                    lists.append(line)
                if line.startswith("[INFO]"):
                    benchmarks = False
                    break
        return lists

    def results(self, repetitions=1):
        with NamedTemporaryFile(suffix=".json") as out:
            argv = ["-Dbenchmark.runs={}".format(repetitions),
                    "-Dbenchmark.resultfile={}".format(out.name),
                    "-Dbenchmark.resultformat=json"]
            if self.benchmark_filter:
                argv.append(
                    "-Dbenchmark.filter={}".format(self.benchmark_filter)
                )

            #self.maven.run(*argv, cwd=self.performance_dir, check=True)
            self.build.benchmark(*argv, check=True)
            return json.load(out)


class JavaMicrobenchmarkHarnessObservation:
    """ Represents one run of a single Java Microbenchmark Harness
    """

    def __init__(self, benchmark, primaryMetric,
                 forks, warmupIterations, measurementIterations, **counters):
        self.name = benchmark
        self.primaryMetric = primaryMetric
        self.score = primaryMetric["score"]
        self.scoreUnit =  primaryMetric["scoreUnit"]
        self.forks = forks
        self.warmups = warmupIterations
        self.runs = measurementIterations
        self.counters = {
            "mode" : counters["mode"],
            "threads" : counters["threads"],
            "warmups" : warmupIterations,
            "warmupTime" : counters["warmupTime"],
            "measurements" : measurementIterations,
            "measurementTime" : counters["measurementTime"],
            "jvmArgs" : counters["jvmArgs"]
        }

    @property
    def value(self):
        """ Return the benchmark value."""
        return self.score

    @property
    def unit(self):
        return self.scoreUnit

    def __repr__(self):
        return str(self.value)


class JavaMicrobenchmarkHarness(Benchmark):
    """ A set of JavaMicrobenchmarkHarnessObservations. """

    def __init__(self, name, runs):
        """ Initialize a JavaMicrobenchmarkHarness.

        Parameters
        ----------
        name: str
              Name of the benchmark
        forks: int
        warmups: int
        runs: int
        runs: list(JavaMicrobenchmarkHarnessObservation)
              Repetitions of JavaMicrobenchmarkHarnessObservation run.

        """
        self.name = name
        self.runs = sorted(runs, key=lambda b: b.value)
        unit = self.runs[0].unit
        less_is_better = unit.endswith("/op")
        values = [b.value for b in self.runs]
        # Slight kludge to extract the UserCounters for each benchmark
        self.counters = self.runs[0].counters
        super().__init__(name, unit, less_is_better, values)

    def __repr__(self):
        return "JavaMicrobenchmarkHarness[name={},runs={}]".format(self.name, self.runs)

    @classmethod
    def from_json(cls, payload):
        def group_key(x):
            return x.name

        benchmarks = map(lambda x: JavaMicrobenchmarkHarnessObservation(**x), payload)
        groups = groupby(sorted(benchmarks, key=group_key), group_key)
        return [cls(k, list(bs)) for k, bs in groups]
