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

from .core import Benchmark
from ..utils.command import Command
from ..utils.git import git


def partition(pred, iterable):
    # adapted from python's examples
    t1, t2 = tee(iterable)
    return list(filter(pred, t1)), list(filterfalse(pred, t2))


class GoogleBenchmarkCommand(Command):
    def __init__(self, benchmark_bin, benchmark_filter=None):
        self.bin = benchmark_bin
        self.benchmark_filter = benchmark_filter

    def list_benchmarks(self):
        argv = ["--benchmark_list_tests"]
        if self.benchmark_filter:
            argv.append(f"--benchmark_filter={self.benchmark_filter}")
        result = self.run(*argv, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
        return [b for b in result.stdout]

    def results(self):
        argv = ["--benchmark_format=json", "--benchmark_repetitions=20"]

        if self.benchmark_filter:
            argv.append(f"--benchmark_filter={self.benchmark_filter}")

        return json.loads(self.run(*argv, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE).stdout)


class GoogleContext:
    """ Represents the runtime environment """

    def __init__(self, date=None, executable=None, **kwargs):
        self.date = date
        self.executable = executable

    @property
    def host(self):
        host = {
            "hostname": gethostname(),
            # Not sure if we should leak this.
            "mac_address": getnode(),
        }
        return host

    @property
    def git(self):
        head = git.head()
        # %ai: author date, ISO 8601-like format
        timestamp = git.log("-1", "--pretty='%ai'", head)
        branch = git.current_branch(),
        git_info = {
            "git_commit_timestamp": timestamp,
            "git_hash": head,
            "git_branch": branch,
        }
        return git_info

    @property
    def toolchain(self):
        # TODO parse local CMake generated info to extract compile flags and
        # arrow features
        deps = {}
        toolchain = {
            "language_implementation_version": "c++11",
            "dependencies": deps,
        }
        return toolchain

    def as_arrow(self):
        ctx = {
            "benchmark_language": "C++",
            "run_timestamp": self.date,
        }

        for extra in (self.host, self.git, self.toolchain):
            ctx.update(extra)

        return ctx

    @classmethod
    def from_json(cls, payload):
        return cls(**payload)


class BenchmarkObservation:
    """ Represents one run of a single benchmark. """

    def __init__(self, **kwargs):
        self._name = kwargs.get("name")
        self.real_time = kwargs.get("real_time")
        self.cpu_time = kwargs.get("cpu_time")
        self.time_unit = kwargs.get("time_unit")
        self.size = kwargs.get("size")
        self.bytes_per_second = kwargs.get("bytes_per_second")

    @property
    def is_agg(self):
        suffixes = ["_mean", "_median", "_stddev"]
        return any(map(lambda x: self._name.endswith(x), suffixes))

    @property
    def is_realtime(self):
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
    def __init__(self, name, runs):
        self.name = name
        # exclude google benchmark aggregate artifacts
        _, runs = partition(lambda b: b.is_agg, runs)
        self.runs = sorted(runs, key=lambda b: b.value)
        super().__init__(name, [b.value for b in self.runs])

    @property
    def parameters(self):
        """ Extract parameters from Benchmark's name"""
        def parse_param(idx, param):
            k_v = param.split(":")
            # nameless parameter are transformed into positional names
            name = k_v[0] if len(k_v) > 1 else f"arg{idx}"
            return name, k_v[-1]

        params = enumerate(self.name.split("/")[1:])
        named_params = [parse_param(idx, p) for idx, p in params if p]
        return {k: v for k, v in named_params}

    @property
    def unit(self):
        return self.runs[0].unit

    @property
    def less_is_better(self):
        return self.runs[0].size is None

    def __repr__(self):
        return f"GoogleBenchmark[name={self.name},runs={self.runs}]"

    @classmethod
    def from_json(cls, payload):
        def group_key(x):
            return x.name

        benchmarks = map(lambda x: BenchmarkObservation(**x), payload)
        groups = groupby(sorted(benchmarks, key=group_key), group_key)
        return [cls(k, list(bs)) for k, bs in groups]
