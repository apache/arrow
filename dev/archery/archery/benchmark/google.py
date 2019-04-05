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

from functools import cmp_to_key, lru_cache
from itertools import filterfalse, groupby, tee
import json
import six
from socket import gethostname
import subprocess
import sys
from uuid import getnode

from ..utils.command import Command


def memoize(fn):
    return lru_cache(maxsize=1)(fn)


def partition(pred, iterable):
    # adapted from python's examples
    t1, t2 = tee(iterable)
    return list(filter(pred, t1)), list(filterfalse(pred, t2))


# Taken from merge_arrow_pr.py
def run_cmd(cmd):
    if isinstance(cmd, six.string_types):
        cmd = cmd.split(' ')

    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        print('Command failed: %s' % cmd)
        print('With output:')
        print('--------------')
        print(e.output)
        print('--------------')
        raise e

    if isinstance(output, six.binary_type):
        output = output.decode('utf-8')
    return output.rstrip()


class GoogleBenchmarkCommand(Command):
    def __init__(self, benchmark_bin):
        self.bin = benchmark_bin

    def list_benchmarks(self, benchmark_filter=None):
        argv = ["--benchmark_list_tests"]
        if benchmark_filter:
            argv.append(f"--benchmark_filter={benchmark_filter}")
        result = self.run(*argv, stdout=subprocess.PIPE)
        return [b for b in result.stdout]


class Context:
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
        head = run_cmd("git rev-parse HEAD")
        # %ai: author date, ISO 8601-like format
        fmt = "%ai"
        timestamp = run_cmd(f"git log -1 --pretty='{fmt}' {head}")
        branch = run_cmd("git rev-parse --abbrev-ref HEAD")
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
    def from_json(cls, version, payload):
        return cls(**payload)


class GoogleBenchmarkObservation:
    """ Represents one run of a single benchmark. """

    def __init__(self, version, **kwargs):
        self._name = kwargs.get("name")
        self.version = version
        self.real_time = kwargs.get("real_time")
        self.cpu_time = kwargs.get("cpu_time")
        self.time_unit = kwargs.get("time_unit")
        self.size = kwargs.get("size")
        self.bytes_per_second = kwargs.get("bytes_per_second")

    @property
    def is_mean(self):
        return self._name.endswith("_mean")

    @property
    def is_median(self):
        return self._name.endswith("_median")

    @property
    def is_stddev(self):
        return self._name.endswith("_stddev")

    @property
    def is_agg(self):
        return self.is_mean or self.is_median or self.is_stddev

    @property
    def is_realtime(self):
        return self.name.find("/realtime") != -1

    @property
    @memoize
    def name(self):
        name = self._name
        return name.rsplit("_", maxsplit=1)[0] if self.is_agg else name

    @property
    def value(self):
        """ Return the benchmark value."""
        if self.size:
            return self.bytes_per_second
        return self.real_time if self.is_realtime else self.cpu_time

    @property
    def unit(self):
        if self.size:
            return "bytes_per_second"
        return self.time_unit

    def __str__(self):
        return f"BenchmarkObservation[name={self.name}]"


class BenchmarkSuite:
    def __init__(self, name, version, runs):
        self.name = name
        self.version = version
        # exclude google benchmark aggregate artifacts
        aggs, runs = partition(lambda b: b.is_agg, runs)
        self.runs = sorted(runs, key=lambda b: b.value)
        self.values = [b.value for b in self.runs]
        self.aggregates = aggs

    @property
    def n_runs(self):
        return len(self.runs)

    @property
    @memoize
    def mean(self):
        maybe_mean = [b for b in self.aggregates if b.is_mean]
        if maybe_mean:
            return maybe_mean[0].value
        # fallback
        return sum(self.values) / self.n_runs

    @property
    @memoize
    def median(self):
        maybe_median = [b for b in self.aggregates if b.is_median]
        if maybe_median:
            return maybe_median[0].value
        # fallback
        return self.runs[int(self.n_runs / 2)].value

    @property
    @memoize
    def stddev(self):
        maybe_stddev = [b for b in self.aggregates if b.is_stddev]
        if maybe_stddev:
            return maybe_stddev[0].value

        sum_diff = sum([(val - self.mean)**2 for val in self.values])
        return (sum_diff / (self.n_runs - 1))**0.5 if self.n_runs > 1 else 0.0

    @property
    def min(self):
        return self.values[0]

    @property
    def max(self):
        return self.values[-1]

    def quartile(self, q):
        return self.values[int(q * self.n_runs / 4)]

    @property
    def q1(self):
        return self.quartile(1)

    @property
    def q3(self):
        return self.quartile(3)

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

    def __str__(self):
        return f"BenchmarkSuite[name={self.name},runs={len(self.runs)}]"

    @classmethod
    def from_json(cls, version, payload):
        def group_key(x):
            return x.name

        benchmarks = map(lambda x: BenchmarkObservation(version, **x), payload)
        groups = groupby(sorted(benchmarks, key=group_key), group_key)
        return [cls(k, version, list(bs)) for k, bs in groups]
