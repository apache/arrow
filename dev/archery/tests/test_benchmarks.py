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

import json

from archery.benchmark.core import Benchmark, median
from archery.benchmark.compare import BenchmarkComparator
from archery.benchmark.google import (
    GoogleBenchmark, GoogleBenchmarkObservation
)
from archery.utils.codec import JsonEncoder


def test_benchmark_comparator():
    unit = "micros"

    assert not BenchmarkComparator(
        Benchmark("contender", unit, True, [10]),
        Benchmark("baseline", unit, True, [20])).regression

    assert BenchmarkComparator(
        Benchmark("contender", unit, False, [10]),
        Benchmark("baseline", unit, False, [20])).regression

    assert BenchmarkComparator(
        Benchmark("contender", unit, True, [20]),
        Benchmark("baseline", unit, True, [10])).regression

    assert not BenchmarkComparator(
        Benchmark("contender", unit, False, [20]),
        Benchmark("baseline", unit, False, [10])).regression


def test_benchmark_median():
    assert median([10]) == 10
    assert median([1, 2, 3]) == 2
    assert median([1, 2]) == 1.5
    assert median([1, 2, 3, 4]) == 2.5
    assert median([1, 1, 1, 1]) == 1
    try:
        median([])
        assert False
    except ValueError:
        pass


def test_omits_aggregates():
    name = "AllocateDeallocate<Jemalloc>/size:1048576/real_time"
    google_aggregate = {
        "aggregate_name": "mean",
        "cpu_time": 1757.428694267678,
        "iterations": 3,
        "name": "AllocateDeallocate<Jemalloc>/size:1048576/real_time_mean",
        "real_time": 1849.3869337041162,
        "repetitions": 0,
        "run_name": "AllocateDeallocate<Jemalloc>/size:1048576/real_time",
        "run_type": "aggregate",
        "threads": 1,
        "time_unit": "ns",
    }
    google_result = {
        "cpu_time": 1778.6004847419827,
        "iterations": 352765,
        "name": name,
        "real_time": 1835.3137357788837,
        "repetition_index": 0,
        "repetitions": 0,
        "run_name": "AllocateDeallocate<Jemalloc>/size:1048576/real_time",
        "run_type": "iteration",
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "name": name,
        "unit": "ns",
        "less_is_better": True,
        "values": [1778.6004847419827],
    }
    assert google_aggregate["run_type"] == "aggregate"
    assert google_result["run_type"] == "iteration"
    observation1 = GoogleBenchmarkObservation(**google_aggregate)
    observation2 = GoogleBenchmarkObservation(**google_result)
    benchmark = GoogleBenchmark(name, [observation1, observation2])
    result = json.dumps(benchmark, cls=JsonEncoder)
    assert json.loads(result) == archery_result
