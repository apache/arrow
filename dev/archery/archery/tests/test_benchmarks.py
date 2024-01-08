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

from archery.benchmark.codec import JsonEncoder
from archery.benchmark.core import Benchmark, median
from archery.benchmark.compare import (
    BenchmarkComparator, RunnerComparator
)
from archery.benchmark.google import (
    GoogleBenchmark, GoogleBenchmarkObservation
)
from archery.benchmark.runner import StaticBenchmarkRunner


def test_benchmark_comparator():
    unit = "micros"

    assert not BenchmarkComparator(
        Benchmark("contender", unit, True, [10], unit, [1]),
        Benchmark("baseline", unit, True, [20], unit, [1]),
    ).regression

    assert BenchmarkComparator(
        Benchmark("contender", unit, False, [10], unit, [1]),
        Benchmark("baseline", unit, False, [20], unit, [1]),
    ).regression

    assert BenchmarkComparator(
        Benchmark("contender", unit, True, [20], unit, [1]),
        Benchmark("baseline", unit, True, [10], unit, [1]),
    ).regression

    assert not BenchmarkComparator(
        Benchmark("contender", unit, False, [20], unit, [1]),
        Benchmark("baseline", unit, False, [10], unit, [1]),
    ).regression


def test_static_runner_from_json_not_a_regression():
    archery_result = {
        "suites": [
            {
                "name": "arrow-value-parsing-benchmark",
                "benchmarks": [
                    {
                        "name": "FloatParsing<DoubleType>",
                        "unit": "items_per_second",
                        "less_is_better": False,
                        "values": [
                            109941112.87296811
                        ],
                        "time_unit": "ns",
                        "times": [
                            9095.800104330105
                        ]
                    },
                ]
            }
        ]
    }

    contender = StaticBenchmarkRunner.from_json(json.dumps(archery_result))
    baseline = StaticBenchmarkRunner.from_json(json.dumps(archery_result))
    [comparison] = RunnerComparator(contender, baseline).comparisons
    assert not comparison.regression


def test_static_runner_from_json_multiple_values_not_a_regression():
    # Same as above, but with multiple repetitions
    archery_result = {
        "suites": [
            {
                "name": "arrow-value-parsing-benchmark",
                "benchmarks": [
                    {
                        "name": "FloatParsing<DoubleType>",
                        "unit": "items_per_second",
                        "less_is_better": False,
                        "values": [
                            93588476.22327498,
                            94873831.3818328,
                            95593675.20810866,
                            95797325.6543961,
                            96134728.05794072
                        ],
                        "time_unit": "ns",
                        "times": [
                            10537.724568456104,
                            10575.162068480413,
                            10599.271208720838,
                            10679.028059166194,
                            10827.995119861762
                        ],
                        "counters": {
                            "family_index": 0,
                            "per_family_instance_index": 0,
                            "run_name": "FloatParsing<DoubleType>",
                            "repetitions": 5,
                            "repetition_index": 0,
                            "threads": 1,
                            "iterations": 10656
                        }
                    }
                ]
            }
        ]
    }

    contender = StaticBenchmarkRunner.from_json(json.dumps(archery_result))
    baseline = StaticBenchmarkRunner.from_json(json.dumps(archery_result))
    [comparison] = RunnerComparator(contender, baseline).comparisons
    assert not comparison.regression


def test_static_runner_from_json_regression():
    archery_result = {
        "suites": [
            {
                "name": "arrow-value-parsing-benchmark",
                "benchmarks": [
                    {
                        "name": "FloatParsing<DoubleType>",
                        "unit": "items_per_second",
                        "less_is_better": False,
                        "values": [
                            109941112.87296811
                        ],
                        "time_unit": "ns",
                        "times": [
                            9095.800104330105
                        ]
                    },
                ]
            }
        ]
    }

    contender = StaticBenchmarkRunner.from_json(json.dumps(archery_result))

    # introduce artificial regression
    archery_result['suites'][0]['benchmarks'][0]['values'][0] *= 2
    baseline = StaticBenchmarkRunner.from_json(json.dumps(archery_result))

    [comparison] = RunnerComparator(contender, baseline).comparisons
    assert comparison.regression


def test_static_runner_from_json_multiple_values_regression():
    # Same as above, but with multiple repetitions
    archery_result = {
        "suites": [
            {
                "name": "arrow-value-parsing-benchmark",
                "benchmarks": [
                    {
                        "name": "FloatParsing<DoubleType>",
                        "unit": "items_per_second",
                        "less_is_better": False,
                        "values": [
                            93588476.22327498,
                            94873831.3818328,
                            95593675.20810866,
                            95797325.6543961,
                            96134728.05794072
                        ],
                        "time_unit": "ns",
                        "times": [
                            10537.724568456104,
                            10575.162068480413,
                            10599.271208720838,
                            10679.028059166194,
                            10827.995119861762
                        ],
                        "counters": {
                            "family_index": 0,
                            "per_family_instance_index": 0,
                            "run_name": "FloatParsing<DoubleType>",
                            "repetitions": 5,
                            "repetition_index": 0,
                            "threads": 1,
                            "iterations": 10656
                        }
                    }
                ]
            }
        ]
    }

    contender = StaticBenchmarkRunner.from_json(json.dumps(archery_result))

    # introduce artificial regression
    values = archery_result['suites'][0]['benchmarks'][0]['values']
    values[:] = [v * 2 for v in values]
    baseline = StaticBenchmarkRunner.from_json(json.dumps(archery_result))

    [comparison] = RunnerComparator(contender, baseline).comparisons
    assert comparison.regression


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


def assert_benchmark(name, google_result, archery_result):
    observation = GoogleBenchmarkObservation(**google_result)
    benchmark = GoogleBenchmark(name, [observation])
    result = json.dumps(benchmark, cls=JsonEncoder)
    assert json.loads(result) == archery_result


def test_items_per_second():
    name = "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0"
    google_result = {
        "cpu_time": 116292.58886653671,
        "items_per_second": 281772039.9844759,
        "iterations": 5964,
        "name": name,
        "null_percent": 0.0,
        "real_time": 119811.77313729875,
        "repetition_index": 0,
        "repetitions": 0,
        "run_name": name,
        "run_type": "iteration",
        "size": 32768.0,
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "counters": {"iterations": 5964,
                     "null_percent": 0.0,
                     "repetition_index": 0,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "items_per_second",
        "less_is_better": False,
        "values": [281772039.9844759],
        "time_unit": "ns",
        "times": [119811.77313729875],
    }
    assert "items_per_second" in google_result
    assert "bytes_per_second" not in google_result
    assert_benchmark(name, google_result, archery_result)


def test_bytes_per_second():
    name = "BufferOutputStreamLargeWrites/real_time"
    google_result = {
        "bytes_per_second": 1890209037.3405428,
        "cpu_time": 17018127.659574457,
        "iterations": 47,
        "name": name,
        "real_time": 17458386.53190963,
        "repetition_index": 1,
        "repetitions": 0,
        "run_name": name,
        "run_type": "iteration",
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "counters": {"iterations": 47,
                     "repetition_index": 1,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "bytes_per_second",
        "less_is_better": False,
        "values": [1890209037.3405428],
        "time_unit": "ns",
        "times": [17458386.53190963],
    }
    assert "items_per_second" not in google_result
    assert "bytes_per_second" in google_result
    assert_benchmark(name, google_result, archery_result)


def test_both_items_and_bytes_per_second():
    name = "ArrayArrayKernel<AddChecked, UInt8Type>/32768/0"
    google_result = {
        "bytes_per_second": 281772039.9844759,
        "cpu_time": 116292.58886653671,
        "items_per_second": 281772039.9844759,
        "iterations": 5964,
        "name": name,
        "null_percent": 0.0,
        "real_time": 119811.77313729875,
        "repetition_index": 0,
        "repetitions": 0,
        "run_name": name,
        "run_type": "iteration",
        "size": 32768.0,
        "threads": 1,
        "time_unit": "ns",
    }
    # Note that bytes_per_second trumps items_per_second
    archery_result = {
        "counters": {"iterations": 5964,
                     "null_percent": 0.0,
                     "repetition_index": 0,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "bytes_per_second",
        "less_is_better": False,
        "values": [281772039.9844759],
        "time_unit": "ns",
        "times": [119811.77313729875],
    }
    assert "items_per_second" in google_result
    assert "bytes_per_second" in google_result
    assert_benchmark(name, google_result, archery_result)


def test_neither_items_nor_bytes_per_second():
    name = "AllocateDeallocate<Jemalloc>/size:1048576/real_time"
    google_result = {
        "cpu_time": 1778.6004847419827,
        "iterations": 352765,
        "name": name,
        "real_time": 1835.3137357788837,
        "repetition_index": 0,
        "repetitions": 0,
        "run_name": name,
        "run_type": "iteration",
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "counters": {"iterations": 352765,
                     "repetition_index": 0,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "ns",
        "less_is_better": True,
        "values": [1835.3137357788837],
        "time_unit": "ns",
        "times": [1835.3137357788837],
    }
    assert "items_per_second" not in google_result
    assert "bytes_per_second" not in google_result
    assert_benchmark(name, google_result, archery_result)


def test_prefer_real_time():
    name = "AllocateDeallocate<Jemalloc>/size:1048576/real_time"
    google_result = {
        "cpu_time": 1778.6004847419827,
        "iterations": 352765,
        "name": name,
        "real_time": 1835.3137357788837,
        "repetition_index": 0,
        "repetitions": 0,
        "run_name": name,
        "run_type": "iteration",
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "counters": {"iterations": 352765,
                     "repetition_index": 0,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "ns",
        "less_is_better": True,
        "values": [1835.3137357788837],
        "time_unit": "ns",
        "times": [1835.3137357788837],
    }
    assert name.endswith("/real_time")
    assert_benchmark(name, google_result, archery_result)


def test_prefer_cpu_time():
    name = "AllocateDeallocate<Jemalloc>/size:1048576"
    google_result = {
        "cpu_time": 1778.6004847419827,
        "iterations": 352765,
        "name": name,
        "real_time": 1835.3137357788837,
        "repetition_index": 0,
        "repetitions": 0,
        "run_name": name,
        "run_type": "iteration",
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "counters": {"iterations": 352765,
                     "repetition_index": 0,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "ns",
        "less_is_better": True,
        "values": [1778.6004847419827],
        "time_unit": "ns",
        "times": [1835.3137357788837],
    }
    assert not name.endswith("/real_time")
    assert_benchmark(name, google_result, archery_result)


def test_omits_aggregates():
    name = "AllocateDeallocate<Jemalloc>/size:1048576/real_time"
    google_aggregate = {
        "aggregate_name": "mean",
        "cpu_time": 1757.428694267678,
        "iterations": 3,
        "name": "AllocateDeallocate<Jemalloc>/size:1048576/real_time_mean",
        "real_time": 1849.3869337041162,
        "repetitions": 0,
        "run_name": name,
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
        "run_name": name,
        "run_type": "iteration",
        "threads": 1,
        "time_unit": "ns",
    }
    archery_result = {
        "counters": {"iterations": 352765,
                     "repetition_index": 0,
                     "repetitions": 0,
                     "run_name": name,
                     "threads": 1},
        "name": name,
        "unit": "ns",
        "less_is_better": True,
        "values": [1835.3137357788837],
        "time_unit": "ns",
        "times": [1835.3137357788837],
    }
    assert google_aggregate["run_type"] == "aggregate"
    assert google_result["run_type"] == "iteration"
    observation1 = GoogleBenchmarkObservation(**google_aggregate)
    observation2 = GoogleBenchmarkObservation(**google_result)
    benchmark = GoogleBenchmark(name, [observation1, observation2])
    result = json.dumps(benchmark, cls=JsonEncoder)
    assert json.loads(result) == archery_result


def test_multiple_observations():
    name = "FloatParsing<DoubleType>"
    google_results = [
        {
            'cpu_time': 10627.38199641615,
            'family_index': 0,
            'items_per_second': 94096551.75067839,
            'iterations': 9487,
            'name': 'FloatParsing<DoubleType>',
            'per_family_instance_index': 0,
            'real_time': 10628.84905663701,
            'repetition_index': 0,
            'repetitions': 3,
            'run_name': 'FloatParsing<DoubleType>',
            'run_type': 'iteration',
            'threads': 1,
            'time_unit': 'ns'
        },
        {
            'cpu_time': 10633.318014124594,
            'family_index': 0,
            'items_per_second': 94044022.63448404,
            'iterations': 9487,
            'name': 'FloatParsing<DoubleType>',
            'per_family_instance_index': 0,
            'real_time': 10634.858754122948,
            'repetition_index': 1,
            'repetitions': 3,
            'run_name': 'FloatParsing<DoubleType>',
            'run_type': 'iteration',
            'threads': 1,
            'time_unit': 'ns'
        },
        {
            'cpu_time': 10664.315484347,
            'family_index': 0,
            'items_per_second': 93770669.24434038,
            'iterations': 9487,
            'name': 'FloatParsing<DoubleType>',
            'per_family_instance_index': 0,
            'real_time': 10665.584589337563,
            'repetition_index': 2,
            'repetitions': 3,
            'run_name': 'FloatParsing<DoubleType>',
            'run_type': 'iteration',
            'threads': 1,
            'time_unit': 'ns'
        }
    ]

    archery_result = {
        'counters': {
            'family_index': 0,
            'iterations': 9487,
            'per_family_instance_index': 0,
            'repetition_index': 2,
            'repetitions': 3,
            'run_name': 'FloatParsing<DoubleType>',
            'threads': 1
        },
        'less_is_better': False,
        'name': 'FloatParsing<DoubleType>',
        'time_unit': 'ns',
        'times': [10628.84905663701, 10634.858754122948, 10665.584589337563],
        'unit': 'items_per_second',
        'values': [93770669.24434038, 94044022.63448404, 94096551.75067839]
    }

    observations = [GoogleBenchmarkObservation(**g) for g in google_results]
    benchmark = GoogleBenchmark(name, observations)
    result = json.dumps(benchmark, cls=JsonEncoder)
    assert json.loads(result) == archery_result
