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

from ..benchmark.core import Benchmark, BenchmarkSuite
from ..benchmark.runner import BenchmarkRunner, StaticBenchmarkRunner
from ..benchmark.compare import BenchmarkComparator


class JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Benchmark):
            return BenchmarkCodec.encode(o)

        if isinstance(o, BenchmarkSuite):
            return BenchmarkSuiteCodec.encode(o)

        if isinstance(o, BenchmarkRunner):
            return BenchmarkRunnerCodec.encode(o)

        if isinstance(o, BenchmarkComparator):
            return BenchmarkComparatorCodec.encode(o)

        return json.JSONEncoder.default(self, o)


class BenchmarkCodec:
    @staticmethod
    def encode(b):
        return {
            "name": b.name,
            "unit": b.unit,
            "less_is_better": b.less_is_better,
            "values": b.values.tolist(),
        }

    @staticmethod
    def decode(dct, **kwargs):
        return Benchmark(**dct, **kwargs)


class BenchmarkSuiteCodec:
    @staticmethod
    def encode(bs):
        return {
            "name": bs.name,
            "benchmarks": [BenchmarkCodec.encode(b) for b in bs.benchmarks]
        }

    @staticmethod
    def decode(dct, **kwargs):
        benchmarks = [BenchmarkCodec.decode(b)
                      for b in dct.pop("benchmarks", [])]
        return BenchmarkSuite(benchmarks=benchmarks, **dct, **kwargs)


class BenchmarkRunnerCodec:
    @staticmethod
    def encode(br):
        return {"suites": [BenchmarkSuiteCodec.encode(s) for s in br.suites]}

    @staticmethod
    def decode(dct, **kwargs):
        suites = [BenchmarkSuiteCodec.decode(s)
                  for s in dct.pop("suites", [])]
        return StaticBenchmarkRunner(suites=suites, **dct, **kwargs)


class BenchmarkComparatorCodec:
    @staticmethod
    def encode(bc):
        comparator = {
            "benchmark": bc.name,
            "change": bc.change,
            "regression": bc.regression,
            "baseline": bc.baseline.value,
            "contender": bc.contender.value,
            "unit": bc.unit,
            "less_is_better": bc.less_is_better,
        }

        suite_name = bc.suite_name
        if suite_name:
            comparator["suite"] = suite_name

        return comparator
