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


def median(values):
    n = len(values)
    if n == 0:
        raise ValueError("median requires at least one value")
    elif n % 2 == 0:
        return (values[(n // 2) - 1] + values[n // 2]) / 2
    else:
        return values[n // 2]


class Benchmark:
    def __init__(self, name, unit, less_is_better, values, time_unit,
                 times, counters=None):
        self.name = name
        self.unit = unit
        self.less_is_better = less_is_better
        self.values = sorted(values)
        self.time_unit = time_unit
        self.times = sorted(times)
        self.median = median(self.values)
        self.counters = counters or {}

    @property
    def value(self):
        return self.median

    def __repr__(self):
        return "Benchmark[name={},value={}]".format(self.name, self.value)


class BenchmarkSuite:
    def __init__(self, name, benchmarks):
        self.name = name
        self.benchmarks = benchmarks

    def __repr__(self):
        return "BenchmarkSuite[name={}, benchmarks={}]".format(
            self.name, self.benchmarks
        )
