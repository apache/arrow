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


class Statistics:
    def __init__(self, min, max, mean, stddev, q1, median, q3):
        self.min = min
        self.max = max
        self.mean = mean
        self.stddev = stddev
        self.q1 = q1
        self.median = median
        self.q3 = q3

    @staticmethod
    def from_values(values):
        sorted(values)
        n = len(values)
        mean = sum(values) / len(values)
        sum_diff = sum([(val - mean)**2 for val in values])
        stddev = (sum_diff / (n - 1))**0.5 if n > 1 else 0.0

        return Statistics(
            values[0],
            values[-1],
            mean,
            stddev,
            values[int(n/4)],
            values[int(n/2)],
            values[int((3*n)/4)],
        )


class Benchmark:
    def __init__(self, name, values, stats=None):
        self.name = name
        self.values = values
        self.statistics = stats if stats else Statistics.from_values(values)

    @property
    def value(self):
        return self.statistics.median

    @property
    def unit(self):
        return None

    @property
    def less_is_better(self):
        return True


class BenchmarkSuite:
    def __init__(self, name, benchmarks):
        self.name = name
        self.benchmarks = benchmarks

    def __repr__(self):
        name = self.name
        benchmarks = self.benchmarks
        return f"BenchmarkSuite[name={name}, benchmarks={benchmarks}]"


def regress(change, threshold=0.05):
    # negative change is better, positive is tolerable until it exceeds
    # threshold
    return change > threshold


def changes(old, new):
    if old == 0 and new == 0:
        return 0.0
    if old == 0:
        return float(new - old) / (float(old + new) / 2)
    return float(new - old) / abs(old)


class BenchmarkComparator:
    def __init__(self, contender, baseline):
        self.contender = contender
        self.baseline = baseline

    def compare(self, comparator=None):
        base = self.baseline.value
        cont = self.contender.value
        change = changes(base, cont)
        adjusted_change = change
        if not self.baseline.less_is_better:
            adjusted_change = change * -1.0
        return {
            "benchmark": self.baseline.name,
            "change": change,
            "regression": regress(adjusted_change),
            "baseline": base,
            "contender": cont,
            "unit": self.baseline.unit,
            "less_is_better": self.baseline.less_is_better,
        }

    def confidence(self):
        """ Indicate if a comparison of benchmarks should be trusted. """
        return True

    def __call__(self, **kwargs):
        return self.compare(**kwargs)
