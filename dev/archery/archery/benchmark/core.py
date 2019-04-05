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
        return None


class Benchmark:
    def __init__(self, name, values, stats=None):
        self.name = name
        self.values = values
        self.statistics = stats if stats else Statistics.from_values(values)
        self.less_is_better = less_is_better

    @property
    def value(self):
        return self.statistics.median


def changes(old, new):
    if old == 0 and new == 0:
        return 0.0
    if old == 0:
        return float(new - old) / (float(old + new) / 2)
    return float(new - old) / abs(old)


class BenchmarkSuite:
    def __init__(self, name, benchmarks):
        self.name = name
        self.benchmarks = benchmarks


def default_comparator(old, new, threshold=0.05):
    # negative change is better, positive is tolerable until it exceeds
    # threshold
    return changes(old, new) < threshold


class BenchmarkComparator:
    def __init__(self, baseline, contender):
        self.baseline = baseline
        self.contender = contender

    def compare(self, comparator=None):
        comparator = comparator if comparator else default_comparator
        return comparator(self.baseline.value, self.contender.value)

    def confidence(self):
        """ Indicate if a comparison of benchmarks should be trusted. """
        return True

    def __call__(self, **kwargs):
        return self.compare(**kwargs)
