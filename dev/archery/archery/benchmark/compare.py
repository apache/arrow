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


# Define a global regression threshold as 5%. This is purely subjective and
# flawed. This does not track cumulative regression.
DEFAULT_THRESHOLD = 0.05


def items_per_seconds_fmt(value):
    if value < 1000:
        return f"{value} items/sec"
    if value < 1000**2:
        return f"{value / 1000:.3f}K items/sec"
    if value < 1000**3:
        return f"{value / 1000**2:.3f}M items/sec"
    else:
        return f"{value / 1000**3:.3f}G items/sec"


def bytes_per_seconds_fmt(value):
    if value < 1024:
        return f"{value} bytes/sec"
    if value < 1024**2:
        return f"{value / 1024:.3f} KiB/sec"
    if value < 1024**3:
        return f"{value / 1024**2:.3f} MiB/sec"
    if value < 1024**4:
        return f"{value / 1024**3:.3f} GiB/sec"
    else:
        return f"{value / 1024**4:.3f} TiB/sec"


def change_fmt(value):
    return f"{value:.3%}"


def formatter_for_unit(unit):
    if unit == "bytes_per_second":
        return bytes_per_seconds_fmt
    elif unit == "items_per_second":
        return items_per_seconds_fmt
    else:
        return lambda x: x


class BenchmarkComparator:
    """ Compares two benchmarks.

    Encodes the logic of comparing two benchmarks and taking a decision on
    if it induce a regression.
    """

    def __init__(self, contender, baseline, threshold=DEFAULT_THRESHOLD,
                 suite_name=None):
        self.contender = contender
        self.baseline = baseline
        self.threshold = threshold
        self.suite_name = suite_name

    @property
    def name(self):
        return self.baseline.name

    @property
    def less_is_better(self):
        return self.baseline.less_is_better

    @property
    def unit(self):
        return self.baseline.unit

    @property
    def change(self):
        new = self.contender.value
        old = self.baseline.value

        if old == 0 and new == 0:
            return 0.0
        if old == 0:
            return 0.0

        return float(new - old) / abs(old)

    @property
    def confidence(self):
        """ Indicate if a comparison of benchmarks should be trusted. """
        return True

    @property
    def regression(self):
        change = self.change
        adjusted_change = change if self.less_is_better else -change
        return (self.confidence and adjusted_change > self.threshold)

    @property
    def formatted(self):
        fmt = formatter_for_unit(self.unit)
        return {
            "benchmark": self.name,
            "change": change_fmt(self.change),
            "regression": self.regression,
            "baseline": fmt(self.baseline.value),
            "contender": fmt(self.contender.value),
            "unit": self.unit,
            "less_is_better": self.less_is_better,
            "counters": str(self.baseline.counters)
        }

    def compare(self, comparator=None):
        return {
            "benchmark": self.name,
            "change": self.change,
            "regression": self.regression,
            "baseline": self.baseline.value,
            "contender": self.contender.value,
            "unit": self.unit,
            "less_is_better": self.less_is_better,
            "counters": self.baseline.counters
        }

    def __call__(self, **kwargs):
        return self.compare(**kwargs)


def pairwise_compare(contender, baseline):
    dict_contender = {e.name: e for e in contender}
    dict_baseline = {e.name: e for e in baseline}

    for name in (dict_contender.keys() & dict_baseline.keys()):
        yield name, (dict_contender[name], dict_baseline[name])


class RunnerComparator:
    """ Compares suites/benchmarks from runners.

    It is up to the caller that ensure that runners are compatible (both from
    the same language implementation).
    """

    def __init__(self, contender, baseline, threshold=DEFAULT_THRESHOLD):
        self.contender = contender
        self.baseline = baseline
        self.threshold = threshold

    @property
    def comparisons(self):
        contender = self.contender.suites
        baseline = self.baseline.suites
        suites = pairwise_compare(contender, baseline)

        for suite_name, (suite_cont, suite_base) in suites:
            benchmarks = pairwise_compare(
                suite_cont.benchmarks, suite_base.benchmarks)

            for _, (bench_cont, bench_base) in benchmarks:
                yield BenchmarkComparator(bench_cont, bench_base,
                                          threshold=self.threshold,
                                          suite_name=suite_name)
