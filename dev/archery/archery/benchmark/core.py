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

import pandas as pa


class Benchmark:
    def __init__(self, name, unit, less_is_better, values, stats=None):
        self.name = name
        self.unit = unit
        self.less_is_better = less_is_better
        self.values = pa.Series(values)
        self.statistics = self.values.describe()

    @property
    def value(self):
        median = "50%"
        return float(self.statistics[median])

    def __repr__(self):
        return f"Benchmark[name={self.name},value={self.value}]"


class BenchmarkSuite:
    def __init__(self, name, benchmarks):
        self.name = name
        self.benchmarks = benchmarks

    def __repr__(self):
        name = self.name
        benchmarks = self.benchmarks
        return f"BenchmarkSuite[name={name}, benchmarks={benchmarks}]"
