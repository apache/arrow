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

import numpy as np

import pyarrow as pa
import pyarrow.plasma as plasma

from . import common


class PlasmaBenchmarks(object):

    def setup(self):
        self.plasma_store_ctx = plasma.start_plasma_store(plasma_store_memory=10**9)
        plasma_store_name, p = self.plasma_store_ctx.__enter__()
        self.plasma_client = plasma.connect(plasma_store_name, "", 64)

        self.data_1kb = np.random.randn(1000 // 8)
        self.data_10kb = np.random.randn(10000 // 8)
        self.data_100kb = np.random.randn(100000 // 8)
        self.data_1mb = np.random.randn(1000000 // 8)
        self.data_10mb = np.random.randn(10000000 // 8)
        self.data_100mb = np.random.randn(100000000 // 8)

    def teardown(self):
        self.plasma_store_ctx.__exit__(None, None, None)

    def time_plasma_put(self):
        for i in range(1000):
            self.plasma_client.put(1)

    def time_plasma_putget(self):
        for i in range(1000):
            x = self.plasma_client.put(1)
            self.plasma_client.get(x)

    def time_plasma_put_1kb(self):
        self.plasma_client.put(self.data_1kb)

    def time_plasma_put_10kb(self):
        self.plasma_client.put(self.data_10kb)

    def time_plasma_put_100kb(self):
        self.plasma_client.put(self.data_100kb)

    def time_plasma_put_1mb(self):
        self.plasma_client.put(self.data_1mb)

    def time_plasma_put_10mb(self):
        self.plasma_client.put(self.data_10mb)

    def time_plasma_put_100mb(self):
        self.plasma_client.put(self.data_100mb)
