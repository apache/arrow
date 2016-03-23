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

from pyarrow.compat import unittest
import pyarrow as arrow

A = arrow


class TestRowBatch(unittest.TestCase):

    def test_basics(self):
        data = [
            A.from_pylist(range(5)),
            A.from_pylist([-10, -5, 0, 5, 10])
        ]
        num_rows = 5

        descr = A.schema([A.field('c0', data[0].type),
                          A.field('c1', data[1].type)])

        batch = A.RowBatch(descr, num_rows, data)

        assert len(batch) == num_rows
        assert batch.num_rows == num_rows
        assert batch.num_columns == len(data)
