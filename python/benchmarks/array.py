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

import pyarrow

class Conversions(object):
    params = (1, 10 ** 5, 10 ** 6, 10 ** 7)

    def time_from_pylist(self, n):
        pyarrow.from_pylist(list(range(n)))

    def peakmem_from_pylist(self, n):
        pyarrow.from_pylist(list(range(n)))

class ScalarAccess(object):
    params = (1, 10 ** 5, 10 ** 6, 10 ** 7)

    def setUp(self, n):
        self._array = pyarrow.from_pylist(list(range(n)))

    def time_as_py(self, n):
        for i in range(n):
            self._array[i].as_py()

