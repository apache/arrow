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

from linking import _remove_weak_symbols, _remove_symbol_versions


def test_remove_weak_symbols(self):
    symbol_info = ["symbol1 v", "symbol2 V", "symbol3", "symbol4 w", "symbol5 W", "symbol6"]
    expected_result = ["symbol3", "symbol6"]
    self.assertEqual(_remove_weak_symbols(self, symbol_info), expected_result)

def test_remove_symbol_versions(self):
        symbol_info = ["symbol1@version1", "symbol2@version2", "symbol3", "symbol4@version4"]
        expected_result = ["symbol1", "symbol2", "symbol3", "symbol4"]
        self.assertEqual(_remove_symbol_versions(self, symbol_info), expected_result)