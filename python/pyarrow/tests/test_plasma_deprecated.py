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

import os
import pytest


@pytest.mark.plasma
def test_plasma_deprecated():
    import pyarrow.plasma as plasma

    with pytest.warns(DeprecationWarning):
        plasma_store_ctx = plasma.start_plasma_store(
            plasma_store_memory=10 ** 8,
            use_valgrind=os.getenv("PLASMA_VALGRIND") == "1")
        plasma_store_name, _ = plasma_store_ctx.__enter__()
        plasma.connect(plasma_store_name)

    with pytest.warns(DeprecationWarning):
        plasma.ObjectID(20 * b"a")
