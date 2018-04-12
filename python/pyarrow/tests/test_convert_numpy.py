# -*- coding: utf-8 -*-
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

import pytest

# Regression test for ARROW-2101
def test_convert_numpy_array_of_bytes_to_arrow_array_of_strings():
    converted = pa.array(np.array([b'x'], dtype=object), pa.string())
    assert converted.type == pa.string()

# Make sure that if an ndarray of bytes is passed to the array
# constructor and the type is string, it will fail if those bytes
# cannot be converted to utf-8
def test_convert_numpy_array_of_bytes_to_arrow_array_of_strings_bad_data():
    with pytest.raises(pa.lib.ArrowException,
                       message="Unknown error: 'utf-8' codec can't decode byte 0x80 in position 0: invalid start byte"):
        pa.array(np.array([b'\x80\x81'], dtype=object), pa.string())
