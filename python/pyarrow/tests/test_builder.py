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

import pytest
import pyarrow as pa
from pyarrow.lib import StringBuilder
import numpy as np
import pandas as pd


@pytest.fixture
def sbuilder():
    return StringBuilder()


def test_string_builder_append_string(sbuilder):
    sbuilder.append(b'jsaafakjh')
    assert (sbuilder.length() == 1)
    sbuilder.append('jsaafakjh')
    assert (sbuilder.length() == 2)
    arr = sbuilder.finish()
    assert (sbuilder.length() == 0)
    assert (isinstance(arr, pa.Array))
    assert (arr.null_count == 0)
    assert (arr.type == 'str')
    assert(len(arr.to_pylist()) == 2)


def test_string_builder_append_none(sbuilder):
    sbuilder.append(None)
    assert (sbuilder.null_count() == 1)
    for i in range(10):
        sbuilder.append(None)
    assert (sbuilder.null_count() == 11)
    arr = sbuilder.finish()
    assert (arr.null_count == 11)


def test_string_builder_append_nan(sbuilder):
    sbuilder.append(np.nan)
    sbuilder.append(np.nan)
    assert (sbuilder.null_count() == 2)
    arr = sbuilder.finish()
    assert (arr.null_count == 2)


def test_string_builder_append_both(sbuilder):
    sbuilder.append(np.nan)
    sbuilder.append(None)
    sbuilder.append("Some text")
    sbuilder.append("Some text")
    sbuilder.append(np.nan)
    sbuilder.append(None)
    assert (sbuilder.null_count() == 4)
    arr = sbuilder.finish()
    assert (arr.null_count == 4)
    expected = [None, None, "Some text", "Some text", None, None]
    assert (arr.to_pylist() == expected)


def test_string_builder_append_pylist(sbuilder):
    sbuilder.append_values([np.nan, None, "text", None, "other text"])
    assert (sbuilder.null_count() == 3)
    arr = sbuilder.finish()
    assert (arr.null_count == 3)
    expected = [None, None, "text", None, "other text"]
    assert (arr.to_pylist() == expected)


def test_string_builder_append_series(sbuilder):
    sbuilder.append_values(
        pd.Series([np.nan, None, "text", None, "other text"])
    )
    assert (sbuilder.null_count() == 3)
    arr = sbuilder.finish()
    assert (arr.null_count == 3)
    expected = [None, None, "text", None, "other text"]
    assert (arr.to_pylist() == expected)


def test_string_builder_append_numpy(sbuilder):
    sbuilder.append_values(
        np.asarray([np.nan, None, "text", None, "other text"])
    )
    assert (sbuilder.null_count() == 3)
    arr = sbuilder.finish()
    assert (arr.null_count == 3)
    expected = [None, None, "text", None, "other text"]
    assert (arr.to_pylist() == expected)


def test_string_builder_append_after_finish(sbuilder):
    sbuilder.append_values([np.nan, None, "text", None, "other text"])
    assert (sbuilder.null_count() == 3)
    arr = sbuilder.finish()
    sbuilder.append("No effect")
    assert (arr.null_count == 3)
    expected = [None, None, "text", None, "other text"]
    assert (arr.to_pylist() == expected)
