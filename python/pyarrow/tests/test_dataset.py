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

from collections.abc import Generator

import pytest

import pyarrow as pa

try:
    from pyarrow.dataset import (
        DataFragment,
        SimpleDataFragment,
        ScanOptions,
        ScanTask,
        SimpleDataSource
    )
except ImportError as e:
    ds = None
    raise e

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not dataset'
pytestmark = pytest.mark.dataset


@pytest.fixture
def record_batch():
    schema = pa.schema([
        pa.field('i32', pa.int32()),
        pa.field('f64', pa.float64())
    ])
    data = [
        list(range(5)),
        list(map(float, range(5)))
    ]
    return pa.record_batch(data, schema=schema)


def test_scan_options():
    pass


def test_scan_context():
    pass


def test_simple_data_fragment(record_batch):
    fragment = SimpleDataFragment([record_batch] * 5)

    assert isinstance(fragment, SimpleDataFragment)
    assert fragment.splittable is False
    assert isinstance(fragment.scan_options, ScanOptions)

    # scan is an alias for tasks
    for result in [fragment.scan(), fragment.tasks()]:
        result = fragment.tasks()
        assert isinstance(result, Generator)

        tasks = list(result)
        assert len(tasks) == 5
        for task in tasks:
            assert isinstance(task, ScanTask)


def test_simple_data_source(record_batch):
    fragment = SimpleDataFragment([record_batch] * 5)
    source = SimpleDataSource([fragment] * 4)

    assert isinstance(source, SimpleDataSource)
    assert source.type == 'simple_data_source'

    result = source.fragments()
    assert isinstance(result, Generator)

    fragments = list(result)
    assert len(fragments) == 4
    for fragment in fragments:
        assert isinstance(fragment, DataFragment)
