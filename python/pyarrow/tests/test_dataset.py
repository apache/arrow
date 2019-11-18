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
        Dataset,
        DataSource,
        DataFragment,
        SimpleDataFragment,
        ScanOptions,
        ScanTask,
        Scanner,
        ScanOptions,
        ScanContext,
        SimpleScanner,
        SimpleDataSource,
        TreeDataSource
    )
except ImportError as e:
    ds = None
    raise e

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not dataset'
pytestmark = pytest.mark.dataset


@pytest.fixture
def schema():
    return pa.schema([
        pa.field('i32', pa.int32()),
        pa.field('f64', pa.float64())
    ])


@pytest.fixture
def record_batch(schema):
    data = [
        list(range(5)),
        list(map(float, range(5)))
    ]
    return pa.record_batch(data, schema=schema)


@pytest.fixture
def simple_data_fragment(record_batch):
    return SimpleDataFragment([record_batch] * 5)


@pytest.fixture
def simple_data_source(simple_data_fragment):
    return SimpleDataSource([simple_data_fragment] * 4)


@pytest.fixture
def tree_data_source(simple_data_source):
    return TreeDataSource([simple_data_source] * 2)


def test_scan_options():
    pass


def test_scan_context():
    pass


def test_simple_data_fragment(record_batch):
    fragment = SimpleDataFragment([record_batch] * 5)

    assert isinstance(fragment, SimpleDataFragment)
    assert fragment.splittable is False
    assert isinstance(fragment.scan_options, ScanOptions)

    assert isinstance(fragment.scan(), Generator)
    tasks = list(fragment.scan())
    assert len(tasks) == 5
    for task in tasks:
        assert isinstance(task, ScanTask)


def test_simple_data_source(simple_data_fragment):
    source = SimpleDataSource([simple_data_fragment] * 4)

    assert isinstance(source, SimpleDataSource)
    assert source.type == 'simple_data_source'

    result = source.fragments()
    assert isinstance(result, Generator)

    fragments = list(result)
    assert len(fragments) == 4
    for fragment in fragments:
        assert isinstance(fragment, DataFragment)


def test_tree_data_source(simple_data_source):
    source = TreeDataSource([simple_data_source] * 2)

    assert isinstance(source, TreeDataSource)
    assert source.type == 'tree_data_source'

    result = source.fragments()
    assert isinstance(result, Generator)

    fragments = list(result)
    assert len(fragments) == 8
    for fragment in fragments:
        assert isinstance(fragment, DataFragment)


def test_dataset(simple_data_source, tree_data_source):
    dataset = Dataset([simple_data_source, tree_data_source])

    assert isinstance(dataset, Dataset)
    assert isinstance(dataset.schema, pa.Schema)
    for source in dataset.sources:
        assert isinstance(source, DataSource)

    scanner = dataset.new_scan()
    assert isinstance(scanner, Scanner)
    assert isinstance(scanner, SimpleScanner)

    for task in scanner.scan():
        assert isinstance(task, ScanTask)
        for record_batch in task.scan():
            assert isinstance(record_batch, pa.RecordBatch)


# def test_scanner(simple_data_source):
#     sources = [simple_data_source]
#     options = ScanOptions()
#     context = ScanContext()
#     scanner = SimpleScanner(sources, options, context)

#     for task in scanner.scan():
#         assert isinstance(task, ScanTask)


def test_projector():
    pass
