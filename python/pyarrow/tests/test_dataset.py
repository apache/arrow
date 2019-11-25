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
from pyarrow.fs import _MockFileSystem, Selector as FileSelector

try:
    from pyarrow.dataset import (
        Dataset,
        DataSource,
        DataFragment,
        ParquetFileFormat,
        SimpleDataFragment,
        ScanOptions,
        FileSource,
        ScanTask,
        Scanner,
        ScannerBuilder,
        ScanOptions,
        ScanContext,
        SimpleDataSource,
        TreeDataSource,
        FileSystemDiscoveryOptions,
        FileSystemDataSourceDiscovery
    )
except ImportError as e:
    ds = None
    raise e

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not dataset'
pytestmark = pytest.mark.dataset


@pytest.fixture
def fs():
    return _MockFileSystem()


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


@pytest.fixture
def dataset(simple_data_source, tree_data_source):
    return Dataset([simple_data_source, tree_data_source])


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

    builder = dataset.new_scan()
    assert isinstance(builder, ScannerBuilder)

    scanner = builder.finish()
    assert isinstance(scanner, Scanner)

    for task in scanner.scan():
        assert isinstance(task, ScanTask)
        for record_batch in task.scan():
            assert isinstance(record_batch, pa.RecordBatch)


def test_scanner(schema, simple_data_source):
    sources = [simple_data_source]
    # FIXME(kszucs): if schema is not set to options it segfaults
    options = ScanOptions(schema=schema)
    context = ScanContext()

    scanner = Scanner(sources, options, context)
    for task in scanner.scan():
        assert isinstance(task, ScanTask)
        for record_batch in task.scan():  # call it execute?
            assert isinstance(record_batch, pa.RecordBatch)

    table = scanner.to_table()
    assert isinstance(table, pa.Table)


def test_scanner_builder(dataset):
    context = ScanContext()
    builder = ScannerBuilder(dataset, context)

    builder.project([])
    scanner = builder.finish()
    assert isinstance(scanner, Scanner)


def test_projector():
    pass


def test_file_source(fs):
    source1 = FileSource('/path/to/file.ext', fs, compression=None)
    source2 = FileSource('/path/to/file.ext.gz', fs, compression='gzip')

    assert source1.path == '/path/to/file.ext'
    assert source1.fs == fs
    assert source1.compression == 0  # None
    assert source2.path == '/path/to/file.ext.gz'
    assert source2.fs == fs
    assert source2.compression == 2  # 'gzip'


def test_file_system_data_source():
    pass


def test_file_system_discovery(fs):
    selector = FileSelector('/base', recursive=True)
    fileformat = ParquetFileFormat()
    discovery = FileSystemDataSourceDiscovery(fs, selector, fileformat)


