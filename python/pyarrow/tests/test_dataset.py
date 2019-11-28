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
import pyarrow.fs as fs

try:
    import pyarrow.dataset as ds
except ImportError:
    ds = None

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not dataset'
pytestmark = pytest.mark.dataset


@pytest.fixture
@pytest.mark.parquet
def mockfs(table):
    import pyarrow.parquet as pq

    mockfs = fs._MockFileSystem()

    directories = [
        'subdir/1/xxx',
        'subdir/2/yyy',
    ]
    for i, directory in enumerate(directories):
        path = '{}/file{}.parquet'.format(directory, i)
        mockfs.create_dir(directory)
        with mockfs.open_output_stream(path) as out:
            pq.write_table(table, out)

    return mockfs


@pytest.fixture
def schema():
    return pa.schema([
        pa.field('i64', pa.int64()),
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
def table(record_batch):
    return pa.Table.from_batches([record_batch] * 10)


@pytest.fixture
def simple_data_fragment(record_batch):
    return ds.SimpleDataFragment([record_batch] * 5)


@pytest.fixture
def simple_data_source(simple_data_fragment):
    return ds.SimpleDataSource([simple_data_fragment] * 4)


@pytest.fixture
def tree_data_source(simple_data_source):
    return ds.TreeDataSource([simple_data_source] * 2)


@pytest.fixture
def dataset(simple_data_source, tree_data_source, schema):
    return ds.Dataset([simple_data_source, tree_data_source], schema)


def test_scan_options():
    pass


def test_scan_context():
    pass


def test_simple_data_fragment(record_batch):
    batches = [record_batch] * 5
    fragment = ds.SimpleDataFragment(batches)

    assert isinstance(fragment, ds.SimpleDataFragment)
    assert fragment.splittable is False
    assert isinstance(fragment.scan_options, ds.ScanOptions)

    assert isinstance(fragment.scan(), Generator)
    tasks = list(fragment.scan())
    assert len(tasks) == 5
    for task, batch in zip(tasks, batches):
        assert isinstance(task, ds.ScanTask)
        result_batches = list(task.scan())
        assert len(result_batches) == 1
        assert result_batches[0].equals(batch)


def test_simple_data_source(simple_data_fragment):
    source = ds.SimpleDataSource([simple_data_fragment] * 4)
    assert isinstance(source, ds.SimpleDataSource)
    assert source.partition_expression is None

    result = source.fragments()
    assert isinstance(result, Generator)

    fragments = list(result)
    assert len(fragments) == 4
    for fragment in fragments:
        assert isinstance(fragment, ds.DataFragment)


def test_tree_data_source(simple_data_source):
    source = ds.TreeDataSource([simple_data_source] * 2)
    assert isinstance(source, ds.TreeDataSource)

    result = source.fragments()
    assert isinstance(result, Generator)

    fragments = list(result)
    assert len(fragments) == 8
    for fragment in fragments:
        assert isinstance(fragment, ds.DataFragment)


def test_dataset(simple_data_source, tree_data_source, schema):
    dataset = ds.Dataset([simple_data_source, tree_data_source], schema)

    assert isinstance(dataset, ds.Dataset)
    assert isinstance(dataset.schema, pa.Schema)
    for source in dataset.sources:
        assert isinstance(source, ds.DataSource)

    condition = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.FieldExpression('i64'),
        ds.ScalarExpression(1)
    )
    builder = dataset.new_scan().use_threads(True).filter(condition)
    assert isinstance(builder, ds.ScannerBuilder)
    assert isinstance(builder.schema(), pa.Schema)

    scanner = builder.finish()
    assert isinstance(scanner, ds.Scanner)

    for task in scanner.scan():
        assert isinstance(task, ds.ScanTask)
        for record_batch in task.scan():
            assert isinstance(record_batch, pa.RecordBatch)


def test_scanner(schema, simple_data_source):
    sources = [simple_data_source]
    # FIXME(kszucs): if schema is not set to options it segfaults
    options = ds.ScanOptions(schema=schema)
    context = ds.ScanContext()

    scanner = ds.Scanner(sources, options, context)
    for task in scanner.scan():
        assert isinstance(task, ds.ScanTask)
        for record_batch in task.scan():  # call it execute?
            assert isinstance(record_batch, pa.RecordBatch)

    table = scanner.to_table()
    assert isinstance(table, pa.Table)


def test_scanner_builder(dataset):
    context = ds.ScanContext()
    builder = ds.ScannerBuilder(dataset, context)
    scanner = builder.finish()
    assert isinstance(scanner, ds.Scanner)
    scanner.scan()

    builder = dataset.new_scan()
    builder.project(['i64'])
    scanner = builder.finish()
    scanner.scan()
    assert isinstance(scanner, ds.Scanner)


def test_file_source(mockfs):
    source1 = ds.FileSource('/path/to/file.ext', mockfs, compression=None)
    source2 = ds.FileSource('/path/to/file.ext.gz', mockfs, compression='gzip')
    assert source1.path == '/path/to/file.ext'
    assert source1.fs == mockfs
    assert source1.compression == 0  # None
    assert source2.path == '/path/to/file.ext.gz'
    assert source2.fs == mockfs
    assert source2.compression == 2  # 'gzip'


def test_abstract_classes():
    classes = [
        ds.FileFormat,
        ds.DataFragment,
        ds.DataSource,
        ds.Expression,
        ds.PartitionScheme,
    ]
    for klass in classes:
        with pytest.raises(TypeError):
            klass()


def test_partition_scheme(schema):
    for klass in [ds.SchemaPartitionScheme, ds.HivePartitionScheme]:
        scheme = klass(schema)
        assert isinstance(scheme, ds.PartitionScheme)


def test_expression(schema):
    a = ds.ScalarExpression(1)
    b = ds.ScalarExpression(1.1)
    c = ds.ScalarExpression(True)

    equal = ds.ComparisonExpression(ds.CompareOperator.Equal, a, b)
    assert equal.op() == ds.CompareOperator.Equal

    and_ = ds.AndExpression(a, b)
    assert isinstance(and_.left_operand, ds.Expression)
    assert isinstance(and_.right_operand, ds.Expression)
    assert and_.equals(ds.AndExpression(a, b))
    assert and_.equals(and_)
    assert and_ == ds.AndExpression(a, b)
    assert and_ != 'other object'

    ds.AndExpression(a, b, c)
    ds.OrExpression(a, b)
    ds.OrExpression(a, b, c)
    ds.NotExpression(ds.OrExpression(a, b, c))

    condition = ds.ComparisonExpression(
        ds.CompareOperator.Greater,
        ds.FieldExpression('i64'),
        ds.ScalarExpression(5)
    )
    assert condition.validate(schema) == pa.bool_()

    i64_is_5 = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.FieldExpression('i64'),
        ds.ScalarExpression(5)
    )
    i64_is_7 = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.FieldExpression('i64'),
        ds.ScalarExpression(7)
    )
    true_ = ds.ScalarExpression(True)
    false_ = ds.ScalarExpression(False)
    assert condition.assume(i64_is_5) == false_
    assert condition.assume(i64_is_7) == true_
    assert str(condition) == "(i64 > 5:int64)"


def test_file_system_discovery(mockfs):
    selector = fs.Selector('subdir', recursive=True)
    assert selector.base_dir == 'subdir'
    assert selector.recursive is True

    format = ds.ParquetFileFormat()
    assert format.name() == 'parquet'

    options = ds.FileSystemDiscoveryOptions('/')
    assert options.partition_base_dir == '/'
    assert options.ignore_prefixes == ['.', '_']
    assert options.exclude_invalid_files is True

    discovery = ds.FileSystemDataSourceDiscovery(mockfs, selector, format,
                                                 options)
    assert isinstance(discovery.inspect(), pa.Schema)
    # assert isinstance(discovery.schema(), pa.Schema)
    assert isinstance(discovery.finish(), ds.FileSystemDataSource)

    scheme = ds.SchemaPartitionScheme(
        pa.schema([
            pa.field('group', pa.int32()),
            pa.field('key', pa.string())
        ])
    )
    discovery.partition_scheme = scheme
    assert isinstance(discovery.partition_scheme, ds.SchemaPartitionScheme)

    data_source = discovery.finish()
    assert isinstance(data_source, ds.DataSource)

    inspected_schema = discovery.inspect()
    dataset = ds.Dataset([data_source], inspected_schema)

    scanner = dataset.new_scan().finish()
    for task in scanner.scan():
        assert isinstance(task, ds.ScanTask)
        for record_batch in task.scan():
            assert isinstance(record_batch, pa.RecordBatch)
