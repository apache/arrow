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
import pyarrow.fs as fs

try:
    import pyarrow.dataset as ds
except ImportError:
    raise
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
def simple_data_source(record_batch):
    return ds.SimpleDataSource([record_batch] * 4)


@pytest.fixture
def tree_data_source(simple_data_source):
    return ds.TreeDataSource([simple_data_source] * 2)


@pytest.fixture
def dataset(simple_data_source, tree_data_source, schema):
    return ds.Dataset([simple_data_source, tree_data_source], schema)


def test_scan_context():
    context = ds.ScanContext()
    assert isinstance(context.memory_pool, pa.MemoryPool)
    context = ds.ScanContext(pa.default_memory_pool())
    assert isinstance(context.memory_pool, pa.MemoryPool)


def test_simple_data_source(record_batch):
    source = ds.SimpleDataSource([record_batch] * 4)
    assert isinstance(source, ds.SimpleDataSource)
    assert source.partition_expression is None


def test_tree_data_source(simple_data_source):
    source = ds.TreeDataSource([simple_data_source] * 2)
    assert isinstance(source, ds.TreeDataSource)


def test_filesystem_data_source(mockfs):
    file_stats = mockfs.get_target_stats([
        'subdir/1/xxx/file0.parquet',
        'subdir/2/yyy/file1.parquet',
    ])
    file_format = ds.ParquetFileFormat()
    source_partition = None
    path_partitions = {}

    source = ds.FileSystemDataSource(
        mockfs,
        file_stats,
        source_partition=source_partition,
        path_partitions=path_partitions,
        file_format=file_format
    )

    source_partition = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.FieldExpression('source'),
        ds.ScalarExpression(1337)
    )
    path_partitions = {
        'subdir/1/xxx/file0.parquet': ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('part'),
            ds.ScalarExpression(1)
        ),
        'subdir/2/xxx/file1.parquet': ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('part'),
            ds.ScalarExpression(2)
        )
    }
    source = ds.FileSystemDataSource(
        mockfs,
        file_stats,
        source_partition=source_partition,
        path_partitions=path_partitions,
        file_format=file_format
    )
    assert source.partition_expression.equals(source_partition)


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
    # TODO(kszucs): test non-boolean expressions for filter do raise
    builder = dataset.new_scan().use_threads(True).filter(condition)
    assert isinstance(builder, ds.ScannerBuilder)
    assert isinstance(builder.schema, pa.Schema)

    scanner = builder.finish()
    assert isinstance(scanner, ds.Scanner)

    for task in scanner.scan():
        assert isinstance(task, ds.ScanTask)
        for record_batch in task.scan():
            assert isinstance(record_batch, pa.RecordBatch)


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


def test_abstract_classes():
    classes = [
        ds.FileFormat,
        ds.Scanner,
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

    scheme = ds.SchemaPartitionScheme(
        pa.schema([
            pa.field('group', pa.int64()),
            pa.field('key', pa.float64())
        ])
    )
    expr = scheme.parse('/3/3.14')
    assert isinstance(expr, ds.Expression)

    expected = ds.AndExpression(
        ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('group'),
            ds.ScalarExpression(3)
        ),
        ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('key'),
            ds.ScalarExpression(3.14)
        )
    )
    assert expr.equals(expected)

    with pytest.raises(pa.ArrowInvalid):
        scheme.parse('/prefix/3/aaa')

    scheme = ds.HivePartitionScheme(
        pa.schema([
            pa.field('alpha', pa.int64()),
            pa.field('beta', pa.int64())
        ])
    )
    expr = scheme.parse('/alpha=0/beta=3')
    expected = ds.AndExpression(
        ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('alpha'),
            ds.ScalarExpression(0)
        ),
        ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('beta'),
            ds.ScalarExpression(3)
        )
    )
    assert expr.equals(expected)


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

    ds.AndExpression(a, b, c)
    ds.OrExpression(a, b)
    ds.OrExpression(a, b, c)
    ds.NotExpression(ds.OrExpression(a, b, c))
    ds.IsValidExpression(a)
    ds.CastExpression(a, pa.int32())
    ds.CastExpression(a, pa.int32(), ds.CastOptions.unsafe())
    ds.InExpression(a, pa.array([1, 2, 3]))

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
    assert condition.assume(i64_is_5).equals(ds.ScalarExpression(False))
    assert condition.assume(i64_is_7).equals(ds.ScalarExpression(True))
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
    assert discovery.schema is None
    assert isinstance(discovery.finish(), ds.FileSystemDataSource)
    assert discovery.partition_scheme is None

    scheme = ds.SchemaPartitionScheme(
        pa.schema([
            pa.field('group', pa.int32()),
            pa.field('key', pa.string())
        ])
    )
    discovery.partition_scheme = scheme
    assert isinstance(discovery.partition_scheme, ds.SchemaPartitionScheme)
    assert discovery.schema is None
    assert discovery.root_partition is None

    data_source = discovery.finish()
    assert isinstance(data_source, ds.DataSource)

    inspected_schema = discovery.inspect()
    dataset = ds.Dataset([data_source], inspected_schema)

    scanner = dataset.new_scan().finish()
    for task in scanner.scan():
        assert isinstance(task, ds.ScanTask)
        for record_batch in task.scan():
            assert isinstance(record_batch, pa.RecordBatch)
