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

import sys
import operator

import numpy as np
import pytest

import pyarrow as pa
import pyarrow.fs as fs

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow.dataset as ds
except ImportError:
    ds = None

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not dataset'
pytestmark = pytest.mark.dataset


def _generate_data(n):
    import datetime
    import itertools

    day = datetime.datetime(2000, 1, 1)
    interval = datetime.timedelta(days=5)
    colors = itertools.cycle(['green', 'blue', 'yellow', 'red', 'orange'])

    data = []
    for i in range(n):
        data.append((day, i, float(i), next(colors)))
        day += interval

    return pd.DataFrame(data, columns=['date', 'index', 'value', 'color'])


def _table_from_pandas(df):
    schema = pa.schema([
        pa.field('date', pa.date32()),
        pa.field('index', pa.int64()),
        pa.field('value', pa.float64()),
        pa.field('color', pa.string()),
    ])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    return table.replace_schema_metadata()


@pytest.fixture
@pytest.mark.parquet
def mockfs():
    import pyarrow.parquet as pq

    mockfs = fs._MockFileSystem()

    data = [
        list(range(5)),
        list(map(float, range(5)))
    ]
    schema = pa.schema([
        pa.field('i64', pa.int64()),
        pa.field('f64', pa.float64())
    ])
    batch = pa.record_batch(data, schema=schema)
    table = pa.Table.from_batches([batch])

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


@pytest.fixture(scope='module')
@pytest.mark.pandas
@pytest.mark.parquet
def multisourcefs():
    import pyarrow.parquet as pq

    df = _generate_data(1000)
    mockfs = fs._MockFileSystem()

    # simply split the dataframe into three chunks to construct a data source
    # from each chunk into its own directory
    df_a, df_b, df_c, df_d = np.array_split(df, 4)

    # create a directory containing a flat sequence of parquet files without
    # any partitioning involved
    mockfs.create_dir('plain')
    for i, chunk in enumerate(np.array_split(df_a, 10)):
        path = 'plain/chunk-{}.parquet'.format(i)
        with mockfs.open_output_stream(path) as out:
            pq.write_table(_table_from_pandas(chunk), out)

    # create one with schema partitioning by week and color
    mockfs.create_dir('schema')
    for part, chunk in df_b.groupby([df_b.date.dt.week, df_b.color]):
        folder = 'schema/{}/{}'.format(*part)
        path = '{}/chunk.parquet'.format(folder)
        mockfs.create_dir(folder)
        with mockfs.open_output_stream(path) as out:
            pq.write_table(_table_from_pandas(chunk), out)

    # create one with hive partitioning by year and month
    mockfs.create_dir('hive')
    for part, chunk in df_c.groupby([df_c.date.dt.year, df_c.date.dt.month]):
        folder = 'hive/year={}/month={}'.format(*part)
        path = '{}/chunk.parquet'.format(folder)
        mockfs.create_dir(folder)
        with mockfs.open_output_stream(path) as out:
            pq.write_table(_table_from_pandas(chunk), out)

    # create one with hive partitioning by color
    mockfs.create_dir('hive_color')
    for part, chunk in df_d.groupby(["color"]):
        folder = 'hive_color/color={}'.format(*part)
        path = '{}/chunk.parquet'.format(folder)
        mockfs.create_dir(folder)
        with mockfs.open_output_stream(path) as out:
            pq.write_table(_table_from_pandas(chunk), out)

    return mockfs


@pytest.fixture
def dataset(mockfs):
    format = ds.ParquetFileFormat()
    selector = fs.FileSelector('subdir', recursive=True)
    options = ds.FileSystemFactoryOptions('subdir')
    options.partitioning = ds.DirectoryPartitioning(
        pa.schema([
            pa.field('group', pa.int32()),
            pa.field('key', pa.string())
        ])
    )
    factory = ds.FileSystemSourceFactory(mockfs, selector, format, options)
    schema = factory.inspect()
    source = factory.finish()
    return ds.Dataset([source], schema)


def test_filesystem_source(mockfs):
    schema = pa.schema([])

    file_format = ds.ParquetFileFormat()

    paths = ['subdir/1/xxx/file0.parquet', 'subdir/2/yyy/file1.parquet']
    partitions = [ds.ScalarExpression(True), ds.ScalarExpression(True)]

    source = ds.FileSystemSource(schema,
                                 root_partition=None,
                                 file_format=file_format,
                                 filesystem=mockfs,
                                 paths_or_selector=paths,
                                 partitions=partitions)

    root_partition = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.FieldExpression('source'),
        ds.ScalarExpression(1337)
    )
    partitions = [
        ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('part'),
            ds.ScalarExpression(1)
        ),
        ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.FieldExpression('part'),
            ds.ScalarExpression(2)
        )
    ]
    source = ds.FileSystemSource(paths_or_selector=paths, schema=schema,
                                 root_partition=root_partition,
                                 filesystem=mockfs, partitions=partitions,
                                 file_format=file_format)
    assert source.partition_expression.equals(root_partition)


def test_dataset(dataset):
    assert isinstance(dataset, ds.Dataset)
    assert isinstance(dataset.schema, pa.Schema)

    # TODO(kszucs): test non-boolean Exprs for filter do raise

    expected_i64 = pa.array([0, 1, 2, 3, 4], type=pa.int64())
    expected_f64 = pa.array([0, 1, 2, 3, 4], type=pa.float64())
    for task in dataset.scan():
        assert isinstance(task, ds.ScanTask)
        for batch in task.execute():
            assert batch.column(0).equals(expected_i64)
            assert batch.column(1).equals(expected_f64)

    batches = dataset.to_batches()
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)

    table = dataset.to_table()
    assert isinstance(table, pa.Table)
    assert len(table) == 10

    condition = ds.field('i64') == 1
    scanner = ds.Scanner(dataset, use_threads=True, filter=condition)
    result = scanner.to_table().to_pydict()

    # don't rely on the scanning order
    assert result['i64'] == [1, 1]
    assert result['f64'] == [1., 1.]
    assert sorted(result['group']) == [1, 2]
    assert sorted(result['key']) == ['xxx', 'yyy']


def test_scanner(dataset):
    scanner = ds.Scanner(dataset, memory_pool=pa.default_memory_pool())
    assert isinstance(scanner, ds.Scanner)
    assert len(list(scanner.scan())) == 2

    with pytest.raises(pa.ArrowInvalid):
        dataset.scan(columns=['unknown'])

    scanner = ds.Scanner(dataset, columns=['i64'],
                         memory_pool=pa.default_memory_pool())

    assert isinstance(scanner, ds.Scanner)
    assert len(list(scanner.scan())) == 2
    for task in scanner.scan():
        for batch in task.execute():
            assert batch.num_columns == 1


def test_abstract_classes():
    classes = [
        ds.FileFormat,
        ds.Scanner,
        ds.Source,
        ds.Expression,
        ds.Partitioning,
    ]
    for klass in classes:
        with pytest.raises(TypeError):
            klass()


def test_partitioning():
    schema = pa.schema([
        pa.field('i64', pa.int64()),
        pa.field('f64', pa.float64())
    ])
    for klass in [ds.DirectoryPartitioning, ds.HivePartitioning]:
        partitioning = klass(schema)
        assert isinstance(partitioning, ds.Partitioning)

    partitioning = ds.DirectoryPartitioning(
        pa.schema([
            pa.field('group', pa.int64()),
            pa.field('key', pa.float64())
        ])
    )
    expr = partitioning.parse('/3/3.14')
    assert isinstance(expr, ds.Expression)

    expected = (ds.field('group') == 3) & (ds.field('key') == 3.14)
    assert expr.equals(expected)

    with pytest.raises(pa.ArrowInvalid):
        partitioning.parse('/prefix/3/aaa')

    partitioning = ds.HivePartitioning(
        pa.schema([
            pa.field('alpha', pa.int64()),
            pa.field('beta', pa.int64())
        ])
    )
    expr = partitioning.parse('/alpha=0/beta=3')
    expected = (
        (ds.field('alpha') == ds.scalar(0)) &
        (ds.field('beta') == ds.scalar(3))
    )
    assert expr.equals(expected)


def test_expression():
    a = ds.ScalarExpression(1)
    b = ds.ScalarExpression(1.1)
    c = ds.ScalarExpression(True)
    d = ds.ScalarExpression("string")

    equal = ds.ComparisonExpression(ds.CompareOperator.Equal, a, b)
    assert equal.op() == ds.CompareOperator.Equal

    and_ = ds.AndExpression(a, b)
    assert and_.left_operand.equals(a)
    assert and_.right_operand.equals(b)
    assert and_.equals(ds.AndExpression(a, b))
    assert and_.equals(and_)

    ds.AndExpression(a, b, c)
    ds.OrExpression(a, b)
    ds.OrExpression(a, b, c, d)
    ds.NotExpression(ds.OrExpression(a, b, c))
    ds.IsValidExpression(a)
    ds.CastExpression(a, pa.int32())
    ds.CastExpression(a, pa.int32(), safe=True)
    ds.InExpression(a, pa.array([1, 2, 3]))

    condition = ds.ComparisonExpression(
        ds.CompareOperator.Greater,
        ds.FieldExpression('i64'),
        ds.ScalarExpression(5)
    )
    schema = pa.schema([
        pa.field('i64', pa.int64()),
        pa.field('f64', pa.float64())
    ])
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


def test_expression_ergonomics():
    zero = ds.scalar(0)
    one = ds.scalar(1)
    true = ds.scalar(True)
    false = ds.scalar(False)
    string = ds.scalar("string")
    field = ds.field("field")

    assert one.equals(ds.ScalarExpression(1))
    assert zero.equals(ds.ScalarExpression(0))
    assert true.equals(ds.ScalarExpression(True))
    assert false.equals(ds.ScalarExpression(False))
    assert string.equals(ds.ScalarExpression("string"))
    assert field.equals(ds.FieldExpression("field"))

    expected = ds.AndExpression(ds.ScalarExpression(1), ds.ScalarExpression(0))
    for expr in [one & zero, 1 & zero, one & 0]:
        assert expr.equals(expected)

    expected = ds.OrExpression(ds.ScalarExpression(1), ds.ScalarExpression(0))
    for expr in [one | zero, 1 | zero, one | 0]:
        assert expr.equals(expected)

    comparison_ops = [
        (operator.eq, ds.CompareOperator.Equal),
        (operator.ne, ds.CompareOperator.NotEqual),
        (operator.ge, ds.CompareOperator.GreaterEqual),
        (operator.le, ds.CompareOperator.LessEqual),
        (operator.lt, ds.CompareOperator.Less),
        (operator.gt, ds.CompareOperator.Greater),
    ]
    for op, compare_op in comparison_ops:
        expr = op(zero, one)
        expected = ds.ComparisonExpression(compare_op, zero, one)
        assert expr.equals(expected)

    expr = ~true == false
    expected = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.NotExpression(ds.ScalarExpression(True)),
        ds.ScalarExpression(False)
    )
    assert expr.equals(expected)

    for typ in ("bool", pa.bool_()):
        expr = field.cast(typ) == true
        expected = ds.ComparisonExpression(
            ds.CompareOperator.Equal,
            ds.CastExpression(ds.FieldExpression("field"), pa.bool_()),
            ds.ScalarExpression(True)
        )
        assert expr.equals(expected)

    expr = field.isin([1, 2])
    expected = ds.InExpression(ds.FieldExpression("field"), pa.array([1, 2]))
    assert expr.equals(expected)

    with pytest.raises(TypeError):
        field.isin(1)


@pytest.mark.parametrize('paths_or_selector', [
    fs.FileSelector('subdir', recursive=True),
    [
        'subdir',
        'subdir/1',
        'subdir/1/xxx',
        'subdir/1/xxx/file0.parquet',
        'subdir/2',
        'subdir/2/yyy',
        'subdir/2/yyy/file1.parquet',
    ]
])
def test_file_system_factory(mockfs, paths_or_selector):
    format = ds.ParquetFileFormat()

    options = ds.FileSystemFactoryOptions('subdir')
    options.partitioning = ds.DirectoryPartitioning(
        pa.schema([
            pa.field('group', pa.int32()),
            pa.field('key', pa.string())
        ])
    )
    assert options.partition_base_dir == 'subdir'
    assert options.ignore_prefixes == ['.', '_']
    assert options.exclude_invalid_files is True

    factory = ds.FileSystemSourceFactory(
        mockfs, paths_or_selector, format, options
    )
    inspected_schema = factory.inspect()

    assert isinstance(factory.inspect(), pa.Schema)
    assert isinstance(factory.inspect_schemas(), list)
    assert isinstance(factory.finish(inspected_schema),
                      ds.FileSystemSource)
    assert factory.root_partition.equals(ds.ScalarExpression(True))

    source = factory.finish()
    assert isinstance(source, ds.Source)

    dataset = ds.Dataset([source], inspected_schema)
    assert len(list(dataset.scan())) == 2

    scanner = ds.Scanner(dataset)
    expected_i64 = pa.array([0, 1, 2, 3, 4], type=pa.int64())
    expected_f64 = pa.array([0, 1, 2, 3, 4], type=pa.float64())
    for task, group, key in zip(scanner.scan(), [1, 2], ['xxx', 'yyy']):
        expected_group_column = pa.array([group] * 5, type=pa.int32())
        expected_key_column = pa.array([key] * 5, type=pa.string())
        for batch in task.execute():
            assert batch.num_columns == 4
            assert batch[0].equals(expected_i64)
            assert batch[1].equals(expected_f64)
            assert batch[2].equals(expected_group_column)
            assert batch[3].equals(expected_key_column)

    table = dataset.to_table()
    assert isinstance(table, pa.Table)
    assert len(table) == 10
    assert table.num_columns == 4


def test_paritioning_factory(mockfs):
    paths_or_selector = fs.FileSelector('subdir', recursive=True)
    format = ds.ParquetFileFormat()

    options = ds.FileSystemFactoryOptions('subdir')
    partitioning_factory = ds.DirectoryPartitioning.discover(['group', 'key'])
    assert isinstance(partitioning_factory, ds.PartitioningFactory)
    options.partitioning_factory = partitioning_factory

    factory = ds.FileSystemSourceFactory(
        mockfs, paths_or_selector, format, options
    )
    inspected_schema = factory.inspect()
    # i64/f64 from data, group/key from "/1/xxx" and "/2/yyy" paths
    expected_schema = pa.schema([
        ("i64", pa.int64()),
        ("f64", pa.float64()),
        ("group", pa.int32()),
        ("key", pa.string()),
    ])
    assert inspected_schema.equals(expected_schema, check_metadata=False)

    hive_partitioning_factory = ds.HivePartitioning.discover()
    assert isinstance(hive_partitioning_factory, ds.PartitioningFactory)


def test_partitioning_function():
    schema = pa.schema([("year", pa.int16()), ("month", pa.int8())])
    names = ["year", "month"]

    # default DirectoryPartitioning

    part = ds.partitioning(schema)
    assert isinstance(part, ds.DirectoryPartitioning)
    part = ds.partitioning(field_names=names)
    assert isinstance(part, ds.PartitioningFactory)
    # needs schema or list of names
    with pytest.raises(ValueError):
        ds.partitioning()
    with pytest.raises(ValueError, match="Expected list"):
        ds.partitioning(field_names=schema)
    with pytest.raises(ValueError, match="Cannot specify both"):
        ds.partitioning(schema, field_names=schema)

    # Hive partitioning

    part = ds.partitioning(schema, flavor="hive")
    assert isinstance(part, ds.HivePartitioning)
    part = ds.partitioning(flavor="hive")
    assert isinstance(part, ds.PartitioningFactory)
    # cannot pass list of names
    with pytest.raises(ValueError):
        ds.partitioning(names, flavor="hive")
    with pytest.raises(ValueError, match="Cannot specify 'field_names'"):
        ds.partitioning(field_names=names, flavor="hive")

    # unsupported flavor
    with pytest.raises(ValueError):
        ds.partitioning(schema, flavor="unsupported")


def _create_single_file(base_dir, table=None):
    import pyarrow.parquet as pq
    if table is None:
        table = pa.table({'a': range(9), 'b': [0.] * 4 + [1.] * 5})
    path = base_dir / "test.parquet"
    pq.write_table(table, path)
    return table, path


def _create_directory_of_files(base_dir):
    import pyarrow.parquet as pq
    table1 = pa.table({'a': range(9), 'b': [0.] * 4 + [1.] * 5})
    path1 = base_dir / "test1.parquet"
    pq.write_table(table1, path1)
    table2 = pa.table({'a': range(9, 18), 'b': [0.] * 4 + [1.] * 5})
    path2 = base_dir / "test2.parquet"
    pq.write_table(table2, path2)
    return (table1, table2), (path1, path2)


def _check_dataset_from_path(path, table, **kwargs):
    import pathlib

    # pathlib object
    assert isinstance(path, pathlib.Path)
    dataset = ds.dataset(ds.source(path, **kwargs))
    assert dataset.schema.equals(table.schema, check_metadata=False)
    result = dataset.to_table(use_threads=False)  # deterministic row order
    assert result.replace_schema_metadata().equals(table)

    # string path
    dataset = ds.dataset(ds.source(str(path), **kwargs))
    assert dataset.schema.equals(table.schema, check_metadata=False)
    result = dataset.to_table(use_threads=False)  # deterministic row order
    assert result.replace_schema_metadata().equals(table)

    # passing directly to dataset
    dataset = ds.dataset(str(path), **kwargs)
    assert dataset.schema.equals(table.schema, check_metadata=False)
    result = dataset.to_table(use_threads=False)  # deterministic row order
    assert result.replace_schema_metadata().equals(table)


@pytest.mark.parquet
def test_open_dataset_single_file(tempdir):
    table, path = _create_single_file(tempdir)
    _check_dataset_from_path(path, table)


@pytest.mark.parquet
def test_open_dataset_directory(tempdir):
    tables, _ = _create_directory_of_files(tempdir)
    table = pa.concat_tables(tables)
    _check_dataset_from_path(tempdir, table)


@pytest.mark.parquet
def test_open_dataset_list_of_files(tempdir):
    tables, (path1, path2) = _create_directory_of_files(tempdir)
    table = pa.concat_tables(tables)

    # list of exact files needs to be passed to source() function
    # (dataset() will interpret it as separate sources)
    for dataset in [
            ds.dataset(ds.source([path1, path2])),
            ds.dataset(ds.source([str(path1), str(path2)]))]:
        assert dataset.schema.equals(table.schema, check_metadata=False)
        result = dataset.to_table(use_threads=False)  # deterministic row order
        assert result.replace_schema_metadata().equals(table)


@pytest.mark.skipif(sys.platform == "win32", reason="fails on windows")
@pytest.mark.parquet
def test_open_dataset_partitioned_directory(tempdir):
    import pyarrow.parquet as pq
    table = pa.table({'a': range(9), 'b': [0.] * 4 + [1.] * 5})
    for part in range(3):
        path = tempdir / "part={0}".format(part)
        path.mkdir()
        pq.write_table(table, path / "test.parquet")

    # no partitioning specified, just read all individual files
    full_table = pa.concat_tables([table] * 3)
    _check_dataset_from_path(tempdir, full_table)

    # specify partition scheme with discovery
    dataset = ds.dataset(
        str(tempdir), partitioning=ds.partitioning(flavor="hive"))
    expected_schema = table.schema.append(pa.field("part", pa.int32()))
    assert dataset.schema.equals(expected_schema, check_metadata=False)

    # specify partition scheme with string short-cut
    dataset = ds.dataset(str(tempdir), partitioning="hive")
    assert dataset.schema.equals(expected_schema, check_metadata=False)

    # specify partition scheme with explicit scheme
    dataset = ds.dataset(
        str(tempdir),
        partitioning=ds.partitioning(
            pa.schema([("part", pa.int8())]), flavor="hive"))
    expected_schema = table.schema.append(pa.field("part", pa.int8()))
    assert dataset.schema.equals(expected_schema, check_metadata=False)

    result = dataset.to_table(use_threads=False)
    expected = full_table.append_column(
        "part", pa.array(np.repeat([0, 1, 2], 9), type=pa.int8()))
    assert result.replace_schema_metadata().equals(expected)


@pytest.mark.parquet
def test_open_dataset_filesystem(tempdir):
    # # single file
    table, path = _create_single_file(tempdir)

    # filesystem inferred from path
    dataset1 = ds.dataset(str(path))
    assert dataset1.schema.equals(table.schema, check_metadata=False)

    # filesystem specified
    dataset2 = ds.dataset(str(path), filesystem=fs.LocalFileSystem())
    assert dataset2.schema.equals(table.schema, check_metadata=False)

    # passing different filesystem
    with pytest.raises(FileNotFoundError):
        ds.dataset(str(path), filesystem=fs._MockFileSystem())


@pytest.mark.parquet
def test_open_dataset_unsupported_format(tempdir):
    _, path = _create_single_file(tempdir)
    with pytest.raises(ValueError, match="format 'blabla' is not supported"):
        ds.dataset([path], format="blabla")


@pytest.mark.parquet
def test_open_dataset_from_source_additional_kwargs(tempdir):
    _, path = _create_single_file(tempdir)
    with pytest.raises(ValueError, match="cannot pass any additional"):
        ds.dataset(ds.source(path), format="parquet")


@pytest.mark.parquet
def test_open_dataset_validate_sources(tempdir):
    _, path = _create_single_file(tempdir)
    dataset = ds.dataset(path)
    with pytest.raises(TypeError, match="Expected a path-like or Source, got"):
        ds.dataset([dataset])


@pytest.mark.parquet
def test_filter_implicit_cast(tempdir):
    # ARROW-7652
    table = pa.table({'a': pa.array([0, 1, 2, 3, 4, 5], type=pa.int8())})
    _, path = _create_single_file(tempdir, table)
    dataset = ds.dataset(str(path))

    filter_ = ds.field('a') > 2
    scanner = ds.Scanner(dataset, filter=filter_)
    result = scanner.to_table()
    assert len(result) == 3


@pytest.mark.parquet
@pytest.mark.pandas
def test_dataset_factory(multisourcefs):
    src = ds.source('/plain', filesystem=multisourcefs, format='parquet')
    factory = ds.DatasetFactory([src])

    assert len(factory.sources) == 1
    assert len(factory.inspect_schemas()) == 1
    assert all(isinstance(s, ds.SourceFactory) for s in factory.sources)
    assert all(isinstance(s, pa.Schema) for s in factory.inspect_schemas())
    assert factory.inspect_schemas()[0].equals(src.inspect())
    assert factory.inspect().equals(src.inspect())
    assert isinstance(factory.finish(), ds.Dataset)


@pytest.mark.parquet
@pytest.mark.pandas
def test_multiple_sources(multisourcefs):
    src1 = ds.source('/plain', filesystem=multisourcefs, format='parquet')
    src2 = ds.source('/schema', filesystem=multisourcefs, format='parquet',
                     partitioning=['week', 'color'])
    src3 = ds.source('/hive', filesystem=multisourcefs, format='parquet',
                     partitioning='hive')

    assembled = ds.dataset([src1, src2, src3])
    assert isinstance(assembled, ds.Dataset)

    expected_schema = pa.schema([
        ('date', pa.date32()),
        ('index', pa.int64()),
        ('value', pa.float64()),
        ('color', pa.string()),
        ('week', pa.int32()),
        ('month', pa.int32()),
        ('year', pa.int32()),
    ])
    assert assembled.schema.equals(expected_schema, check_metadata=False)


@pytest.mark.parquet
@pytest.mark.pandas
def test_multiple_sources_with_selectors(multisourcefs):
    # without partitioning
    dataset = ds.dataset(['/plain', '/schema', '/hive'],
                         filesystem=multisourcefs, format='parquet')
    expected_schema = pa.schema([
        ('date', pa.date32()),
        ('index', pa.int64()),
        ('value', pa.float64()),
        ('color', pa.string())
    ])
    assert dataset.schema.equals(expected_schema, check_metadata=False)

    # with hive partitioning for two hive sources
    dataset = ds.dataset(['/hive', '/hive_color'], filesystem=multisourcefs,
                         format='parquet', partitioning='hive')
    expected_schema = pa.schema([
        ('date', pa.date32()),
        ('index', pa.int64()),
        ('value', pa.float64()),
        ('color', pa.string()),
        ('month', pa.int32()),
        ('year', pa.int32())
    ])
    assert dataset.schema.equals(expected_schema, check_metadata=False)
