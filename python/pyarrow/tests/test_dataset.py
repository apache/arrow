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

import contextlib
import operator
import os
import pickle

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


@contextlib.contextmanager
def change_cwd(path):
    curdir = os.getcwd()
    os.chdir(str(path))
    try:
        yield
    finally:
        os.chdir(curdir)


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
def mockfs(request):
    request.config.pyarrow.requires('parquet')
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
            data = [
                list(range(5)),
                list(map(float, range(5))),
                list(map(str, range(5))),
                [i] * 5
            ]
            schema = pa.schema([
                pa.field('i64', pa.int64()),
                pa.field('f64', pa.float64()),
                pa.field('str', pa.string()),
                pa.field('const', pa.int64()),
            ])
            batch = pa.record_batch(data, schema=schema)
            table = pa.Table.from_batches([batch])

            pq.write_table(table, out)

    return mockfs


@pytest.fixture(scope='module')
def multisourcefs(request):
    request.config.pyarrow.requires('pandas')
    request.config.pyarrow.requires('parquet')
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
    factory = ds.FileSystemDatasetFactory(mockfs, selector, format, options)
    return factory.finish()


def test_filesystem_dataset(mockfs):
    schema = pa.schema([
        pa.field('const', pa.int64())
    ])

    file_format = ds.ParquetFileFormat()

    paths = ['subdir/1/xxx/file0.parquet', 'subdir/2/yyy/file1.parquet']
    partitions = [ds.ScalarExpression(True), ds.ScalarExpression(True)]

    dataset = ds.FileSystemDataset(
        schema,
        root_partition=None,
        file_format=file_format,
        filesystem=mockfs,
        paths_or_selector=paths,
        partitions=partitions
    )
    assert isinstance(dataset.format, ds.ParquetFileFormat)

    root_partition = ds.ComparisonExpression(
        ds.CompareOperator.Equal,
        ds.FieldExpression('level'),
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
    dataset = ds.FileSystemDataset(
        paths_or_selector=paths,
        schema=schema,
        root_partition=root_partition,
        filesystem=mockfs,
        partitions=partitions,
        file_format=file_format
    )
    assert dataset.partition_expression.equals(root_partition)
    assert set(dataset.files) == set(paths)

    fragments = list(dataset.get_fragments())
    for fragment, partition, path in zip(fragments, partitions, paths):
        assert fragment.partition_expression.equals(
            ds.AndExpression(root_partition, partition))
        assert fragment.path == path
        assert isinstance(fragment, ds.ParquetFileFragment)
        assert fragment.row_groups is None

        row_group_fragments = list(fragment.get_row_group_fragments())
        assert len(row_group_fragments) == 1
        assert isinstance(fragment, ds.ParquetFileFragment)
        assert row_group_fragments[0].path == path
        assert row_group_fragments[0].row_groups == {0}

    # test predicate pushdown using row group metadata
    fragments = list(dataset.get_fragments(filter=ds.field("const") == 0))
    assert len(fragments) == 2
    assert len(list(fragments[0].get_row_group_fragments())) == 1
    assert len(list(fragments[1].get_row_group_fragments())) == 0


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
    e = ds.ScalarExpression(None)

    equal = ds.ComparisonExpression(ds.CompareOperator.Equal, a, b)
    greater = a > b
    assert equal.op == ds.CompareOperator.Equal

    and_ = ds.AndExpression(a, b)
    assert and_.left_operand.equals(a)
    assert and_.right_operand.equals(b)
    assert and_.equals(ds.AndExpression(a, b))
    assert and_.equals(and_)

    or_ = ds.OrExpression(a, b)
    not_ = ds.NotExpression(ds.OrExpression(a, b))
    is_valid = ds.IsValidExpression(a)
    cast_safe = ds.CastExpression(a, pa.int32())
    cast_unsafe = ds.CastExpression(a, pa.int32(), safe=False)
    in_ = ds.InExpression(a, pa.array([1, 2, 3]))

    assert is_valid.operand == a
    assert in_.set_.equals(pa.array([1, 2, 3]))
    assert cast_unsafe.to == pa.int32()
    assert cast_unsafe.safe is False
    assert cast_safe.safe is True

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
    assert "(i64 > 5:int64)" in repr(condition)

    all_exprs = [a, b, c, d, e, equal, greater, and_, or_, not_, is_valid,
                 cast_unsafe, cast_safe, in_, condition, i64_is_5, i64_is_7]
    for expr in all_exprs:
        restored = pickle.loads(pickle.dumps(expr))
        assert expr.equals(restored)


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

    # operations with non-scalar values
    with pytest.raises(TypeError):
        field == [1]

    with pytest.raises(TypeError):
        field != {1}

    with pytest.raises(TypeError):
        field & [1]

    with pytest.raises(TypeError):
        field | [1]


def test_parquet_read_options():
    opts1 = ds.ParquetReadOptions()
    opts2 = ds.ParquetReadOptions(buffer_size=4096,
                                  dictionary_columns=['a', 'b'])
    opts3 = ds.ParquetReadOptions(buffer_size=2**13, use_buffered_stream=True,
                                  dictionary_columns={'a', 'b'})

    assert opts1.use_buffered_stream is False
    assert opts1.buffer_size == 2**13
    assert opts1.dictionary_columns == set()

    assert opts2.use_buffered_stream is False
    assert opts2.buffer_size == 2**12
    assert opts2.dictionary_columns == {'a', 'b'}

    assert opts3.use_buffered_stream is True
    assert opts3.buffer_size == 2**13
    assert opts3.dictionary_columns == {'a', 'b'}

    assert opts1 == opts1
    assert opts1 != opts2
    assert opts2 != opts3


def test_file_format_pickling():
    formats = [
        ds.IpcFileFormat(),
        ds.ParquetFileFormat(),
        ds.ParquetFileFormat(
            read_options=ds.ParquetReadOptions(use_buffered_stream=True)
        ),
        ds.ParquetFileFormat(
            read_options={
                'use_buffered_stream': True,
                'buffer_size': 4096,
            }
        )
    ]
    for file_format in formats:
        assert pickle.loads(pickle.dumps(file_format)) == file_format


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
def test_filesystem_factory(mockfs, paths_or_selector):
    format = ds.ParquetFileFormat(
        read_options=ds.ParquetReadOptions(dictionary_columns={"str"})
    )

    options = ds.FileSystemFactoryOptions('subdir')
    options.partitioning = ds.DirectoryPartitioning(
        pa.schema([
            pa.field('group', pa.int32()),
            pa.field('key', pa.string())
        ])
    )
    assert options.partition_base_dir == 'subdir'
    assert options.ignore_prefixes == ['.', '_']
    assert options.exclude_invalid_files is False

    factory = ds.FileSystemDatasetFactory(
        mockfs, paths_or_selector, format, options
    )
    inspected_schema = factory.inspect()

    assert factory.inspect().equals(pa.schema([
        pa.field('i64', pa.int64()),
        pa.field('f64', pa.float64()),
        pa.field('str', pa.dictionary(pa.int32(), pa.string())),
        pa.field('const', pa.int64()),
        pa.field('group', pa.int32()),
        pa.field('key', pa.string()),
    ]), check_metadata=False)

    assert isinstance(factory.inspect_schemas(), list)
    assert isinstance(factory.finish(inspected_schema),
                      ds.FileSystemDataset)
    assert factory.root_partition.equals(ds.ScalarExpression(True))

    dataset = factory.finish()
    assert isinstance(dataset, ds.FileSystemDataset)
    assert len(list(dataset.scan())) == 2

    scanner = ds.Scanner(dataset)
    expected_i64 = pa.array([0, 1, 2, 3, 4], type=pa.int64())
    expected_f64 = pa.array([0, 1, 2, 3, 4], type=pa.float64())
    expected_str = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 3, 4], type=pa.int32()),
        pa.array("0 1 2 3 4".split(), type=pa.string()))
    for task, group, key in zip(scanner.scan(), [1, 2], ['xxx', 'yyy']):
        expected_group = pa.array([group] * 5, type=pa.int32())
        expected_key = pa.array([key] * 5, type=pa.string())
        expected_const = pa.array([group - 1] * 5, type=pa.int64())
        for batch in task.execute():
            assert batch.num_columns == 6
            assert batch[0].equals(expected_i64)
            assert batch[1].equals(expected_f64)
            assert batch[2].equals(expected_str)
            assert batch[3].equals(expected_const)
            assert batch[4].equals(expected_group)
            assert batch[5].equals(expected_key)

    table = dataset.to_table()
    assert isinstance(table, pa.Table)
    assert len(table) == 10
    assert table.num_columns == 6


def test_make_fragment(multisourcefs):
    parquet_format = ds.ParquetFileFormat()
    dataset = ds.dataset('/plain', filesystem=multisourcefs,
                         format=parquet_format)

    for path in dataset.files:
        fragment = parquet_format.make_fragment(path, multisourcefs)
        row_group_fragment = parquet_format.make_fragment(path, multisourcefs,
                                                          row_groups=[0])
        for f in [fragment, row_group_fragment]:
            assert isinstance(f, ds.ParquetFileFragment)
            assert f.path == path
            assert isinstance(f.filesystem, type(multisourcefs))
        assert fragment.row_groups is None
        assert row_group_fragment.row_groups == {0}


@pytest.mark.pandas
def test_parquet_row_group_fragments(tempdir):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({'a': ['a', 'a', 'b', 'b'], 'b': [1, 2, 3, 4]})

    # write_to_dataset currently requires pandas
    pq.write_to_dataset(table, str(tempdir / "test_parquet_dataset"),
                        partition_cols=["a"])

    import pyarrow.dataset as ds
    dataset = ds.dataset(str(tempdir / "test_parquet_dataset/"),
                         format="parquet", partitioning="hive")

    fragments = list(dataset.get_fragments())
    f = fragments[0]
    parquet_format = f.format
    parquet_format.make_fragment(f.path, f.filesystem,
                                 partition_expression=f.partition_expression)
    parquet_format.make_fragment(
        f.path, f.filesystem, partition_expression=f.partition_expression,
        row_groups={1})


def test_partitioning_factory(mockfs):
    paths_or_selector = fs.FileSelector('subdir', recursive=True)
    format = ds.ParquetFileFormat()

    options = ds.FileSystemFactoryOptions('subdir')
    partitioning_factory = ds.DirectoryPartitioning.discover(['group', 'key'])
    assert isinstance(partitioning_factory, ds.PartitioningFactory)
    options.partitioning_factory = partitioning_factory

    factory = ds.FileSystemDatasetFactory(
        mockfs, paths_or_selector, format, options
    )
    inspected_schema = factory.inspect()
    # i64/f64 from data, group/key from "/1/xxx" and "/2/yyy" paths
    expected_schema = pa.schema([
        ("i64", pa.int64()),
        ("f64", pa.float64()),
        ("str", pa.string()),
        ("const", pa.int64()),
        ("group", pa.int32()),
        ("key", pa.string()),
    ])
    assert inspected_schema.equals(expected_schema)

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


def _check_dataset(dataset, table):
    assert dataset.schema.equals(table.schema)
    result = dataset.to_table(use_threads=False)  # deterministic row order
    assert result.equals(table)


def _check_dataset_from_path(path, table, **kwargs):
    import pathlib

    # pathlib object
    assert isinstance(path, pathlib.Path)
    dataset = ds.dataset(ds.factory(path, **kwargs))
    assert isinstance(dataset, ds.FileSystemDataset)
    _check_dataset(dataset, table)

    # string path
    dataset = ds.dataset(ds.factory(str(path), **kwargs))
    assert isinstance(dataset, ds.FileSystemDataset)
    _check_dataset(dataset, table)

    # relative string path
    with change_cwd(path.parent):
        dataset = ds.dataset(ds.factory(path.name, **kwargs))
        assert isinstance(dataset, ds.FileSystemDataset)
        _check_dataset(dataset, table)

    # passing directly to dataset
    dataset = ds.dataset(path, **kwargs)
    assert isinstance(dataset, ds.FileSystemDataset)
    _check_dataset(dataset, table)

    dataset = ds.dataset(str(path), **kwargs)
    assert isinstance(dataset, ds.FileSystemDataset)
    _check_dataset(dataset, table)

    # passing list of files (even of length-1) gives UnionDataset
    dataset = ds.dataset([path], **kwargs)
    assert isinstance(dataset, ds.UnionDataset)
    _check_dataset(dataset, table)

    dataset = ds.dataset([str(path)], **kwargs)
    assert isinstance(dataset, ds.UnionDataset)
    _check_dataset(dataset, table)


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
    datasets = [
        ds.dataset(ds.factory([path1, path2])),
        ds.dataset(ds.factory([str(path1), str(path2)]))
    ]
    for dataset in datasets:
        assert dataset.schema.equals(table.schema)
        result = dataset.to_table(use_threads=False)  # deterministic row order
        assert result.equals(table)


@pytest.mark.parquet
def test_open_dataset_partitioned_directory(tempdir):
    import pyarrow.parquet as pq
    table = pa.table({'a': range(9), 'b': [0.] * 4 + [1.] * 5})

    path = tempdir / "dataset"
    path.mkdir()

    for part in range(3):
        part = path / "part={}".format(part)
        part.mkdir()
        pq.write_table(table, part / "test.parquet")

    # no partitioning specified, just read all individual files
    full_table = pa.concat_tables([table] * 3)
    _check_dataset_from_path(path, full_table)

    # specify partition scheme with discovery
    dataset = ds.dataset(
        str(path), partitioning=ds.partitioning(flavor="hive"))
    expected_schema = table.schema.append(pa.field("part", pa.int32()))
    assert dataset.schema.equals(expected_schema)

    # specify partition scheme with discovery and relative path
    with change_cwd(tempdir):
        dataset = ds.dataset(
            "dataset/", partitioning=ds.partitioning(flavor="hive"))
        expected_schema = table.schema.append(pa.field("part", pa.int32()))
        assert dataset.schema.equals(expected_schema)

    # specify partition scheme with string short-cut
    dataset = ds.dataset(str(path), partitioning="hive")
    assert dataset.schema.equals(expected_schema)

    # specify partition scheme with explicit scheme
    dataset = ds.dataset(
        str(path),
        partitioning=ds.partitioning(
            pa.schema([("part", pa.int8())]), flavor="hive"))
    expected_schema = table.schema.append(pa.field("part", pa.int8()))
    assert dataset.schema.equals(expected_schema)

    result = dataset.to_table(use_threads=False)
    expected = full_table.append_column(
        "part", pa.array(np.repeat([0, 1, 2], 9), type=pa.int8()))
    assert result.equals(expected)


@pytest.mark.parquet
def test_open_dataset_filesystem(tempdir):
    # single file
    table, path = _create_single_file(tempdir)

    # filesystem inferred from path
    dataset1 = ds.dataset(str(path))
    assert dataset1.schema.equals(table.schema)

    # filesystem specified
    dataset2 = ds.dataset(str(path), filesystem=fs.LocalFileSystem())
    assert dataset2.schema.equals(table.schema)

    # local filesystem specified with relative path
    with change_cwd(tempdir):
        dataset3 = ds.dataset("test.parquet", filesystem=fs.LocalFileSystem())
    assert dataset3.schema.equals(table.schema)

    # passing different filesystem
    with pytest.raises(FileNotFoundError):
        ds.dataset(str(path), filesystem=fs._MockFileSystem())


@pytest.mark.parquet
def test_open_dataset_unsupported_format(tempdir):
    _, path = _create_single_file(tempdir)
    with pytest.raises(ValueError, match="format 'blabla' is not supported"):
        ds.dataset([path], format="blabla")


@pytest.mark.parquet
def test_open_dataset_validate_sources(tempdir):
    _, path = _create_single_file(tempdir)
    dataset = ds.dataset(path)
    with pytest.raises(TypeError,
                       match="Dataset objects are currently not supported"):
        ds.dataset([dataset])


def test_open_dataset_from_source_additional_kwargs(multisourcefs):
    child = ds.FileSystemDatasetFactory(
        multisourcefs, fs.FileSelector('/plain'),
        format=ds.ParquetFileFormat()
    )
    with pytest.raises(ValueError, match="cannot pass any additional"):
        ds.dataset(child, format="parquet")


@pytest.mark.parquet
@pytest.mark.s3
def test_open_dataset_from_uri_s3(s3_connection, s3_server):
    # open dataset from non-localfs string path
    from pyarrow.fs import FileSystem
    import pyarrow.parquet as pq

    host, port, access_key, secret_key = s3_connection
    uri = (
        "s3://{}:{}@mybucket/data.parquet?scheme=http&endpoint_override={}:{}"
        .format(access_key, secret_key, host, port)
    )

    fs, path = FileSystem.from_uri(uri)

    fs.create_dir("mybucket")
    table = pa.table({'a': [1, 2, 3]})
    with fs.open_output_stream("mybucket/data.parquet") as out:
        pq.write_table(table, out)

    # full string URI
    dataset = ds.dataset(uri, format="parquet")
    assert dataset.to_table().equals(table)

    # passing filesystem object
    dataset = ds.dataset(path, format="parquet", filesystem=fs)
    assert dataset.to_table().equals(table)


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


def test_dataset_factory(multisourcefs):
    child = ds.factory('/plain', filesystem=multisourcefs, format='parquet')
    factory = ds.UnionDatasetFactory([child])

    # TODO(bkietz) reintroduce factory.children property
    assert len(factory.inspect_schemas()) == 1
    assert all(isinstance(s, pa.Schema) for s in factory.inspect_schemas())
    assert factory.inspect_schemas()[0].equals(child.inspect())
    assert factory.inspect().equals(child.inspect())
    assert isinstance(factory.finish(), ds.Dataset)


def test_multiple_factories(multisourcefs):
    src1 = ds.factory('/plain', filesystem=multisourcefs, format='parquet')
    src2 = ds.factory('/schema', filesystem=multisourcefs, format='parquet',
                      partitioning=['week', 'color'])
    src3 = ds.factory('/hive', filesystem=multisourcefs, format='parquet',
                      partitioning='hive')

    assembled = ds.dataset([src1, src2, src3])
    assert isinstance(assembled, ds.Dataset)

    expected_schema = pa.schema([
        ('date', pa.date32()),
        ('index', pa.int64()),
        ('value', pa.float64()),
        ('color', pa.string()),
        ('week', pa.int32()),
        ('year', pa.int32()),
        ('month', pa.int32()),
    ])
    assert assembled.schema.equals(expected_schema)


def test_multiple_factories_with_selectors(multisourcefs):
    # without partitioning
    dataset = ds.dataset(['/plain', '/schema', '/hive'],
                         filesystem=multisourcefs, format='parquet')
    expected_schema = pa.schema([
        ('date', pa.date32()),
        ('index', pa.int64()),
        ('value', pa.float64()),
        ('color', pa.string()),
    ])
    assert dataset.schema.equals(expected_schema)

    # with hive partitioning for two hive sources
    dataset = ds.dataset(['/hive', '/hive_color'], filesystem=multisourcefs,
                         format='parquet', partitioning='hive')
    expected_schema = pa.schema([
        ('date', pa.date32()),
        ('index', pa.int64()),
        ('value', pa.float64()),
        ('color', pa.string()),
        ('year', pa.int32()),
        ('month', pa.int32()),
    ])
    assert dataset.schema.equals(expected_schema)


@pytest.mark.parquet
def test_specified_schema(tempdir):
    import pyarrow.parquet as pq

    table = pa.table({'a': [1, 2, 3], 'b': [.1, .2, .3]})
    pq.write_table(table, tempdir / "data.parquet")

    def _check_dataset(schema, expected, expected_schema=None):
        dataset = ds.dataset(str(tempdir / "data.parquet"), schema=schema)
        if expected_schema is not None:
            assert dataset.schema.equals(expected_schema)
        else:
            assert dataset.schema.equals(schema)
        result = dataset.to_table()
        assert result.equals(expected)

    # no schema specified
    schema = None
    expected = table
    _check_dataset(schema, expected, expected_schema=table.schema)

    # identical schema specified
    schema = table.schema
    expected = table
    _check_dataset(schema, expected)

    # Specifying schema with change column order
    schema = pa.schema([('b', 'float64'), ('a', 'int64')])
    expected = pa.table([[.1, .2, .3], [1, 2, 3]], names=['b', 'a'])
    _check_dataset(schema, expected)

    # Specifying schema with missing column
    schema = pa.schema([('a', 'int64')])
    expected = pa.table({'a': [1, 2, 3]})
    _check_dataset(schema, expected)

    # Specifying schema with additional column
    schema = pa.schema([('a', 'int64'), ('c', 'int32')])
    expected = pa.table({'a': [1, 2, 3],
                         'c': pa.array([None, None, None], type='int32')})
    _check_dataset(schema, expected)

    # Specifying with incompatible schema
    schema = pa.schema([('a', 'int32'), ('b', 'float64')])
    dataset = ds.dataset(str(tempdir / "data.parquet"), schema=schema)
    assert dataset.schema.equals(schema)
    with pytest.raises(TypeError):
        dataset.to_table()


def test_ipc_format(tempdir):
    table = pa.table({'a': pa.array([1, 2, 3], type="int8"),
                      'b': pa.array([.1, .2, .3], type="float64")})

    path = str(tempdir / 'test.arrow')
    with pa.output_stream(path) as sink:
        writer = pa.RecordBatchFileWriter(sink, table.schema)
        writer.write_batch(table.to_batches()[0])
        writer.close()

    dataset = ds.dataset(path, format=ds.IpcFileFormat())
    result = dataset.to_table()
    assert result.equals(table)

    dataset = ds.dataset(path, format="ipc")
    result = dataset.to_table()
    assert result.equals(table)
