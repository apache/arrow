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

import decimal
import io
import math
import random

try:
    import numpy as np
except ImportError:
    np = None
import pytest

import pyarrow as pa
from pyarrow.tests import util
from pyarrow.tests.parquet.common import _check_roundtrip, _roundtrip_table

try:
    import pyarrow.parquet as pq
    from pyarrow.tests.parquet.common import _read_table, _write_table
except ImportError:
    pq = None


try:
    import pandas as pd
    import pandas.testing as tm

    from pyarrow.tests.pandas_examples import (dataframe_with_arrays,
                                               dataframe_with_lists)
    from pyarrow.tests.parquet.common import alltypes_sample
except ImportError:
    pd = tm = None


# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not parquet'
pytestmark = pytest.mark.parquet


# General roundtrip of data types
# -----------------------------------------------------------------------------


@pytest.mark.pandas
@pytest.mark.parametrize('chunk_size', [None, 1000])
def test_parquet_2_6_roundtrip(tempdir, chunk_size):
    df = alltypes_sample(size=10000, categorical=True)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df)
    assert arrow_table.schema.pandas_metadata is not None

    _write_table(arrow_table, filename, version='2.6',
                 chunk_size=chunk_size)
    table_read = pq.read_pandas(filename)
    assert table_read.schema.pandas_metadata is not None

    read_metadata = table_read.schema.metadata
    assert arrow_table.schema.metadata == read_metadata

    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@pytest.mark.pandas
def test_parquet_1_0_roundtrip(tempdir):
    size = 10000
    np.random.seed(0)
    df = pd.DataFrame({
        'uint8': np.arange(size, dtype=np.uint8),
        'uint16': np.arange(size, dtype=np.uint16),
        'uint32': np.arange(size, dtype=np.uint32),
        'uint64': np.arange(size, dtype=np.uint64),
        'int8': np.arange(size, dtype=np.int16),
        'int16': np.arange(size, dtype=np.int16),
        'int32': np.arange(size, dtype=np.int32),
        'int64': np.arange(size, dtype=np.int64),
        'float32': np.arange(size, dtype=np.float32),
        'float64': np.arange(size, dtype=np.float64),
        'bool': np.random.randn(size) > 0,
        'str': [str(x) for x in range(size)],
        'str_with_nulls': [None] + [str(x) for x in range(size - 2)] + [None],
        'empty_str': [''] * size
    })
    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df)
    _write_table(arrow_table, filename, version='1.0')
    table_read = _read_table(filename)
    df_read = table_read.to_pandas()

    # We pass uint32_t as int64_t if we write Parquet version 1.0
    df['uint32'] = df['uint32'].values.astype(np.int64)

    tm.assert_frame_equal(df, df_read)


# Dictionary
# -----------------------------------------------------------------------------


def _simple_table_write_read(table):
    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    contents = bio.getvalue()
    return pq.read_table(
        pa.BufferReader(contents)
    )


@pytest.mark.pandas
def test_direct_read_dictionary():
    # ARROW-3325
    repeats = 10
    nunique = 5

    data = [
        [util.rands(10) for i in range(nunique)] * repeats,

    ]
    table = pa.table(data, names=['f0'])

    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    contents = bio.getvalue()

    result = pq.read_table(pa.BufferReader(contents),
                           read_dictionary=['f0'])

    # Compute dictionary-encoded subfield
    expected = pa.table([table[0].dictionary_encode()], names=['f0'])
    assert result.equals(expected)


@pytest.mark.pandas
def test_direct_read_dictionary_subfield():
    repeats = 10
    nunique = 5

    data = [
        [[util.rands(10)] for i in range(nunique)] * repeats,
    ]
    table = pa.table(data, names=['f0'])

    bio = pa.BufferOutputStream()
    pq.write_table(table, bio)
    contents = bio.getvalue()
    result = pq.read_table(pa.BufferReader(contents),
                           read_dictionary=['f0.list.element'])

    arr = pa.array(data[0])
    values_as_dict = arr.values.dictionary_encode()

    inner_indices = values_as_dict.indices.cast('int32')
    new_values = pa.DictionaryArray.from_arrays(inner_indices,
                                                values_as_dict.dictionary)

    offsets = pa.array(range(51), type='int32')
    expected_arr = pa.ListArray.from_arrays(offsets, new_values)
    expected = pa.table([expected_arr], names=['f0'])

    assert result.equals(expected)
    assert result[0].num_chunks == 1


@pytest.mark.numpy
def test_dictionary_array_automatically_read():
    # ARROW-3246

    # Make a large dictionary, a little over 4MB of data
    dict_length = 4000
    dict_values = pa.array([('x' * 1000 + f'_{i}')
                            for i in range(dict_length)])

    num_chunks = 10
    chunk_size = 100
    chunks = []
    for i in range(num_chunks):
        indices = np.random.randint(0, dict_length,
                                    size=chunk_size).astype(np.int32)
        chunks.append(pa.DictionaryArray.from_arrays(pa.array(indices),
                                                     dict_values))

    table = pa.table([pa.chunked_array(chunks)], names=['f0'])
    result = _simple_table_write_read(table)

    assert result.equals(table)

    # The only key in the metadata was the Arrow schema key
    assert result.schema.metadata is None


# Decimal
# -----------------------------------------------------------------------------


@pytest.mark.pandas
def test_decimal_roundtrip(tempdir):
    num_values = 10

    columns = {}
    for precision in range(1, 39):
        for scale in range(0, precision + 1):
            with util.random_seed(0):
                random_decimal_values = [
                    util.randdecimal(precision, scale)
                    for _ in range(num_values)
                ]
            column_name = f'dec_precision_{precision}_scale_{scale}'
            columns[column_name] = random_decimal_values

    expected = pd.DataFrame(columns)
    filename = tempdir / 'decimals.parquet'
    string_filename = str(filename)
    table = pa.Table.from_pandas(expected)
    _write_table(table, string_filename)
    result_table = _read_table(string_filename)
    result = result_table.to_pandas()
    tm.assert_frame_equal(result, expected)


@pytest.mark.pandas
@pytest.mark.xfail(
    raises=OSError, reason='Parquet does not support negative scale'
)
def test_decimal_roundtrip_negative_scale(tempdir):
    expected = pd.DataFrame({'decimal_num': [decimal.Decimal('1.23E4')]})
    filename = tempdir / 'decimals.parquet'
    string_filename = str(filename)
    t = pa.Table.from_pandas(expected)
    _write_table(t, string_filename)
    result_table = _read_table(string_filename)
    result = result_table.to_pandas()
    tm.assert_frame_equal(result, expected)


# List types
# -----------------------------------------------------------------------------


@pytest.mark.parametrize('dtype', [int, float])
def test_single_pylist_column_roundtrip(tempdir, dtype,):
    filename = tempdir / f'single_{dtype.__name__}_column.parquet'
    data = [pa.array(list(map(dtype, range(5))))]
    table = pa.Table.from_arrays(data, names=['a'])
    _write_table(table, filename)
    table_read = _read_table(filename)
    for i in range(table.num_columns):
        col_written = table[i]
        col_read = table_read[i]
        assert table.field(i).name == table_read.field(i).name
        assert col_read.num_chunks == 1
        data_written = col_written.chunk(0)
        data_read = col_read.chunk(0)
        assert data_written.equals(data_read)


def test_empty_lists_table_roundtrip():
    # ARROW-2744: Shouldn't crash when writing an array of empty lists
    arr = pa.array([[], []], type=pa.list_(pa.int32()))
    table = pa.Table.from_arrays([arr], ["A"])
    _check_roundtrip(table)


def test_nested_list_nonnullable_roundtrip_bug():
    # Reproduce failure in ARROW-5630
    typ = pa.list_(pa.field("item", pa.float32(), False))
    num_rows = 10000
    t = pa.table([
        pa.array(([[0] * ((i + 5) % 10) for i in range(0, 10)] *
                  (num_rows // 10)), type=typ)
    ], ['a'])
    _check_roundtrip(
        t, data_page_size=4096)


def test_nested_list_struct_multiple_batches_roundtrip(tempdir):
    # Reproduce failure in ARROW-11024
    data = [[{'x': 'abc', 'y': 'abc'}]]*100 + [[{'x': 'abc', 'y': 'gcb'}]]*100
    table = pa.table([pa.array(data)], names=['column'])
    _check_roundtrip(
        table, row_group_size=20)

    # Reproduce failure in ARROW-11069 (plain non-nested structs with strings)
    data = pa.array(
        [{'a': '1', 'b': '2'}, {'a': '3', 'b': '4'}, {'a': '5', 'b': '6'}]*10
    )
    table = pa.table({'column': data})
    _check_roundtrip(table, row_group_size=10)


def test_writing_empty_lists():
    # ARROW-2591: [Python] Segmentation fault issue in pq.write_table
    arr1 = pa.array([[], []], pa.list_(pa.int32()))
    table = pa.Table.from_arrays([arr1], ['list(int32)'])
    _check_roundtrip(table)


@pytest.mark.pandas
def test_column_of_arrays(tempdir):
    df, schema = dataframe_with_arrays()

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    _write_table(arrow_table, filename, version='2.6', coerce_timestamps='ms')
    table_read = _read_table(filename)
    df_read = table_read.to_pandas()
    tm.assert_frame_equal(df, df_read)


@pytest.mark.pandas
def test_column_of_lists(tempdir):
    df, schema = dataframe_with_lists(parquet_compatible=True)

    filename = tempdir / 'pandas_roundtrip.parquet'
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    _write_table(arrow_table, filename, version='2.6')
    table_read = _read_table(filename)
    df_read = table_read.to_pandas()

    tm.assert_frame_equal(df, df_read)


def test_large_list_records():
    # This was fixed in PARQUET-1100

    list_lengths = [random.randint(0, 500) for _ in range(50)]
    list_lengths[::10] = [0, 0, 0, 0, 0]

    list_values = [list(map(int, [random.randint(0, 100) for _ in range(x)]))
                   if i % 8 else None
                   for i, x in enumerate(list_lengths)]

    a1 = pa.array(list_values)

    table = pa.Table.from_arrays([a1], ['int_lists'])
    _check_roundtrip(table)


list_types = [
    (pa.ListType, pa.list_),
    (pa.LargeListType, pa.large_list),
]


def test_list_types():
    data = [[1, 2, None]] * 50
    for _, in_factory in list_types:
        array = pa.array(data, type=in_factory(pa.int32()))
        table = pa.Table.from_arrays([array], ['lists'])
        for out_type, out_factory in list_types:
            for store_schema in (True, False):
                if store_schema:
                    expected_table = table
                else:
                    expected_table = pa.Table.from_arrays(
                        [pa.array(data, type=out_factory(pa.int32()))], ['lists'])
                result = _roundtrip_table(
                    table, write_table_kwargs=dict(store_schema=store_schema),
                    read_table_kwargs=dict(list_type=out_type))
                assert result == expected_table


@pytest.mark.pandas
def test_parquet_nested_convenience(tempdir):
    # ARROW-1684
    df = pd.DataFrame({
        'a': [[1, 2, 3], None, [4, 5], []],
        'b': [[1.], None, None, [6., 7.]],
    })

    path = str(tempdir / 'nested_convenience.parquet')

    table = pa.Table.from_pandas(df, preserve_index=False)
    _write_table(table, path)

    read = pq.read_table(
        path, columns=['a'])
    tm.assert_frame_equal(read.to_pandas(), df[['a']])

    read = pq.read_table(
        path, columns=['a', 'b'])
    tm.assert_frame_equal(read.to_pandas(), df)


# Binary
# -----------------------------------------------------------------------------


def test_fixed_size_binary():
    t0 = pa.binary(10)
    data = [b'fooooooooo', None, b'barooooooo', b'quxooooooo']
    a0 = pa.array(data, type=t0)

    table = pa.Table.from_arrays([a0],
                                 ['binary[10]'])
    _check_roundtrip(table)


def test_binary_types():
    types = [pa.binary(), pa.large_binary(), pa.binary_view()]
    data = [b'abc', None, b'defg', b'x' * 30]
    for in_type in types:
        array = pa.array(data, in_type)
        table = pa.Table.from_arrays([array], ['binary'])
        for out_type in types:
            for store_schema in (False, True):
                result = _roundtrip_table(
                    table, write_table_kwargs=dict(store_schema=store_schema),
                    read_table_kwargs=dict(binary_type=out_type))
                if store_schema:
                    expected_table = table
                else:
                    expected_table = pa.Table.from_arrays(
                        [pa.array(data, out_type)], ['binary'])
                assert result == expected_table


# Large types
# -----------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.large_memory
def test_large_table_int32_overflow():
    size = np.iinfo('int32').max + 1

    arr = np.ones(size, dtype='uint8')

    parr = pa.array(arr, type=pa.uint8())

    table = pa.Table.from_arrays([parr], names=['one'])
    f = io.BytesIO()
    _write_table(table, f)


def _simple_table_roundtrip(table, **write_kwargs):
    stream = pa.BufferOutputStream()
    _write_table(table, stream, **write_kwargs)
    buf = stream.getvalue()
    return _read_table(buf)


@pytest.mark.slow
@pytest.mark.large_memory
def test_byte_array_exactly_2gb():
    # Test edge case reported in ARROW-3762
    val = b'x' * (1 << 10)

    base = pa.array([val] * ((1 << 21) - 1))
    cases = [
        [b'x' * 1023],  # 2^31 - 1
        [b'x' * 1024],  # 2^31
        [b'x' * 1025]   # 2^31 + 1
    ]
    for case in cases:
        values = pa.chunked_array([base, pa.array(case)])
        t = pa.table([values], names=['f0'])
        result = _simple_table_roundtrip(
            t, use_dictionary=False)
        assert t.equals(result)


@pytest.mark.slow
@pytest.mark.pandas
@pytest.mark.large_memory
def test_binary_array_overflow_to_chunked():
    # ARROW-3762

    # 2^31 + 1 bytes
    values = [b'x'] + [
        b'x' * (1 << 20)
    ] * 2 * (1 << 10)
    df = pd.DataFrame({'byte_col': values})

    tbl = pa.Table.from_pandas(df, preserve_index=False)
    read_tbl = _simple_table_roundtrip(tbl)

    col0_data = read_tbl[0]
    assert isinstance(col0_data, pa.ChunkedArray)

    # Split up into 2GB chunks
    assert col0_data.num_chunks == 2

    assert tbl.equals(read_tbl)


@pytest.mark.slow
@pytest.mark.pandas
@pytest.mark.large_memory
def test_list_of_binary_large_cell():
    # ARROW-4688
    data = []

    # TODO(wesm): handle chunked children
    # 2^31 - 1 bytes in a single cell
    # data.append([b'x' * (1 << 20)] * 2047 + [b'x' * ((1 << 20) - 1)])

    # A little under 2GB in cell each containing approximately 10MB each
    data.extend([[b'x' * 1000000] * 10] * 214)

    arr = pa.array(data)
    table = pa.Table.from_arrays([arr], ['chunky_cells'])
    read_table = _simple_table_roundtrip(table)
    assert table.equals(read_table)


def test_large_binary_and_binary_view():
    data = [b'foo', b'bar'] * 50
    for type in [pa.large_binary(), pa.binary_view()]:
        arr = pa.array(data, type=type)
        table = pa.Table.from_arrays([arr], names=['strs'])
        for use_dictionary in [False, True]:
            _check_roundtrip(table, use_dictionary=use_dictionary)


@pytest.mark.slow
@pytest.mark.large_memory
def test_large_binary_and_binary_view_huge():
    s = b'xy' * 997
    data = [s] * ((1 << 33) // len(s))
    for type in [pa.large_binary(), pa.binary_view()]:
        arr = pa.array(data, type=type)
        table = pa.Table.from_arrays([arr], names=['strs'])
        for use_dictionary in [False, True]:
            _check_roundtrip(table, use_dictionary=use_dictionary)
        del arr, table


@pytest.mark.large_memory
def test_large_binary_overflow():
    s = b'x' * (1 << 31)
    arr = pa.array([s], type=pa.large_binary())
    table = pa.Table.from_arrays([arr], names=['strs'])
    for use_dictionary in [False, True]:
        writer = pa.BufferOutputStream()
        with pytest.raises(
                pa.ArrowInvalid,
                match="Parquet cannot store strings with size 2GB or more"):
            _write_table(table, writer, use_dictionary=use_dictionary)


@pytest.mark.parametrize("storage_type", (
    pa.string(), pa.large_string()))
def test_json_extension_type(storage_type):
    data = ['{"a": 1}', '{"b": 2}', None]
    arr = pa.array(data, type=pa.json_(storage_type))

    table = pa.table([arr], names=["ext"])

    # With defaults, this should roundtrip (because store_schema=True)
    _check_roundtrip(table, table)

    # When store_schema is False, we get a string back by default
    _check_roundtrip(
        table,
        pa.table({"ext": pa.array(data, pa.string())}),
        {"arrow_extensions_enabled": False},
        store_schema=False)

    # With arrow_extensions_enabled=True on read, we get a arrow.json back
    # (but with string() storage)
    _check_roundtrip(
        table,
        pa.table({"ext": pa.array(data, pa.json_(pa.string()))}),
        {"arrow_extensions_enabled": True},
        store_schema=False)


def test_uuid_extension_type():
    data = [
        b'\xe4`\xf9p\x83QGN\xac\x7f\xa4g>K\xa8\xcb',
        b'\x1et\x14\x95\xee\xd5C\xea\x9b\xd7s\xdc\x91BK\xaf',
        None
    ]
    arr = pa.array(data, type=pa.uuid())

    table = pa.table([arr], names=["ext"])

    _check_roundtrip(table, table)
    _check_roundtrip(
        table,
        pa.table({"ext": pa.array(data, pa.binary(16))}),
        {"arrow_extensions_enabled": False},
        store_schema=False)
    _check_roundtrip(
        table,
        table,
        {"arrow_extensions_enabled": True},
        store_schema=False)


@pytest.mark.pandas
def test_uuid_roundtrip(tempdir):
    import uuid
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    df = pd.DataFrame({"id": [u1, None, u2]})
    table = pa.Table.from_pandas(df)
    assert table.column("id").type == pa.uuid()

    path = tempdir / "uuid_pandas_roundtrip.parquet"
    pq.write_table(table, path)
    read_table = pq.read_table(path)
    assert read_table.column("id").type == pa.uuid()

    result_df = read_table.to_pandas()
    assert isinstance(result_df.loc[0, "id"], uuid.UUID)
    assert isinstance(result_df.loc[2, "id"], uuid.UUID)
    assert result_df.loc[0, "id"] == u1
    assert result_df.loc[2, "id"] == u2
    assert pd.isna(result_df.loc[1, "id"])


def test_undefined_logical_type(parquet_test_datadir):
    test_file = f"{parquet_test_datadir}/unknown-logical-type.parquet"

    table = _read_table(test_file)
    assert table.column_names == ["column with known type", "column with unknown type"]
    assert table["column with unknown type"].to_pylist() == [
        b"unknown string 1",
        b"unknown string 2",
        b"unknown string 3"
    ]


# Parquet FINITE_VECTOR logical type
# -----------------------------------------------------------------------------

def _write_finite_vector_table(table, **write_table_kwargs):
    buf = pa.BufferOutputStream()
    _write_table(table, buf, use_finite_vector_logical_type=True, **write_table_kwargs)
    return buf.getvalue()


def _parquet_schema_string(buf):
    return repr(pq.ParquetFile(pa.BufferReader(buf)).schema)


@pytest.mark.parametrize("value_type", [
    pa.float16(), pa.float32(), pa.float64(), pa.bool_(), pa.int8(),
    pa.int32(), pa.int64(), pa.uint64(), pa.decimal128(10, 2),
])
def test_finite_vector_roundtrip(value_type):
    if value_type == pa.float16():
        if np is None:
            pytest.skip("float16 values require numpy")
        values = [np.float16(v) for v in (1.0, -2.0, 0.5, 65504.0)]
    elif value_type == pa.bool_():
        values = [True, False, False, True]
    elif pa.types.is_decimal(value_type):
        values = [decimal.Decimal(v) for v in ("1.25", "-3.50", "0.00", "99.99")]
    else:
        values = [1, 0, 3, 4]
    vectors = [values[:2], None, values[2:]]
    # pa.list_(value_type, 2) has a nullable element field
    table = pa.table({"v": pa.array(vectors, pa.list_(value_type, 2))})

    buf = _write_finite_vector_table(table)
    assert "group field_id=-1 v (FiniteVector(num_elements=2))" in \
        _parquet_schema_string(buf)
    metadata = pq.read_metadata(pa.BufferReader(buf))
    assert metadata.schema.column(0).path == "v.list.element"
    assert metadata.schema.column(0).max_definition_level == 2

    # The stored Arrow schema restores the original type
    result = _read_table(pa.BufferReader(buf))
    assert result.schema == table.schema
    assert result.equals(table)

    # Without it, FINITE_VECTOR is read as a FixedSizeList with required elements
    buf = _write_finite_vector_table(table, store_schema=False)
    result = _read_table(pa.BufferReader(buf))
    expected_type = pa.list_(pa.field("element", value_type, nullable=False), 2)
    assert result.schema == pa.schema([pa.field("v", expected_type)])
    assert result.column("v").to_pylist() == table.column("v").to_pylist()


def test_finite_vector_nested_roundtrip():
    vector_type = pa.list_(pa.float32(), 3)
    table = pa.table({
        "list_of_vectors": pa.array(
            [[[1, 2, 3], None], None, []], pa.list_(vector_type)),
        "struct_with_vector": pa.array(
            [{"a": 1, "v": [1, 2, 3]}, None, {"a": None, "v": None}],
            pa.struct([("a", pa.int32()), ("v", vector_type)])),
        # Null structs carry no elements for their required vector
        "struct_with_required_vector": pa.array(
            [{"v": [1, 2, 3]}, None, {"v": [4, 5, 6]}],
            pa.struct([pa.field("v", vector_type, nullable=False)])),
        "map_of_vectors": pa.array(
            [[("x", [1, 2, 3])], None, [("y", None)]],
            pa.map_(pa.string(), vector_type)),
        # Only the inner FixedSizeList is a FINITE_VECTOR
        "vector_of_vectors": pa.array(
            [[[1, 2, 3], [4, 5, 6]], None, [None, [7, 8, 9]]],
            pa.list_(vector_type, 2)),
    })
    buf = _write_finite_vector_table(table)
    assert _parquet_schema_string(buf).count("(FiniteVector(num_elements=3))") == 5
    result = _read_table(pa.BufferReader(buf))
    assert result.schema == table.schema
    assert result.equals(table)


def test_finite_vector_fixed_shape_tensor_roundtrip():
    tensor_type = pa.fixed_shape_tensor(pa.float32(), [2, 2])
    storage = pa.array([[1, 2, 3, 4], [5, 6, 7, 8]], pa.list_(pa.float32(), 4))
    table = pa.table({"t": pa.ExtensionArray.from_storage(tensor_type, storage)})
    buf = _write_finite_vector_table(table)
    assert "(FiniteVector(num_elements=4))" in _parquet_schema_string(buf)
    result = _read_table(pa.BufferReader(buf))
    assert result.schema == table.schema
    assert result.equals(table)


def test_finite_vector_disabled_by_default():
    table = pa.table({"v": pa.array([[1, 2], None], pa.list_(pa.float32(), 2))})
    buf = pa.BufferOutputStream()
    _write_table(table, buf)
    schema_string = _parquet_schema_string(buf.getvalue())
    assert "FiniteVector" not in schema_string
    assert "group field_id=-1 v (List)" in schema_string


@pytest.mark.parametrize("value_type", [
    pa.string(), pa.binary(), pa.date32(), pa.timestamp("us"), pa.list_(pa.int32()),
    pa.null(),
])
def test_finite_vector_ineligible_falls_back_to_list(value_type):
    arr = pa.array([None, None], pa.list_(value_type, 2))
    table = pa.table({"v": arr})
    buf = _write_finite_vector_table(table)
    schema_string = _parquet_schema_string(buf)
    assert "FiniteVector" not in schema_string
    assert "group field_id=-1 v (List)" in schema_string
    result = _read_table(pa.BufferReader(buf))
    assert result.equals(table)


class DictionaryFloatType(pa.ExtensionType):
    # An extension type whose storage is dictionary-encoded floats

    def __init__(self):
        super().__init__(pa.dictionary(pa.int32(), pa.float32()),
                         "pyarrow.tests.parquet.dictionary_float")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


class WrapperType(pa.ExtensionType):
    # An extension type over an arbitrary storage type, used to nest extensions

    def __init__(self, storage_type, name):
        super().__init__(storage_type, name)

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls(storage_type, "pyarrow.tests.parquet.wrapper")


def test_finite_vector_nested_extension_types():
    # Extension<Extension<FixedSizeList<float>[2]>>: every extension layer
    # must be unwrapped when the element is made required for FINITE_VECTOR.
    storage_type = pa.list_(pa.float32(), 2)
    inner_type = WrapperType(storage_type, "pyarrow.tests.parquet.inner")
    outer_type = WrapperType(inner_type, "pyarrow.tests.parquet.outer")
    storage = pa.array([[1, 2], None, [3, 4]], storage_type)
    inner = pa.ExtensionArray.from_storage(inner_type, storage)
    outer = pa.ExtensionArray.from_storage(outer_type, inner)
    table = pa.table({"v": outer})

    buf = _write_finite_vector_table(table)
    assert "(FiniteVector(num_elements=2))" in _parquet_schema_string(buf)
    # The extension types are not registered, so the storage is read back
    result = _read_table(pa.BufferReader(buf))
    assert result.column("v").to_pylist() == storage.to_pylist()


@pytest.mark.parametrize("wrap_in_extension", [False, True])
def test_finite_vector_dictionary_elements_fall_back_to_list(wrap_in_extension):
    # Dictionary-encoded values are not validated as FINITE_VECTOR elements, so such
    # columns are written as ordinary LISTs even when the dictionary contains
    # values a FINITE_VECTOR would reject.
    values = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1, 0], pa.int32()),
        pa.array([1.0, float("nan")], pa.float32()))
    if wrap_in_extension:
        ext_type = DictionaryFloatType()
        pa.register_extension_type(ext_type)
        try:
            values = pa.ExtensionArray.from_storage(ext_type, values)
            table = pa.table({"v": pa.FixedSizeListArray.from_arrays(values, 2)})
            buf = _write_finite_vector_table(table)
            result = _read_table(pa.BufferReader(buf))
        finally:
            pa.unregister_extension_type(ext_type.extension_name)
    else:
        table = pa.table({"v": pa.FixedSizeListArray.from_arrays(values, 2)})
        buf = _write_finite_vector_table(table)
        result = _read_table(pa.BufferReader(buf))
    schema_string = _parquet_schema_string(buf)
    assert "FiniteVector" not in schema_string
    assert "group field_id=-1 v (List)" in schema_string
    # Dictionary-encoded floats are read back decoded
    assert pa.types.is_float32(result.schema.field("v").type.value_type)
    [first, second] = result.column("v").to_pylist()
    assert first[0] == 1.0 and math.isnan(first[1])
    assert math.isnan(second[0]) and second[1] == 1.0


@pytest.mark.parametrize("value_type", [pa.float32(), pa.float64()])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_finite_vector_non_finite_element_raises(value_type, value):
    table = pa.table({"v": pa.array([[1, 2], [3, value]], pa.list_(value_type, 2))})
    with pytest.raises(pa.ArrowInvalid,
                       match="FINITE_VECTOR column 'v.list.element' contains a "
                             "non-finite element"):
        _write_finite_vector_table(table)
    # Allowed when written as an ordinary LIST
    result = _roundtrip_table(table)
    assert result.schema == table.schema


def test_finite_vector_null_element_raises():
    table = pa.table({"v": pa.array([[1, 2], [3, None]], pa.list_(pa.float32(), 2))})
    with pytest.raises(pa.ArrowInvalid,
                       match="FINITE_VECTOR column 'v.list.element' contains a null "
                             "element"):
        _write_finite_vector_table(table)


def test_finite_vector_dataset_write_option(tempdir):
    ds = pytest.importorskip("pyarrow.dataset")
    table = pa.table({"v": pa.array([[1, 2], None], pa.list_(pa.float32(), 2))})
    file_options = ds.ParquetFileFormat().make_write_options(
        use_finite_vector_logical_type=True)
    ds.write_dataset(table, tempdir, format="parquet", file_options=file_options)
    path = next(tempdir.glob("*.parquet"))
    assert "(FiniteVector(num_elements=2))" in repr(pq.ParquetFile(path).schema)
    assert pq.read_table(path).equals(table)
