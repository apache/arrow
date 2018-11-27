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

from collections import OrderedDict

import pickle
import pytest

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.types as types


def get_many_types():
    # returning them from a function is required because of pa.dictionary
    # type holds a pyarrow array and test_array.py::test_toal_bytes_allocated
    # checks that the default memory pool has zero allocated bytes
    return (
        pa.null(),
        pa.bool_(),
        pa.int32(),
        pa.time32('s'),
        pa.time64('us'),
        pa.date32(),
        pa.timestamp('us'),
        pa.timestamp('us', tz='UTC'),
        pa.timestamp('us', tz='Europe/Paris'),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.decimal128(19, 4),
        pa.string(),
        pa.binary(),
        pa.binary(10),
        pa.list_(pa.int32()),
        pa.struct([pa.field('a', pa.int32()),
                   pa.field('b', pa.int8()),
                   pa.field('c', pa.string())]),
        pa.struct([pa.field('a', pa.int32(), nullable=False),
                   pa.field('b', pa.int8(), nullable=False),
                   pa.field('c', pa.string())]),
        pa.union([pa.field('a', pa.binary(10)),
                  pa.field('b', pa.string())], mode=pa.lib.UnionMode_DENSE),
        pa.union([pa.field('a', pa.binary(10)),
                  pa.field('b', pa.string())], mode=pa.lib.UnionMode_SPARSE),
        pa.union([pa.field('a', pa.binary(10), nullable=False),
                  pa.field('b', pa.string())], mode=pa.lib.UnionMode_SPARSE),
        pa.dictionary(pa.int32(), pa.array(['a', 'b', 'c']))
    )


def test_is_boolean():
    assert types.is_boolean(pa.bool_())
    assert not types.is_boolean(pa.int8())


def test_is_integer():
    signed_ints = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
    unsigned_ints = [pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]

    for t in signed_ints + unsigned_ints:
        assert types.is_integer(t)

    for t in signed_ints:
        assert types.is_signed_integer(t)
        assert not types.is_unsigned_integer(t)

    for t in unsigned_ints:
        assert types.is_unsigned_integer(t)
        assert not types.is_signed_integer(t)

    assert not types.is_integer(pa.float32())
    assert not types.is_signed_integer(pa.float32())


def test_is_floating():
    for t in [pa.float16(), pa.float32(), pa.float64()]:
        assert types.is_floating(t)

    assert not types.is_floating(pa.int32())


def test_is_null():
    assert types.is_null(pa.null())
    assert not types.is_null(pa.list_(pa.int32()))


def test_is_decimal():
    assert types.is_decimal(pa.decimal128(19, 4))
    assert not types.is_decimal(pa.int32())


def test_is_list():
    assert types.is_list(pa.list_(pa.int32()))
    assert not types.is_list(pa.int32())


def test_is_dictionary():
    assert types.is_dictionary(
        pa.dictionary(pa.int32(),
                      pa.array(['a', 'b', 'c'])))
    assert not types.is_dictionary(pa.int32())


def test_is_nested_or_struct():
    struct_ex = pa.struct([pa.field('a', pa.int32()),
                           pa.field('b', pa.int8()),
                           pa.field('c', pa.string())])

    assert types.is_struct(struct_ex)
    assert not types.is_struct(pa.list_(pa.int32()))

    assert types.is_nested(struct_ex)
    assert types.is_nested(pa.list_(pa.int32()))
    assert not types.is_nested(pa.int32())


def test_is_union():
    for mode in [pa.lib.UnionMode_SPARSE, pa.lib.UnionMode_DENSE]:
        assert types.is_union(pa.union([pa.field('a', pa.int32()),
                                        pa.field('b', pa.int8()),
                                        pa.field('c', pa.string())],
                                       mode=mode))
    assert not types.is_union(pa.list_(pa.int32()))


# TODO(wesm): is_map, once implemented


def test_is_binary_string():
    assert types.is_binary(pa.binary())
    assert not types.is_binary(pa.string())

    assert types.is_string(pa.string())
    assert types.is_unicode(pa.string())
    assert not types.is_string(pa.binary())

    assert types.is_fixed_size_binary(pa.binary(5))
    assert not types.is_fixed_size_binary(pa.binary())


def test_is_temporal_date_time_timestamp():
    date_types = [pa.date32(), pa.date64()]
    time_types = [pa.time32('s'), pa.time64('ns')]
    timestamp_types = [pa.timestamp('ms')]

    for case in date_types + time_types + timestamp_types:
        assert types.is_temporal(case)

    for case in date_types:
        assert types.is_date(case)
        assert not types.is_time(case)
        assert not types.is_timestamp(case)

    for case in time_types:
        assert types.is_time(case)
        assert not types.is_date(case)
        assert not types.is_timestamp(case)

    for case in timestamp_types:
        assert types.is_timestamp(case)
        assert not types.is_date(case)
        assert not types.is_time(case)

    assert not types.is_temporal(pa.int32())


def test_is_primitive():
    assert types.is_primitive(pa.int32())
    assert not types.is_primitive(pa.list_(pa.int32()))


def test_timestamp():
    for unit in ('s', 'ms', 'us', 'ns'):
        for tz in (None, 'UTC', 'Europe/Paris'):
            ty = pa.timestamp(unit, tz=tz)
            assert ty.unit == unit
            assert ty.tz == tz

    for invalid_unit in ('m', 'arbit', 'rary'):
        with pytest.raises(ValueError, match='Invalid TimeUnit string'):
            pa.timestamp(invalid_unit)


def test_time32_units():
    for valid_unit in ('s', 'ms'):
        ty = pa.time32(valid_unit)
        assert ty.unit == valid_unit

    for invalid_unit in ('m', 'us', 'ns'):
        error_msg = 'Invalid TimeUnit for time32: {}'.format(invalid_unit)
        with pytest.raises(ValueError, match=error_msg):
            pa.time32(invalid_unit)


def test_time64_units():
    for valid_unit in ('us', 'ns'):
        ty = pa.time64(valid_unit)
        assert ty.unit == valid_unit

    for invalid_unit in ('m', 's', 'ms'):
        error_msg = 'Invalid TimeUnit for time64: {}'.format(invalid_unit)
        with pytest.raises(ValueError, match=error_msg):
            pa.time64(invalid_unit)


def test_list_type():
    ty = pa.list_(pa.int64())
    assert ty.value_type == pa.int64()

    with pytest.raises(TypeError):
        pa.list_(None)


def test_struct_type():
    fields = [pa.field('a', pa.int64()),
              pa.field('a', pa.int32()),
              pa.field('b', pa.int32())]
    ty = pa.struct(fields)

    assert len(ty) == ty.num_children == 3
    assert list(ty) == fields
    assert ty[0].name == 'a'
    assert ty[2].type == pa.int32()
    with pytest.raises(IndexError):
        assert ty[3]

    assert ty['a'] == ty[1]
    assert ty['b'] == ty[2]
    with pytest.raises(KeyError):
        ty['c']

    with pytest.raises(TypeError):
        ty[None]

    for a, b in zip(ty, fields):
        a == b

    # Construct from list of tuples
    ty = pa.struct([('a', pa.int64()),
                    ('a', pa.int32()),
                    ('b', pa.int32())])
    assert list(ty) == fields
    for a, b in zip(ty, fields):
        a == b

    # Construct from mapping
    fields = [pa.field('a', pa.int64()),
              pa.field('b', pa.int32())]
    ty = pa.struct(OrderedDict([('a', pa.int64()),
                                ('b', pa.int32())]))
    assert list(ty) == fields
    for a, b in zip(ty, fields):
        a == b

    # Invalid args
    with pytest.raises(TypeError):
        pa.struct([('a', None)])


def test_union_type():
    def check_fields(ty, fields):
        assert ty.num_children == len(fields)
        assert [ty[i] for i in range(ty.num_children)] == fields

    fields = [pa.field('x', pa.list_(pa.int32())),
              pa.field('y', pa.binary())]
    for mode in ('sparse', pa.lib.UnionMode_SPARSE):
        ty = pa.union(fields, mode=mode)
        assert ty.mode == 'sparse'
        check_fields(ty, fields)
    for mode in ('dense', pa.lib.UnionMode_DENSE):
        ty = pa.union(fields, mode=mode)
        assert ty.mode == 'dense'
        check_fields(ty, fields)
    for mode in ('unknown', 2):
        with pytest.raises(ValueError, match='Invalid union mode'):
            pa.union(fields, mode=mode)


def test_dictionary_type():
    ty0 = pa.dictionary(pa.int32(), pa.array(['a', 'b', 'c']))
    assert ty0.index_type == pa.int32()
    assert isinstance(ty0.dictionary, pa.Array)
    assert ty0.dictionary.to_pylist() == ['a', 'b', 'c']
    assert ty0.ordered is False

    ty1 = pa.dictionary(pa.float32(), pa.array([1.0, 2.0]), ordered=True)
    assert ty1.index_type == pa.float32()
    assert isinstance(ty0.dictionary, pa.Array)
    assert ty1.dictionary.to_pylist() == [1.0, 2.0]
    assert ty1.ordered is True


def test_types_hashable():
    many_types = get_many_types()
    in_dict = {}
    for i, type_ in enumerate(many_types):
        assert hash(type_) == hash(type_)
        in_dict[type_] = i
    assert len(in_dict) == len(many_types)
    for i, type_ in enumerate(many_types):
        assert in_dict[type_] == i


def test_types_picklable():
    for ty in get_many_types():
        data = pickle.dumps(ty)
        assert pickle.loads(data) == ty


def test_fields_hashable():
    in_dict = {}
    fields = [pa.field('a', pa.int32()),
              pa.field('a', pa.int64()),
              pa.field('a', pa.int64(), nullable=False),
              pa.field('b', pa.int32()),
              pa.field('b', pa.int32(), nullable=False)]
    for i, field in enumerate(fields):
        in_dict[field] = i
    assert len(in_dict) == len(fields)
    for i, field in enumerate(fields):
        assert in_dict[field] == i


@pytest.mark.parametrize('t,check_func', [
    (pa.date32(), types.is_date32),
    (pa.date64(), types.is_date64),
    (pa.time32('s'), types.is_time32),
    (pa.time64('ns'), types.is_time64),
    (pa.int8(), types.is_int8),
    (pa.int16(), types.is_int16),
    (pa.int32(), types.is_int32),
    (pa.int64(), types.is_int64),
    (pa.uint8(), types.is_uint8),
    (pa.uint16(), types.is_uint16),
    (pa.uint32(), types.is_uint32),
    (pa.uint64(), types.is_uint64),
    (pa.float16(), types.is_float16),
    (pa.float32(), types.is_float32),
    (pa.float64(), types.is_float64)
])
def test_exact_primitive_types(t, check_func):
    assert check_func(t)


def test_type_id():
    # enum values are not exposed publicly
    for ty in get_many_types():
        assert isinstance(ty.id, int)


def test_bit_width():
    for ty, expected in [(pa.bool_(), 1),
                         (pa.int8(), 8),
                         (pa.uint32(), 32),
                         (pa.float16(), 16),
                         (pa.decimal128(19, 4), 128),
                         (pa.binary(42), 42 * 8)]:
        assert ty.bit_width == expected
    for ty in [pa.binary(), pa.string(), pa.list_(pa.int16())]:
        with pytest.raises(ValueError, match="fixed width"):
            ty.bit_width


def test_fixed_size_binary_byte_width():
    ty = pa.binary(5)
    assert ty.byte_width == 5


def test_decimal_properties():
    ty = pa.decimal128(19, 4)
    assert ty.byte_width == 16
    assert ty.precision == 19
    assert ty.scale == 4


def test_type_equality_operators():
    many_types = get_many_types()
    non_pyarrow = ('foo', 16, {'s', 'e', 't'})

    for index, ty in enumerate(many_types):
        # could use two parametrization levels,
        # but that'd bloat pytest's output
        for i, other in enumerate(many_types + non_pyarrow):
            if i == index:
                assert ty == other
            else:
                assert ty != other


def test_field_basic():
    t = pa.string()
    f = pa.field('foo', t)

    assert f.name == 'foo'
    assert f.nullable
    assert f.type is t
    assert repr(f) == "pyarrow.Field<foo: string>"

    f = pa.field('foo', t, False)
    assert not f.nullable

    with pytest.raises(TypeError):
        pa.field('foo', None)


def test_field_equals():
    meta1 = {b'foo': b'bar'}
    meta2 = {b'bizz': b'bazz'}

    f1 = pa.field('a', pa.int8(), nullable=True)
    f2 = pa.field('a', pa.int8(), nullable=True)
    f3 = pa.field('a', pa.int8(), nullable=False)
    f4 = pa.field('a', pa.int16(), nullable=False)
    f5 = pa.field('b', pa.int16(), nullable=False)
    f6 = pa.field('a', pa.int8(), nullable=True, metadata=meta1)
    f7 = pa.field('a', pa.int8(), nullable=True, metadata=meta1)
    f8 = pa.field('a', pa.int8(), nullable=True, metadata=meta2)

    assert f1.equals(f2)
    assert f6.equals(f7)
    assert not f1.equals(f3)
    assert not f1.equals(f4)
    assert not f3.equals(f4)
    assert not f1.equals(f6)
    assert not f4.equals(f5)
    assert not f7.equals(f8)


def test_field_equality_operators():
    f1 = pa.field('a', pa.int8(), nullable=True)
    f2 = pa.field('a', pa.int8(), nullable=True)
    f3 = pa.field('b', pa.int8(), nullable=True)
    f4 = pa.field('b', pa.int8(), nullable=False)

    assert f1 == f2
    assert f1 != f3
    assert f3 != f4
    assert f1 != 'foo'


def test_field_metadata():
    f1 = pa.field('a', pa.int8())
    f2 = pa.field('a', pa.int8(), metadata={})
    f3 = pa.field('a', pa.int8(), metadata={b'bizz': b'bazz'})

    assert f1.metadata is None
    assert f2.metadata == {}
    assert f3.metadata[b'bizz'] == b'bazz'


def test_field_add_remove_metadata():
    f0 = pa.field('foo', pa.int32())

    assert f0.metadata is None

    metadata = {b'foo': b'bar', b'pandas': b'badger'}

    f1 = f0.add_metadata(metadata)
    assert f1.metadata == metadata

    f3 = f1.remove_metadata()
    assert f3.metadata is None

    # idempotent
    f4 = f3.remove_metadata()
    assert f4.metadata is None

    f5 = pa.field('foo', pa.int32(), True, metadata)
    f6 = f0.add_metadata(metadata)
    assert f5.equals(f6)


def test_empty_table():
    schema = pa.schema([
        pa.field('oneField', pa.int64())
    ])
    table = schema.empty_table()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 0
    assert table.schema == schema


def test_is_integer_value():
    assert pa.types.is_integer_value(1)
    assert pa.types.is_integer_value(np.int64(1))
    assert not pa.types.is_integer_value('1')


def test_is_float_value():
    assert not pa.types.is_float_value(1)
    assert pa.types.is_float_value(1.)
    assert pa.types.is_float_value(np.float64(1))
    assert not pa.types.is_float_value('1.0')


def test_is_boolean_value():
    assert not pa.types.is_boolean_value(1)
    assert pa.types.is_boolean_value(True)
    assert pa.types.is_boolean_value(False)
    assert pa.types.is_boolean_value(np.bool_(True))
    assert pa.types.is_boolean_value(np.bool_(False))


@pytest.mark.parametrize('data', [
    list(range(10)),
    pd.Categorical(list(range(10))),
    ['foo', 'bar', None, 'baz', 'qux'],
    np.array([
        '2007-07-13T01:23:34.123456789',
        '2006-01-13T12:34:56.432539784',
        '2010-08-13T05:46:57.437699912'
    ], dtype='datetime64[ns]')
])
def test_schema_from_pandas(data):
    df = pd.DataFrame({'a': data})
    schema = pa.Schema.from_pandas(df)
    expected = pa.Table.from_pandas(df).schema
    assert schema == expected
