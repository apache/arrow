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
import numpy as np

import pyarrow as pa


def test_type_integers():
    dtypes = ['int8', 'int16', 'int32', 'int64',
              'uint8', 'uint16', 'uint32', 'uint64']

    for name in dtypes:
        factory = getattr(pa, name)
        t = factory()
        assert str(t) == name


def test_type_to_pandas_dtype():
    M8_ns = np.dtype('datetime64[ns]')
    cases = [
        (pa.null(), np.float64),
        (pa.bool_(), np.bool_),
        (pa.int8(), np.int8),
        (pa.int16(), np.int16),
        (pa.int32(), np.int32),
        (pa.int64(), np.int64),
        (pa.uint8(), np.uint8),
        (pa.uint16(), np.uint16),
        (pa.uint32(), np.uint32),
        (pa.uint64(), np.uint64),
        (pa.float16(), np.float16),
        (pa.float32(), np.float32),
        (pa.float64(), np.float64),
        (pa.date32(), M8_ns),
        (pa.date64(), M8_ns),
        (pa.timestamp('ms'), M8_ns),
        (pa.binary(), np.object_),
        (pa.binary(12), np.object_),
        (pa.string(), np.object_),
        (pa.list_(pa.int8()), np.object_),
    ]
    for arrow_type, numpy_type in cases:
        assert arrow_type.to_pandas_dtype() == numpy_type


def test_type_list():
    value_type = pa.int32()
    list_type = pa.list_(value_type)
    assert str(list_type) == 'list<item: int32>'


def test_type_string():
    t = pa.string()
    assert str(t) == 'string'


def test_type_timestamp_with_tz():
    tz = 'America/Los_Angeles'
    t = pa.timestamp('ns', tz=tz)
    assert t.unit == 'ns'
    assert t.tz == tz


def test_time_types():
    t1 = pa.time32('s')
    t2 = pa.time32('ms')
    t3 = pa.time64('us')
    t4 = pa.time64('ns')

    assert t1.unit == 's'
    assert t2.unit == 'ms'
    assert t3.unit == 'us'
    assert t4.unit == 'ns'

    assert str(t1) == 'time32[s]'
    assert str(t4) == 'time64[ns]'

    with pytest.raises(ValueError):
        pa.time32('us')

    with pytest.raises(ValueError):
        pa.time64('s')


def test_type_from_numpy_dtype_timestamps():
    cases = [
        (np.dtype('datetime64[s]'), pa.timestamp('s')),
        (np.dtype('datetime64[ms]'), pa.timestamp('ms')),
        (np.dtype('datetime64[us]'), pa.timestamp('us')),
        (np.dtype('datetime64[ns]'), pa.timestamp('ns'))
    ]

    for dt, pt in cases:
        result = pa.from_numpy_dtype(dt)
        assert result == pt


def test_field():
    t = pa.string()
    f = pa.field('foo', t)

    assert f.name == 'foo'
    assert f.nullable
    assert f.type is t
    assert repr(f) == "pyarrow.Field<foo: string>"

    f = pa.field('foo', t, False)
    assert not f.nullable


def test_schema():
    fields = [
        pa.field('foo', pa.int32()),
        pa.field('bar', pa.string()),
        pa.field('baz', pa.list_(pa.int8()))
    ]
    sch = pa.schema(fields)

    assert sch.names == ['foo', 'bar', 'baz']

    assert len(sch) == 3
    assert sch[0].name == 'foo'
    assert sch[0].type == fields[0].type
    assert sch.field_by_name('foo').name == 'foo'
    assert sch.field_by_name('foo').type == fields[0].type

    assert repr(sch) == """\
foo: int32
bar: string
baz: list<item: int8>"""


def test_field_empty():
    f = pa.Field()
    with pytest.raises(ReferenceError):
        repr(f)


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


def test_schema_add_remove_metadata():
    fields = [
        pa.field('foo', pa.int32()),
        pa.field('bar', pa.string()),
        pa.field('baz', pa.list_(pa.int8()))
    ]

    s1 = pa.schema(fields)

    assert s1.metadata is None

    metadata = {b'foo': b'bar', b'pandas': b'badger'}

    s2 = s1.add_metadata(metadata)
    assert s2.metadata == metadata

    s3 = s2.remove_metadata()
    assert s3.metadata is None

    # idempotent
    s4 = s3.remove_metadata()
    assert s4.metadata is None


def test_schema_equals():
    fields = [
        pa.field('foo', pa.int32()),
        pa.field('bar', pa.string()),
        pa.field('baz', pa.list_(pa.int8()))
    ]

    sch1 = pa.schema(fields)
    sch2 = pa.schema(fields)
    assert sch1.equals(sch2)

    del fields[-1]
    sch3 = pa.schema(fields)
    assert not sch1.equals(sch3)


def test_schema_negative_indexing():
    fields = [
        pa.field('foo', pa.int32()),
        pa.field('bar', pa.string()),
        pa.field('baz', pa.list_(pa.int8()))
    ]

    schema = pa.schema(fields)

    assert schema[-1].equals(schema[2])
    assert schema[-2].equals(schema[1])
    assert schema[-3].equals(schema[0])

    with pytest.raises(IndexError):
        schema[-4]

    with pytest.raises(IndexError):
        schema[3]
