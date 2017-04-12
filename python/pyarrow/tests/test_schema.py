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
    assert repr(f) == "Field('foo', type=string)"

    f = pa.field('foo', t, False)
    assert not f.nullable


def test_schema():
    fields = [
        pa.field('foo', pa.int32()),
        pa.field('bar', pa.string()),
        pa.field('baz', pa.list_(pa.int8()))
    ]
    sch = pa.schema(fields)

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


def test_schema_equals():
    fields = [
        pa.field('foo', pa.int32()),
        pa.field('bar', pa.string()),
        pa.field('baz', pa.list_(pa.int8()))
    ]

    sch1 = pa.schema(fields)
    print(dir(sch1))
    sch2 = pa.schema(fields)
    assert sch1.equals(sch2)

    del fields[-1]
    sch3 = pa.schema(fields)
    assert not sch1.equals(sch3)
