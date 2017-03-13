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

from pyarrow.compat import unittest
import pyarrow as arrow

A = arrow


class TestTypes(unittest.TestCase):

    def test_integers(self):
        dtypes = ['int8', 'int16', 'int32', 'int64',
                  'uint8', 'uint16', 'uint32', 'uint64']

        for name in dtypes:
            factory = getattr(arrow, name)
            t = factory()
            assert str(t) == name

    def test_list(self):
        value_type = arrow.int32()
        list_type = arrow.list_(value_type)
        assert str(list_type) == 'list<item: int32>'

    def test_string(self):
        t = arrow.string()
        assert str(t) == 'string'

    def test_field(self):
        t = arrow.string()
        f = arrow.field('foo', t)

        assert f.name == 'foo'
        assert f.nullable
        assert f.type is t
        assert repr(f) == "Field('foo', type=string)"

        f = arrow.field('foo', t, False)
        assert not f.nullable

    def test_schema(self):
        fields = [
            A.field('foo', A.int32()),
            A.field('bar', A.string()),
            A.field('baz', A.list_(A.int8()))
        ]
        sch = A.schema(fields)

        assert len(sch) == 3
        assert sch[0].name == 'foo'
        assert sch[0].type == fields[0].type
        assert sch.field_by_name('foo').name == 'foo'
        assert sch.field_by_name('foo').type == fields[0].type

        assert repr(sch) == """\
foo: int32
bar: string
baz: list<item: int8>"""

    def test_schema_equals(self):
        fields = [
            A.field('foo', A.int32()),
            A.field('bar', A.string()),
            A.field('baz', A.list_(A.int8()))
        ]

        sch1 = A.schema(fields)
        print(dir(sch1))
        sch2 = A.schema(fields)
        assert sch1.equals(sch2)

        del fields[-1]
        sch3 = A.schema(fields)
        assert not sch1.equals(sch3)


class TestField(unittest.TestCase):
    def test_empty_field(self):
        f = arrow.Field()
        with self.assertRaises(ReferenceError):
            repr(f)
