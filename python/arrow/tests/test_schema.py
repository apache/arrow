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

from arrow.compat import unittest
import arrow


class TestTypes(unittest.TestCase):

    def test_integers(self):
        dtypes = ['int8', 'int16', 'int32', 'int64',
                  'uint8', 'uint16', 'uint32', 'uint64']

        for name in dtypes:
            factory = getattr(arrow, name)
            t = factory()
            t_required = factory(False)

            assert str(t) == name
            assert str(t_required) == '{0} not null'.format(name)

    def test_list(self):
        value_type = arrow.int32()
        list_type = arrow.list_(value_type)
        assert str(list_type) == 'list<int32>'

    def test_string(self):
        t = arrow.string()
        assert str(t) == 'string'

    def test_field(self):
        t = arrow.string()
        f = arrow.field('foo', t)

        assert f.name == 'foo'
        assert f.type is t
        assert repr(f) == "Field('foo', type=string)"
