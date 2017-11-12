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

import pyarrow as pa
import pyarrow.types as types


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
    assert types.is_decimal(pa.decimal(19, 4))
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
    assert types.is_union(pa.union([pa.field('a', pa.int32()),
                                    pa.field('b', pa.int8()),
                                    pa.field('c', pa.string())],
                                   pa.lib.UnionMode_SPARSE))
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


def test_timestamp_type():
    # See ARROW-1683
    assert isinstance(pa.timestamp('ns'), pa.TimestampType)


def test_types_hashable():
    types = [
        pa.null(),
        pa.int32(),
        pa.time32('s'),
        pa.time64('us'),
        pa.date32(),
        pa.timestamp('us'),
        pa.string(),
        pa.binary(),
        pa.binary(10),
        pa.list_(pa.int32()),
        pa.struct([pa.field('a', pa.int32()),
                   pa.field('b', pa.int8()),
                   pa.field('c', pa.string())])
    ]

    in_dict = {}
    for i, type_ in enumerate(types):
        assert hash(type_) == hash(type_)
        in_dict[type_] = i
        assert in_dict[type_] == i
