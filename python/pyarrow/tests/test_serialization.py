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

from __future__ import division

import pytest

from collections import namedtuple
import os
import string
import sys

import pyarrow as pa
import numpy as np


def assert_equal(obj1, obj2):
    module_numpy = (type(obj1).__module__ == np.__name__ or
                    type(obj2).__module__ == np.__name__)
    if module_numpy:
        empty_shape = ((hasattr(obj1, "shape") and obj1.shape == ()) or
                       (hasattr(obj2, "shape") and obj2.shape == ()))
        if empty_shape:
            # This is a special case because currently np.testing.assert_equal
            # fails because we do not properly handle different numerical
            # types.
            assert obj1 == obj2, ("Objects {} and {} are "
                                  "different.".format(obj1, obj2))
        else:
            np.testing.assert_equal(obj1, obj2)
    elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
        special_keys = ["_pytype_"]
        assert (set(list(obj1.__dict__.keys()) + special_keys) ==
                set(list(obj2.__dict__.keys()) + special_keys)), ("Objects {} "
                                                                  "and {} are "
                                                                  "different."
                                                                  .format(
                                                                      obj1,
                                                                      obj2))
        for key in obj1.__dict__.keys():
            if key not in special_keys:
                assert_equal(obj1.__dict__[key], obj2.__dict__[key])
    elif type(obj1) is dict or type(obj2) is dict:
        assert_equal(obj1.keys(), obj2.keys())
        for key in obj1.keys():
            assert_equal(obj1[key], obj2[key])
    elif type(obj1) is list or type(obj2) is list:
        assert len(obj1) == len(obj2), ("Objects {} and {} are lists with "
                                        "different lengths."
                                        .format(obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    elif type(obj1) is tuple or type(obj2) is tuple:
        assert len(obj1) == len(obj2), ("Objects {} and {} are tuples with "
                                        "different lengths."
                                        .format(obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    elif (pa.lib.is_named_tuple(type(obj1)) or
          pa.lib.is_named_tuple(type(obj2))):
        assert len(obj1) == len(obj2), ("Objects {} and {} are named tuples "
                                        "with different lengths."
                                        .format(obj1, obj2))
        for i in range(len(obj1)):
            assert_equal(obj1[i], obj2[i])
    else:
        assert obj1 == obj2, "Objects {} and {} are different.".format(obj1,
                                                                       obj2)


def array_custom_serializer(obj):
    return obj.tolist(), obj.dtype.str


def array_custom_deserializer(serialized_obj):
    return np.array(serialized_obj[0], dtype=np.dtype(serialized_obj[1]))

pa.lib.register_type(np.ndarray, 20 * b"\x00", pickle=False,
                     custom_serializer=array_custom_serializer,
                     custom_deserializer=array_custom_deserializer)

if sys.version_info >= (3, 0):
    long_extras = [0, np.array([["hi", u"hi"], [1.3, 1]])]
else:
    _LONG_ZERO, _LONG_ONE = long(0), long(1)  # noqa: E501,F821
    long_extras = [_LONG_ZERO, np.array([["hi", u"hi"],
                                         [1.3, _LONG_ONE]])]

PRIMITIVE_OBJECTS = [
    0, 0.0, 0.9, 1 << 62, 1 << 100, 1 << 999,
    [1 << 100, [1 << 100]], "a", string.printable, "\u262F",
    u"hello world", u"\xff\xfe\x9c\x001\x000\x00", None, True,
    False, [], (), {}, np.int8(3), np.int32(4), np.int64(5),
    np.uint8(3), np.uint32(4), np.uint64(5), np.float32(1.9),
    np.float64(1.9), np.zeros([100, 100]),
    np.random.normal(size=[100, 100]), np.array(["hi", 3]),
    np.array(["hi", 3], dtype=object)] + long_extras

COMPLEX_OBJECTS = [
    [[[[[[[[[[[[]]]]]]]]]]]],
    {"obj{}".format(i): np.random.normal(size=[100, 100]) for i in range(10)},
    # {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {
    #       (): {(): {}}}}}}}}}}}}},
    ((((((((((),),),),),),),),),),
    {"a": {"b": {"c": {"d": {}}}}}]


class Foo(object):
    def __init__(self, value=0):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return other.value == self.value


class Bar(object):
    def __init__(self):
        for i, val in enumerate(PRIMITIVE_OBJECTS + COMPLEX_OBJECTS):
            setattr(self, "field{}".format(i), val)


class Baz(object):
    def __init__(self):
        self.foo = Foo()
        self.bar = Bar()

    def method(self, arg):
        pass


class Qux(object):
    def __init__(self):
        self.objs = [Foo(), Bar(), Baz()]


class SubQux(Qux):
    def __init__(self):
        Qux.__init__(self)


class CustomError(Exception):
    pass


Point = namedtuple("Point", ["x", "y"])
NamedTupleExample = namedtuple("Example",
                               "field1, field2, field3, field4, field5")


CUSTOM_OBJECTS = [Exception("Test object."), CustomError(), Point(11, y=22),
                  Foo(), Bar(), Baz(), Qux(), SubQux(),
                  NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3])]

pa.lib.register_type(Foo, 20 * b"\x01")
pa.lib.register_type(Bar, 20 * b"\x02")
pa.lib.register_type(Baz, 20 * b"\x03")
pa.lib.register_type(Qux, 20 * b"\x04")
pa.lib.register_type(SubQux, 20 * b"\x05")
pa.lib.register_type(Exception, 20 * b"\x06")
pa.lib.register_type(CustomError, 20 * b"\x07")
pa.lib.register_type(Point, 20 * b"\x08")
pa.lib.register_type(NamedTupleExample, 20 * b"\x09")

# TODO(pcm): This is currently a workaround until arrow supports
# arbitrary precision integers. This is only called on long integers,
# see the associated case in the append method in python_to_arrow.cc
pa.lib.register_type(int, 20 * b"\x10", pickle=False,
                     custom_serializer=lambda obj: str(obj),
                     custom_deserializer=(
                         lambda serialized_obj: int(serialized_obj)))


if (sys.version_info < (3, 0)):
    deserializer = (
        lambda serialized_obj: long(serialized_obj))  # noqa: E501,F821
    pa.lib.register_type(long, 20 * b"\x11", pickle=False,  # noqa: E501,F821
                         custom_serializer=lambda obj: str(obj),
                         custom_deserializer=deserializer)


def serialization_roundtrip(value, f):
    f.seek(0)
    pa.serialize_to(value, f)
    f.seek(0)
    result = pa.deserialize_from(f, None)
    assert_equal(value, result)


@pytest.yield_fixture(scope='session')
def large_memory_map(tmpdir_factory):
    path = (tmpdir_factory.mktemp('data')
            .join('pyarrow-serialization-tmp-file').strpath)

    # Create a large memory mapped file
    SIZE = 100 * 1024 * 1024  # 100 MB
    with open(path, 'wb') as f:
        f.write(np.random.randint(0, 256, size=SIZE)
                .astype('u1')
                .tobytes()
                [:SIZE])
    return path


def test_primitive_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for obj in PRIMITIVE_OBJECTS:
            serialization_roundtrip([obj], mmap)


def test_complex_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for obj in COMPLEX_OBJECTS:
            serialization_roundtrip([obj], mmap)


def test_custom_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for obj in CUSTOM_OBJECTS:
            serialization_roundtrip([obj], mmap)
