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

from collections import namedtuple, OrderedDict, defaultdict
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
        try:
            # Workaround to make comparison of OrderedDicts work on Python 2.7
            if obj1 == obj2:
                return
        except:
            pass
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
        assert obj1 == obj2, ("Objects {} and {} are different."
                              .format(obj1, obj2))


PRIMITIVE_OBJECTS = [
    0, 0.0, 0.9, 1 << 62, 1 << 100, 1 << 999,
    [1 << 100, [1 << 100]], "a", string.printable, "\u262F",
    "hello world", u"hello world", u"\xff\xfe\x9c\x001\x000\x00",
    None, True, False, [], (), {}, {(1, 2): 1}, {(): 2},
    [1, "hello", 3.0], u"\u262F", 42.0, (1.0, "hi"),
    [1, 2, 3, None], [(None,), 3, 1.0], ["h", "e", "l", "l", "o", None],
    (None, None), ("hello", None), (True, False),
    {True: "hello", False: "world"}, {"hello": "world", 1: 42, 2.5: 45},
    {"hello": set([2, 3]), "world": set([42.0]), "this": None},
    np.int8(3), np.int32(4), np.int64(5),
    np.uint8(3), np.uint32(4), np.uint64(5), np.float32(1.9),
    np.float64(1.9), np.zeros([100, 100]),
    np.random.normal(size=[100, 100]), np.array(["hi", 3]),
    np.array(["hi", 3], dtype=object),
    np.random.normal(size=[45, 22]).T]


if sys.version_info >= (3, 0):
    PRIMITIVE_OBJECTS += [0, np.array([["hi", u"hi"], [1.3, 1]])]
else:
    PRIMITIVE_OBJECTS += [long(42), long(1 << 62), long(0),  # noqa
                          np.array([["hi", u"hi"],
                          [1.3, long(1)]])]  # noqa


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


class SubQuxPickle(Qux):
    def __init__(self):
        Qux.__init__(self)


class CustomError(Exception):
    pass


Point = namedtuple("Point", ["x", "y"])
NamedTupleExample = namedtuple("Example",
                               "field1, field2, field3, field4, field5")


CUSTOM_OBJECTS = [Exception("Test object."), CustomError(), Point(11, y=22),
                  Foo(), Bar(), Baz(), Qux(), SubQux(), SubQuxPickle(),
                  NamedTupleExample(1, 1.0, "hi", np.zeros([3, 5]), [1, 2, 3]),
                  OrderedDict([("hello", 1), ("world", 2)])]


def make_serialization_context():

    def array_custom_serializer(obj):
        return obj.tolist(), obj.dtype.str

    def array_custom_deserializer(serialized_obj):
        return np.array(serialized_obj[0], dtype=np.dtype(serialized_obj[1]))

    context = pa.SerializationContext()

    # This is for numpy arrays of "object" only; primitive types are handled
    # efficiently with Arrow's Tensor facilities (see python_to_arrow.cc)
    context.register_type(np.ndarray, 20 * b"\x00",
                          custom_serializer=array_custom_serializer,
                          custom_deserializer=array_custom_deserializer)

    context.register_type(Foo, 20 * b"\x01")
    context.register_type(Bar, 20 * b"\x02")
    context.register_type(Baz, 20 * b"\x03")
    context.register_type(Qux, 20 * b"\x04")
    context.register_type(SubQux, 20 * b"\x05")
    context.register_type(SubQuxPickle, 20 * b"\x05", pickle=True)
    context.register_type(Exception, 20 * b"\x06")
    context.register_type(CustomError, 20 * b"\x07")
    context.register_type(Point, 20 * b"\x08")
    context.register_type(NamedTupleExample, 20 * b"\x09")

    # TODO(pcm): This is currently a workaround until arrow supports
    # arbitrary precision integers. This is only called on long integers,
    # see the associated case in the append method in python_to_arrow.cc
    context.register_type(int, 20 * b"\x10", pickle=False,
                          custom_serializer=lambda obj: str(obj),
                          custom_deserializer=(
                              lambda serialized_obj: int(serialized_obj)))

    if (sys.version_info < (3, 0)):
        deserializer = (
            lambda serialized_obj: long(serialized_obj))  # noqa: E501,F821
        context.register_type(long, 20 * b"\x11", pickle=False,  # noqa: E501,F821
                              custom_serializer=lambda obj: str(obj),
                              custom_deserializer=deserializer)

    def ordered_dict_custom_serializer(obj):
        return list(obj.keys()), list(obj.values())

    def ordered_dict_custom_deserializer(obj):
        return OrderedDict(zip(obj[0], obj[1]))

    context.register_type(OrderedDict, 20 * b"\x12", pickle=False,
                          custom_serializer=ordered_dict_custom_serializer,
                          custom_deserializer=ordered_dict_custom_deserializer)

    def default_dict_custom_serializer(obj):
        return list(obj.keys()), list(obj.values()), obj.default_factory

    def default_dict_custom_deserializer(obj):
        return defaultdict(obj[2], zip(obj[0], obj[1]))

    context.register_type(defaultdict, 20 * b"\x13", pickle=False,
                          custom_serializer=default_dict_custom_serializer,
                          custom_deserializer=default_dict_custom_deserializer)

    context.register_type(type(lambda: 0), 20 * b"\x14", pickle=True)

    return context


serialization_context = make_serialization_context()


def serialization_roundtrip(value, f):
    f.seek(0)
    pa.serialize_to(value, f, serialization_context)
    f.seek(0)
    result = pa.deserialize_from(f, None, serialization_context)
    assert_equal(value, result)


@pytest.yield_fixture(scope='session')
def large_memory_map(tmpdir_factory, size=100*1024*1024):
    path = (tmpdir_factory.mktemp('data')
            .join('pyarrow-serialization-tmp-file').strpath)

    # Create a large memory mapped file
    with open(path, 'wb') as f:
        f.write(np.random.randint(0, 256, size=size)
                .astype('u1')
                .tobytes()
                [:size])
    return path


def test_primitive_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for obj in PRIMITIVE_OBJECTS:
            serialization_roundtrip(obj, mmap)


def test_serialize_to_buffer():
    for nthreads in [1, 4]:
        for value in COMPLEX_OBJECTS:
            buf = pa.serialize(value).to_buffer(nthreads=nthreads)
            result = pa.deserialize(buf)
            assert_equal(value, result)


def test_complex_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for obj in COMPLEX_OBJECTS:
            serialization_roundtrip(obj, mmap)


def test_custom_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for obj in CUSTOM_OBJECTS:
            serialization_roundtrip(obj, mmap)


def test_default_dict_serialization(large_memory_map):
    pytest.importorskip("cloudpickle")
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        obj = defaultdict(lambda: 0, [("hello", 1), ("world", 2)])
        serialization_roundtrip(obj, mmap)


def test_numpy_serialization(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        for t in ["int8", "uint8", "int16", "uint16",
                  "int32", "uint32", "float32", "float64"]:
            obj = np.random.randint(0, 10, size=(100, 100)).astype(t)
            serialization_roundtrip(obj, mmap)


def test_numpy_immutable(large_memory_map):
    with pa.memory_map(large_memory_map, mode="r+") as mmap:
        obj = np.zeros([10])
        mmap.seek(0)
        pa.serialize_to(obj, mmap, serialization_context)
        mmap.seek(0)
        result = pa.deserialize_from(mmap, None, serialization_context)
        with pytest.raises(ValueError):
            result[0] = 1.0


@pytest.mark.skip(reason="extensive memory requirements")
def test_arrow_limits(self):
    def huge_memory_map(temp_dir):
        return large_memory_map(temp_dir, 100 * 1024 * 1024 * 1024)

    with pa.memory_map(huge_memory_map, mode="r+") as mmap:
        # Test that objects that are too large for Arrow throw a Python
        # exception. These tests give out of memory errors on Travis and need
        # to be run on a machine with lots of RAM.
        l = 2 ** 29 * [1.0]
        serialization_roundtrip(l, mmap)
        del l
        l = 2 ** 29 * ["s"]
        serialization_roundtrip(l, mmap)
        del l
        l = 2 ** 29 * [["1"], 2, 3, [{"s": 4}]]
        serialization_roundtrip(l, mmap)
        del l
        l = 2 ** 29 * [{"s": 1}] + 2 ** 29 * [1.0]
        serialization_roundtrip(l, mmap)
        del l
        l = np.zeros(2 ** 25)
        serialization_roundtrip(l, mmap)
        del l
        l = [np.zeros(2 ** 18) for _ in range(2 ** 7)]
        serialization_roundtrip(l, mmap)
        del l


def test_serialization_callback_error():

    class TempClass(object):
            pass

    # Pass a SerializationContext into serialize, but TempClass
    # is not registered
    serialization_context = pa.SerializationContext()
    val = TempClass()
    with pytest.raises(pa.SerializationCallbackError) as err:
        serialized_object = pa.serialize(val, serialization_context)
    assert err.value.example_object == val

    serialization_context.register_type(TempClass, 20*b"\x00")
    serialized_object = pa.serialize(TempClass(), serialization_context)
    deserialization_context = pa.SerializationContext()

    # Pass a Serialization Context into deserialize, but TempClass
    # is not registered
    with pytest.raises(pa.DeserializationCallbackError) as err:
        serialized_object.deserialize(deserialization_context)
    assert err.value.type_id == 20*b"\x00"
