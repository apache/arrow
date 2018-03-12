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
import datetime
import os
import string
import sys

import pyarrow as pa
import numpy as np

import pyarrow.tests.util as test_util

try:
    import torch
except ImportError:
    torch = None
    # Blacklist the module in case `import torch` is costly before
    # failing (ARROW-2071)
    sys.modules['torch'] = None


def assert_equal(obj1, obj2):
    if torch is not None and torch.is_tensor(obj1) and torch.is_tensor(obj2):
        assert torch.equal(obj1, obj2)
        return
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
        except Exception:
            pass
        if obj1.__dict__ == {}:
            print("WARNING: Empty dict in ", obj1)
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
    0, 0.0, 0.9, 1 << 62, 1 << 999,
    [1 << 100, [1 << 100]], "a", string.printable, "\u262F",
    "hello world", u"hello world", u"\xff\xfe\x9c\x001\x000\x00",
    None, True, False, [], (), {}, {(1, 2): 1}, {(): 2},
    [1, "hello", 3.0], u"\u262F", 42.0, (1.0, "hi"),
    [1, 2, 3, None], [(None,), 3, 1.0], ["h", "e", "l", "l", "o", None],
    (None, None), ("hello", None), (True, False),
    {True: "hello", False: "world"}, {"hello": "world", 1: 42, 2.5: 45},
    {"hello": set([2, 3]), "world": set([42.0]), "this": None},
    np.int8(3), np.int32(4), np.int64(5),
    np.uint8(3), np.uint32(4), np.uint64(5), np.float16(1.9), np.float32(1.9),
    np.float64(1.9), np.zeros([8, 20]),
    np.random.normal(size=[17, 10]), np.array(["hi", 3]),
    np.array(["hi", 3], dtype=object),
    np.random.normal(size=[15, 13]).T,
]


if sys.version_info >= (3, 0):
    PRIMITIVE_OBJECTS += [0, np.array([["hi", u"hi"], [1.3, 1]])]
else:
    PRIMITIVE_OBJECTS += [long(42), long(1 << 62), long(0),  # noqa
                          np.array([["hi", u"hi"],
                          [1.3, long(1)]])]  # noqa


COMPLEX_OBJECTS = [
    [[[[[[[[[[[[]]]]]]]]]]]],
    {"obj{}".format(i): np.random.normal(size=[4, 4]) for i in range(5)},
    # {(): {(): {(): {(): {(): {(): {(): {(): {(): {(): {
    #       (): {(): {}}}}}}}}}}}}},
    ((((((((((),),),),),),),),),),
    {"a": {"b": {"c": {"d": {}}}}},
]


class Foo(object):
    def __init__(self, value=0):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return other.value == self.value


class Bar(object):
    def __init__(self):
        for i, val in enumerate(COMPLEX_OBJECTS):
            setattr(self, "field{}".format(i), val)


class Baz(object):
    def __init__(self):
        self.foo = Foo()
        self.bar = Bar()

    def method(self, arg):
        pass


class Qux(object):
    def __init__(self):
        self.objs = [Foo(1), Foo(42)]


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
    context = pa.default_serialization_context()

    context.register_type(Foo, "Foo")
    context.register_type(Bar, "Bar")
    context.register_type(Baz, "Baz")
    context.register_type(Qux, "Quz")
    context.register_type(SubQux, "SubQux")
    context.register_type(SubQuxPickle, "SubQuxPickle", pickle=True)
    context.register_type(Exception, "Exception")
    context.register_type(CustomError, "CustomError")
    context.register_type(Point, "Point")
    context.register_type(NamedTupleExample, "NamedTupleExample")

    return context


global_serialization_context = make_serialization_context()


def serialization_roundtrip(value, scratch_buffer,
                            context=global_serialization_context):
    writer = pa.FixedSizeBufferWriter(scratch_buffer)
    pa.serialize_to(value, writer, context=context)

    reader = pa.BufferReader(scratch_buffer)
    result = pa.deserialize_from(reader, None, context=context)
    assert_equal(value, result)

    _check_component_roundtrip(value, context=context)


def _check_component_roundtrip(value, context=global_serialization_context):
    # Test to/from components
    serialized = pa.serialize(value, context=context)
    components = serialized.to_components()
    from_comp = pa.SerializedPyObject.from_components(components)
    recons = from_comp.deserialize(context=context)
    assert_equal(value, recons)


@pytest.yield_fixture(scope='session')
def large_buffer(size=32*1024*1024):
    return pa.allocate_buffer(size)


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


def test_clone():
    context = pa.SerializationContext()

    class Foo(object):
        pass

    def custom_serializer(obj):
        return 0

    def custom_deserializer(serialized_obj):
        return (serialized_obj, 'a')

    context.register_type(Foo, 'Foo', custom_serializer=custom_serializer,
                          custom_deserializer=custom_deserializer)

    new_context = context.clone()

    f = Foo()
    serialized = pa.serialize(f, context=context)
    deserialized = serialized.deserialize(context=context)
    assert deserialized == (0, 'a')

    serialized = pa.serialize(f, context=new_context)
    deserialized = serialized.deserialize(context=new_context)
    assert deserialized == (0, 'a')


def test_primitive_serialization(large_buffer):
    for obj in PRIMITIVE_OBJECTS:
        serialization_roundtrip(obj, large_buffer)


def test_serialize_to_buffer():
    for nthreads in [1, 4]:
        for value in COMPLEX_OBJECTS:
            buf = pa.serialize(value).to_buffer(nthreads=nthreads)
            result = pa.deserialize(buf)
            assert_equal(value, result)


def test_complex_serialization(large_buffer):
    for obj in COMPLEX_OBJECTS:
        serialization_roundtrip(obj, large_buffer)


def test_custom_serialization(large_buffer):
    for obj in CUSTOM_OBJECTS:
        serialization_roundtrip(obj, large_buffer)


def test_default_dict_serialization(large_buffer):
    pytest.importorskip("cloudpickle")

    obj = defaultdict(lambda: 0, [("hello", 1), ("world", 2)])
    serialization_roundtrip(obj, large_buffer)


def test_numpy_serialization(large_buffer):
    for t in ["bool", "int8", "uint8", "int16", "uint16", "int32",
              "uint32", "float16", "float32", "float64"]:
        obj = np.random.randint(0, 10, size=(100, 100)).astype(t)
        serialization_roundtrip(obj, large_buffer)


def test_datetime_serialization(large_buffer):
    data = [
        #  Principia Mathematica published
        datetime.datetime(year=1687, month=7, day=5),

        # Some random date
        datetime.datetime(year=1911, month=6, day=3, hour=4,
                          minute=55, second=44),
        # End of WWI
        datetime.datetime(year=1918, month=11, day=11),

        # Beginning of UNIX time
        datetime.datetime(year=1970, month=1, day=1),

        # The Berlin wall falls
        datetime.datetime(year=1989, month=11, day=9),

        # Another random date
        datetime.datetime(year=2011, month=6, day=3, hour=4,
                          minute=0, second=3),
        # Another random date
        datetime.datetime(year=1970, month=1, day=3, hour=4,
                          minute=0, second=0)
    ]
    for d in data:
        serialization_roundtrip(d, large_buffer)


def test_torch_serialization(large_buffer):
    pytest.importorskip("torch")

    serialization_context = pa.default_serialization_context()
    pa.register_torch_serialization_handlers(serialization_context)
    # These are the only types that are supported for the
    # PyTorch to NumPy conversion
    for t in ["float32", "float64",
              "uint8", "int16", "int32", "int64"]:
        obj = torch.from_numpy(np.random.randn(1000).astype(t))
        serialization_roundtrip(obj, large_buffer,
                                context=serialization_context)


def test_numpy_immutable(large_buffer):
    obj = np.zeros([10])

    writer = pa.FixedSizeBufferWriter(large_buffer)
    pa.serialize_to(obj, writer, global_serialization_context)

    reader = pa.BufferReader(large_buffer)
    result = pa.deserialize_from(reader, None, global_serialization_context)
    with pytest.raises(ValueError):
        result[0] = 1.0


def test_numpy_base_object(tmpdir):
    # ARROW-2040: deserialized Numpy array should keep a reference to the
    # owner of its memory
    path = os.path.join(str(tmpdir), 'zzz.bin')
    data = np.arange(12, dtype=np.int32)

    with open(path, 'wb') as f:
        f.write(pa.serialize(data).to_buffer())

    serialized = pa.read_serialized(pa.OSFile(path))
    result = serialized.deserialize()
    assert_equal(result, data)
    serialized = None
    assert_equal(result, data)
    assert result.base is not None


# see https://issues.apache.org/jira/browse/ARROW-1695
def test_serialization_callback_numpy():

    class DummyClass(object):
        pass

    def serialize_dummy_class(obj):
        x = np.zeros(4)
        return x

    def deserialize_dummy_class(serialized_obj):
        return serialized_obj

    context = pa.default_serialization_context()
    context.register_type(DummyClass, "DummyClass",
                          custom_serializer=serialize_dummy_class,
                          custom_deserializer=deserialize_dummy_class)

    pa.serialize(DummyClass(), context=context)


def test_numpy_subclass_serialization():
    # Check that we can properly serialize subclasses of np.ndarray.
    class CustomNDArray(np.ndarray):
        def __new__(cls, input_array):
            array = np.asarray(input_array).view(cls)
            return array

    def serializer(obj):
        return {'numpy': obj.view(np.ndarray)}

    def deserializer(data):
        array = data['numpy'].view(CustomNDArray)
        return array

    context = pa.default_serialization_context()

    context.register_type(CustomNDArray, 'CustomNDArray',
                          custom_serializer=serializer,
                          custom_deserializer=deserializer)

    x = CustomNDArray(np.zeros(3))
    serialized = pa.serialize(x, context=context).to_buffer()
    new_x = pa.deserialize(serialized, context=context)
    assert type(new_x) == CustomNDArray
    assert np.alltrue(new_x.view(np.ndarray) == np.zeros(3))


def test_buffer_serialization():

    class BufferClass(object):
        pass

    def serialize_buffer_class(obj):
        return pa.py_buffer(b"hello")

    def deserialize_buffer_class(serialized_obj):
        return serialized_obj

    context = pa.default_serialization_context()
    context.register_type(
        BufferClass, "BufferClass",
        custom_serializer=serialize_buffer_class,
        custom_deserializer=deserialize_buffer_class)

    b = pa.serialize(BufferClass(), context=context).to_buffer()
    assert pa.deserialize(b, context=context).to_pybytes() == b"hello"


@pytest.mark.skip(reason="extensive memory requirements")
def test_arrow_limits(self):
    def huge_memory_map(temp_dir):
        return large_memory_map(temp_dir, 100 * 1024 * 1024 * 1024)

    with pa.memory_map(huge_memory_map, mode="r+") as mmap:
        # Test that objects that are too large for Arrow throw a Python
        # exception. These tests give out of memory errors on Travis and need
        # to be run on a machine with lots of RAM.
        x = 2 ** 29 * [1.0]
        serialization_roundtrip(x, mmap)
        del x
        x = 2 ** 29 * ["s"]
        serialization_roundtrip(x, mmap)
        del x
        x = 2 ** 29 * [["1"], 2, 3, [{"s": 4}]]
        serialization_roundtrip(x, mmap)
        del x
        x = 2 ** 29 * [{"s": 1}] + 2 ** 29 * [1.0]
        serialization_roundtrip(x, mmap)
        del x
        x = np.zeros(2 ** 25)
        serialization_roundtrip(x, mmap)
        del x
        x = [np.zeros(2 ** 18) for _ in range(2 ** 7)]
        serialization_roundtrip(x, mmap)
        del x


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


def test_fallback_to_subclasses():

    class SubFoo(Foo):
        def __init__(self):
            Foo.__init__(self)

    # should be able to serialize/deserialize an instance
    # if a base class has been registered
    serialization_context = pa.SerializationContext()
    serialization_context.register_type(Foo, "Foo")

    subfoo = SubFoo()
    # should fallbact to Foo serializer
    serialized_object = pa.serialize(subfoo, serialization_context)

    reconstructed_object = serialized_object.deserialize(
        serialization_context
    )
    assert type(reconstructed_object) == Foo


class Serializable(object):
    pass


def serialize_serializable(obj):
    return {"type": type(obj), "data": obj.__dict__}


def deserialize_serializable(obj):
    val = obj["type"].__new__(obj["type"])
    val.__dict__.update(obj["data"])
    return val


class SerializableClass(Serializable):
    def __init__(self):
        self.value = 3


def test_serialize_subclasses():

    # This test shows how subclasses can be handled in an idiomatic way
    # by having only a serializer for the base class

    # This technique should however be used with care, since pickling
    # type(obj) with couldpickle will include the full class definition
    # in the serialized representation.
    # This means the class definition is part of every instance of the
    # object, which in general is not desirable; registering all subclasses
    # with register_type will result in faster and more memory
    # efficient serialization.

    context = pa.default_serialization_context()
    context.register_type(
        Serializable, "Serializable",
        custom_serializer=serialize_serializable,
        custom_deserializer=deserialize_serializable)

    a = SerializableClass()
    serialized = pa.serialize(a, context=context)

    deserialized = serialized.deserialize(context=context)
    assert type(deserialized).__name__ == SerializableClass.__name__
    assert deserialized.value == 3


def test_serialize_to_components_invalid_cases():
    buf = pa.py_buffer(b'hello')

    components = {
        'num_tensors': 0,
        'num_buffers': 1,
        'data': [buf]
    }

    with pytest.raises(pa.ArrowException):
        pa.deserialize_components(components)

    components = {
        'num_tensors': 1,
        'num_buffers': 0,
        'data': [buf, buf]
    }

    with pytest.raises(pa.ArrowException):
        pa.deserialize_components(components)


@pytest.mark.skipif(os.name == 'nt', reason="deserialize_regex not pickleable")
def test_deserialize_in_different_process():
    from multiprocessing import Process, Queue
    import re

    regex = re.compile(r"\d+\.\d*")

    serialization_context = pa.SerializationContext()
    serialization_context.register_type(type(regex), "Regex", pickle=True)

    serialized = pa.serialize(regex, serialization_context)
    serialized_bytes = serialized.to_buffer().to_pybytes()

    def deserialize_regex(serialized, q):
        import pyarrow as pa
        q.put(pa.deserialize(serialized))

    q = Queue()
    p = Process(target=deserialize_regex, args=(serialized_bytes, q))
    p.start()
    assert q.get().pattern == regex.pattern
    p.join()


def test_deserialize_buffer_in_different_process():
    import tempfile
    import subprocess

    f = tempfile.NamedTemporaryFile(delete=False)
    b = pa.serialize(pa.py_buffer(b'hello')).to_buffer()
    f.write(b.to_pybytes())
    f.close()

    subprocess_env = test_util.get_modified_env_with_pythonpath()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    python_file = os.path.join(dir_path, 'deserialize_buffer.py')
    subprocess.check_call([sys.executable, python_file, f.name],
                          env=subprocess_env)


def test_set_pickle():
    # Use a custom type to trigger pickling.
    class Foo(object):
        pass

    context = pa.SerializationContext()
    context.register_type(Foo, 'Foo', pickle=True)

    test_object = Foo()

    # Define a custom serializer and deserializer to use in place of pickle.

    def dumps1(obj):
        return b'custom'

    def loads1(serialized_obj):
        return serialized_obj + b' serialization 1'

    # Test that setting a custom pickler changes the behavior.
    context.set_pickle(dumps1, loads1)
    serialized = pa.serialize(test_object, context=context).to_buffer()
    deserialized = pa.deserialize(serialized.to_pybytes(), context=context)
    assert deserialized == b'custom serialization 1'

    # Define another custom serializer and deserializer.

    def dumps2(obj):
        return b'custom'

    def loads2(serialized_obj):
        return serialized_obj + b' serialization 2'

    # Test that setting another custom pickler changes the behavior again.
    context.set_pickle(dumps2, loads2)
    serialized = pa.serialize(test_object, context=context).to_buffer()
    deserialized = pa.deserialize(serialized.to_pybytes(), context=context)
    assert deserialized == b'custom serialization 2'


@pytest.mark.skipif(sys.version_info < (3, 6), reason="need Python 3.6")
def test_path_objects(tmpdir):
    # Test compatibility with PEP 519 path-like objects
    import pathlib
    p = pathlib.Path(tmpdir) / 'zzz.bin'
    obj = 1234
    pa.serialize_to(obj, p)
    res = pa.deserialize_from(p, None)
    assert res == obj
