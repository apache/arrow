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

import pickle
import weakref

import pyarrow as pa

import pytest


class UuidType(pa.ExtensionType):

    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(16))

    def __reduce__(self):
        return UuidType, ()


class ParamExtType(pa.ExtensionType):

    def __init__(self, width):
        self.width = width
        pa.ExtensionType.__init__(self, pa.binary(width))

    def __reduce__(self):
        return ParamExtType, (self.width,)


def ipc_write_batch(batch):
    stream = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return stream.getvalue()


def ipc_read_batch(buf):
    reader = pa.RecordBatchStreamReader(buf)
    return reader.read_next_batch()


def test_ext_type_basics():
    ty = UuidType()
    assert ty.extension_name == "arrow.py_extension_type"


def test_ext_type__lifetime():
    ty = UuidType()
    wr = weakref.ref(ty)
    del ty
    assert wr() is None


def test_ext_type__storage_type():
    ty = UuidType()
    assert ty.storage_type == pa.binary(16)
    assert ty.__class__ is UuidType
    ty = ParamExtType(5)
    assert ty.storage_type == pa.binary(5)
    assert ty.__class__ is ParamExtType


def test_uuid_type_pickle():
    for proto in range(0, pickle.HIGHEST_PROTOCOL + 1):
        ty = UuidType()
        ser = pickle.dumps(ty, protocol=proto)
        del ty
        ty = pickle.loads(ser)
        wr = weakref.ref(ty)
        assert ty.extension_name == "arrow.py_extension_type"
        del ty
        assert wr() is None


def test_ext_type_equality():
    a = ParamExtType(5)
    b = ParamExtType(6)
    c = ParamExtType(6)
    assert a != b
    assert b == c
    d = UuidType()
    e = UuidType()
    assert a != d
    assert d == e


def test_ext_array_basics():
    ty = ParamExtType(3)
    storage = pa.array([b"foo", b"bar"], type=pa.binary(3))
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)


def test_ext_array_lifetime():
    ty = ParamExtType(3)
    storage = pa.array([b"foo", b"bar"], type=pa.binary(3))
    arr = pa.ExtensionArray.from_storage(ty, storage)

    refs = [weakref.ref(obj) for obj in (ty, arr, storage)]
    del ty, storage, arr
    for ref in refs:
        assert ref() is None


def test_ext_array_errors():
    ty = ParamExtType(4)
    storage = pa.array([b"foo", b"bar"], type=pa.binary(3))
    with pytest.raises(TypeError, match="Incompatible storage type"):
        pa.ExtensionArray.from_storage(ty, storage)


def test_ext_array_equality():
    storage1 = pa.array([b"0123456789abcdef"], type=pa.binary(16))
    storage2 = pa.array([b"0123456789abcdef"], type=pa.binary(16))
    storage3 = pa.array([], type=pa.binary(16))
    ty1 = UuidType()
    ty2 = ParamExtType(16)

    a = pa.ExtensionArray.from_storage(ty1, storage1)
    b = pa.ExtensionArray.from_storage(ty1, storage2)
    assert a.equals(b)
    c = pa.ExtensionArray.from_storage(ty1, storage3)
    assert not a.equals(c)
    d = pa.ExtensionArray.from_storage(ty2, storage1)
    assert not a.equals(d)
    e = pa.ExtensionArray.from_storage(ty2, storage2)
    assert d.equals(e)
    f = pa.ExtensionArray.from_storage(ty2, storage3)
    assert not d.equals(f)


def test_ext_array_pickling():
    for proto in range(0, pickle.HIGHEST_PROTOCOL + 1):
        ty = ParamExtType(3)
        storage = pa.array([b"foo", b"bar"], type=pa.binary(3))
        arr = pa.ExtensionArray.from_storage(ty, storage)
        ser = pickle.dumps(arr, protocol=proto)
        del ty, storage, arr
        arr = pickle.loads(ser)
        arr.validate()
        assert isinstance(arr, pa.ExtensionArray)
        assert arr.type == ParamExtType(3)
        assert arr.type.storage_type == pa.binary(3)
        assert arr.storage.type == pa.binary(3)
        assert arr.storage.to_pylist() == [b"foo", b"bar"]


def example_batch():
    ty = ParamExtType(3)
    storage = pa.array([b"foo", b"bar"], type=pa.binary(3))
    arr = pa.ExtensionArray.from_storage(ty, storage)
    return pa.RecordBatch.from_arrays([arr], ["exts"])


def check_example_batch(batch):
    arr = batch.column(0)
    assert isinstance(arr, pa.ExtensionArray)
    assert arr.type.storage_type == pa.binary(3)
    assert arr.storage.to_pylist() == [b"foo", b"bar"]
    return arr


def test_ipc():
    batch = example_batch()
    buf = ipc_write_batch(batch)
    del batch

    batch = ipc_read_batch(buf)
    arr = check_example_batch(batch)
    assert arr.type == ParamExtType(3)


def test_ipc_unknown_type():
    batch = example_batch()
    buf = ipc_write_batch(batch)
    del batch

    orig_type = ParamExtType
    try:
        # Simulate the original Python type being unavailable.
        # Deserialization should not fail but return a placeholder type.
        del globals()['ParamExtType']

        batch = ipc_read_batch(buf)
        arr = check_example_batch(batch)
        assert isinstance(arr.type, pa.UnknownExtensionType)

        # Can be serialized again
        buf2 = ipc_write_batch(batch)
        del batch, arr

        batch = ipc_read_batch(buf2)
        arr = check_example_batch(batch)
        assert isinstance(arr.type, pa.UnknownExtensionType)
    finally:
        globals()['ParamExtType'] = orig_type

    # Deserialize again with the type restored
    batch = ipc_read_batch(buf2)
    arr = check_example_batch(batch)
    assert arr.type == ParamExtType(3)
