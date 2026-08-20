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

import ctypes
from functools import wraps
import gc
import pytest

import pyarrow as pa
from pyarrow.vendored.version import Version

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not numpy'
pytestmark = pytest.mark.numpy
np = pytest.importorskip("numpy")


def PyCapsule_IsValid(capsule, name):
    return ctypes.pythonapi.PyCapsule_IsValid(ctypes.py_object(capsule), name) == 1


def check_dlpack_export(arr, expected_arr):
    with pytest.warns(DeprecationWarning, match="unversioned DLPack capsule"):
        DLTensor = arr.__dlpack__()
    assert PyCapsule_IsValid(DLTensor, b"dltensor") is True

    result = np.from_dlpack(arr)
    np.testing.assert_array_equal(result, expected_arr, strict=True)

    assert arr.__dlpack_device__() == (1, 0)


class DLPackForwarder:
    """Forward ``__dlpack__`` to a wrapped object with forced keyword arguments.

    Consumers such as ``np.from_dlpack`` do not expose every ``__dlpack__``
    keyword, so this makes them reachable from a consumer's point of view.
    """

    def __init__(self, obj, **forced):
        self._obj = obj
        self._forced = forced

    def __dlpack__(self, **kwargs):
        return self._obj.__dlpack__(**{**kwargs, **self._forced})

    def __dlpack_device__(self):
        return self._obj.__dlpack_device__()


def check_bytes_allocated(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        gc.collect()
        allocated_bytes = pa.total_allocated_bytes()
        try:
            return f(*args, **kwargs)
        finally:
            assert pa.total_allocated_bytes() == allocated_bytes
    return wrapper


@check_bytes_allocated
@pytest.mark.parametrize(
    ('value_type', 'np_type_str'),
    [
        (pa.uint8(), "uint8"),
        (pa.uint16(), "uint16"),
        (pa.uint32(), "uint32"),
        (pa.uint64(), "uint64"),
        (pa.int8(), "int8"),
        (pa.int16(), "int16"),
        (pa.int32(), "int32"),
        (pa.int64(), "int64"),
        (pa.float16(), "float16"),
        (pa.float32(), "float32"),
        (pa.float64(), "float64"),
    ]
)
def test_dlpack(value_type, np_type_str):
    if Version(np.__version__) < Version("1.24.0"):
        pytest.skip("No dlpack support in numpy versions older than 1.22.0, "
                    "strict keyword in assert_array_equal added in numpy version "
                    "1.24.0")

    expected = np.array([1, 2, 3], dtype=np.dtype(np_type_str))
    arr = pa.array(expected, type=value_type)
    check_dlpack_export(arr, expected)

    t = pa.Tensor.from_numpy(expected)
    check_dlpack_export(t, expected)

    arr_sliced = arr.slice(1, 1)
    expected = np.array([2], dtype=np.dtype(np_type_str))
    check_dlpack_export(arr_sliced, expected)

    arr_sliced = arr.slice(0, 1)
    expected = np.array([1], dtype=np.dtype(np_type_str))
    check_dlpack_export(arr_sliced, expected)

    arr_sliced = arr.slice(1)
    expected = np.array([2, 3], dtype=np.dtype(np_type_str))
    check_dlpack_export(arr_sliced, expected)

    arr_zero = pa.array([], type=value_type)
    expected = np.array([], dtype=np.dtype(np_type_str))
    check_dlpack_export(arr_zero, expected)

    t = pa.Tensor.from_numpy(expected)
    check_dlpack_export(t, expected)


@check_bytes_allocated
@pytest.mark.parametrize('np_type',
                         [np.uint8, np.uint16, np.uint32, np.uint64,
                          np.int8, np.int16, np.int32, np.int64,
                          np.float16, np.float32, np.float64,])
def test_tensor_dlpack(np_type):
    if Version(np.__version__) < Version("1.24.0"):
        pytest.skip("No dlpack support in numpy versions older than 1.22.0, "
                    "strict keyword in assert_array_equal added in numpy version "
                    "1.24.0")

    arr = np.array([1, 2, 3, 4, 5, 6, 1, 1])
    expected = np.array(arr, dtype=np_type).reshape((2, 2, 2), order='C')
    t = pa.Tensor.from_numpy(expected)
    check_dlpack_export(t, expected)

    expected = np.array(arr, dtype=np_type).reshape((2, 2, 2), order='F')
    t = pa.Tensor.from_numpy(expected)
    check_dlpack_export(t, expected)


def dlpack_objects():
    arr = pa.array([1, 2, 3], type=pa.int32())
    return [
        pytest.param(arr, id="array"),
        pytest.param(arr.slice(1), id="sliced_array"),
        pytest.param(pa.array([], type=pa.int32()), id="empty_array"),
        pytest.param(
            pa.Tensor.from_numpy(np.array([[1, 2], [3, 4]], dtype=np.int32)),
            id="tensor",
        ),
    ]


@check_bytes_allocated
@pytest.mark.parametrize('obj', dlpack_objects())
@pytest.mark.parametrize('max_version', [None, (0, 8)])
def test_dlpack_legacy_capsule(obj, max_version):
    with pytest.warns(DeprecationWarning, match="unversioned DLPack capsule"):
        capsule = obj.__dlpack__(max_version=max_version)
    assert PyCapsule_IsValid(capsule, b"dltensor") is True


def immutable_tensor():
    np_arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
    np_arr.flags.writeable = False
    tensor = pa.Tensor.from_numpy(np_arr)
    assert not tensor.is_mutable
    return tensor


@check_bytes_allocated
@pytest.mark.parametrize('max_version', [None, (0, 8)])
def test_dlpack_legacy_capsule_immutable_tensor(max_version):
    tensor = immutable_tensor()
    with pytest.raises(NotImplementedError,
                       match="Legacy DLPack support is not implemented "
                             "for immutable tensors"):
        tensor.__dlpack__(max_version=max_version)


@check_bytes_allocated
@pytest.mark.parametrize('max_version', [(1, 0), (1, 3), (2, 0)])
@pytest.mark.parametrize('copy', [None, False, True])
def test_dlpack_versioned_capsule_immutable_tensor(max_version, copy):
    tensor = immutable_tensor()
    capsule = tensor.__dlpack__(max_version=max_version, copy=copy)
    assert PyCapsule_IsValid(capsule, b"dltensor_versioned") is True


@check_bytes_allocated
@pytest.mark.parametrize('obj', dlpack_objects())
@pytest.mark.parametrize('max_version', [None, (0, 8)])
@pytest.mark.parametrize('copy', [False, True])
def test_dlpack_legacy_capsule_copy_not_supported(obj, max_version, copy):
    with pytest.raises(BufferError, match="copy argument is not supported"):
        obj.__dlpack__(max_version=max_version, copy=copy)


@check_bytes_allocated
@pytest.mark.parametrize('obj', dlpack_objects())
@pytest.mark.parametrize('max_version', [(1, 0), (1, 3), (2, 0)])
@pytest.mark.parametrize('copy', [None, False, True])
def test_dlpack_versioned_capsule(obj, max_version, copy):
    capsule = obj.__dlpack__(max_version=max_version, copy=copy)
    assert PyCapsule_IsValid(capsule, b"dltensor_versioned") is True


@check_bytes_allocated
@pytest.mark.parametrize('obj', dlpack_objects())
def test_dlpack_versioned_roundtrip(obj):
    if Version(np.__version__) < Version("2.1.0"):
        pytest.skip("Versioned DLPack capsules require numpy 2.1.0 or later")

    expected = np.from_dlpack(DLPackForwarder(obj, max_version=None))
    for copy in [None, False, True]:
        result = np.from_dlpack(
            DLPackForwarder(obj, max_version=(1, 0), copy=copy))
        np.testing.assert_array_equal(result, expected, strict=True)


@check_bytes_allocated
def test_dlpack_copy_is_writeable():
    if Version(np.__version__) < Version("2.1.0"):
        pytest.skip("Read-only DLPack flag requires numpy 2.1.0 or later")

    arr = pa.array([1, 2, 3], type=pa.int32())

    # Arrow arrays are immutable, so a shared export is read-only
    shared = np.from_dlpack(DLPackForwarder(arr, max_version=(1, 3)))
    assert not shared.flags.writeable

    # A copy is solely owned by the consumer, who may mutate it
    copied = np.from_dlpack(DLPackForwarder(arr, max_version=(1, 3), copy=True))
    assert copied.flags.writeable
    copied[0] = 100
    assert arr.to_pylist() == [1, 2, 3]


def test_dlpack_not_supported():
    if Version(np.__version__) < Version("1.22.0"):
        pytest.skip("No dlpack support in numpy versions older than 1.22.0.")

    arr = pa.array([1, None, 3])
    with pytest.raises(TypeError, match="Can only use DLPack "
                       "on arrays with no nulls."):
        np.from_dlpack(arr)

    arr = pa.array(
        [[0, 1], [3, 4]],
        type=pa.list_(pa.int32())
    )
    with pytest.raises(TypeError, match="DataType is not compatible with DLPack spec"):
        np.from_dlpack(arr)

    arr = pa.array([])
    with pytest.raises(TypeError, match="DataType is not compatible with DLPack spec"):
        np.from_dlpack(arr)

    # DLPack doesn't support bit-packed boolean values
    arr = pa.array([True, False, True])
    with pytest.raises(TypeError, match="Bit-packed boolean data type "
                       "not supported by DLPack."):
        np.from_dlpack(arr)


def test_dlpack_cuda_not_supported():
    cuda = pytest.importorskip("pyarrow.cuda", exc_type=ImportError)

    schema = pa.schema([pa.field('f0', pa.int16())])
    a0 = pa.array([1, 2, 3], type=pa.int16())
    batch = pa.record_batch([a0], schema=schema)

    cbuf = cuda.serialize_record_batch(batch, cuda.Context(0))
    cbatch = cuda.read_record_batch(cbuf, batch.schema)
    carr = cbatch["f0"]

    # CudaBuffers not yet supported
    with pytest.raises(NotImplementedError, match="DLPack support is implemented "
                       "only for buffers on CPU device."):
        np.from_dlpack(carr)

    with pytest.raises(NotImplementedError, match="DLPack support is implemented "
                       "only for buffers on CPU device."):
        carr.__dlpack_device__()
