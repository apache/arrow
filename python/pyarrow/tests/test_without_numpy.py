import pytest

import pyarrow as pa

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not nonumpy'
pytestmark = pytest.mark.nonumpy


def test_array_to_np():
    arr = pa.array(range(10))

    msg = "Cannot return a numpy.ndarray if Numpy is not present"

    with pytest.raises(ValueError, match=msg):
        arr.to_numpy()


def test_chunked_array_to_np():
    data = pa.chunked_array([
        [1, 2, 3],
        [4, 5, 6],
        []
    ])
    msg = "Cannot return a numpy.ndarray if Numpy is not present"

    with pytest.raises(ValueError, match=msg):
        data.to_numpy()


def test_tensor_to_np():
    tensor_type = pa.fixed_shape_tensor(pa.int32(), [2, 2])
    arr = [[1, 2, 3, 4], [10, 20, 30, 40], [100, 200, 300, 400]]
    storage = pa.array(arr, pa.list_(pa.int32(), 4))
    tensor_array = pa.ExtensionArray.from_storage(tensor_type, storage)

    tensor = tensor_array.to_tensor()
    msg = "Cannot return a numpy.ndarray if Numpy is not present"

    with pytest.raises(ValueError, match=msg):
        tensor.to_numpy()
