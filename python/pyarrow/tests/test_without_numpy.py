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


