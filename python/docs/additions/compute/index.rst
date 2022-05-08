index
=====

Notes
-----
The `end` parameter is an exclusive value, so if the matching index is the index in the end position, -1 will be returned.

Examples
--------

.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([30, 50, None, 12, 30, 50, 60, 39])
    >>> pc.index(arr, 30)
    <pyarrow.Int64Scalar: 0>
    >>> pc.index(arr, None)
    <pyarrow.Int64Scalar: -1>
    >>> pc.index(arr, 30, start = 2)
    <pyarrow.Int64Scalar: 4>
    >>> pc.index(arr, 30, start = 2, end = 4)
    <pyarrow.Int64Scalar: -1>
    >>> pc.index(arr, 12, start = 4)
    <pyarrow.Int64Scalar: -1>
    >>> pc.index(arr, 30, start = 4)
    <pyarrow.Int64Scalar: 4>