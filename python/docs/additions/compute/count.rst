count
=====

Examples
--------

.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([True, True, None, False, True])
    >>> pc.count(arr)
    <pyarrow.Int64Scalar: 4>
    >>> pc.count(arr, mode = "only_null")
    <pyarrow.Int64Scalar: 1>
    >>> pc.count(arr, mode = "all")
    <pyarrow.Int64Scalar: 5>
    >>> arr = pa.array([0, 1, -1, None])        
    >>> pc.count(arr)
    <pyarrow.Int64Scalar: 3>
    >>> pc.count(arr, mode = "only_null")
    <pyarrow.Int64Scalar: 1>
    >>> pc.count(arr, mode = "all")
    <pyarrow.Int64Scalar: 4>
    >>> arr = pa.array(["string", "", None, " "])        
    >>> pc.count(arr)
    <pyarrow.Int64Scalar: 3>
    >>> pc.count(arr, mode = "only_null")
    <pyarrow.Int64Scalar: 1>
    >>> pc.count(arr, mode = "all")
    <pyarrow.Int64Scalar: 4>