count_distinct
==============

Examples
--------

.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([True, True, None, False, True, None])
    >>> pc.count_distinct(arr)
    <pyarrow.Int64Scalar: 2>
    >>> pc.count_distinct(arr, mode = "only_null")
    <pyarrow.Int64Scalar: 1>
    >>> pc.count_distinct(arr, mode = "all")
    <pyarrow.Int64Scalar: 3>
    >>> arr = pa.array([0, 1, -1, 0, 1, None, None])        
    >>> pc.count_distinct(arr)
    <pyarrow.Int64Scalar: 3>
    >>> pc.count_distinct(arr, mode = "only_null")
    <pyarrow.Int64Scalar: 1>
    >>> pc.count_distinct(arr, mode = "all")
    <pyarrow.Int64Scalar: 4>
    >>> arr = pa.array(["string", "", None, None, " ", ""])        
    >>> pc.count_distinct(arr)
    <pyarrow.Int64Scalar: 3>
    >>> pc.count_distinct(arr, mode = "only_null")
    <pyarrow.Int64Scalar: 1>
    >>> pc.count_distinct(arr, mode = "all")
    <pyarrow.Int64Scalar: 4>
