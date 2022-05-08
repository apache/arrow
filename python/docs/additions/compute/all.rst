all
===

Examples
--------

.. code-block:: python
    
    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([True, True, None, False, True])
    >>> pc.all(arr)
    <pyarrow.BooleanScalar: False>
    >>> arr = pa.array([True, True, None, True, None, None])
    >>> pc.all(arr)
    <pyarrow.BooleanScalar: True>
    >>> pc.all(arr, skip_nulls = False)
    <pyarrow.BooleanScalar: None>
    >>> pc.all(arr, min_count = 4)
    <pyarrow.BooleanScalar: None>
    >>> pc.all(arr, min_count = 10)
    <pyarrow.BooleanScalar: None>
    >>> pc.all(arr, min_count = 2)
    <pyarrow.BooleanScalar: True>