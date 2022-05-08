any
===

Examples
--------

.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([True, True, None, False, True])
    >>> pc.any(arr)
    <pyarrow.BooleanScalar: True>
    >>> pc.any(arr, min_count = 4)
    <pyarrow.BooleanScalar: True>
    >>> pc.any(arr, min_count = len(arr))
    <pyarrow.BooleanScalar: None>
    >>> arr = pa.array([False, False, None, False, True])
    >>> pc.any(arr)
    <pyarrow.BooleanScalar: True>
    >>> pc.any(arr, min_count = 2)
    <pyarrow.BooleanScalar: True>
    >>> pc.any(arr, min_count = len(arr))
    <pyarrow.BooleanScalar: None>
    >>> pc.any(arr, skip_nulls = False)
    <pyarrow.BooleanScalar: True>
    >>> pc.any([False,None], skip_nulls = False)
    <pyarrow.BooleanScalar: None>
    >>> pc.any([False,None], skip_nulls = True)
    <pyarrow.BooleanScalar: False>
