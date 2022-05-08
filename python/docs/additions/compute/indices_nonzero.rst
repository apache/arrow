indices_nonzero
===============

Returns
-------

pyarrow.lib.UInt64Array
    A pyarrow Integer array of the indices that have a non-False and valid value. 

Details
-------
Note that `indices_nonzero` does not work with string arrays.

Examples
--------
.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> intarr = pa.array([-2, -1, 1, 0, None, 3, 5, 22, -9999, 9999])
    >>> okay_indices = pc.indices_nonzero(intarr)
    >>> okay_indices.tolist()
    [0, 1, 2, 5, 6, 7, 8, 9]

    >>> boolarr = pa.array([True, False, None, True, False])
    >>> okay_indices = pc.indices_nonzero(boolarr)
    >>> okay_indices.tolist()
    [0, 3]
