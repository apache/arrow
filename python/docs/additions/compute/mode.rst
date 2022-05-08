mode
====

Examples
--------
.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([1, 1, 2, 2, 3, 2, 2, 2])
    >>> modes = pc.mode(arr, 2)
    >>> modes[0]
    <pyarrow.StructScalar: [('mode', 2), ('count', 5)]>
    >>> modes[1]
    <pyarrow.StructScalar: [('mode', 1), ('count', 2)]>