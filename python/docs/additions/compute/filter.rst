filter 
======

Examples
--------
.. code-block:: python

    >>> import pyarrow as pa
    >>> arr = pa.array(["a", "b", "c", None, "e"])
    >>> mask = pa.array([True, False, None, False, True])
    >>> print(arr.filter(mask))
    [
      "a",
      "e"
    ]
    >>> print(arr.filter(mask, null_selection_behavior='emit_null'))
    [
      "a",
      null,
      "e"
    ]
