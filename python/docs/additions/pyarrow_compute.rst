.. This is the rst file for pyarrow.compute function overrides. 

.. This should not be rendered as-is to be part of the docs.
.. This is processed using the `pyarrow._docutils` module
.. and incorporated into the relevant `pyarrow.compute` functions. 

.. When adding or changing examples, test that the examples are correct:
.. `python -m doctest -v additions/pyarrow_compute.rst`

.. To understanding how this code becomes part of the pyarrow API reference:
.. 
.. In `python/pyarrow/compute.py`, the `decorate_compute_function` assessbles 
.. the function docstrings for the pyarrow.compute functions as so:

.. Details are additive - a barebones default documentation is generated 
.. from the C++ doc information, which you can find defined as `FunctionDoc` objects 
.. in the `cpp/src/arrow` repository directory tree. 

.. Any details included in this file will add to or overwrite the auto-documentation
.. derived from that structure.  

.. To add new details, create a second-level heading for the 
.. pyarrow.compute function if it does not exist and then add 3rd level sections for 
.. each override.  

.. The following substructures are supported.  In most cases, the blocks will add 
.. sequentially in the order that they appear here. However, `Examples` will always be at the bottom.  
.. Feel free to add notes (`.. note::`) as meta-commentary on your intent for the 
.. documentation itself - these will **not** be rendered in the final docs.

.. Recognized section constructs:

.. -  `Description`: overwrite the default Description provided by the cpp docs.
.. - `Details` : preserve the default Description but add the block to it afterward.
.. - `Parameters`: This must be in reStructured definition format. These will overwrite the details
..                 for the given parameter. Classifiers will be used as the parameter type.
.. - `Returns`: This must be in reStructured definition format. The return type should be the definition
..              term and the definition the descriptive text. Any classifiers are ignored. 
.. - `Examples`: The examples section will always be appended to the bottom

Compute Function Override
=========================

all
---

..function:: all 

Note
~~~~

Notes for this function. This will not be added to the actual function doc.

Description
~~~~~~~~~~~

Description override for this function.

And here is what a separate paragraph would look like.

.. code-block:: python

    print("just to mix up the description block even more")

Okay. Fin.

Examples
~~~~~~~~

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
    

choose
------

..function:: choose

Parameters
~~~~~~~~~~

indices
    The 0-based index values to select.

values : Array-like : Scalar-like : Special classifier
    This is effectively an array-of-arrays where each top-level element can be selected from the index.

    Can this have multiple paragraphs?


indices_nonzero
---------------

..function:: indices_nonzero

Returns
~~~~~~~

pyarrow.lib.UInt64Array
    A pyarrow Integer array of the indices that have a non-False and valid value. 

Details
~~~~~~~
Note that `indices_nonzero` does not work with string arrays.

Examples
~~~~~~~~
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

filter 
------

..function:: filter

Examples
~~~~~~~~
.. code-block:: python

    >>> import pyarrow as pa
    >>> arr = pa.array(["a", "b", "c", None, "e"])
    >>> mask = pa.array([True, False, None, False, True])
    >>> arr.filter(mask)
    <pyarrow.lib.StringArray object at 0x7fa826df9200>
    [
        "a",
        "e"
    ]
    >>> arr.filter(mask, null_selection_behavior='emit_null')
    <pyarrow.lib.StringArray object at 0x7fa826df9200>
    [
        "a",
        null,
        "e"
    ]

mode
----

..function:: mode

Examples
~~~~~~~~
.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([1, 1, 2, 2, 3, 2, 2, 2])
    >>> modes = pc.mode(arr, 2)
    >>> modes[0]
    <pyarrow.StructScalar: {'mode': 2, 'count': 5}>
    >>> modes[1]
    <pyarrow.StructScalar: {'mode': 1, 'count': 2}>