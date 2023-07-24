.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Integrating PyArrow with R
==========================

Arrow supports exchanging data within the same process through the
:ref:`c-data-interface`.

This can be used to exchange data between Python and R functions and
methods so that the two languages can interact without any cost of
marshaling and unmarshaling data.

.. note::

    The article takes for granted that you have a ``Python`` environment
    with ``pyarrow`` correctly installed and an ``R`` environment with
    ``arrow`` library correctly installed. 
    See `Python Install Instructions <https://arrow.apache.org/docs/python/install.html>`_
    and `R Install instructions <https://arrow.apache.org/docs/r/#installation>`_
    for further details.

Invoking R functions from Python
--------------------------------

Suppose we have a simple R function receiving an Arrow Array to
add ``3`` to all its elements:

.. code-block:: R

    library(arrow)

    addthree <- function(arr) {
        return(arr + 3L)
    }

We could save such a function in a ``addthree.R`` file so that we can
make it available for reuse.

Once the ``addthree.R`` file is created we can invoke any of its functions
from Python using the 
`rpy2 <https://rpy2.github.io/doc/latest/html/index.html>`_ library which
enables a R runtime within the Python interpreter.

``rpy2`` can be installed using ``pip`` like most Python libraries

.. code-block:: bash

    $ pip install rpy2

The most basic thing we can do with our ``addthree`` function is to
invoke it from Python with a number and see how it will return the result.

To do so we can create an ``addthree.py`` file which uses ``rpy2`` to
import the ``addthree`` function from ``addthree.R`` file and invoke it:

.. code-block:: python

    import rpy2.robjects as robjects

    # Load the addthree.R file
    r_source = robjects.r["source"]
    r_source("addthree.R")

    # Get a reference to the addthree function
    addthree = robjects.r["addthree"]

    # Invoke the function
    r = addthree(3)

    # Access the returned value
    value = r[0]
    print(value)

Running the ``addthree.py`` file will show how our Python code is able
to access the ``R`` function and print the expected result:

.. code-block:: bash

    $ python addthree.py 
    6

If instead of passing around basic data types we want to pass around
Arrow Arrays, we can do so relying on the
`rpy2-arrow <https://rpy2.github.io/rpy2-arrow/version/main/html/index.html>`_ 
module which implements ``rpy2`` support for Arrow types.

``rpy2-arrow`` can be installed through ``pip``:

.. code-block:: bash

    $ pip install rpy2-arrow

``rpy2-arrow`` implements converters from PyArrow objects to R Arrow objects,
this is done without incurring any data copy cost as it relies on the
C Data interface.

To pass to the ``addthree`` function a PyArrow array, our ``addthree.py`` file needs to be modified
to enable ``rpy2-arrow`` converters and then pass the PyArrow array:

.. code-block:: python

    import rpy2.robjects as robjects
    from rpy2_arrow.pyarrow_rarrow import (rarrow_to_py_array,
                                           converter as arrowconverter)
    from rpy2.robjects.conversion import localconverter

    r_source = robjects.r["source"]
    r_source("addthree.R")

    addthree = robjects.r["addthree"]

    import pyarrow

    array = pyarrow.array((1, 2, 3))

    # Enable rpy2-arrow converter so that R can receive the array.
    with localconverter(arrowconverter):
        r_result = addthree(array)

    # The result of the R function will be an R Environment
    # we can convert the Environment back to a pyarrow Array
    # using the rarrow_to_py_array function
    py_result = rarrow_to_py_array(r_result)
    print("RESULT", type(py_result), py_result)

Running the newly modified ``addthree.py`` should now properly execute
the R function and print the resulting PyArrow Array:

.. code-block:: bash

    $ python addthree.py
    RESULT <class 'pyarrow.lib.Int64Array'> [
      4,
      5,
      6
    ]

For additional information you can refer to
`rpy2 Documentation <https://rpy2.github.io/doc/latest/html/index.html>`_
and `rpy2-arrow Documentation <https://rpy2.github.io/rpy2-arrow/version/main/html/index.html>`_

Invoking Python functions from R
--------------------------------

Exposing Python functions to R can be done through the ``reticulate``
library. For example if we want to invoke :func:`pyarrow.compute.add` from
R on an Array created in R we can do so importing ``pyarrow`` in R
through ``reticulate``.

A basic ``addthree.R`` script that invokes ``add`` to add ``3`` to
an R array would look like:

.. code-block:: R

    # Load arrow and reticulate libraries
    library(arrow)
    library(reticulate)

    # Create a new array in R
    a <- Array$create(c(1, 2, 3))

    # Make pyarrow.compute available to R
    pc <- import("pyarrow.compute")

    # Invoke pyarrow.compute.add with the array and 3
    # This will add 3 to all elements of the array and return a new Array
    result <- pc$add(a, 3)

    # Print the result to confirm it's what we expect
    print(result)

Invoking the ``addthree.R`` script will print the outcome of adding
``3`` to all the elements of the original ``Array$create(c(1, 2, 3))`` array:

.. code-block:: bash

    $ R --silent -f addthree.R 
    Array
    <double>
    [
      4,
      5,
      6
    ]

For additional information you can refer to
`Reticulate Documentation <https://rstudio.github.io/reticulate/>`_
and to the `R Arrow documentation <https://arrow.apache.org/docs/r/articles/python.html#using>`_

R to Python communication using the C Data Interface
----------------------------------------------------

Both solutions described above use the Arrow C Data
interface under the hood.

In case we want to extend the previous ``addthree`` example to switch
from using ``rpy2-arrow`` to using the plain C Data interface we can
do so by introducing some modifications to our codebase.

To enable importing the Arrow Array from the C Data interface we have to
wrap our ``addthree`` function in a function that does the extra work
necessary to import an Arrow Array in R from the C Data interface.

That work will be done by the ``addthree_cdata`` function which invokes the
``addthree`` function once the Array is imported.

Our ``addthree.R`` will thus have both the ``addthree_cdata`` and the 
``addthree`` functions:

.. code-block:: R

    library(arrow)

    addthree_cdata <- function(array_ptr_s, schema_ptr_s) {
        a <- Array$import_from_c(array_ptr, schema_ptr)

        return(addthree(a))
    }

    addthree <- function(arr) {
        return(arr + 3L)
    }

We can now provide to R the array and its schema from Python through the
``array_ptr_s`` and ``schema_ptr_s`` arguments so that R can build back
an ``Array`` from them and then invoke ``addthree`` with the array.

Invoking ``addthree_cdata`` from Python involves building the Array we
want to pass to ``R``, exporting it to the C Data interface and then
passing the exported references to the ``R`` function.

Our ``addthree.py`` will thus become:

.. code-block:: python

    # Get a reference to the addthree_cdata R function
    import rpy2.robjects as robjects
    r_source = robjects.r["source"]
    r_source("addthree.R")
    addthree_cdata = robjects.r["addthree_cdata"]

    # Create the pyarrow array we want to pass to R
    import pyarrow
    array = pyarrow.array((1, 2, 3))

    # Import the pyarrow module that provides access to the C Data interface
    from pyarrow.cffi import ffi as arrow_c

    # Allocate structures where we will export the Array data 
    # and the Array schema. They will be released when we exit the with block.
    with arrow_c.new("struct ArrowArray*") as c_array, \
         arrow_c.new("struct ArrowSchema*") as c_schema:
        # Get the references to the C Data structures.
        c_array_ptr = int(arrow_c.cast("uintptr_t", c_array))
        c_schema_ptr = int(arrow_c.cast("uintptr_t", c_schema))

        # Export the Array and its schema to the C Data structures.
        array._export_to_c(c_array_ptr)
        array.type._export_to_c(c_schema_ptr)

        # Invoke the R addthree_cdata function passing the references
        # to the array and schema C Data structures. 
        # Those references are passed as strings as R doesn't have
        # native support for 64bit integers, so the integers are
        # converted to their string representation for R to convert it back.
        r_result_array = addthree_cdata(str(c_array_ptr), str(c_schema_ptr))

        # r_result will be an Environment variable that contains the
        # arrow Array built from R as the return value of addthree.
        # To make it available as a Python pyarrow array we need to export
        # it as a C Data structure invoking the Array$export_to_c R method
        r_result_array["export_to_c"](str(c_array_ptr), str(c_schema_ptr))

        # Once the returned array is exported to a C Data infrastructure
        # we can import it back into pyarrow using Array._import_from_c
        py_array = pyarrow.Array._import_from_c(c_array_ptr, c_schema_ptr)
    
    print("RESULT", py_array)

Running the newly changed ``addthree.py`` will now print the Array resulting
from adding ``3`` to all the elements of the original 
``pyarrow.array((1, 2, 3))`` array:

.. code-block:: bash

    $ python addthree.py 
    R[write to console]: Attaching package: ‘arrow’
    RESULT [
      4,
      5,
      6
    ]