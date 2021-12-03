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


.. SCOPE OF THIS SECTION
.. This section should include extra description to the
.. language-specific documentation. Possible topics to
.. add: How to run one test, test file or all the tests
.. together and why is it good to do that.
.. What if the unconnected tests start failing? etc.


.. _testing:

***********
Testing 🧪
***********

In Arrow we use unit tests to ensure code quality, to help
others understand our code better and to make the process
of review easier.

Adding a new unit tests is needed when adding a new feature,
a binding or a bug fix. 

It can also happen your work will involve adding a unit test
to a code already written, modifying a unit test or even making
more than one unit test for the changes in the code.

It is also possible to make changes that do not require adding
a unit test like :ref:`documentation`.

If you do need to work with unit tests, this section will help
you with the necessary steps.

.. tabs::

   .. tab:: Pytest

      We use `pytest <https://docs.pytest.org/en/latest/>`_ for
      unit tests in Python. For more info about the required
      packages follow
      :ref:`Python unit testing section <python-unit-testing>`.

      What we normally do is run the test we are working on
      only. Once we are finished with our work and then 
      we run other tests also.

      To run a specific unit test use this command in 
      the terminal from the ``arrow/python`` folder:

      .. code-block::

         python -m pytest pyarrow/tests/test_file.py -k test_your_unit_test

      Run all the tests from one file:

      .. code-block::

         python -m pytest pyarrow/tests/test_file.py

      Run all the tests:

      .. code-block::

         python -m pytest pyarrow

      If the tests start failing try to recompile
      PyArrow or C++.
      
      .. note::

         **Recompiling Cython**

         If you change only the .py file you do not need to
         recompile PyArrow. But you have to that if you make
         changes in .pyx or .pxd files.
        
         To do that run this command again:

         .. code-block::

            python setup.py build_ext --inplace

      .. note::
		
         **Recompiling C++**

         Similarly you will need to recompile C++ if you have
         done some changes in C++ files. In this case
         re-run the cmake commands again. 

   .. tab:: R tests

      .. TODO
