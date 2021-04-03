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


.. currentmodule:: pyarrow.gandiva

Gandiva Expression Compiler
===========================

.. ifconfig:: not gandiva_enabled

   .. error::
      This documentation was built without Gandiva enabled.  The Gandiva
      API docs are not available.

.. warning:: As present, Gandiva is only built on PyArrow Conda distributions, not PyPI wheels.


Function Registry
-----------------

.. autosummary::
    :toctree: ../generated/

    get_registered_function_signatures
    FunctionSignature

.. seealso::

    :ref:`Available Gandiva functions (C++ documentation) <gandiva-function-list>`.


Expression Tree Builder
-----------------------

.. autosummary::
    :toctree: ../generated/

    TreeExprBuilder
    Condition
    Expression
    Node


Execution Engines
-----------------

.. autosummary::
    :toctree: ../generated/

    make_filter
    make_projector
    Filter
    Projector
    SelectionVector
