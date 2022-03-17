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

.. currentmodule:: pyarrow

=====================
Environment Variables
=====================

The following environment variables can be used to affect the behavior of
PyArrow.

.. envvar:: ARROW_HOME

   The base path to the PyArrow installation.  This variable overrides the
   default computation of library paths in introspection functions such
   as :func:`get_library_dirs`.

.. envvar:: ARROW_PRE_0_15_IPC_FORMAT

   If this environment variable is set to a non-zero integer value, the PyArrow
   IPC writer will default to the pre-0.15 Arrow IPC format.
   This behavior can also be enabled using :attr:`IpcWriteOptions.use_legacy_format`.

.. envvar:: ARROW_PRE_1_0_METADATA_VERSION

   If this environment variable is set to a non-zero integer value, the PyArrow
   IPC writer will write V4 Arrow metadata (corresponding to pre-1.0 Arrow
   with an incompatible Union data layout).
   This behavior can also be enabled using :attr:`IpcWriteOptions.metadata_version`.

.. envvar:: PKG_CONFIG

   The path to the ``pkg-config`` executable.  This may be required for
   proper functioning of introspection functions such as
   :func:`get_library_dirs` if ``pkg-config`` is not available on the system
   ``PATH``.

.. envvar:: PYARROW_IGNORE_TIMEZONE

   By default, PyArrow propagates the timezone value when converting
   Arrow data to/from Python datetime objects. If this environment variable
   is set to a non-empty value, the timezone is not propagated.


.. note::

   Since PyArrow is based on Arrow C++, its behavior is also affected by
   the :ref:`Arrow C++ environment variables <cpp_env_vars>`.
