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

.. highlight:: cpp

===========
Conventions
===========

This section provides some information about some of the abstractions and
development approaches we use to solve problems common to many parts of the C++
project.

File Naming
===========

C++ source and header files should use underscores for word separation, not hyphens.
Compiled executables, however, will automatically use hyphens (such that
e.g. ``src/arrow/scalar_test.cc`` will be compiled into ``arrow-scalar-test``).

C++ header files use the ``.h`` extension. Any header file name not
containing ``internal`` is considered to be a public header, and will be
automatically installed by the build.

Comments and Docstrings
=======================

Regular comments start with ``//``.

Doxygen docstrings start with ``///``, and Doxygen directives start with ``\``,
like this::

   /// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
   ///
   /// \param[in] size size of buffer to allocate
   /// \param[in] pool a memory pool
   ARROW_EXPORT
   Result<std::unique_ptr<Buffer>> AllocateBuffer(const int64_t size,
                                                  MemoryPool* pool = NULLPTR);

The summary line of a docstring uses the infinitive, not the indicative
(for example, "Allocate a buffer" rather than "Allocates a buffer").

Memory Pools
============

We provide a default memory pool with ``arrow::default_memory_pool()``.

Error Handling and Exceptions
=============================

For error handling, we return ``arrow::Status`` values instead of throwing C++
exceptions. Since the Arrow C++ libraries are intended to be useful as a
component in larger C++ projects, using ``Status`` objects can help with good
code hygiene by making explicit when a function is expected to be able to fail.

A more recent option is to return a ``arrow::Result<T>`` object that can
represent either a successful result with a ``T`` value, or an error result
with a ``Status`` value.

For expressing internal invariants and "cannot fail" errors, we use ``DCHECK`` macros
defined in ``arrow/util/logging.h``. These checks are disabled in release builds
and are intended to catch internal development errors, particularly when
refactoring. These macros are not to be included in any public header files.

Since we do not use exceptions, we avoid doing expensive work in object
constructors. Objects that are expensive to construct may often have private
constructors, with public static factory methods that return ``Status`` or
``Result<T>``.

There are a number of object constructors, like ``arrow::Schema`` and
``arrow::RecordBatch`` where larger STL container objects like ``std::vector`` may
be created. While it is possible for ``std::bad_alloc`` to be thrown in these
constructors, the circumstances where they would are somewhat esoteric, and it
is likely that an application would have encountered other more serious
problems prior to having ``std::bad_alloc`` thrown in a constructor.
