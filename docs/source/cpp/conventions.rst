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

.. default-domain:: cpp
.. highlight:: cpp

Conventions
===========

The Arrow C++ API follows a few simple guidelines.  As with many rules,
there may be exceptions.

Language version
----------------

Arrow is C++11-compatible.  A few backports are used for newer functionality,
for example the :class:`std::string_view` class.

Namespacing
-----------

All the Arrow API (except macros) is namespaced inside a ``arrow`` namespace,
and nested namespaces thereof.

Safe pointers
-------------

Arrow objects are usually passed and stored using safe pointers -- most of
the time :class:`std::shared_ptr` but sometimes also :class:`std::unique_ptr`.

Immutability
------------

Many Arrow objects are immutable: once constructed, their logical properties
cannot change anymore.  This makes it possible to use them in multi-threaded
scenarios without requiring tedious and error-prone synchronization.

There are obvious exceptions to this, such as IO objects or mutable data buffers.

Error reporting
---------------

Most APIs indicate a successful or erroneous outcome by returning a
:class:`arrow::Status` instance.  Arrow doesn't throw exceptions of its
own, but third-party exceptions might propagate through, especially
:class:`std::bad_alloc` (but Arrow doesn't use the standard allocators for
large data).

As a consequence, the result value of a function is generally passed as an
out-pointer parameter, rather than as a function return value.

(however, functions which always determiniscally succeed may eschew this
convention and return their result directly)

Here is an example of checking the outcome of an operation::

   const int64_t buffer_size = 4096;
   std::shared_ptr<arrow::Buffer> buffer;

   auto status = arrow::AllocateBuffer(buffer_size, &buffer);
   if (!status.ok()) {
      // ... handle error
   }

If the caller function itself returns a :class:`arrow::Status` and wants
to propagate any non-successful outcomes, a convenience macro
:cpp:func:`ARROW_RETURN_NON_OK` is available::

   arrow::Status DoSomething() {
      const int64_t buffer_size = 4096;
      std::shared_ptr<arrow::Buffer> buffer;
      ARROW_RETURN_NON_OK(arrow::AllocateBuffer(buffer_size, &buffer));
      // ... allocation successful, do something with buffer below

      // return success at the end
      return Status::OK();
   }
