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
.. highlight:: console

==========================
Debugging code using Arrow
==========================

.. _cpp_gdb_extension:

GDB extension for Arrow C++
===========================

By default, when asked to print the value of a C++ object,
`GDB <https://www.sourceware.org/gdb/>`_ displays the contents of its
member variables.  However, for C++ objects this does not often yield
a very useful output, as C++ classes tend to hide their implementation details
behind methods and accessors.

For example, here is how a :class:`arrow::Status` instance may be displayed
by GDB::

   $3 = {
     <arrow::util::EqualityComparable<arrow::Status>> = {<No data fields>},
     <arrow::util::ToStringOstreamable<arrow::Status>> = {<No data fields>},
     members of arrow::Status:
     state_ = 0x0
   }

and here is a :class:`arrow::Decimal128Scalar`::

   $4 = (arrow::Decimal128Scalar) {
     <arrow::DecimalScalar<arrow::Decimal128Type, arrow::Decimal128>> = {
       <arrow::internal::PrimitiveScalarBase> = {
         <arrow::Scalar> = {
           <arrow::util::EqualityComparable<arrow::Scalar>> = {<No data fields>},
           members of arrow::Scalar:
           _vptr.Scalar = 0x7ffff6870e78 <vtable for arrow::Decimal128Scalar+16>,
           type = std::shared_ptr<arrow::DataType> (use count 1, weak count 0) = {
             get() = 0x555555ce58a0
           },
           is_valid = true
         }, <No data fields>},
       members of arrow::DecimalScalar<arrow::Decimal128Type, arrow::Decimal128>:
       value = {
         <arrow::BasicDecimal128> = {
           <arrow::GenericBasicDecimal<arrow::BasicDecimal128, 128, 2>> = {
             static kHighWordIndex = <optimized out>,
             static kBitWidth = 128,
             static kByteWidth = 16,
             static LittleEndianArray = <optimized out>,
             array_ = {
               _M_elems = {[0] = 1234567, [1] = 0}
             }
           },
           members of arrow::BasicDecimal128:
           static kMaxPrecision = 38,
           static kMaxScale = 38
         }, <No data fields>}
     }, <No data fields>}

Fortunately, GDB also allows custom extensions to override the default printing
for specific types.  We provide a
`GDB extension <https://github.com/apache/arrow/blob/main/cpp/gdb_arrow.py>`_
written in Python that enables pretty-printing for common Arrow C++ classes,
so as to enable a more productive debugging experience.  For example,
here is how the aforementioned :class:`arrow::Status` instance will be
displayed::

   $5 = arrow::Status::OK()

and here is the same :class:`arrow::Decimal128Scalar` instance as above::

   $6 = arrow::Decimal128Scalar of value 123.4567 [precision=10, scale=4]


Manual loading
--------------

To enable the GDB extension for Arrow, you can simply
`download it <https://github.com/apache/arrow/blob/main/cpp/gdb_arrow.py>`_
somewhere on your computer and ``source`` it from the GDB prompt::

   (gdb) source path/to/gdb_arrow.py

You will have to ``source`` it on each new GDB session.  You might want to
make this implicit by adding the ``source`` invocation in a
`gdbinit <https://sourceware.org/gdb/onlinedocs/gdb/gdbinit-man.html>`_ file.


Automatic loading
-----------------

GDB provides a facility to automatically load scripts or extensions for each
object file or library that is involved in a debugging session.  You will need
to:

1. Find out what the *auto-load* locations are for your GDB install.
   This can be determined using ``show`` subcommands on the GDB prompt;
   the answer will depend on the operating system.

   Here is an example on Ubuntu::

      (gdb) show auto-load scripts-directory
      List of directories from which to load auto-loaded scripts is $debugdir:$datadir/auto-load.
      (gdb) show data-directory
      GDB's data directory is "/usr/share/gdb".
      (gdb) show debug-file-directory
      The directory where separate debug symbols are searched for is "/usr/lib/debug".

   This tells you that the directories used for auto-loading are
   ``$debugdir`` and ``$datadir/auto-load``, which expand to
   ``/usr/lib/debug/`` and ``/usr/share/gdb/auto-load`` respectively.

2. Find out the full path to the Arrow C++ DLL, *with all symlinks resolved*.
   For example, you might have installed Arrow 7.0 in ``/usr/local`` and the
   path to the Arrow C++ DLL could then be ``/usr/local/lib/libarrow.so.700.0.0``.

3. Determine the actual auto-load script path.  It is computed by *a)* taking
   the path of the auto-load directory of your choice, *b)* appending the full
   path to the Arrow C++ DLL, *c)* appending ``-gdb.py`` at the tail.

   In the example above, if we choose ``/usr/share/gdb/auto-load`` as auto-load
   directory, the full path to the auto-load script will have to be
   ``/usr/share/gdb/auto-load/usr/local/lib/libarrow.so.700.0.0-gdb.py``.

4. Either copy or symlink the `GDB extension`_ to the file path determined
   in step 3 above.

If everything went well, then as soon as GDB encounters the Arrow C++ DLL,
it will automatically load the Arrow GDB extension so as to pretty-print
Arrow C++ classes on the display prompt.


Supported classes
-----------------

The Arrow GDB extension provides pretty-printing for the core Arrow C++ classes:

* :class:`arrow::DataType` and subclasses
* :class:`arrow::Field`, :class:`arrow::Schema` and :class:`arrow::KeyValueMetadata`
* :class:`arrow::ArrayData`, :class:`arrow::Array` and subclasses
* :class:`arrow::Scalar` and subclasses
* :class:`arrow::ChunkedArray`, :class:`arrow::RecordBatch` and :class:`arrow::Table`
* :class:`arrow::Datum`

Important utility classes are also covered:

* :class:`arrow::Status` and :class:`arrow::Result`
* :class:`arrow::Buffer` and subclasses
* :class:`arrow::Decimal128`, :class:`arrow::Decimal256`
