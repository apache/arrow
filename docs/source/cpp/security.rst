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

.. _cpp-security:

=======================
Security Considerations
=======================

.. important::
   This document describes the security model for using the Arrow C++ APIs.
   For better understanding of this document, we recommend that you first read
   the :ref:`overall security model <format_security>` for the Arrow project.

API parameter validity
======================

Many Arrow C++ APIs report errors using the :class:`arrow::Status` and
:class:`arrow::Result` types. Such APIs can be assumed to detect common errors
in the provided arguments. However, there are also often implicit pre-conditions
that have to be upheld; these can usually be deduced from the semantics of an
API as described by its documentation.

.. seealso:: Arrow C++ :ref:`cpp-conventions`

Pointer validity
----------------

Pointers are always assumed to be valid and point to memory of the size required
by the API. In particular, it is *forbidden to pass a null pointer* except where
the API documentation explicitly says otherwise.

Type restrictions
-----------------

Some APIs are specified to operate on specific Arrow data types and may not
verify that their arguments conform to the expected data types. Passing the
wrong kind of data as input may lead to undefined behavior.

.. _cpp-valid-data:

Data validity
-------------

Arrow data, for example passed as :class:`arrow::Array` or :class:`arrow::Table`,
is always assumed to be :ref:`valid <format-invalid-data>`. If your program may
encounter invalid data, it must explicitly check its validity by calling one of
the following validation APIs.

Structural validity
'''''''''''''''''''

The ``Validate`` methods exposed on various Arrow C++ classes perform relatively
inexpensive validity checks that the data is structurally valid. This implies
checking the number of buffers, child arrays, and other similar conditions.

* :func:`arrow::Array::Validate`
* :func:`arrow::RecordBatch::Validate`
* :func:`arrow::ChunkedArray::Validate`
* :func:`arrow::Table::Validate`
* :func:`arrow::Scalar::Validate`

These checks typically are constant-time against the number of rows in the data,
but linear in the number of descendant fields. They can be good enough to detect
potential bugs in your own code. However, they are not enough to detect all classes of
invalid data, and they won't protect against all kinds of malicious payloads.

Full validity
'''''''''''''

The ``ValidateFull`` methods exposed by the same classes perform the same validity
checks as the ``Validate`` methods, but they also check the data extensively for
any non-conformance to the Arrow spec. In particular, they check all the offsets
of variable-length data types, which is of fundamental importance when ingesting
untrusted data from sources such as the IPC format (otherwise the variable-length
offsets could point outside of the corresponding data buffer). They also check
for invalid values, such as invalid UTF-8 strings or decimal values out of range
for the advertised precision.

* :func:`arrow::Array::ValidateFull`
* :func:`arrow::RecordBatch::ValidateFull`
* :func:`arrow::ChunkedArray::ValidateFull`
* :func:`arrow::Table::ValidateFull`
* :func:`arrow::Scalar::ValidateFull`

"Safe" and "unsafe" APIs
------------------------

Some APIs are exposed in both "safe" and "unsafe" variants. The naming convention
for such pairs varies: sometimes the former has a ``Safe`` suffix (for example
``SliceSafe`` vs. ``Slice``), sometimes the latter has an ``Unsafe`` prefix or
suffix (for example ``Append`` vs. ``UnsafeAppend``).

In all cases, the "unsafe" API is intended as a more efficient API that
eschews some of the checks that the "safe" API performs. It is then up to the
caller to ensure that the preconditions are met, otherwise undefined behavior
may ensue.

The API documentation usually spells out the differences between "safe" and "unsafe"
variants, but these typically fall into two categories:

* structural checks, such as passing the right Arrow data type or numbers of buffers;
* allocation size checks, such as having preallocated enough data for the given input
  arguments (this is typical of the :ref:`array builders <cpp-api-array-builders>`
  and :ref:`buffer builders <cpp-api-buffer-builders>`).

Ingesting untrusted data
========================

As an exception to the above (see :ref:`cpp-valid-data`), some APIs support ingesting
untrusted, potentially malicious data. These are:

* the :ref:`IPC reader <cpp-ipc-reading>` APIs
* the :ref:`Parquet reader <cpp-parquet-reading>` APIs
* the :ref:`CSV reader <cpp-csv-reading>` APIs

IPC and Parquet readers
-----------------------

You must not assume that these will always return valid Arrow data. The reason
for not validating data automatically is that validation can be expensive but
unnecessary when reading from trusted data sources.

Instead, when using these APIs with potentially invalid data (such as data coming
from an untrusted source), you **must** follow these steps:

1. Check any error returned by the API, as with any other API
2. If the API returned successfully, validate the returned Arrow data in full
   (see "Full validity" above)

CSV reader
----------

With the default :class:`conversion options <arrow::csv::ConvertOptions>`,
the CSV reader will either return valid Arrow data or error out. Some options,
however, allow relaxing the corresponding checks in favor of performance.
