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

.. _format_security:

***********************
Security Considerations
***********************

This document describes security considerations when reading Arrow
data from untrusted sources. It focuses specifically on data passed in a
standardized serialized form (such as a IPC stream), as opposed to an
implementation-specific native representation (such as ``arrow::Array`` in C++).

.. note::
   Implementation-specific concerns, such as bad API usage, are out of scope
   for this document. Please refer to the implementation's own documentation.


Who should read this
====================

You should read this document if you belong to either of these two categories:

1. *users* of Arrow: that is, developers of third-party libraries or applications
   that don't directly implement the Arrow formats or protocols, but instead
   call language-specific APIs provided by an Arrow library (as defined below);

2. *implementors* of Arrow libraries: that is, libraries that provide APIs
   abstracting away from the details of the Arrow formats and protocols; such
   libraries include, but are not limited to, the official Arrow implementations
   documented on https://arrow.apache.org.


Columnar Format
===============

Invalid data
------------

The Arrow :ref:`columnar format <_format_columnar>` is an efficient binary
representation with a focus on performance and efficiency. While the format
does not store raw pointers, the contents of Arrow buffers are often
combined and converted to pointers into the process' address space.
Invalid Arrow data may therefore cause invalid memory accesses
(potentially crashing the process) or access to non-Arrow data
(potentially allowing an attacker to exfiltrate confidential information).

For instance, to read a value from a Binary array, one needs to 1) read the
values' offsets from the array's offsets buffer, and 2) read the range of bytes
delimited by these offsets in the array's data buffer. If the offsets are
invalid (deliberately or not), then step 2) can access memory outside of the
data buffer's range.

Another instance of invalid data lies in the values themselves. For example,
a String array is only allowed to contain valid UTF-8 data, but an untrusted
source might have emitted invalid UTF-8 under the disguise of a String array.
An unsuspecting algorithm that is only specified for valid UTF-8 inputs might
lead to dangerous behavior (for example by reading memory out of bounds when
looking for an UTF-8 character boundary).

Fortunately, knowing its schema, it is possible to validate Arrow data up front,
so that reading this data will not pose any danger later on.

.. TODO:
   For each layout, we should list the associated security risks and the recommended
   steps to validate (perhaps in Columnar.rst)

Advice for users
''''''''''''''''

Arrow implementations often assume inputs follow the specification to provide
high speed processing. It is **extremely recommended** that your application
explicitly validates any Arrow data it receives under serialized form
from untrusted sources. Many Arrow implementations provide explicit APIs to
perform such validation.

.. TODO: link to some validation APIs for the main implementations here?

Advice for implementors
'''''''''''''''''''''''

It is **recommended** that you provide dedicated APIs to validate Arrow arrays
and/or record batches. Users will be able to utilize those APIs to assert whether
data coming from untrusted sources can be safely accessed.

A typical validation API must return a well-defined error, not crash, if the
given Arrow data is invalid; it must always be safe to execute regardless of
whether the data is valid or not.

Uninitialized data
------------------

A less obvious pitfall is when some parts of an Arrow array are left uninitialized.
For example, if an element of a primitive Arrow array is marked null through its
validity bitmap, the corresponding value slot in the values buffer can be ignored
for all purposes. It is therefore tempting, when creating an array with null
values, to not initialize the corresponding value slots.

However, this then introduces a serious security risk if the Arrow data is
serialized and published (e.g. using IPC or Flight) such that it can be
accessed by untrusted users. Indeed, the uninitialized value slot can
reveal data left by a previous memory allocation made in the same process.
Depending on the application, this data could contain confidential information.

Advice for users and implementors
'''''''''''''''''''''''''''''''''

When creating a Arrow array, it is **recommended** that you never leave any
data uninitialized in a buffer if the array might be sent to, or read by, an
untrusted third-party, even when the uninitialized data is logically
irrelevant. The easiest way to do this is to zero-initialize any buffer that
will not be populated in full.

If it is determined, through benchmarking, that zero-initialization imposes
an excessive performance cost, a library or application may instead decide
to use uninitialized memory internally as an optimization; but it should then
ensure all such uninitialized values are cleared before passing the Arrow data
to another system.

.. note::
   Sending Arrow data out of the current process can happen *indirectly*,
   for example if you produce it over the C Data Interface and the consumer
   persists it using the IPC format on some public storage.


C Data Interface
================

The C Data Interface contains raw pointers into the process' address space.
It is generally not possible to validate that those pointers are legitimate;
read from such a pointer may crash or access unrelated or bogus data.

Advice for users
----------------

You should **never** consume a C Data Interface structure from an untrusted
producer, as it is by construction impossible to guard against dangerous
behavior in this case.

Advice for implementors
-----------------------

When consuming a C Data Interface structure, you can assume that it comes from
a trusted producer, for the reason explained above. However, it is still
**recommended** that you validate it for soundness (for example that the right
number of buffers is passed for a given datatype), as a trusted producer can
have bugs anyway.


IPC Format
==========

The :ref:`IPC format <_ipc-message-format>` is a serialization format for the
columnar format with associated metadata. Reading an IPC stream or file from
an untrusted source comes with similar caveats as reading the Arrow columnar
format.

The additional signalisation and metadata in the IPC format come with
their own risks. For example, buffer offsets and sizes encoded in IPC messages
may be out of bounds for the IPC stream; Flatbuffers-encoded metadata payloads
may carry incorrect offsets pointing outside of the designated metadata area.

Advice for users
----------------

Arrow libraries will typically ensure IPC streams are structurally valid
but may not also validate the underlying Array data. It is **extremely recommended**
that you use the appropriate APIs to validate the Arrow data read from an untrusted IPC stream.

Advice for implementors
-----------------------

It is **extremely recommended** to run dedicated validation checks when decoding
the IPC format, to make sure that the decoding can not induce unwanted behavior.
Failing those checks should return a well-known error to the caller, not crash.


Extension Types
===============

Extension types typically register a custom deserialization hook so that they
can be automatically recreated when reading from an external source (for example
using IPC). The deserialization hook has to decode the extension type's parameters
from a string or binary payload specific to the extension type.
:ref:`Typical examples <opaque_extension>` use a bespoke JSON representation
with object fields representing the various parameters.

When reading data from an untrusted source, any registered deserialization hook
could be called with an arbitrary payload. It is therefore of primary importance
that the hook be safe to call on invalid, potentially malicious, data. This mandates
the use of a robust metadata serialization schema (such as JSON, but not Python's
`pickle <https://docs.python.org/3/library/pickle.html>`__ or R's
`serialize() <https://stat.ethz.ch/R-manual/R-devel/library/base/html/serialize.html>`__,
for example).

Advice for users and implementors
---------------------------------

When designing an extension type, it is **extremely recommended** to choose a
metadata serialization format that is robust against potentially malicious
data.

When implementing an extension type, it is **recommended** to ensure that the
deserialization hook is able to detect, and error out gracefully, if the
serialized metadata payload is invalid.


Testing for robustness
======================

Advice for implementors
-----------------------

For APIs that may process untrusted inputs, it is **extremely recommended**
that your unit tests exercise your APIs against typical kinds of invalid data.
For example, your validation APIs will have to be tested against invalid Binary
or List offsets, invalid UTF-8 data in a String array, etc.

Testing against known regression files
''''''''''''''''''''''''''''''''''''''

The `arrow-testing <https://github.com/apache/arrow-testing/>`__ repository
contains regression files for various formats, such as the IPC format.

Two categories of files are especially noteworthy and can serve to exercise
an Arrow implementation's robustness:

1. :ref:`gold integration files <format-gold-integration-files>` that are valid
   files to exercise compliance with Arrow IPC features;
2. :ref:`fuzz regression files <fuzz-regression-files>` that have been automatically
   generated each time a fuzzer founds a bug triggered by a specific (usually invalid)
   input for a given format.

Fuzzing
'''''''

It is **recommended** that you go one step further and set up some kind of
automated robustness testing against unforeseen inputs. One typical approach
is though fuzzing, possibly coupled with a runtime instrumentation framework
that detects dangerous behavior (such as Address Sanitizer in C++ or
Rust).

A reasonable way of setting up fuzzing for Arrow is using the IPC format as
a binary payload; the fuzz target should not only attempt to decode the IPC
stream as Arrow data, but it should then validate the Arrow data.
This will strengthen both the IPC decoder and the validation routines
against invalid, potentially malicious data. Finally, if validation comes out
successfully, the fuzz target may exercise some important core functionality,
such as printing the data for human display; this will help ensure that the
validation routine did not let through invalid data that may lead to dangerous
behavior.


Non-Arrow formats and protocols
===============================

Arrow data can also be sent or stored using third-party formats such as Apache
Parquet. Those formats may or may not present the same security risks as listed
above (for example, the precautions around uninitialized data may not apply
in a format like Parquet that does not create any value slots for null elements).
We suggest you refer to these projects' own documentation for more concrete
guidelines.
