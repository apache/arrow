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

How to read this
================

Hereafter we try list potential security concerns when dealing with the various
Arrow specifications. Some of these concerns will apply directly to users of
Arrow through existing implementations. Others should only be relevant for the
implementors of Arrow libraries: by this, we mean libraries that provide APIs
abstracting away from the details of the Arrow formats and protocols.

Columnar Format
===============

The Arrow :ref:`columnar format <_format_columnar>` involves direct access to the
process' address space. As such, in-memory Arrow data should not be accessed
without care.

Invalid data
------------

Reading and interpreting Arrow data involves reading into several buffers,
sometimes in non-trivial ways. This may for instance involve data-dependent
indirect addressing: to read a value from a Binary array, you need to
1) read its offsets in buffer #2, and 2) read the range of bytes delimited by
these offsets in buffer #3. If the offsets are invalid (deliberately or not),
then step 2) can access invalid memory (potentially crashing the process) or
memory unrelated to Arrow (potentially allowing an attacker to exfiltrate
confidential data).

.. TODO:
   For each layout, we should list the associated security risks and the recommended
   steps to validate (perhaps in Columnar.rst)

Advice for users
''''''''''''''''

If you receive Arrow in-memory data from an untrusted source, it is
**extremely recommended** that you first validate the data for structural
soundness before reading it. Many Arrow implementations provide APIs to do
such validation.

.. TODO: link to some validation APIs for the main implementations here?

Advice for implementors
'''''''''''''''''''''''

It is **recommended** that you provide APIs to validate Arrow data, so that users
can assert whether data coming from untrusted sources can be safely accessed.

Uninitialized data
------------------

A less obvious pitfall is when some parts of an Arrow array are left uninitialized.
For example, if a element of a primitive Arrow array is marked null through its
validity bitmap, the corresponding value in the values buffer can be ignored for all
purposes. It is therefore tempting, when creating an array with null values, to
not initialize the corresponding value slots.

However, this then introduces a serious security if the Arrow data is serialized
and published such that it can be accessed by untrusted users. Indeed, the
uninitialized value slot can reveal data left by a previous memory allocation
made in the same process. Depending on the application, this data could contain
confidential information.

Advice for users and implementors
'''''''''''''''''''''''''''''''''

When creating a Arrow array, it is **recommended** that you never leave any data
uninitialized in a buffer if the array might be sent to, or read by, a untrusted
third-party, even when the uninitialized data is logically irrelevant. The
easiest way to do this, though perhaps not the most efficient, is to zero-initialize
any buffer that will not be populated in full.

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
''''''''''''''''

If you produce a C Data Interface structure for data that nevertheless comes
from an untrusted source (for example because you are writing a proxy to
an arbitrary third-party service), it is **recommended** that you validate
the data first, as the consumer may assume that the data is valid already.

You should **never** consume a C Data Interface structure from an untrusted
producer.

Advice for implementors
'''''''''''''''''''''''

When consuming a C Data Interface structure, you can assume that it comes from
a trusted producer, for the reason explained above. However, it is still **recommended**
that you validate it for soundness, as a trusted producer can have bugs anyway.

IPC Format
==========

The :ref:`IPC format <_ipc-message-format>` is a serialization format for the
columnar format with associated metadata. Reading an IPC stream or file from
an untrusted source comes with similar caveats as reading the Arrow columnar
format.

The additional signalisation and metadata in the IPC format come with
its own risks. For example, offsets and sizes encoded in IPC messages may be
out of bounds for the IPC stream; Flatbuffers-encoded payloads may carry
incorrect offsets pointing outside of the IPC stream.

Advice for users
''''''''''''''''

As with the columnar format, it is **extremely recommended** that you validate
any Arrow data read from the IPC format if it comes from an untrusted source.

Advice for implementors
'''''''''''''''''''''''

It is **extremely recommended** to run additional validation checks when decoding
the IPC format, to make sure that the decoding can not induce unwanted behavior
(such as crashing or accessing unrelated memory when decoding a Flatbuffers payload).
