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

.. highlight:: c

.. _c-stream-interface:

============================
The Arrow C stream interface
============================

.. warning::
   This interface is experimental and may evolve based on feedback from
   early users.  ABI stability is not guaranteed yet.  Feel free to
   `contact us <https://arrow.apache.org/community/>`__.

The C stream interface builds on the structures defined in the
:ref:`C data interface <c-data-interface>` and combines them into a higher-level
specification so as to ease the communication of streaming data within a single
process.

Semantics
=========

An Arrow C stream exposes a streaming source of data chunks, each with the
same schema.  Chunks are obtained by calling a blocking pull-style iteration
function.

Structure definition
====================

The C stream interface is defined by a single ``struct`` definition::

   struct ArrowArrayStream {
     // Callbacks providing stream functionality
     int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
     int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
     const char* (*get_last_error)(struct ArrowArrayStream*);

     // Release callback
     void (*release)(struct ArrowArrayStream*);

     // Opaque producer-specific data
     void* private_data;
   };

The ArrowArrayStream structure
------------------------------

The ``ArrowArrayStream`` provides the required callbacks to interact with a
streaming source of Arrow arrays.  It has the following fields:

.. c:member:: int (*ArrowArrayStream.get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out)

   *Mandatory.*  This callback allows the consumer to query the schema of
   the chunks of data in the stream.  The schema is the same for all
   data chunks.

   This callback must NOT be called on a released ``ArrowArrayStream``.

   *Return value:* 0 on success, a non-zero
   :ref:`error code <c-stream-interface-error-codes>` otherwise.

.. c:member:: int (*ArrowArrayStream.get_next)(struct ArrowArrayStream*, struct ArrowArray* out)

   *Mandatory.*  This callback allows the consumer to get the next chunk
   of data in the stream.

   This callback must NOT be called on a released ``ArrowArrayStream``.

   *Return value:* 0 on success, a non-zero
   :ref:`error code <c-stream-interface-error-codes>` otherwise.

   On success, the consumer must check whether the ``ArrowArray`` is
   marked :ref:`released <c-data-interface-released>`.  If the
   ``ArrowArray`` is released, then the end of stream has been reached.
   Otherwise, the ``ArrowArray`` contains a valid data chunk.

.. c:member:: const char* (*ArrowArrayStream.get_last_error)(struct ArrowArrayStream*)

   *Mandatory.*  This callback allows the consumer to get a textual description
   of the last error.

   This callback must ONLY be called if the last operation on the
   ``ArrowArrayStream`` returned an error.  It must NOT be called on a
   released ``ArrowArrayStream``.

   *Return value:* a pointer to a NULL-terminated character string (UTF8-encoded).
   NULL can also be returned if no detailed description is available.

   The returned pointer is only guaranteed to be valid until the next call of
   one of the stream's callbacks.  The character string it points to should
   be copied to consumer-managed storage if it is intended to survive longer.

.. c:member:: void (*ArrowArrayStream.release)(struct ArrowArrayStream*)

   *Mandatory.*  A pointer to a producer-provided release callback.

.. c:member:: void* ArrowArrayStream.private_data

   *Optional.*  An opaque pointer to producer-provided private data.

   Consumers MUST not process this member.  Lifetime of this member
   is handled by the producer, and especially by the release callback.


.. _c-stream-interface-error-codes:

Error codes
-----------

The ``get_schema`` and ``get_next`` callbacks may return an error under the form
of a non-zero integer code.  Such error codes should be interpreted like
``errno`` numbers (as defined by the local platform).  Note that the symbolic
forms of these constants are stable from platform to platform, but their numeric
values are platform-specific.

In particular, it is recommended to recognize the following values:

* ``EINVAL``: for a parameter or input validation error
* ``ENOMEM``: for a memory allocation failure (out of memory)
* ``EIO``: for a generic input/output error

.. seealso::
   `Standard POSIX error codes <https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/errno.h.html>`__.

   `Error codes recognized by the Windows C runtime library
   <https://docs.microsoft.com/en-us/cpp/c-runtime-library/errno-doserrno-sys-errlist-and-sys-nerr>`__.

Result lifetimes
----------------

The data returned by the ``get_schema`` and ``get_next`` callbacks must be
released independently.  Their lifetimes are not tied to that of the
``ArrowArrayStream``.

Stream lifetime
---------------

Lifetime of the C stream is managed using a release callback with similar
usage as in the :ref:`C data interface <c-data-interface-released>`.

Thread safety
-------------

The stream source is not assumed to be thread-safe.  Consumers wanting to
call ``get_next`` from several threads should ensure those calls are
serialized.

C consumer example
==================

Let's say a particular database provides the following C API to execute
a SQL query and return the result set as a Arrow C stream::

   void MyDB_Query(const char* query, struct ArrowArrayStream* result_set);

Then a consumer could use the following code to iterate over the results::

   static void handle_error(int errcode, struct ArrowArrayStream* stream) {
      // Print stream error
      const char* errdesc = stream->get_last_error(stream);
      if (errdesc != NULL) {
         fputs(errdesc, stderr);
      } else {
         fputs(strerror(errcode), stderr);
      }
      // Release stream and abort
      stream->release(stream),
      exit(1);
   }

   void run_query() {
      struct ArrowArrayStream stream;
      struct ArrowSchema schema;
      struct ArrowArray chunk;
      int errcode;

      MyDB_Query("SELECT * FROM my_table", &stream);

      // Query result set schema
      errcode = stream.get_schema(&stream, &schema);
      if (errcode != 0) {
         handle_error(errcode, &stream);
      }

      int64_t num_rows = 0;

      // Iterate over results: loop until error or end of stream
      while ((errcode = stream.get_next(&stream, &chunk) == 0) &&
             chunk.release != NULL) {
         // Do something with chunk...
         fprintf(stderr, "Result chunk: got %lld rows\n", chunk.length);
         num_rows += chunk.length;

         // Release chunk
         chunk.release(&chunk);
      }

      // Was it an error?
      if (errcode != 0) {
         handle_error(errcode, &stream);
      }

      fprintf(stderr, "Result stream ended: total %lld rows\n", num_rows);

      // Release schema and stream
      schema.release(&schema);
      stream.release(&stream);
   }
