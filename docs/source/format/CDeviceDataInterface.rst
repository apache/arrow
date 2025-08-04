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

.. _c-device-data-interface:

=================================
The Arrow C Device data interface
=================================

.. warning:: The Arrow C Device Data Interface should be considered experimental

Rationale
=========

The current :ref:`C Data Interface <c-data-interface>`, and most
implementations of it, make the assumption that all data buffers provided
are CPU buffers. Since Apache Arrow is designed to be a universal in-memory
format for representing tabular ("columnar") data, there will be the desire
to leverage this data on non-CPU hardware such as GPUs. One example of such
a case is the `RAPIDS cuDF library`_ which uses the Arrow memory format with
CUDA for NVIDIA GPUs. Since copying data from host to device and back is
expensive, the ideal would be to be able to leave the data on the device
for as long as possible, even when passing it between runtimes and
libraries.

The Arrow C Device data interface builds on the existing C data interface
by adding a very small, stable set of C definitions to it. These definitions
are equivalents to the ``ArrowArray`` and ``ArrowArrayStream`` structures
from the C Data Interface which add members to allow specifying the device
type and pass necessary information to synchronize with the producer.
For non-C/C++ languages and runtimes, translating the C definitions to
corresponding C FFI declarations should be just as simple as with the
current C data interface.

Applications and libraries can then use Arrow schemas and Arrow formatted
memory on non-CPU devices to exchange data just as easily as they do
now with CPU data. This will enable leaving data on those devices longer
and avoiding costly copies back and forth between the host and device
just to leverage new libraries and runtimes.

Goals
-----

* Expose an ABI-stable interface built on the existing C data interface.
* Make it easy for third-party projects to implement support with little
  initial investment.
* Allow zero-copy sharing of Arrow formatted device memory between
  independent runtimes and components running in the same process.
* Avoid the need for one-to-one adaptation layers such as the
  `CUDA Array Interface`_ for Python processes to pass CUDA data.
* Enable integration without explicit dependencies (either at compile-time
  or runtime) on the Arrow software project itself.

The intent is for the Arrow C Device data interface to expand the reach
of the current C data interface, allowing it to also become the standard
low-level building block for columnar processing on devices like GPUs or
FPGAs.

Structure definitions
=====================

Because this is built on the C data interface, the C Device data interface
uses the ``ArrowSchema`` and ``ArrowArray`` structures as defined in the
:ref:`C data interface spec <c-data-interface-struct-defs>`. It then adds the
following free-standing definitions. Like the rest of the Arrow project,
they are available under the Apache License 2.0.

.. code-block:: c

    #ifndef ARROW_C_DEVICE_DATA_INTERFACE
    #define ARROW_C_DEVICE_DATA_INTERFACE

    // Device type for the allocated memory
    typedef int32_t ArrowDeviceType;

    // CPU device, same as using ArrowArray directly
    #define ARROW_DEVICE_CPU 1
    // CUDA GPU Device
    #define ARROW_DEVICE_CUDA 2
    // Pinned CUDA CPU memory by cudaMallocHost
    #define ARROW_DEVICE_CUDA_HOST 3
    // OpenCL Device
    #define ARROW_DEVICE_OPENCL 4
    // Vulkan buffer for next-gen graphics
    #define ARROW_DEVICE_VULKAN 7
    // Metal for Apple GPU
    #define ARROW_DEVICE_METAL 8
    // Verilog simulator buffer
    #define ARROW_DEVICE_VPI 9
    // ROCm GPUs for AMD GPUs
    #define ARROW_DEVICE_ROCM 10
    // Pinned ROCm CPU memory allocated by hipMallocHost
    #define ARROW_DEVICE_ROCM_HOST 11
    // Reserved for extension
    //
    // used to quickly test extension devices, semantics
    // can differ based on implementation
    #define ARROW_DEVICE_EXT_DEV 12
    // CUDA managed/unified memory allocated by cudaMallocManaged
    #define ARROW_DEVICE_CUDA_MANAGED 13
    // Unified shared memory allocated on a oneAPI
    // non-partitioned device.
    //
    // A call to the oneAPI runtime is required to determine the
    // device type, the USM allocation type and the sycl context
    // that it is bound to.
    #define ARROW_DEVICE_ONEAPI 14
    // GPU support for next-gen WebGPU standard
    #define ARROW_DEVICE_WEBGPU 15
    // Qualcomm Hexagon DSP
    #define ARROW_DEVICE_HEXAGON 16

    struct ArrowDeviceArray {
      struct ArrowArray array;
      int64_t device_id;
      ArrowDeviceType device_type;
      void* sync_event;

      // reserved bytes for future expansion
      int64_t reserved[3];
    };

    #endif  // ARROW_C_DEVICE_DATA_INTERFACE

.. note::
   The canonical guard ``ARROW_C_DEVICE_DATA_INTERFACE`` is meant to avoid
   duplicate definitions if two projects copy the definitions in their own
   headers, and a third-party project includes from these two projects. It
   is therefore important that this guard is kept exactly as-is when these
   definitions are copied.

ArrowDeviceType
---------------

The ``ArrowDeviceType`` typedef is used to indicate what type of device the
provided memory buffers were allocated on. This, in conjunction with the
``device_id``, should be sufficient to reference the correct data buffers.

We then use macros to define values for different device types. The provided
macro values are compatible with the widely used `dlpack`_ ``DLDeviceType``
definition values, using the same value for each as the equivalent
``kDL<type>`` enum from ``dlpack.h``. The list will be kept in sync with those
equivalent enum values over time to ensure compatibility, rather than
potentially diverging. To avoid the Arrow project having to be in the
position of vetting new hardware devices, new additions should first be
added to dlpack before we add a corresponding macro here.

To ensure predictability with the ABI, we use macros instead of an ``enum``
so the storage type is not compiler dependent.

.. c:macro:: ARROW_DEVICE_CPU

    CPU Device, equivalent to just using ``ArrowArray`` directly instead of
    using ``ArrowDeviceArray``.

.. c:macro:: ARROW_DEVICE_CUDA

    A `CUDA`_ GPU Device. This could represent data allocated either with the
    runtime library (``cudaMalloc``) or the device driver (``cuMemAlloc``).

.. c:macro:: ARROW_DEVICE_CUDA_HOST

    CPU memory that was pinned and page-locked by CUDA by using
    ``cudaMallocHost`` or ``cuMemAllocHost``.

.. c:macro:: ARROW_DEVICE_OPENCL

    Data allocated on the device by using the `OpenCL (Open Computing Language)`_
    framework.

.. c:macro:: ARROW_DEVICE_VULKAN

    Data allocated by the `Vulkan`_ framework and libraries.

.. c:macro:: ARROW_DEVICE_METAL

    Data on Apple GPU devices using the `Metal`_ framework and libraries.

.. c:macro:: ARROW_DEVICE_VPI

    Indicates usage of a Verilog simulator buffer.

.. c:macro:: ARROW_DEVICE_ROCM

    An AMD device using the `ROCm`_ stack.

.. c:macro:: ARROW_DEVICE_ROCM_HOST

    CPU memory that was pinned and page-locked by ROCm by using ``hipMallocHost``.

.. c:macro:: ARROW_DEVICE_EXT_DEV

    This value is an escape-hatch for devices to extend which aren't
    currently represented otherwise. Producers would need to provide
    additional information/context specific to the device if using
    this device type. This is used to quickly test extension devices
    and semantics can differ based on the implementation.

.. c:macro:: ARROW_DEVICE_CUDA_MANAGED

    CUDA managed/unified memory which is allocated by ``cudaMallocManaged``.

.. c:macro:: ARROW_DEVICE_ONEAPI

    Unified shared memory allocated on an Intel `oneAPI`_ non-partitioned
    device. A call to the ``oneAPI`` runtime is required to determine
    the specific device type, the USM allocation type and the sycl context
    that it is bound to.

.. c:macro:: ARROW_DEVICE_WEBGPU

    GPU support for next-gen WebGPU standards

.. c:macro:: ARROW_DEVICE_HEXAGON

    Data allocated on a Qualcomm Hexagon DSP device.

The ArrowDeviceArray structure
------------------------------

The ``ArrowDeviceArray`` structure embeds the C data ``ArrowArray`` structure
and adds additional information necessary for consumers to use the data. It
has the following fields:

.. c:member:: struct ArrowArray ArrowDeviceArray.array

    *Mandatory.* The allocated array data. The values in the ``void**`` buffers (along
    with the buffers of any children) are what is allocated on the device.
    The buffer values should be device pointers. The rest of the structure
    should be accessible to the CPU.

    The ``private_data`` and ``release`` callback of this structure should
    contain any necessary information and structures related to freeing
    the array according to the device it is allocated on, rather than
    having a separate release callback and ``private_data`` pointer here.

.. c:member:: int64_t ArrowDeviceArray.device_id

    *Mandatory.* The device id to identify a specific device if multiple devices of this
    type are on the system. The semantics of the id will be hardware dependent,
    but we use an ``int64_t`` to future-proof the id as devices change over time.

    For device types that do not have an intrinsic notion of a device identifier (e.g.,
    ``ARROW_DEVICE_CPU``), it is recommended to use a ``device_id`` of -1 as a
    convention.

.. c:member:: ArrowDeviceType ArrowDeviceArray.device_type

    *Mandatory.* The type of the device which can access the buffers in the array.

.. c:member:: void* ArrowDeviceArray.sync_event

    *Optional.* An event-like object to synchronize on if needed.

    Many devices, like GPUs, are primarily asynchronous with respect to
    CPU processing. As such, in order to safely access device memory, it is often
    necessary to have an object to synchronize processing with. Since
    different devices will use different types to specify this, we use a
    ``void*`` which can be coerced into a pointer to whatever the device
    appropriate type is.

    If synchronization is not needed, this can be null. If this is non-null
    then it MUST be used to call the appropriate sync method for the device
    (e.g. ``cudaStreamWaitEvent`` or ``hipStreamWaitEvent``) before attempting
    to access the memory in the buffers.

    If an event is provided, then the producer MUST ensure that the exported
    data is available on the device before the event is triggered. The
    consumer SHOULD wait on the event before trying to access the exported
    data.

.. seealso::
    The :ref:`synchronization event types <c-device-data-interface-event-types>`
    section below.

.. c:member:: int64_t ArrowDeviceArray.reserved[3]

    As non-CPU development expands, there may be a need to expand this
    structure. In order to do so without potentially breaking ABI changes,
    we reserve 24 bytes at the end of the object. These bytes MUST be zero'd
    out after initialization by the producer in order to ensure safe
    evolution of the ABI in the future.

.. _c-device-data-interface-event-types:

Synchronization event types
---------------------------

The table below lists the expected event types for each device type.
If no event type is supported ("N/A"), then the ``sync_event`` member
should always be null.

Remember that the event *CAN* be null if synchronization is not needed
to access the data.

+---------------------------+--------------------+---------+
| Device Type               | Actual Event Type  | Notes   |
+===========================+====================+=========+
| ARROW_DEVICE_CPU          | N/A                |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_CUDA         | ``cudaEvent_t*``   |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_CUDA_HOST    | ``cudaEvent_t*``   |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_OPENCL       | ``cl_event*``      |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_VULKAN       | ``VkEvent*``       |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_METAL        | ``MTLEvent*``      |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_VPI          | N/A                | (1)     |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_ROCM         | ``hipEvent_t*``    |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_ROCM_HOST    | ``hipEvent_t*``    |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_EXT_DEV      |                    | (2)     |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_CUDA_MANAGED | ``cudaEvent_t*``   |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_ONEAPI       | ``sycl::event*``   |         |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_WEBGPU       | N/A                | (1)     |
+---------------------------+--------------------+---------+
| ARROW_DEVICE_HEXAGON      | N/A                | (1)     |
+---------------------------+--------------------+---------+

Notes:

* \(1) Currently unknown if framework has an event type to support.
* \(2) Extension Device has producer defined semantics and thus if
  synchronization is needed for an extension device, the producer
  should document the type.

.. _c-device-data-interface-semantics:

Semantics
=========

Memory management
-----------------

First and foremost: Out of everything in this interface, it is *only* the
data buffers themselves which reside in device memory (i.e. the ``buffers``
member of the ``ArrowArray`` struct). Everything else should be in CPU
memory.

The ``ArrowDeviceArray`` structure contains an ``ArrowArray`` object which
itself has :ref:`specific semantics <c-data-interface-semantics>` for releasing
memory. The term *"base structure"* below refers to the ``ArrowDeviceArray``
object that is passed directly between the producer and consumer -- not any
child structure thereof.

It is intended for the base structure to be stack- or heap-allocated by the
*consumer*. In this case, the producer API should take a pointer to the
consumer-allocated structure.

However, any data pointed to by the struct MUST be allocated and maintained
by the producer. This includes the ``sync_event`` member if it is not null,
along with any pointers in the ``ArrowArray`` object as usual. Data lifetime
is managed through the ``release`` callback of the ``ArrowArray`` member.

For an ``ArrowDeviceArray``, the semantics of a released structure and the
callback semantics are identical to those for
:ref:`ArrowArray itself <c-data-interface-released>`. Any producer specific context
information necessary for releasing the device data buffers, in addition to
any allocated event, should be stored in the ``private_data`` member of
the ``ArrowArray`` and managed by the ``release`` callback.

Moving an array
'''''''''''''''

The consumer can *move* the ``ArrowDeviceArray`` structure by bitwise copying
or shallow member-wise copying. Then it MUST mark the source structure released
by setting the ``release`` member of the embedded ``ArrowArray`` structure to
``NULL``, but *without* calling that release callback. This ensures that only
one live copy of the struct is active at any given time and that lifetime is
correctly communicated to the producer.

As usual, the release callback will be called on the destination structure
when it is not needed anymore.

Record batches
--------------
As with the C data interface itself, a record batch can be trivially considered
as an equivalent struct array. In this case the metadata of the top-level
``ArrowSchema`` can be used for schema-level metadata of the record batch.

Mutability
----------

Both the producer and the consumer SHOULD consider the exported data (that
is, the data reachable on the device through the ``buffers`` member of
the embedded ``ArrowArray``) to be immutable, as either party could otherwise
see inconsistent data while the other is mutating it.

Synchronization
---------------

If the ``sync_event`` member is non-NULL, the consumer should not attempt
to access or read the data until they have synchronized on that event. If
the ``sync_event`` member is NULL, then it MUST be safe to access the data
without any synchronization necessary on the part of the consumer.

C producer example
====================

Exporting a simple ``int32`` device array
-----------------------------------------

Export a non-nullable ``int32`` type with empty metadata. An example of this
can be seen in the :ref:`C data interface docs directly <c-data-interface-export-int32-schema>`.

To export the data itself, we transfer ownership to the consumer through
the release callback. This example will use CUDA, but the equivalent calls
could be used for any device:

.. code-block:: c

    static void release_int32_device_array(struct ArrowArray* array) {
        assert(array->n_buffers == 2);
        // destroy the event
        cudaEvent_t* ev_ptr = (cudaEvent_t*)(array->private_data);
        cudaError_t status = cudaEventDestroy(*ev_ptr);
        assert(status == cudaSuccess);
        free(ev_ptr);

        // free the buffers and the buffers array
        status = cudaFree(array->buffers[1]);
        assert(status == cudaSuccess);
        free(array->buffers);

        // mark released
        array->release = NULL;
    }

    void export_int32_device_array(void* cudaAllocedPtr,
                                   cudaStream_t stream,
                                   int64_t length,
                                   struct ArrowDeviceArray* array) {
        // get device id
        int device;
        cudaError_t status;
        status = cudaGetDevice(&device);
        assert(status == cudaSuccess);

        cudaEvent_t* ev_ptr = (cudaEvent_t*)malloc(sizeof(cudaEvent_t));
        assert(ev_ptr != NULL);
        status = cudaEventCreate(ev_ptr);
        assert(status == cudaSuccess);

        // record event on the stream, assuming that the passed in
        // stream is where the work to produce the data will be processing.
        status = cudaEventRecord(*ev_ptr, stream);
        assert(status == cudaSuccess);

        memset(array, 0, sizeof(struct ArrowDeviceArray));
        // initialize fields
        *array = (struct ArrowDeviceArray) {
            .array = (struct ArrowArray) {
                .length = length,
                .null_count = 0,
                .offset = 0,
                .n_buffers = 2,
                .n_children = 0,
                .children = NULL,
                .dictionary = NULL,
                // bookkeeping
                .release = &release_int32_device_array,
                // store the event pointer as private data in the array
                // so that we can access it in the release callback.
                .private_data = (void*)(ev_ptr),
            },
            .device_id = (int64_t)(device),
            .device_type = ARROW_DEVICE_CUDA,
            // pass the event pointer to the consumer
            .sync_event = (void*)(ev_ptr),
        };

        // allocate list of buffers
        array->array.buffers = (const void**)malloc(sizeof(void*) * array->array.n_buffers);
        assert(array->array.buffers != NULL);
        array->array.buffers[0] = NULL;
        array->array.buffers[1] = cudaAllocedPtr;
    }

    // calling the release callback should be done using the array member
    // of the device array.
    static void release_device_array_helper(struct ArrowDeviceArray* arr) {
        arr->array.release(&arr->array);
    }

.. _c-device-stream-interface:

Device Stream Interface
=======================

Like the :ref:`C stream interface <c-stream-interface>`, the C Device data
interface also specifies a higher-level structure for easing communication
of streaming data within a single process.

Semantics
---------

An Arrow C device stream exposes a streaming source of data chunks, each with
the same schema. Chunks are obtained by calling a blocking pull-style iteration
function. It is expected that all chunks should be providing data on the same
device type (but not necessarily the same device id). If it is necessary
to provide a stream of data on multiple device types, a producer should
provide a separate stream object for each device type.

Structure definition
--------------------

The C device stream interface is defined by a single ``struct`` definition:

.. code-block:: c

    #ifndef ARROW_C_DEVICE_STREAM_INTERFACE
    #define ARROW_C_DEVICE_STREAM_INTERFACE

    struct ArrowDeviceArrayStream {
        // device type that all arrays will be accessible from
        ArrowDeviceType device_type;
        // callbacks
        int (*get_schema)(struct ArrowDeviceArrayStream*, struct ArrowSchema*);
        int (*get_next)(struct ArrowDeviceArrayStream*, struct ArrowDeviceArray*);
        const char* (*get_last_error)(struct ArrowDeviceArrayStream*);

        // release callback
        void (*release)(struct ArrowDeviceArrayStream*);

        // opaque producer-specific data
        void* private_data;
    };

    #endif  // ARROW_C_DEVICE_STREAM_INTERFACE

.. note::
    The canonical guard ``ARROW_C_DEVICE_STREAM_INTERFACE`` is meant to avoid
    duplicate definitions if two projects copy the C device stream interface
    definitions into their own headers, and a third-party project includes
    from these two projects. It is therefore important that this guard is
    kept exactly as-is when these definitions are copied.

The ArrowDeviceArrayStream structure
''''''''''''''''''''''''''''''''''''

The ``ArrowDeviceArrayStream`` provides a device type that can access the
resulting data along with the required callbacks to interact with a
streaming source of Arrow arrays. It has the following fields:

.. c:member:: ArrowDeviceType device_type

    *Mandatory.* The device type that this stream produces data on. All
    ``ArrowDeviceArray`` s that are produced by this stream should have the
    same device type as is set here. This is a convenience for the consumer
    to not have to check every array that is retrieved and instead allows
    higher-level coding constructs for streams.

.. c:member:: int (*ArrowDeviceArrayStream.get_schema)(struct ArrowDeviceArrayStream*, struct ArrowSchema* out)

    *Mandatory.* This callback allows the consumer to query the schema of
    the chunks of data in the stream. The schema is the same for all data
    chunks.

    This callback must NOT be called on a released ``ArrowDeviceArrayStream``.

    *Return value:* 0 on success, a non-zero
    :ref:`error code <c-stream-interface-error-codes>` otherwise.

.. c:member:: int (*ArrowDeviceArrayStream.get_next)(struct ArrowDeviceArrayStream*, struct ArrowDeviceArray* out)

    *Mandatory.* This callback allows the consumer to get the next chunk of
    data in the stream.

    This callback must NOT be called on a released ``ArrowDeviceArrayStream``.

    The next chunk of data MUST be accessible from a device type matching the
    :c:member:`ArrowDeviceArrayStream.device_type`.

    *Return value:* 0 on success, a non-zero
    :ref:`error code <c-stream-interface-error-codes>` otherwise.

    On success, the consumer must check whether the ``ArrowDeviceArray``'s
    embedded ``ArrowArray`` is marked :ref:`released <c-data-interface-released>`.
    If the embedded ``ArrowDeviceArray.array`` is released, then the end of the
    stream has been reached. Otherwise, the ``ArrowDeviceArray`` contains a
    valid data chunk.

.. c:member:: const char* (*ArrowDeviceArrayStream.get_last_error)(struct ArrowDeviceArrayStream*)

    *Mandatory.* This callback allows the consumer to get a textual description
    of the last error.

    This callback must ONLY be called if the last operation on the
    ``ArrowDeviceArrayStream`` returned an error. It must NOT be called on a
    released ``ArrowDeviceArrayStream``.

    *Return value:* a pointer to a NULL-terminated character string
    (UTF8-encoded). NULL can also be returned if no detailed description is
    available.

    The returned pointer is only guaranteed to be valid until the next call
    of one of the stream's callbacks. The character string it points to should
    be copied to consumer-managed storage if it is intended to survive longer.

.. c:member:: void (*ArrowDeviceArrayStream.release)(struct ArrowDeviceArrayStream*)

    *Mandatory.* A pointer to a producer-provided release callback.

.. c:member:: void* ArrowDeviceArrayStream.private_data

    *Optional.* An opaque pointer to producer-provided private data.

    Consumers MUST NOT process this member. Lifetime of this member is
    handled by the producer, and especially by the release callback.

Result lifetimes
''''''''''''''''

The data returned by the ``get_schema`` and ``get_next`` callbacks must be
released independently. Their lifetimes are not tied to that of
``ArrowDeviceArrayStream``.

Stream lifetime
'''''''''''''''

Lifetime of the C stream is managed using a release callback with similar
usage as in :ref:`C data interface <c-data-interface-released>`.

Thread safety
'''''''''''''

The stream source is not assumed to be thread-safe. Consumers wanting to
call ``get_next`` from several threads should ensure those calls are
serialized.

Async Device Stream Interface
=============================

.. warning::

    Experimental: The Async C Device Stream interface is experimental in its current
    form. Based on feedback and usage the protocol definition may change until
    it is fully standardized.

The :ref:`C stream interface <c-device-stream-interface>` provides a synchronous
API centered around the consumer calling the producer functions to retrieve
the next record batch. For concurrent communication between producer and consumer,
the ``ArrowAsyncDeviceStreamHandler`` can be used. This interface is non-opinionated
and may fit into different concurrent communication models.

Semantics
---------

Rather than the producer providing a structure of callbacks for a consumer to
call and retrieve records, the Async interface is a structure allocated and populated by the consumer.
The consumer allocated struct provides handler callbacks for the producer to call
when the schema and chunks of data are available.

In addition to the ``ArrowAsyncDeviceStreamHandler``, there are also two additional
structs used for the full data flow: ``ArrowAsyncTask`` and ``ArrowAsyncProducer``.

Structure Definition
--------------------

The C device async stream interface consists of three ``struct`` definitions:

.. code-block:: c

    #ifndef ARROW_C_ASYNC_STREAM_INTERFACE
    #define ARROW_C_ASYNC_STREAM_INTERFACE

    struct ArrowAsyncTask {
      int (*extract_data)(struct ArrowArrayTask* self, struct ArrowDeviceArray* out);

      void* private_data;
    };

    struct ArrowAsyncProducer {
      ArrowDeviceType device_type;

      void (*request)(struct ArrowAsyncProducer* self, int64_t n);
      void (*cancel)(struct ArrowAsyncProducer* self);

      void (*release)(struct ArrowAsyncProducer* self);
      const char* additional_metadata;
      void* private_data;
    };

    struct ArrowAsyncDeviceStreamHandler {
      // consumer-specific handlers
      int (*on_schema)(struct ArrowAsyncDeviceStreamHandler* self,
                       struct ArrowSchema* stream_schema);
      int (*on_next_task)(struct ArrowAsyncDeviceStreamHandler* self,
                          struct ArrowAsyncTask* task, const char* metadata);
      void (*on_error)(struct ArrowAsyncDeviceStreamHandler* self,
                       int code, const char* message, const char* metadata);

      // release callback
      void (*release)(struct ArrowAsyncDeviceStreamHandler* self);

      // must be populated before calling any callbacks
      struct ArrowAsyncProducer* producer;

      // opaque handler-specific data
      void* private_data;
    };

    #endif  // ARROW_C_ASYNC_STREAM_INTERFACE

.. note::
    The canonical guard ``ARROW_C_ASYNC_STREAM_INTERFACE`` is meant to avoid
    duplicate definitions if two projects copy the C async stream interface
    definitions into their own headers, and a third-party project includes
    from these two projects. It is therefore important that this guard is kept
    exactly as-is when these definitions are copied.

The ArrowAsyncDeviceStreamHandler structure
'''''''''''''''''''''''''''''''''''''''''''

The structure has the following fields:

.. c:member:: int (*ArrowAsyncDeviceStreamHandler.on_schema)(struct ArrowAsyncDeviceStreamHandler*, struct ArrowSchema*)

    *Mandatory.* Handler for receiving the schema of the stream. All incoming records should
    match the provided schema. If successful, the function should return 0, otherwise
    it should return an ``errno``-compatible error code.

    If there is any extra contextual information that the producer wants to provide, it can set
    :c:member:`ArrowAsyncProducer.additional_metadata` to a non-NULL value. This is encoded in the
    same format as :c:member:`ArrowSchema.metadata`. The lifetime of this metadata, if not ``NULL``,
    should be tied to the lifetime of the ``ArrowAsyncProducer`` object.

    Unless the ``on_error`` handler is called, this will always get called exactly once and will be
    the first method called on this object. As such the producer *MUST* populate the ``ArrowAsyncProducer``
    member before calling this function to allow the consumer to apply back-pressure and control the flow of data.
    The producer maintains ownership of the ``ArrowAsyncProducer`` and must clean it up *after*
    calling the release callback on the ``ArrowAsyncDeviceStreamHandler``.

    A producer that receives a non-zero result here must not subsequently call anything other than
    the release callback on this object.

.. c:member:: int (*ArrowAsyncDeviceStreamHandler.on_next_task)(struct ArrowAsyncDeviceStreamHandler*, struct ArrowAsyncTask*, const char*)

    *Mandatory.* Handler to be called when a new record is available for processing. The
    schema for each record should be the same as the schema that ``on_schema`` was called with.
    If successfully handled, the function should return 0, otherwise it should return an
    ``errno``-compatible error code.

    Rather than passing the record itself it receives an ``ArrowAsyncTask`` instead to facilitate
    better consumer-focused thread control as far as receiving the data. A call to this function
    simply indicates that data is available via the provided task.

    The producer signals the end of the stream by passing ``NULL`` for the ``ArrowAsyncTask``
    pointer instead of a valid address. This task object is only valid during the lifetime of
    this function call. If the consumer wants to use the task beyond the scope of this method, it
    must copy or move its contents to a new ArrowAsyncTask object.

    The ``const char*`` parameter exists for producers to provide any extra contextual information
    they want. This is encoded in the same format as :c:member:`ArrowSchema.metadata`. If not ``NULL``,
    the lifetime is only the scope of the call to this function. A consumer who wants to maintain
    the additional metadata beyond the lifetime of this call *MUST* copy the value themselves.

    A producer *MUST NOT* call this concurrently from multiple threads.

    The :c:member:`ArrowAsyncProducer.request` callback must be called to start receiving calls to this
    handler.

.. c:member:: void (*ArrowAsyncDeviceStreamHandler.on_error)(struct ArrowAsyncDeviceStreamHandler, int, const char*, const char*)

    *Mandatory.* Handler to be called when an error is encountered by the producer. After calling
    this, the ``release`` callback will be called as the last call on this struct. The parameters
    are an ``errno``-compatible error code and an optional error message and metadata.

    If the message and metadata are not ``NULL``, their lifetime is only valid during the scope
    of this call. A consumer who wants to maintain these values past the return of this function
    *MUST* copy the values themselves.

    If the metadata parameter is not ``NULL``, to provide key-value error metadata, then it should
    be encoded identically to the way that metadata is encoded in :c:member:`ArrowSchema.metadata`.

    It is valid for this to be called by a producer with or without a preceding call to
    :c:member:`ArrowAsyncProducer.request`. This callback *MUST NOT* call any methods of an
    ``ArrowAsyncProducer`` object.

.. c:member:: void (*ArrowAsyncDeviceStreamHandler.release)(struct ArrowAsyncDeviceStreamHandler*)

    *Mandatory.* A pointer to a consumer-provided release callback for the handler.

    It is valid for this to be called by a producer with or without a preceding call to
    :c:member:`ArrowAsyncProducer.request`. This must not call any methods of an ``ArrowAsyncProducer``
    object.

.. c:member:: struct ArrowAsyncProducer ArrowAsyncDeviceStreamHandler.producer

    *Mandatory.* The producer object that the consumer will use to request additional data or cancel.

    This object *MUST* be populated by the producer before calling the :c:member:`ArrowAsyncDeviceStreamHandler.on_schema`
    callback. The producer maintains ownership of this object and must clean it up *after* calling
    the release callback on the ``ArrowAsyncDeviceStreamHandler``.

    The consumer *CANNOT* assume that this is valid until the ``on_schema`` callback is called.

.. c:member:: void* ArrowAsyncDeviceStreamHandler.private_data

    *Optional.* An opaque pointer to consumer-provided private data.

    Producers *MUST NOT* process this member. Lifetime of this member is handled by
    the consumer, and especially by the release callback.

The ArrowAsyncTask structure
''''''''''''''''''''''''''''

The purpose of using a Task object rather than passing the array directly to the ``on_next``
callback is to allow for more complex and efficient thread handling. Utilizing a Task
object allows for a producer to separate the "decoding" logic from the I/O, enabling a
consumer to avoid transferring data between CPU cores (e.g. from one L1/L2 cache to another).

This producer-provided structure has the following fields:

.. c:member:: int (*ArrowArrayTask.extract_data)(struct ArrowArrayTask*, struct ArrowDeviceArray*)

  *Mandatory.* A callback to populate the provided ``ArrowDeviceArray`` with the available data.
  The order of ``ArrowAsyncTasks`` provided by the producer enables a consumer to know the order of
  the data to process. If the consumer does not care about the data that is owned by this task,
  it must still call ``extract_data`` so that the producer can perform any required cleanup. ``NULL``
  should be passed as the device array pointer to indicate that the consumer doesn't want the
  actual data, letting the task perform necessary cleanup.

  If a non-zero value is returned from this, it should be followed only by the producer calling
  the ``on_error`` callback of the ``ArrowAsyncDeviceStreamHandler``. Because calling this method
  is likely to be separate from the current control flow, returning a non-zero value to signal
  an error occurring allows the current thread to decide handle the case accordingly, while still
  allowing all error logging and handling to be centralized in the
  :c:member:`ArrowAsyncDeviceStreamHandler.on_error` callback.

  Rather than having a separate release callback, any required cleanup should be performed as part
  of the invocation of this callback. Ownership of the Array is given to the pointer passed in as
  a parameter, and this array must be released separately.

  It is only valid to call this method exactly once.

.. c:member:: void* ArrowArrayTask.private_data

  *Optional.* An opaque pointer to producer-provided private data.

  Consumers *MUST NOT* process this member. Lifetime of this member is handled by
  the producer who created this object, and should be cleaned up if necessary during
  the call to :c:member:`ArrowArrayTask.extract_data`.

The ArrowAsyncProducer structure
''''''''''''''''''''''''''''''''

This producer-provided and managed object has the following fields:

.. c:member:: ArrowDeviceType ArrowAsyncProducer.device_type

  *Mandatory.* The device type that this producer will provide data on. All
  ``ArrowDeviceArray`` structs that are produced by this producer should have the
  same device type as is set here.

.. c:member:: void (*ArrowAsyncProducer.request)(struct ArrowAsyncProducer*, uint64_t)

  *Mandatory.* This function must be called by a consumer to start receiving calls to
  :c:member:`ArrowAsyncDeviceStreamHandler.on_next_task`. It *MUST* be valid to call
  this synchronously from within :c:member:`ArrowAsyncDeviceStreamHandler.on_next_task`
  or :c:member:`ArrowAsyncDeviceStreamHandler.on_schema`. As a result, this function
  *MUST NOT* synchronously call ``on_next_task`` or ``on_error`` to avoid recursive
  and reentrant callbacks.

  After ``cancel`` is called, additional calls to this function must be a NOP, but allowed.

  While not cancelled, calling this function registers the given number of additional
  arrays/batches to be produced by the producer. A producer should only call
  the appropriate ``on_next_task`` callback up to a maximum of the total sum of calls to
  this method before propagating back-pressure / waiting.

  Any error encountered by calling request must be propagated by calling the ``on_error``
  callback of the ``ArrowAsyncDeviceStreamHandler``.

  It is invalid to call this function with a value of ``n`` that is ``<= 0``. Producers should
  error (e.g. call ``on_error``) if receiving such a value for ``n``.

.. c:member:: void (*ArrowAsyncProducer.cancel)(struct ArrowAsyncProducer*)

  *Mandatory.* This function signals to the producer that it must *eventually* stop calling
  ``on_next_task``. Calls to ``cancel`` must be idempotent and thread-safe. After calling
  it once, subsequent calls *MUST* be a NOP. This *MUST NOT* call any consumer-side handlers
  other than ``on_error``.

  It is not required that calling ``cancel`` affect the producer *immediately*, only that it
  must eventually stop calling ``on_next_task`` and then subsequently call ``release``
  on the async handler object. As such, a consumer *MUST* be prepared to receive one or more
  calls to ``on_next_task`` or ``on_error`` even after calling ``cancel`` if there are still
  requested arrays pending.

  Successful cancelling *MUST NOT* result in a producer calling
  :c:member:`ArrowAsyncDeviceStreamHandler.on_error`, instead it should finish out any remaining
  tasks (calling ``on_next_task`` accordingly) and eventually just call ``release``.

  Any error encountered during handling a call to cancel must be reported via the ``on_error``
  callback on the async stream handler.

.. c:member:: const char* ArrowAsyncProducer.additional_metadata

    *Optional.* An additional metadata string to provide any extra context to the consumer. This *MUST*
    either be ``NULL`` or a valid string that is encoded in the same way as :c:member:`ArrowSchema.metadata`.
    As an example, a producer could utilize this metadata to provide the total number of rows and/or batches
    in the stream if known.

    If not ``NULL`` it *MUST* be valid for at least the lifetime of this object.

.. c:member:: void* ArrowAsyncProducer.private_data

  *Optional.* An opaque pointer to producer-provided specific data.

  Consumers *MUST NOT* process this member, the lifetime is owned by the producer
  that constructed this object.

Error Handling
''''''''''''''

Unlike the regular C Stream interface, the Async interface allows for errors to flow in
both directions. As a result, error handling can be slightly more complex. Thus this spec
designates the following rules:

* If the producer encounters an error during processing, it should call the ``on_error``
  callback, and then call ``release`` after it returns.

* If ``on_schema`` or ``on_next_task`` returns a non-zero integer value, the producer *should not*
  call the ``on_error`` callback, but instead should eventually call ``release`` at some point
  before or after any logging or processing of the error code.

Result lifetimes
''''''''''''''''

The ``ArrowSchema`` passed to the ``on_schema`` callback must be released independently,
with the object itself needing to be moved to a consumer owned ``ArrowSchema`` object. The
``ArrowSchema*`` passed as a parameter to the callback *MUST NOT* be stored and kept.

The ``ArrowAsyncTask`` object provided to ``on_next_task`` is owned by the producer and
will be cleaned up during the invocation of calling ``extract_data`` on it. If the consumer
doesn't care about the data, it should pass ``NULL`` instead of a valid ``ArrowDeviceArray*``.

The ``const char*`` error ``message`` and ``metadata`` which are passed to ``on_error``
are only valid within the scope of the ``on_error`` function itself. They must be copied
if it is necessary for them to exist after it returns.

Stream Handler Lifetime
'''''''''''''''''''''''

Lifetime of the async stream handler is managed using a release callback with similar
usage as in :ref:`C data interface <c-data-interface-released>`.

ArrowAsyncProducer Lifetime
'''''''''''''''''''''''''''

The lifetime of the ``ArrowAsyncProducer`` is owned by the producer itself and should
be managed by it. It *MUST* be populated before calling any methods other than ``release``
and *MUST* remain valid at least until just before calling ``release`` on the stream handler object.

Thread safety
'''''''''''''

All handler functions on the ``ArrowAsyncDeviceStreamHandler`` should only be called in a
serialized manner, but are not guaranteed to be called from the same thread every time. A
producer should wait for handler callbacks to return before calling the next handler callback,
and before calling the ``release`` callback.

Back-pressure is managed by the consumer making calls to :c:member:`ArrowAsyncProducer.request`
to indicate how many arrays it is ready to receive.

The ``ArrowAsyncDeviceStreamHandler`` object should be able to handle callbacks as soon as
it is passed to the producer, any initialization should be performed before it is provided.

Possible Sequence Diagram
-------------------------

.. mermaid::

  sequenceDiagram
    Consumer->>+Producer: ArrowAsyncDeviceStreamHandler*
    Producer-->>+Consumer: on_schema(ArrowAsyncProducer*, ArrowSchema*)
    Consumer->>Producer: ArrowAsyncProducer->request(n)

    par
        loop up to n times
            Producer-->>Consumer: on_next_task(ArrowAsyncTask*)
        end
    and for each task
        Consumer-->>Producer: ArrowAsyncTask.extract_data(...)
        Consumer-->>Producer: ArrowAsyncProducer->request(1)
    end

    break Optionally
        Consumer->>-Producer: ArrowAsyncProducer->cancel()
    end

    loop possible remaining
        Producer-->>Consumer: on_next_task(ArrowAsyncTask*)
    end

    Producer->>-Consumer: ArrowAsyncDeviceStreamHandler->release()


Interoperability with other interchange formats
===============================================

Other interchange APIs, such as the `CUDA Array Interface`_, include
members to pass the shape and the data types of the data buffers being
exported. This information is necessary to interpret the raw bytes in the
device data buffers that are being shared. Rather than store the
shape / types of the data alongside the ``ArrowDeviceArray``, users
should utilize the existing ``ArrowSchema`` structure to pass any data
type and shape information.

Updating this specification
===========================

.. note::
    Since this specification is still considered experimental, there is the
    (still very low) possibility it might change slightly. The reason for
    tagging this as "experimental" is because we don't know what we don't know.
    Work and research was done to ensure a generic ABI compatible with many
    different frameworks, but it is always possible something was missed.
    Once this is supported in an official Arrow release and usage is observed
    to confirm there aren't any modifications necessary, the "experimental"
    tag will be removed and the ABI frozen.

Once this specification is supported in an official Arrow release, the C ABI
is frozen. This means that the ``ArrowDeviceArray`` structure definition
should not change in any way -- including adding new members.

Backwards-compatible changes are allowed, for example new macro values for
:c:type:`ArrowDeviceType` or converting the reserved 24 bytes into a
different type/member without changing the size of the structure.

Any incompatible changes should be part of a new specification, for example
``ArrowDeviceArrayV2``.


.. _RAPIDS cuDF library: https://docs.rapids.ai/api/cudf/stable/
.. _CUDA Array Interface: https://numba.readthedocs.io/en/stable/cuda/cuda_array_interface.html
.. _dlpack: https://dmlc.github.io/dlpack/latest/c_api.html#c-api
.. _CUDA: https://developer.nvidia.com/cuda-toolkit
.. _OpenCL (Open Computing Language): https://www.khronos.org/opencl/
.. _Vulkan: https://www.vulkan.org/
.. _Metal: https://developer.apple.com/metal/
.. _ROCm: https://www.amd.com/en/graphics/servers-solutions-rocm
.. _oneAPI: https://www.intel.com/content/www/us/en/developer/tools/oneapi/overview.html
