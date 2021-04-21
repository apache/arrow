// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

// EXPERIMENTAL: C stream interface (pull-based)

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

// EXPERIMENTAL: C push producer interface

// Consumer-provided callbacks for push producers
//
// Rules:
// - any of these callbacks may be called concurrently from multiple threads
// - any of these callbacks may call back into
//   `ArrowArrayProducer::pause_producing`, `ArrowArrayProducer::resume_producing`
//   or `ArrowArrayProducer::stop_producing`
//   (but *not* into `ArrowArrayProducer::release`).
struct ArrowArrayReceiver {
  // The producer calls this callback to push a data item after it
  // has filled the pointer-passed ArrowArray struct.
  //
  // `array` points to the data item pushed by the producer.
  //
  // `seqnum` is a 0-based sequence number identifying the position of
  // this array of this data stream.  Even if the data is semantically
  // unordered, the producer *must* arrange to produce a contiguous range
  // of distinct sequence numbers, to help the consumer handle EOF reliably
  // in multi-threaded situations.
  //
  // The consumer *must* move the ArrowArray struct contents before this
  // callback returns, as the producer is free to release it immediately
  // afterwards.
  //
  // This callback is allowed to call back any ArrowArrayProducer callback,
  // except the `release` callback.
  void (*receive_array)(struct ArrowArrayReceiver*, struct ArrowArray* array,
                        int seqnum);

  // The producer calls this callback to signal successful end of data.
  //
  // `seqstop` is a sequence number equal to the total number of data items
  // in the stream.  In other words, all past and future calls to
  // `receive_array` will pass a `seqnum` contained within `[0, seqstop)`.
  //
  // This callback allows the consumer to know how many data items are
  // expected.  This is important with multi-threaded producers, as
  // the order in which consumer callbacks are called is not guaranteed:
  // when `receive_eof` is called, some `receive_array` calls may still
  // be ongoing.
  //
  // This callback is allowed to call back any ArrowArrayProducer callback,
  // except the `release` callback.
  void (*receive_eof)(struct ArrowArrayReceiver*, int seqstop);

  // The producer calls this callback to signal an error occurred while
  // producing data.
  //
  // `error` is a non-zero `errno`-compatible error code.
  //
  // `message` is an optional null-terminated character array describing
  // the error, or NULL if no description is available.  The `message`
  // pointer is only valid until this callback returns, therefore the
  // consumer must copy its contents if it wants to store the error message.
  //
  // This callback is allowed to call back any ArrowArrayProducer callback,
  // except the `release` callback.
  void (*receive_error)(struct ArrowArrayReceiver*, int error, const char* message);

  // Opaque consumer-specific data.
  //
  // This is meant to help the consumer associate calls to the above
  // callbacks to its internal structures.  If such resources were
  // dynamically allocated, they should only be released by the consumer
  // after `ArrowArrayProducer::release` has been called and has returned.
  void* private_data;
};

// Push-based array producer
struct ArrowArrayProducer {
  // Callback to get the produced data type
  // (will be the same for all pushed arrays).
  //
  // The ArrowSchema must be released independently from the ArrowArrayProducer
  //
  // XXX add error return?
  void (*get_schema)(struct ArrowArrayProducer*, struct ArrowSchema* out);

  // Callback to start producing data
  //
  // This function should be called once by the consumer.
  // It tells the producer that the consumer is ready to be called
  // on the ArrowArrayReceiver callbacks.
  //
  // The ArrowArrayReceiver callbacks may be called *before* this function
  // returns.  Also, each of the receiver callbacks may be called concurrently,
  // from multiple threads.
  void (*start_producing)(struct ArrowArrayProducer*, struct ArrowArrayReceiver*);

  // Callback to temporarily pause producing data
  //
  // The consumer can use this function to apply backpressure when it is
  // not ready to receive more data.  However, the producer may still push
  // data after this function is called (especially if the producer is
  // multi-threaded, as ensuring serialization may not be convenient).
  void (*pause_producing)(struct ArrowArrayProducer*);

  // Callback to resume producing data after a pause
  //
  // The consumer can use this function to release backpressure when it is
  // ready to receive data again.  This must only be called after a
  // call to `pause_producing`.
  //
  // Given that `pause_producing` and `resume_producing` may be called
  // concurrently, it is recommended to implement their functionality
  // using a counter or semaphore.
  void (*resume_producing)(struct ArrowArrayProducer*);

  // Callback to start producing data
  //
  // This function should be called once by the consumer, and only
  // after `start_producing` was called. After this call, the producer
  // should stop pushing data. However, with multi-threaded producers,
  // it is still possible for the receiver callbacks to be called after
  // `stop_producing` has been called.
  //
  // Consumers may choose to call this function before all data has been
  // produced, and producers must be to handle this situation correctly.
  //
  // This function must be implemented idempotently: calling it a second time
  // (including concurrently) is a no-op.
  void (*stop_producing)(struct ArrowArrayProducer*);

  // Release callback: release the push producer's own resources.
  //
  // A multi-threaded producer must be careful to establish mutual exclusion
  // between this function and any place where it calls ArrowArrayReceiver
  // callbacks.  Once this function returns, ArrowArrayReceiver callbacks
  // must *not* be called anymore as the ArrowArrayReceiver pointer
  // may become invalid.
  void (*release)(struct ArrowArrayProducer*);

  // Opaque producer-specific data
  void* private_data;
};

#ifdef __cplusplus
}
#endif
