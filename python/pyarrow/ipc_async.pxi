# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


class _AsyncioCall:
    """State for an async operation using asyncio."""

    def __init__(self):
        import asyncio
        self._future = asyncio.get_running_loop().create_future()

    def as_awaitable(self):
        return self._future

    def wakeup(self, result_or_exception):
        loop = self._future.get_loop()
        if isinstance(result_or_exception, BaseException):
            loop.call_soon_threadsafe(
                self._future.set_exception, result_or_exception)
        else:
            loop.call_soon_threadsafe(
                self._future.set_result, result_or_exception)


cdef object _wrap_record_batch_or_none(CRecordBatchWithMetadata batch_with_md):
    """Wrap a CRecordBatchWithMetadata as a RecordBatch, or return None at end-of-stream."""
    if batch_with_md.batch.get() == NULL:
        return None
    return pyarrow_wrap_batch(batch_with_md.batch)


cdef object _wrap_async_generator(CAsyncRecordBatchGenerator gen):
    """Wrap a CAsyncRecordBatchGenerator into an AsyncRecordBatchReader."""
    cdef AsyncRecordBatchReader reader = AsyncRecordBatchReader.__new__(
        AsyncRecordBatchReader)
    cdef CAsyncRecordBatchGenerator* p = new CAsyncRecordBatchGenerator()
    p.schema = gen.schema
    p.device_type = gen.device_type
    p.generator = move(gen.generator)
    reader.generator.reset(p)
    reader._schema = None
    return reader


cdef class AsyncRecordBatchReader(_Weakrefable):
    """Asynchronous reader for a stream of record batches.

    This class provides an async iterator interface for consuming record
    batches from an asynchronous device stream.

    This interface is EXPERIMENTAL.

    Examples
    --------
    >>> async for batch in reader:  # doctest: +SKIP
    ...     process(batch)
    """

    def __init__(self):
        raise TypeError(
            f"Do not call {self.__class__.__name__}'s constructor directly, "
            "use factory methods instead.")

    @property
    def schema(self):
        """
        Shared schema of the record batches in the stream.

        Returns
        -------
        Schema
        """
        if self._schema is None:
            self._schema = pyarrow_wrap_schema(self.generator.get().schema)
        return self._schema

    def __aiter__(self):
        return self

    async def __anext__(self):
        batch = await self._read_next_async()
        if batch is None:
            raise StopAsyncIteration
        return batch

    async def _read_next_async(self):
        call = _AsyncioCall()
        self._read_next(call)
        return await call.as_awaitable()

    cdef _read_next(self, call):
        cdef CFuture[CRecordBatchWithMetadata] c_future

        with nogil:
            c_future = CallAsyncGenerator(self.generator.get().generator)

        BindFuture(move(c_future), call.wakeup, _wrap_record_batch_or_none)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


async def _test_roundtrip_async(schema, batches, queue_size=5):
    """Test helper: create an async producer+consumer pair and return reader.

    EXPERIMENTAL: This function is intended for testing purposes only.

    Parameters
    ----------
    schema : Schema
        The schema of the record batches.
    batches : list of RecordBatch
        The record batches to produce.
    queue_size : int, default 5
        Number of batches to request ahead.

    Returns
    -------
    AsyncRecordBatchReader
    """
    call = _AsyncioCall()
    _start_roundtrip(call, schema, batches, queue_size)
    return await call.as_awaitable()


cdef _start_roundtrip(call, Schema schema, list batches, uint64_t queue_size):
    cdef:
        shared_ptr[CSchema] c_schema = pyarrow_unwrap_schema(schema)
        vector[shared_ptr[CRecordBatch]] c_batches
        CFuture[CAsyncRecordBatchGenerator] c_future

    for batch in batches:
        c_batches.push_back((<RecordBatch?>batch).sp_batch)

    with nogil:
        c_future = RoundtripAsyncBatches(
            c_schema, move(c_batches), GetCpuThreadPool(), queue_size)

    BindFuture(move(c_future), call.wakeup, _wrap_async_generator)
