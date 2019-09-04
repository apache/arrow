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

cdef class Message:
    """
    Container for an Arrow IPC message with metadata and optional body
    """
    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use "
                        "`pyarrow.ipc.read_message` function instead."
                        .format(self.__class__.__name__))

    @property
    def type(self):
        return frombytes(FormatMessageType(self.message.get().type()))

    @property
    def metadata(self):
        return pyarrow_wrap_buffer(self.message.get().metadata())

    @property
    def body(self):
        cdef shared_ptr[CBuffer] body = self.message.get().body()
        if body.get() == NULL:
            return None
        else:
            return pyarrow_wrap_buffer(body)

    def equals(self, Message other):
        """
        Returns True if the message contents (metadata and body) are identical

        Parameters
        ----------
        other : Message

        Returns
        -------
        are_equal : bool
        """
        cdef c_bool result
        with nogil:
            result = self.message.get().Equals(deref(other.message.get()))
        return result

    def serialize_to(self, NativeFile sink, alignment=8, memory_pool=None):
        """
        Write message to generic OutputStream

        Parameters
        ----------
        sink : NativeFile
        alignment : int, default 8
            Byte alignment for metadata and body
        memory_pool : MemoryPool, default None
            Uses default memory pool if not specified
        """
        cdef:
            int64_t output_length = 0
            int32_t c_alignment = alignment
            OutputStream* out

        out = sink.get_output_stream().get()
        with nogil:
            check_status(self.message.get()
                         .SerializeTo(out, c_alignment, &output_length))

    def serialize(self, alignment=8, memory_pool=None):
        """
        Write message as encapsulated IPC message

        Parameters
        ----------
        alignment : int, default 8
            Byte alignment for metadata and body
        memory_pool : MemoryPool, default None
            Uses default memory pool if not specified

        Returns
        -------
        serialized : Buffer
        """
        stream = BufferOutputStream(memory_pool)
        self.serialize_to(stream, alignment=alignment, memory_pool=memory_pool)
        return stream.getvalue()

    def __repr__(self):
        metadata_len = self.metadata.size
        body = self.body
        body_len = 0 if body is None else body.size

        return """pyarrow.Message
type: {0}
metadata length: {1}
body length: {2}""".format(self.type, metadata_len, body_len)


cdef class MessageReader:
    """
    Interface for reading Message objects from some source (like an
    InputStream)
    """
    cdef:
        unique_ptr[CMessageReader] reader

    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use "
                        "`pyarrow.ipc.MessageReader.open_stream` function "
                        "instead.".format(self.__class__.__name__))

    @staticmethod
    def open_stream(source):
        cdef MessageReader result = MessageReader.__new__(MessageReader)
        cdef shared_ptr[InputStream] in_stream
        cdef unique_ptr[CMessageReader] reader
        _get_input_stream(source, &in_stream)
        with nogil:
            reader = CMessageReader.Open(in_stream)
            result.reader.reset(reader.release())

        return result

    def __iter__(self):
        while True:
            yield self.read_next_message()

    def read_next_message(self):
        """
        Read next Message from the stream. Raises StopIteration at end of
        stream
        """
        cdef Message result = Message.__new__(Message)

        with nogil:
            check_status(self.reader.get().ReadNextMessage(&result.message))

        if result.message.get() == NULL:
            raise StopIteration

        return result

# ----------------------------------------------------------------------
# File and stream readers and writers

cdef class _CRecordBatchWriter:
    """The base RecordBatchWriter wrapper.

    Provides common implementations of convenience methods. Should not
    be instantiated directly by user code.
    """

    # cdef block is in lib.pxd

    def write(self, table_or_batch):
        """
        Write RecordBatch or Table to stream

        Parameters
        ----------
        table_or_batch : {RecordBatch, Table}
        """
        if isinstance(table_or_batch, RecordBatch):
            self.write_batch(table_or_batch)
        elif isinstance(table_or_batch, Table):
            self.write_table(table_or_batch)
        else:
            raise ValueError(type(table_or_batch))

    def write_batch(self, RecordBatch batch):
        """
        Write RecordBatch to stream

        Parameters
        ----------
        batch : RecordBatch
        """
        with nogil:
            check_status(self.writer.get()
                         .WriteRecordBatch(deref(batch.batch)))

    def write_table(self, Table table, chunksize=None):
        """
        Write RecordBatch to stream

        Parameters
        ----------
        batch : RecordBatch
        """
        cdef:
            # Chunksize must be > 0 to have any impact
            int64_t c_chunksize = -1

        if chunksize is not None:
            c_chunksize = chunksize

        with nogil:
            check_status(self.writer.get().WriteTable(table.table[0],
                                                      c_chunksize))

    def close(self):
        """
        Close stream and write end-of-stream 0 marker
        """
        with nogil:
            check_status(self.writer.get().Close())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


cdef class _RecordBatchStreamWriter(_CRecordBatchWriter):
    cdef:
        shared_ptr[OutputStream] sink
        bint closed

    def __cinit__(self):
        pass

    def __dealloc__(self):
        pass

    def _open(self, sink, Schema schema):
        get_writer(sink, &self.sink)

        with nogil:
            check_status(
                CRecordBatchStreamWriter.Open(self.sink.get(),
                                              schema.sp_schema,
                                              &self.writer))


cdef _get_input_stream(object source, shared_ptr[InputStream]* out):
    try:
        source = as_buffer(source)
    except TypeError:
        # Non-buffer-like
        pass

    get_input_stream(source, True, out)


cdef class _CRecordBatchReader:
    """The base RecordBatchReader wrapper.

    Provides common implementations of convenience methods. Should not
    be instantiated directly by user code.
    """

    # cdef block is in lib.pxd

    def __iter__(self):
        while True:
            yield self.read_next_batch()

    def get_next_batch(self):
        import warnings
        warnings.warn('Please use read_next_batch instead of '
                      'get_next_batch', FutureWarning)
        return self.read_next_batch()

    def read_next_batch(self):
        """
        Read next RecordBatch from the stream. Raises StopIteration at end of
        stream
        """
        cdef shared_ptr[CRecordBatch] batch

        with nogil:
            check_status(self.reader.get().ReadNext(&batch))

        if batch.get() == NULL:
            raise StopIteration

        return pyarrow_wrap_batch(batch)

    def read_all(self):
        """
        Read all record batches as a pyarrow.Table
        """
        cdef shared_ptr[CTable] table
        with nogil:
            check_status(self.reader.get().ReadAll(&table))
        return pyarrow_wrap_table(table)


cdef class _RecordBatchStreamReader(_CRecordBatchReader):
    cdef:
        shared_ptr[InputStream] in_stream

    cdef readonly:
        Schema schema

    def __cinit__(self):
        pass

    def _open(self, source):
        _get_input_stream(source, &self.in_stream)
        with nogil:
            check_status(CRecordBatchStreamReader.Open(
                self.in_stream.get(), &self.reader))

        self.schema = pyarrow_wrap_schema(self.reader.get().schema())


cdef class _RecordBatchFileWriter(_RecordBatchStreamWriter):

    def _open(self, sink, Schema schema):
        get_writer(sink, &self.sink)

        with nogil:
            check_status(
                CRecordBatchFileWriter.Open(self.sink.get(), schema.sp_schema,
                                            &self.writer))


cdef class _RecordBatchFileReader:
    cdef:
        shared_ptr[CRecordBatchFileReader] reader
        shared_ptr[RandomAccessFile] file

    cdef readonly:
        Schema schema

    def __cinit__(self):
        pass

    def _open(self, source, footer_offset=None):
        try:
            source = as_buffer(source)
        except TypeError:
            pass

        get_reader(source, True, &self.file)

        cdef int64_t offset = 0
        if footer_offset is not None:
            offset = footer_offset

        with nogil:
            if offset != 0:
                check_status(
                    CRecordBatchFileReader.Open2(self.file.get(), offset,
                                                 &self.reader))
            else:
                check_status(
                    CRecordBatchFileReader.Open(self.file.get(), &self.reader))

        self.schema = pyarrow_wrap_schema(self.reader.get().schema())

    @property
    def num_record_batches(self):
        return self.reader.get().num_record_batches()

    def get_batch(self, int i):
        cdef shared_ptr[CRecordBatch] batch

        if i < 0 or i >= self.num_record_batches:
            raise ValueError('Batch number {0} out of range'.format(i))

        with nogil:
            check_status(self.reader.get().ReadRecordBatch(i, &batch))

        return pyarrow_wrap_batch(batch)

    # TODO(wesm): ARROW-503: Function was renamed. Remove after a period of
    # time has passed
    get_record_batch = get_batch

    def read_all(self):
        """
        Read all record batches as a pyarrow.Table
        """
        cdef:
            vector[shared_ptr[CRecordBatch]] batches
            shared_ptr[CTable] table
            int i, nbatches

        nbatches = self.num_record_batches

        batches.resize(nbatches)
        with nogil:
            for i in range(nbatches):
                check_status(self.reader.get().ReadRecordBatch(i, &batches[i]))
            check_status(CTable.FromRecordBatches(self.schema.sp_schema,
                                                  batches, &table))

        return pyarrow_wrap_table(table)


def get_tensor_size(Tensor tensor):
    """
    Return total size of serialized Tensor including metadata and padding
    """
    cdef int64_t size
    with nogil:
        check_status(GetTensorSize(deref(tensor.tp), &size))
    return size


def get_record_batch_size(RecordBatch batch):
    """
    Return total size of serialized RecordBatch including metadata and padding
    """
    cdef int64_t size
    with nogil:
        check_status(GetRecordBatchSize(deref(batch.batch), &size))
    return size


def write_tensor(Tensor tensor, NativeFile dest):
    """
    Write pyarrow.Tensor to pyarrow.NativeFile object its current position

    Parameters
    ----------
    tensor : pyarrow.Tensor
    dest : pyarrow.NativeFile

    Returns
    -------
    bytes_written : int
        Total number of bytes written to the file
    """
    cdef:
        int32_t metadata_length
        int64_t body_length

    handle = dest.get_output_stream()

    with nogil:
        check_status(
            WriteTensor(deref(tensor.tp), handle.get(),
                        &metadata_length, &body_length))

    return metadata_length + body_length


cdef NativeFile as_native_file(source):
    if not isinstance(source, NativeFile):
        if hasattr(source, 'read'):
            source = PythonFile(source)
        else:
            source = BufferReader(source)

    if not isinstance(source, NativeFile):
        raise ValueError('Unable to read message from object with type: {0}'
                         .format(type(source)))
    return source


def read_tensor(source):
    """Read pyarrow.Tensor from pyarrow.NativeFile object from current
    position. If the file source supports zero copy (e.g. a memory map), then
    this operation does not allocate any memory. This function not assume that
    the stream is aligned

    Parameters
    ----------
    source : pyarrow.NativeFile

    Returns
    -------
    tensor : Tensor

    """
    cdef:
        shared_ptr[CTensor] sp_tensor
        InputStream* c_stream

    cdef NativeFile nf = as_native_file(source)

    c_stream = nf.get_input_stream().get()
    with nogil:
        check_status(ReadTensor(c_stream, &sp_tensor))
    return pyarrow_wrap_tensor(sp_tensor)


def read_message(source):
    """
    Read length-prefixed message from file or buffer-like object

    Parameters
    ----------
    source : pyarrow.NativeFile, file-like object, or buffer-like object

    Returns
    -------
    message : Message
    """
    cdef:
        Message result = Message.__new__(Message)
        InputStream* c_stream

    cdef NativeFile nf = as_native_file(source)
    c_stream = nf.get_input_stream().get()

    with nogil:
        check_status(ReadMessage(c_stream, &result.message))

    return result


def read_schema(obj, DictionaryMemo dictionary_memo=None):
    """
    Read Schema from message or buffer

    Parameters
    ----------
    obj : buffer or Message
    dictionary_memo : DictionaryMemo, optional
        Needed to be able to reconstruct dictionary-encoded fields
        with read_record_batch

    Returns
    -------
    schema : Schema
    """
    cdef:
        shared_ptr[CSchema] result
        shared_ptr[RandomAccessFile] cpp_file
        CDictionaryMemo temp_memo
        CDictionaryMemo* arg_dict_memo

    if isinstance(obj, Message):
        raise NotImplementedError(type(obj))

    get_reader(obj, True, &cpp_file)

    if dictionary_memo is not None:
        arg_dict_memo = dictionary_memo.memo
    else:
        arg_dict_memo = &temp_memo

    with nogil:
        check_status(ReadSchema(cpp_file.get(), arg_dict_memo, &result))

    return pyarrow_wrap_schema(result)


def read_record_batch(obj, Schema schema,
                      DictionaryMemo dictionary_memo=None):
    """
    Read RecordBatch from message, given a known schema

    Parameters
    ----------
    obj : Message or Buffer-like
    schema : Schema
    dictionary_memo : DictionaryMemo, optional
        If message contains dictionaries, must pass a populated
        DictionaryMemo

    Returns
    -------
    batch : RecordBatch
    """
    cdef:
        shared_ptr[CRecordBatch] result
        Message message
        CDictionaryMemo temp_memo
        CDictionaryMemo* arg_dict_memo

    if isinstance(obj, Message):
        message = obj
    else:
        message = read_message(obj)

    if dictionary_memo is not None:
        arg_dict_memo = dictionary_memo.memo
    else:
        arg_dict_memo = &temp_memo

    with nogil:
        check_status(ReadRecordBatch(deref(message.message.get()),
                                     schema.sp_schema,
                                     arg_dict_memo, &result))

    return pyarrow_wrap_batch(result)
