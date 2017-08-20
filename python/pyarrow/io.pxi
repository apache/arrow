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

# Cython wrappers for IO interfaces defined in arrow::io and messaging in
# arrow::ipc

from libc.stdlib cimport malloc, free
from pyarrow.compat import frombytes, tobytes, encode_file_path

import re
import six
import sys
import threading
import time


# 64K
DEFAULT_BUFFER_SIZE = 2 ** 16


# To let us get a PyObject* and avoid Cython auto-ref-counting
cdef extern from "Python.h":
    PyObject* PyBytes_FromStringAndSizeNative" PyBytes_FromStringAndSize"(
        char *v, Py_ssize_t len) except NULL


cdef class NativeFile:

    def __cinit__(self):
        self.is_open = False
        self.own_file = False

    def __dealloc__(self):
        if self.is_open and self.own_file:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    property mode:
        """
        The file mode. Currently instances of NativeFile may support:

        * rb: binary read
        * wb: binary write
        * rb+: binary read and write
        """

        def __get__(self):
            # Emulate built-in file modes
            if self.is_readable and self.is_writeable:
                return 'rb+'
            elif self.is_readable:
                return 'rb'
            elif self.is_writeable:
                return 'wb'
            else:
                raise ValueError('File object is malformed, has no mode')

    def close(self):
        if self.is_open:
            with nogil:
                if self.is_readable:
                    check_status(self.rd_file.get().Close())
                else:
                    check_status(self.wr_file.get().Close())
        self.is_open = False

    cdef read_handle(self, shared_ptr[RandomAccessFile]* file):
        self._assert_readable()
        file[0] = <shared_ptr[RandomAccessFile]> self.rd_file

    cdef write_handle(self, shared_ptr[OutputStream]* file):
        self._assert_writeable()
        file[0] = <shared_ptr[OutputStream]> self.wr_file

    def _assert_readable(self):
        if not self.is_readable:
            raise IOError("only valid on readonly files")

        if not self.is_open:
            raise IOError("file not open")

    def _assert_writeable(self):
        if not self.is_writeable:
            raise IOError("only valid on writeable files")

        if not self.is_open:
            raise IOError("file not open")

    def size(self):
        """
        Return file size
        """
        cdef int64_t size
        self._assert_readable()
        with nogil:
            check_status(self.rd_file.get().GetSize(&size))
        return size

    def tell(self):
        """
        Return current stream position
        """
        cdef int64_t position
        with nogil:
            if self.is_readable:
                check_status(self.rd_file.get().Tell(&position))
            else:
                check_status(self.wr_file.get().Tell(&position))
        return position

    def seek(self, int64_t position, int whence=0):
        """
        Change current file stream position

        Parameters
        ----------
        position : int
            Byte offset, interpreted relative to value of whence argument
        whence : int, default 0
            Point of reference for seek offset

        Notes
        -----
        Values of whence:
        * 0 -- start of stream (the default); offset should be zero or positive
        * 1 -- current stream position; offset may be negative
        * 2 -- end of stream; offset is usually negative

        Returns
        -------
        new_position : the new absolute stream position
        """
        cdef int64_t offset
        self._assert_readable()
        with nogil:
            if whence == 0:
                offset = position
            elif whence == 1:
                check_status(self.rd_file.get().Tell(&offset))
                offset = offset + position
            elif whence == 2:
                check_status(self.rd_file.get().GetSize(&offset))
                offset = offset + position
            else:
                with gil:
                    raise ValueError("Invalid value of whence: {0}"
                                     .format(whence))
            check_status(self.rd_file.get().Seek(offset))

        return self.tell()

    def write(self, data):
        """
        Write byte from any object implementing buffer protocol (bytes,
        bytearray, ndarray, pyarrow.Buffer)
        """
        self._assert_writeable()

        if isinstance(data, six.string_types):
            data = tobytes(data)

        cdef Buffer arrow_buffer = frombuffer(data)

        cdef const uint8_t* buf = arrow_buffer.buffer.get().data()
        cdef int64_t bufsize = len(arrow_buffer)
        with nogil:
            check_status(self.wr_file.get().Write(buf, bufsize))

    def read(self, nbytes=None):
        """
        Read indicated number of bytes from file, or read all remaining bytes
        if no argument passed

        Parameters
        ----------
        nbytes : int, default None

        Returns
        -------
        data : bytes
        """
        cdef:
            int64_t c_nbytes
            int64_t bytes_read = 0
            PyObject* obj

        if nbytes is None:
            c_nbytes = self.size() - self.tell()
        else:
            c_nbytes = nbytes

        self._assert_readable()

        # Allocate empty write space
        obj = PyBytes_FromStringAndSizeNative(NULL, c_nbytes)

        cdef uint8_t* buf = <uint8_t*> cp.PyBytes_AS_STRING(<object> obj)
        with nogil:
            check_status(self.rd_file.get().Read(c_nbytes, &bytes_read, buf))

        if bytes_read < c_nbytes:
            cp._PyBytes_Resize(&obj, <Py_ssize_t> bytes_read)

        return PyObject_to_object(obj)

    def read_buffer(self, nbytes=None):
        cdef:
            int64_t c_nbytes
            int64_t bytes_read = 0
            shared_ptr[CBuffer] output
        self._assert_readable()

        if nbytes is None:
            c_nbytes = self.size() - self.tell()
        else:
            c_nbytes = nbytes

        with nogil:
            check_status(self.rd_file.get().ReadB(c_nbytes, &output))

        return pyarrow_wrap_buffer(output)

    def download(self, stream_or_path, buffer_size=None):
        """
        Read file completely to local path (rather than reading completely into
        memory). First seeks to the beginning of the file.
        """
        cdef:
            int64_t bytes_read = 0
            uint8_t* buf
        self._assert_readable()

        buffer_size = buffer_size or DEFAULT_BUFFER_SIZE

        write_queue = Queue(50)

        if not hasattr(stream_or_path, 'read'):
            stream = open(stream_or_path, 'wb')

            def cleanup():
                stream.close()
        else:
            stream = stream_or_path

            def cleanup():
                pass

        done = False
        exc_info = None

        def bg_write():
            try:
                while not done or write_queue.qsize() > 0:
                    try:
                        buf = write_queue.get(timeout=0.01)
                    except QueueEmpty:
                        continue
                    stream.write(buf)
            except Exception as e:
                exc_info = sys.exc_info()
            finally:
                cleanup()

        self.seek(0)

        writer_thread = threading.Thread(target=bg_write)

        # This isn't ideal -- PyBytes_FromStringAndSize copies the data from
        # the passed buffer, so it's hard for us to avoid doubling the memory
        buf = <uint8_t*> malloc(buffer_size)
        if buf == NULL:
            raise MemoryError("Failed to allocate {0} bytes"
                              .format(buffer_size))

        writer_thread.start()

        cdef int64_t total_bytes = 0
        cdef int32_t c_buffer_size = buffer_size

        try:
            while True:
                with nogil:
                    check_status(self.rd_file.get()
                                 .Read(c_buffer_size, &bytes_read, buf))

                total_bytes += bytes_read

                # EOF
                if bytes_read == 0:
                    break

                pybuf = cp.PyBytes_FromStringAndSize(<const char*>buf,
                                                     bytes_read)

                write_queue.put_nowait(pybuf)
        finally:
            free(buf)
            done = True

        writer_thread.join()
        if exc_info is not None:
            raise exc_info[0], exc_info[1], exc_info[2]

    def upload(self, stream, buffer_size=None):
        """
        Pipe file-like object to file
        """
        write_queue = Queue(50)
        self._assert_writeable()

        buffer_size = buffer_size or DEFAULT_BUFFER_SIZE

        done = False
        exc_info = None

        def bg_write():
            try:
                while not done or write_queue.qsize() > 0:
                    try:
                        buf = write_queue.get(timeout=0.01)
                    except QueueEmpty:
                        continue

                    self.write(buf)

            except Exception as e:
                exc_info = sys.exc_info()

        writer_thread = threading.Thread(target=bg_write)
        writer_thread.start()

        try:
            while True:
                buf = stream.read(buffer_size)
                if not buf:
                    break

                if writer_thread.is_alive():
                    while write_queue.full():
                        time.sleep(0.01)
                else:
                    break

                write_queue.put_nowait(buf)
        finally:
            done = True

        writer_thread.join()
        if exc_info is not None:
            raise exc_info[0], exc_info[1], exc_info[2]


# ----------------------------------------------------------------------
# Python file-like objects


cdef class PythonFile(NativeFile):
    cdef:
        object handle

    def __cinit__(self, handle, mode='w'):
        self.handle = handle

        if mode.startswith('w'):
            self.wr_file.reset(new PyOutputStream(handle))
            self.is_readable = 0
            self.is_writeable = 1
        elif mode.startswith('r'):
            self.rd_file.reset(new PyReadableFile(handle))
            self.is_readable = 1
            self.is_writeable = 0
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        self.is_open = True


cdef class MemoryMappedFile(NativeFile):
    """
    Supports 'r', 'r+w', 'w' modes
    """
    cdef:
        object path

    def __cinit__(self):
        self.is_open = False
        self.is_readable = 0
        self.is_writeable = 0

    @staticmethod
    def create(path, size):
        cdef:
            shared_ptr[CMemoryMappedFile] handle
            c_string c_path = encode_file_path(path)
            int64_t c_size = size

        with nogil:
            check_status(CMemoryMappedFile.Create(c_path, c_size, &handle))

        cdef MemoryMappedFile result = MemoryMappedFile()
        result.path = path
        result.is_readable = 1
        result.is_writeable = 1
        result.wr_file = <shared_ptr[OutputStream]> handle
        result.rd_file = <shared_ptr[RandomAccessFile]> handle
        result.is_open = True

        return result

    def _open(self, path, mode='r'):
        self.path = path

        cdef:
            FileMode c_mode
            shared_ptr[CMemoryMappedFile] handle
            c_string c_path = encode_file_path(path)

        if mode in ('r', 'rb'):
            c_mode = FileMode_READ
            self.is_readable = 1
        elif mode in ('w', 'wb'):
            c_mode = FileMode_WRITE
            self.is_writeable = 1
        elif mode in ('r+', 'r+b', 'rb+'):
            c_mode = FileMode_READWRITE
            self.is_readable = 1
            self.is_writeable = 1
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        with nogil:
            check_status(CMemoryMappedFile.Open(c_path, c_mode, &handle))

        self.wr_file = <shared_ptr[OutputStream]> handle
        self.rd_file = <shared_ptr[RandomAccessFile]> handle
        self.is_open = True


def memory_map(path, mode='r'):
    """
    Open memory map at file path. Size of the memory map cannot change

    Parameters
    ----------
    path : string
    mode : {'r', 'w'}, default 'r'

    Returns
    -------
    mmap : MemoryMappedFile
    """
    cdef MemoryMappedFile mmap = MemoryMappedFile()
    mmap._open(path, mode)
    return mmap


def create_memory_map(path, size):
    """
    Create memory map at indicated path of the given size, return open
    writeable file object

    Parameters
    ----------
    path : string
    size : int

    Returns
    -------
    mmap : MemoryMappedFile
    """
    return MemoryMappedFile.create(path, size)


cdef class OSFile(NativeFile):
    """
    Supports 'r', 'w' modes
    """
    cdef:
        object path

    def __cinit__(self, path, mode='r', MemoryPool memory_pool=None):
        self.path = path

        cdef:
            FileMode c_mode
            shared_ptr[Readable] handle
            c_string c_path = encode_file_path(path)

        self.is_readable = self.is_writeable = 0

        if mode in ('r', 'rb'):
            self._open_readable(c_path, maybe_unbox_memory_pool(memory_pool))
        elif mode in ('w', 'wb'):
            self._open_writeable(c_path)
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        self.is_open = True

    cdef _open_readable(self, c_string path, CMemoryPool* pool):
        cdef shared_ptr[ReadableFile] handle

        with nogil:
            check_status(ReadableFile.Open(path, pool, &handle))

        self.is_readable = 1
        self.rd_file = <shared_ptr[RandomAccessFile]> handle

    cdef _open_writeable(self, c_string path):
        cdef shared_ptr[FileOutputStream] handle

        with nogil:
            check_status(FileOutputStream.Open(path, &handle))
        self.is_writeable = 1
        self.wr_file = <shared_ptr[OutputStream]> handle


cdef class FixedSizeBufferOutputStream(NativeFile):

    def __cinit__(self, Buffer buffer):
        self.wr_file.reset(new CFixedSizeBufferWriter(buffer.buffer))
        self.is_readable = 0
        self.is_writeable = 1
        self.is_open = True

    def set_memcopy_threads(self, int num_threads):
        cdef CFixedSizeBufferWriter* writer = \
            <CFixedSizeBufferWriter*> self.wr_file.get()
        writer.set_memcopy_threads(num_threads)

    def set_memcopy_blocksize(self, int64_t blocksize):
        cdef CFixedSizeBufferWriter* writer = \
            <CFixedSizeBufferWriter*> self.wr_file.get()
        writer.set_memcopy_blocksize(blocksize)

    def set_memcopy_threshold(self, int64_t threshold):
        cdef CFixedSizeBufferWriter* writer = \
            <CFixedSizeBufferWriter*> self.wr_file.get()
        writer.set_memcopy_threshold(threshold)


# ----------------------------------------------------------------------
# Arrow buffers


cdef class Buffer:

    def __cinit__(self):
        pass

    cdef void init(self, const shared_ptr[CBuffer]& buffer):
        self.buffer = buffer
        self.shape[0] = self.size
        self.strides[0] = <Py_ssize_t>(1)

    def __len__(self):
        return self.size

    property size:

        def __get__(self):
            return self.buffer.get().size()

    property parent:

        def __get__(self):
            cdef shared_ptr[CBuffer] parent_buf = self.buffer.get().parent()

            if parent_buf.get() == NULL:
                return None
            else:
                return pyarrow_wrap_buffer(parent_buf)

    def __getitem__(self, key):
        # TODO(wesm): buffer slicing
        raise NotImplementedError

    def to_pybytes(self):
        return cp.PyBytes_FromStringAndSize(
            <const char*>self.buffer.get().data(),
            self.buffer.get().size())

    def __getbuffer__(self, cp.Py_buffer* buffer, int flags):

        buffer.buf = <char *>self.buffer.get().data()
        buffer.format = 'b'
        buffer.internal = NULL
        buffer.itemsize = 1
        buffer.len = self.size
        buffer.ndim = 1
        buffer.obj = self
        if self.buffer.get().is_mutable():
            buffer.readonly = 0
        else:
            buffer.readonly = 1
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL

    def __getsegcount__(self, Py_ssize_t *len_out):
        if len_out != NULL:
            len_out[0] = <Py_ssize_t>self.size
        return 1

    def __getreadbuffer__(self, Py_ssize_t idx, void **p):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        if p != NULL:
            p[0] = <void*> self.buffer.get().data()
        return self.size

    def __getwritebuffer__(self, Py_ssize_t idx, void **p):
        if not self.buffer.get().is_mutable():
            raise SystemError("trying to write an immutable buffer")
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        if p != NULL:
            p[0] = <void*> self.buffer.get().data()
        return self.size


cdef shared_ptr[PoolBuffer] allocate_buffer(CMemoryPool* pool):
    cdef shared_ptr[PoolBuffer] result
    result.reset(new PoolBuffer(pool))
    return result


cdef class BufferOutputStream(NativeFile):

    cdef:
        shared_ptr[PoolBuffer] buffer

    def __cinit__(self, MemoryPool memory_pool=None):
        self.buffer = allocate_buffer(maybe_unbox_memory_pool(memory_pool))
        self.wr_file.reset(new CBufferOutputStream(
            <shared_ptr[ResizableBuffer]> self.buffer))
        self.is_readable = 0
        self.is_writeable = 1
        self.is_open = True

    def get_result(self):
        with nogil:
            check_status(self.wr_file.get().Close())
        self.is_open = False
        return pyarrow_wrap_buffer(<shared_ptr[CBuffer]> self.buffer)


cdef class MockOutputStream(NativeFile):

    def __cinit__(self):
        self.wr_file.reset(new CMockOutputStream())
        self.is_readable = 0
        self.is_writeable = 1
        self.is_open = True

    def size(self):
        return (<CMockOutputStream*>self.wr_file.get()).GetExtentBytesWritten()


cdef class BufferReader(NativeFile):
    """
    Zero-copy reader from objects convertible to Arrow buffer

    Parameters
    ----------
    obj : Python bytes or pyarrow.Buffer
    """
    cdef:
        Buffer buffer

    def __cinit__(self, object obj):

        if isinstance(obj, Buffer):
            self.buffer = obj
        else:
            self.buffer = frombuffer(obj)

        self.rd_file.reset(new CBufferReader(self.buffer.buffer))
        self.is_readable = 1
        self.is_writeable = 0
        self.is_open = True


def frombuffer(object obj):
    """
    Construct an Arrow buffer from a Python bytes object
    """
    cdef shared_ptr[CBuffer] buf
    try:
        memoryview(obj)
        buf.reset(new PyBuffer(obj))
        return pyarrow_wrap_buffer(buf)
    except TypeError:
        raise ValueError('Must pass object that implements buffer protocol')


cdef get_reader(object source, shared_ptr[RandomAccessFile]* reader):
    cdef NativeFile nf

    if isinstance(source, six.string_types):
        source = memory_map(source, mode='r')
    elif isinstance(source, Buffer):
        source = BufferReader(source)
    elif not isinstance(source, NativeFile) and hasattr(source, 'read'):
        # Optimistically hope this is file-like
        source = PythonFile(source, mode='r')

    if isinstance(source, NativeFile):
        nf = source

        # TODO: what about read-write sources (e.g. memory maps)
        if not nf.is_readable:
            raise IOError('Native file is not readable')

        nf.read_handle(reader)
    else:
        raise TypeError('Unable to read from object of type: {0}'
                        .format(type(source)))


cdef get_writer(object source, shared_ptr[OutputStream]* writer):
    cdef NativeFile nf

    if isinstance(source, six.string_types):
        source = OSFile(source, mode='w')
    elif not isinstance(source, NativeFile) and hasattr(source, 'write'):
        # Optimistically hope this is file-like
        source = PythonFile(source, mode='w')

    if isinstance(source, NativeFile):
        nf = source

        if nf.is_readable:
            raise IOError('Native file is not writeable')

        nf.write_handle(writer)
    else:
        raise TypeError('Unable to read from object of type: {0}'
                        .format(type(source)))
