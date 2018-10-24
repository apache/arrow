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

import re
import six
import sys
import threading
import time
import warnings
from io import BufferedIOBase, UnsupportedOperation

from pyarrow.util import _stringify_path
from pyarrow.compat import builtin_pickle, frombytes, tobytes, encode_file_path


# 64K
DEFAULT_BUFFER_SIZE = 2 ** 16


# To let us get a PyObject* and avoid Cython auto-ref-counting
cdef extern from "Python.h":
    PyObject* PyBytes_FromStringAndSizeNative" PyBytes_FromStringAndSize"(
        char *v, Py_ssize_t len) except NULL


cdef class NativeFile:

    def __cinit__(self):
        self.closed = True
        self.own_file = False
        self.is_readable = False
        self.is_writable = False

    def __dealloc__(self):
        if self.own_file and not self.closed:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    @property
    def mode(self):
        """
        The file mode. Currently instances of NativeFile may support:

        * rb: binary read
        * wb: binary write
        * rb+: binary read and write
        """
        # Emulate built-in file modes
        if self.is_readable and self.is_writable:
            return 'rb+'
        elif self.is_readable:
            return 'rb'
        elif self.is_writable:
            return 'wb'
        else:
            raise ValueError('File object is malformed, has no mode')

    def readable(self):
        self._assert_open()
        return self.is_readable

    def writable(self):
        self._assert_open()
        return self.is_writable

    def seekable(self):
        self._assert_open()
        return self.is_readable

    def isatty(self):
        self._assert_open()
        return False

    def fileno(self):
        """
        NOT IMPLEMENTED
        """
        raise UnsupportedOperation()

    def close(self):
        if not self.closed:
            with nogil:
                if self.is_readable:
                    check_status(self.rd_file.get().Close())
                else:
                    check_status(self.wr_file.get().Close())
        self.closed = True

    def flush(self):
        """Flush the buffer stream, if applicable.

        No-op to match the IOBase interface."""
        self._assert_open()

    cdef read_handle(self, shared_ptr[RandomAccessFile]* file):
        self._assert_readable()
        file[0] = <shared_ptr[RandomAccessFile]> self.rd_file

    cdef write_handle(self, shared_ptr[OutputStream]* file):
        self._assert_writable()
        file[0] = <shared_ptr[OutputStream]> self.wr_file

    def _assert_open(self):
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def _assert_readable(self):
        self._assert_open()
        if not self.is_readable:
            raise IOError("only valid on readonly files")

    def _assert_writable(self):
        self._assert_open()
        if not self.is_writable:
            raise IOError("only valid on writable files")

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
        self._assert_open()
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

        Parameters
        ----------
        data : bytes-like object or exporter of buffer protocol

        Returns
        -------
        nbytes : number of bytes written
        """
        self._assert_writable()

        cdef Buffer arrow_buffer = py_buffer(data)

        cdef const uint8_t* buf = arrow_buffer.buffer.get().data()
        cdef int64_t bufsize = len(arrow_buffer)
        with nogil:
            check_status(self.wr_file.get().Write(buf, bufsize))
        return bufsize

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

    def read1(self, nbytes=None):
        """Read and return up to n bytes.

        Alias for read, needed to match the IOBase interface."""
        return self.read(nbytes=None)

    def readall(self):
        return self.read()

    def readinto(self, b):
        """
        Read into the supplied buffer

        Parameters
        -----------
        b: any python object supporting buffer interface

        Returns
        --------
        number of bytes written
        """

        cdef:
            int64_t bytes_read
            uint8_t* buf
            Buffer py_buf
            int64_t buf_len

        self._assert_readable()

        py_buf = py_buffer(b)
        buf_len = py_buf.size

        buf = py_buf.buffer.get().mutable_data()

        with nogil:
            check_status(self.rd_file.get().Read(buf_len, &bytes_read, buf))

        return bytes_read

    def readline(self, size=None):
        """NOT IMPLEMENTED. Read and return a line of bytes from the file.

        If size is specified, read at most size bytes.

        Line terminator is always b"\\n".
        """

        raise UnsupportedOperation()

    def readlines(self, hint=None):
        """NOT IMPLEMENTED. Read lines of the file

        Parameters
        -----------

        hint: int maximum number of bytes read until we stop
        """

        raise UnsupportedOperation()

    def __iter__(self):
        self._assert_readable()
        return self

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line

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

    def truncate(self):
        """
        NOT IMPLEMENTED
        """
        raise UnsupportedOperation()

    def writelines(self, lines):
        self._assert_writable()

        for line in lines:
            self.write(line)

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

                if writer_thread.is_alive():
                    while write_queue.full():
                        time.sleep(0.01)
                else:
                    break

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
        self._assert_writable()

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

BufferedIOBase.register(NativeFile)

# ----------------------------------------------------------------------
# Python file-like objects


cdef class PythonFile(NativeFile):
    cdef:
        object handle

    def __cinit__(self, handle, mode=None):
        self.handle = handle

        if mode is None:
            try:
                mode = handle.mode
            except AttributeError:
                # Not all file-like objects have a mode attribute
                # (e.g. BytesIO)
                try:
                    mode = 'w' if handle.writable() else 'r'
                except AttributeError:
                    raise ValueError("could not infer open mode for file-like "
                                     "object %r, please pass it explicitly"
                                     % (handle,))
        if mode.startswith('w'):
            self.wr_file.reset(new PyOutputStream(handle))
            self.is_writable = True
        elif mode.startswith('r'):
            self.rd_file.reset(new PyReadableFile(handle))
            self.is_readable = True
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        self.closed = False

    def truncate(self, pos=None):
        self.handle.truncate(pos)

    def readline(self, size=None):
        return self.handle.readline(size)

    def readlines(self, hint=None):
        return self.handle.readlines(hint)


cdef class MemoryMappedFile(NativeFile):
    """
    Supports 'r', 'r+w', 'w' modes
    """
    cdef:
        shared_ptr[CMemoryMappedFile] handle
        object path

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
        result.is_readable = True
        result.is_writable = True
        result.wr_file = <shared_ptr[OutputStream]> handle
        result.rd_file = <shared_ptr[RandomAccessFile]> handle
        result.handle = handle
        result.closed = False

        return result

    def _open(self, path, mode='r'):
        self.path = path

        cdef:
            FileMode c_mode
            shared_ptr[CMemoryMappedFile] handle
            c_string c_path = encode_file_path(path)

        if mode in ('r', 'rb'):
            c_mode = FileMode_READ
            self.is_readable = True
        elif mode in ('w', 'wb'):
            c_mode = FileMode_WRITE
            self.is_writable = True
        elif mode in ('r+', 'r+b', 'rb+'):
            c_mode = FileMode_READWRITE
            self.is_readable = True
            self.is_writable = True
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        with nogil:
            check_status(CMemoryMappedFile.Open(c_path, c_mode, &handle))

        self.wr_file = <shared_ptr[OutputStream]> handle
        self.rd_file = <shared_ptr[RandomAccessFile]> handle
        self.handle = handle
        self.closed = False

    def resize(self, new_size):
        """
        Resize the map and underlying file.

        Parameters
        ----------
        new_size : new size in bytes
        """
        check_status(self.handle.get().Resize(new_size))

    def fileno(self):
        self._assert_open()
        return self.handle.get().file_descriptor()


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
    writable file object

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

        if mode in ('r', 'rb'):
            self._open_readable(c_path, maybe_unbox_memory_pool(memory_pool))
        elif mode in ('w', 'wb'):
            self._open_writable(c_path)
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        self.closed = False

    cdef _open_readable(self, c_string path, CMemoryPool* pool):
        cdef shared_ptr[ReadableFile] handle

        with nogil:
            check_status(ReadableFile.Open(path, pool, &handle))

        self.is_readable = True
        self.rd_file = <shared_ptr[RandomAccessFile]> handle

    cdef _open_writable(self, c_string path):
        with nogil:
            check_status(FileOutputStream.Open(path, &self.wr_file))
        self.is_writable = True

    def fileno(self):
        self._assert_open()
        return self.handle.file_descriptor()


cdef class FixedSizeBufferWriter(NativeFile):

    def __cinit__(self, Buffer buffer):
        self.wr_file.reset(new CFixedSizeBufferWriter(buffer.buffer))
        self.is_writable = True
        self.closed = False

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

    def __init__(self):
        raise TypeError("Do not call Buffer's constructor directly, use "
                        "`pyarrow.py_buffer` function instead.")

    cdef void init(self, const shared_ptr[CBuffer]& buffer):
        self.buffer = buffer
        self.shape[0] = self.size
        self.strides[0] = <Py_ssize_t>(1)

    def __len__(self):
        return self.size

    @property
    def size(self):
        return self.buffer.get().size()

    @property
    def address(self):
        return <uintptr_t> self.buffer.get().data()

    @property
    def is_mutable(self):
        return self.buffer.get().is_mutable()

    @property
    def parent(self):
        cdef shared_ptr[CBuffer] parent_buf = self.buffer.get().parent()

        if parent_buf.get() == NULL:
            return None
        else:
            return pyarrow_wrap_buffer(parent_buf)

    def __getitem__(self, key):
        if PySlice_Check(key):
            return _normalize_slice(self, key)

        return self.getitem(_normalize_index(key, self.size))

    cdef getitem(self, int64_t i):
        return self.buffer.get().data()[i]

    def slice(self, offset=0, length=None):
        """
        Compute slice of this buffer

        Parameters
        ----------
        offset : int, default 0
            Offset from start of buffer to slice
        length : int, default None
            Length of slice (default is until end of Buffer starting from
            offset)

        Returns
        -------
        sliced : Buffer
        """
        cdef shared_ptr[CBuffer] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        if length is None:
            result = SliceBuffer(self.buffer, offset)
        else:
            result = SliceBuffer(self.buffer, offset, max(length, 0))

        return pyarrow_wrap_buffer(result)

    def equals(self, Buffer other):
        """
        Determine if two buffers contain exactly the same data

        Parameters
        ----------
        other : Buffer

        Returns
        -------
        are_equal : True if buffer contents and size are equal
        """
        cdef c_bool result = False
        with nogil:
            result = self.buffer.get().Equals(deref(other.buffer.get()))
        return result

    def __eq__(self, other):
        if isinstance(other, Buffer):
            return self.equals(other)
        else:
            return NotImplemented

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            return py_buffer, (builtin_pickle.PickleBuffer(self),)
        else:
            return py_buffer, (self.to_pybytes(),)

    def to_pybytes(self):
        return cp.PyBytes_FromStringAndSize(
            <const char*>self.buffer.get().data(),
            self.buffer.get().size())

    def __getbuffer__(self, cp.Py_buffer* buffer, int flags):
        if self.buffer.get().is_mutable():
            buffer.readonly = 0
        else:
            if flags & cp.PyBUF_WRITABLE:
                raise BufferError("Writable buffer requested but Arrow "
                                  "buffer was not mutable")
            buffer.readonly = 1
        buffer.buf = <char *>self.buffer.get().data()
        buffer.format = 'b'
        buffer.internal = NULL
        buffer.itemsize = 1
        buffer.len = self.size
        buffer.ndim = 1
        buffer.obj = self
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


cdef class ResizableBuffer(Buffer):

    cdef void init_rz(self, const shared_ptr[CResizableBuffer]& buffer):
        self.init(<shared_ptr[CBuffer]> buffer)

    def resize(self, int64_t new_size, shrink_to_fit=False):
        """
        Resize buffer to indicated size

        Parameters
        ----------
        new_size : int64_t
            New size of buffer (padding may be added internally)
        shrink_to_fit : boolean, default False
            If new_size is less than the current size, shrink internal
            capacity, otherwise leave at current capacity
        """
        cdef c_bool c_shrink_to_fit = shrink_to_fit
        with nogil:
            check_status((<CResizableBuffer*> self.buffer.get())
                         .Resize(new_size, c_shrink_to_fit))


cdef shared_ptr[CResizableBuffer] _allocate_buffer(CMemoryPool* pool):
    cdef shared_ptr[CResizableBuffer] result
    with nogil:
        check_status(AllocateResizableBuffer(pool, 0, &result))
    return result


def allocate_buffer(int64_t size, MemoryPool memory_pool=None,
                    resizable=False):
    """
    Allocate mutable fixed-size buffer

    Parameters
    ----------
    size : int
        Number of bytes to allocate (plus internal padding)
    memory_pool : MemoryPool, optional
        Uses default memory pool if not provided
    resizable : boolean, default False

    Returns
    -------
    buffer : Buffer or ResizableBuffer
    """
    cdef:
        shared_ptr[CBuffer] buffer
        shared_ptr[CResizableBuffer] rz_buffer
        CMemoryPool* cpool = maybe_unbox_memory_pool(memory_pool)

    if resizable:
        with nogil:
            check_status(AllocateResizableBuffer(cpool, size, &rz_buffer))
        return pyarrow_wrap_resizable_buffer(rz_buffer)
    else:
        with nogil:
            check_status(AllocateBuffer(cpool, size, &buffer))
        return pyarrow_wrap_buffer(buffer)


cdef class BufferOutputStream(NativeFile):

    cdef:
        shared_ptr[CResizableBuffer] buffer

    def __cinit__(self, MemoryPool memory_pool=None):
        self.buffer = _allocate_buffer(maybe_unbox_memory_pool(memory_pool))
        self.wr_file.reset(new CBufferOutputStream(
            <shared_ptr[CResizableBuffer]> self.buffer))
        self.is_writable = True
        self.closed = False

    def getvalue(self):
        """
        Finalize output stream and return result as pyarrow.Buffer.

        Returns
        -------
        value : Buffer
        """
        with nogil:
            check_status(self.wr_file.get().Close())
        self.closed = True
        return pyarrow_wrap_buffer(<shared_ptr[CBuffer]> self.buffer)


cdef class MockOutputStream(NativeFile):

    def __cinit__(self):
        self.wr_file.reset(new CMockOutputStream())
        self.is_writable = True
        self.closed = False

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
            self.buffer = py_buffer(obj)

        self.rd_file.reset(new CBufferReader(self.buffer.buffer))
        self.is_readable = True
        self.closed = False


def py_buffer(object obj):
    """
    Construct an Arrow buffer from a Python bytes object
    """
    cdef shared_ptr[CBuffer] buf
    check_status(PyBuffer.FromPyObject(obj, &buf))
    return pyarrow_wrap_buffer(buf)


def foreign_buffer(address, size, base):
    """
    Construct an Arrow buffer with the given *address* and *size*,
    backed by the Python *base* object.
    """
    cdef:
        intptr_t c_addr = address
        int64_t c_size = size
        shared_ptr[CBuffer] buf

    check_status(PyForeignBuffer.Make(<uint8_t*> c_addr, c_size,
                                      base, &buf))
    return pyarrow_wrap_buffer(buf)


def as_buffer(object o):
    if isinstance(o, Buffer):
        return o
    return py_buffer(o)


cdef get_reader(object source, c_bool use_memory_map,
                shared_ptr[RandomAccessFile]* reader):
    cdef NativeFile nf

    try:
        source_path = _stringify_path(source)
    except TypeError:
        if isinstance(source, Buffer):
            source = BufferReader(source)
        elif not isinstance(source, NativeFile) and hasattr(source, 'read'):
            # Optimistically hope this is file-like
            source = PythonFile(source, mode='r')
    else:
        if use_memory_map:
            source = memory_map(source_path, mode='r')
        else:
            source = OSFile(source_path, mode='r')

    if isinstance(source, NativeFile):
        nf = source

        # TODO: what about read-write sources (e.g. memory maps)
        if not nf.is_readable:
            raise IOError('Native file is not readable')

        nf.read_handle(reader)
    else:
        raise TypeError('Unable to read from object of type: {0}'
                        .format(type(source)))


cdef get_input_stream(object source, c_bool use_memory_map,
                      shared_ptr[InputStream]* out):
    """
    Like get_reader(), but can automatically decompress, and returns
    an InputStream.
    """
    cdef:
        NativeFile nf
        shared_ptr[RandomAccessFile] random_access_file
        shared_ptr[InputStream] input_stream
        shared_ptr[CompressedInputStream] compressed_stream
        CompressionType compression_type = CompressionType_UNCOMPRESSED
        unique_ptr[CCodec] codec

    try:
        source_path = _stringify_path(source)
    except TypeError:
        pass
    else:
        compression_type = _get_compression_type_by_filename(source_path)

    get_reader(source, use_memory_map, &random_access_file)
    input_stream = <shared_ptr[InputStream]> random_access_file

    if compression_type != CompressionType_UNCOMPRESSED:
        check_status(CCodec.Create(compression_type, &codec))
        check_status(CompressedInputStream.Make(codec.get(), input_stream,
                                                &compressed_stream))
        input_stream = <shared_ptr[InputStream]> compressed_stream

    out[0] = input_stream


cdef get_writer(object source, shared_ptr[OutputStream]* writer):
    cdef NativeFile nf

    try:
        source_path = _stringify_path(source)
    except TypeError:
        if not isinstance(source, NativeFile) and hasattr(source, 'write'):
            # Optimistically hope this is file-like
            source = PythonFile(source, mode='w')
    else:
        source = OSFile(source_path, mode='w')

    if isinstance(source, NativeFile):
        nf = source

        if not nf.is_writable:
            raise IOError('Native file is not writable')

        nf.write_handle(writer)
    else:
        raise TypeError('Unable to read from object of type: {0}'
                        .format(type(source)))


# ---------------------------------------------------------------------

cdef CompressionType _get_compression_type(object name):
    if name is None or name == 'uncompressed':
        return CompressionType_UNCOMPRESSED
    elif name == 'snappy':
        return CompressionType_SNAPPY
    elif name == 'gzip':
        return CompressionType_GZIP
    elif name == 'brotli':
        return CompressionType_BROTLI
    elif name == 'zstd':
        return CompressionType_ZSTD
    elif name == 'lz4':
        return CompressionType_LZ4
    else:
        raise ValueError("Unrecognized compression type: {0}"
                         .format(str(name)))


cdef CompressionType _get_compression_type_by_filename(filename):
    if filename.endswith('.gz'):
        return CompressionType_GZIP
    elif filename.endswith('.bz2'):
        return CompressionType_BZ2
    elif filename.endswith('.lz4'):
        return CompressionType_LZ4
    elif filename.endswith('.zst'):
        return CompressionType_ZSTD
    else:
        return CompressionType_UNCOMPRESSED


def compress(object buf, codec='lz4', asbytes=False, memory_pool=None):
    """
    Compress pyarrow.Buffer or Python object supporting the buffer (memoryview)
    protocol

    Parameters
    ----------
    buf : pyarrow.Buffer, bytes, or other object supporting buffer protocol
    codec : string, default 'lz4'
        Compression codec.
        Supported types: {'brotli, 'gzip', 'lz4', 'snappy', 'zstd'}
    asbytes : boolean, default False
        Return result as Python bytes object, otherwise Buffer
    memory_pool : MemoryPool, default None
        Memory pool to use for buffer allocations, if any

    Returns
    -------
    compressed : pyarrow.Buffer or bytes (if asbytes=True)
    """
    cdef:
        CompressionType c_codec = _get_compression_type(codec)
        unique_ptr[CCodec] compressor
        cdef CBuffer* c_buf
        cdef PyObject* pyobj
        cdef ResizableBuffer out_buf

    with nogil:
        check_status(CCodec.Create(c_codec, &compressor))

    if not isinstance(buf, Buffer):
        buf = py_buffer(buf)

    c_buf = (<Buffer> buf).buffer.get()

    cdef int64_t max_output_size = (compressor.get()
                                    .MaxCompressedLen(c_buf.size(),
                                                      c_buf.data()))
    cdef uint8_t* output_buffer = NULL

    if asbytes:
        pyobj = PyBytes_FromStringAndSizeNative(NULL, max_output_size)
        output_buffer = <uint8_t*> cp.PyBytes_AS_STRING(<object> pyobj)
    else:
        out_buf = allocate_buffer(max_output_size, memory_pool=memory_pool,
                                  resizable=True)
        output_buffer = out_buf.buffer.get().mutable_data()

    cdef int64_t output_length = 0
    with nogil:
        check_status(compressor.get()
                     .Compress(c_buf.size(), c_buf.data(),
                               max_output_size, output_buffer,
                               &output_length))

    if asbytes:
        cp._PyBytes_Resize(&pyobj, <Py_ssize_t> output_length)
        return PyObject_to_object(pyobj)
    else:
        out_buf.resize(output_length)
        return out_buf


def decompress(object buf, decompressed_size=None, codec='lz4',
               asbytes=False, memory_pool=None):
    """
    Decompress data from buffer-like object

    Parameters
    ----------
    buf : pyarrow.Buffer, bytes, or memoryview-compatible object
    decompressed_size : int64_t, default None
        If not specified, will be computed if the codec is able to determine
        the uncompressed buffer size
    codec : string, default 'lz4'
        Compression codec.
        Supported types: {'brotli, 'gzip', 'lz4', 'snappy', 'zstd'}
    asbytes : boolean, default False
        Return result as Python bytes object, otherwise Buffer
    memory_pool : MemoryPool, default None
        Memory pool to use for buffer allocations, if any

    Returns
    -------
    uncompressed : pyarrow.Buffer or bytes (if asbytes=True)
    """
    cdef:
        CompressionType c_codec = _get_compression_type(codec)
        unique_ptr[CCodec] compressor
        cdef CBuffer* c_buf
        cdef Buffer out_buf

    with nogil:
        check_status(CCodec.Create(c_codec, &compressor))

    if not isinstance(buf, Buffer):
        buf = py_buffer(buf)

    c_buf = (<Buffer> buf).buffer.get()

    if decompressed_size is None:
        raise ValueError("Must pass decompressed_size for {0} codec"
                         .format(codec))

    cdef int64_t output_size = decompressed_size
    cdef uint8_t* output_buffer = NULL

    if asbytes:
        pybuf = cp.PyBytes_FromStringAndSize(NULL, output_size)
        output_buffer = <uint8_t*> cp.PyBytes_AS_STRING(pybuf)
    else:
        out_buf = allocate_buffer(output_size, memory_pool=memory_pool)
        output_buffer = out_buf.buffer.get().mutable_data()

    with nogil:
        check_status(compressor.get()
                     .Decompress(c_buf.size(), c_buf.data(),
                                 output_size, output_buffer))

    return pybuf if asbytes else out_buf
