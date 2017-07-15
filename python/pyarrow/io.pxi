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
        cdef int64_t size
        self._assert_readable()
        with nogil:
            check_status(self.rd_file.get().GetSize(&size))
        return size

    def tell(self):
        cdef int64_t position
        with nogil:
            if self.is_readable:
                check_status(self.rd_file.get().Tell(&position))
            else:
                check_status(self.wr_file.get().Tell(&position))
        return position

    def seek(self, int64_t position):
        self._assert_readable()
        with nogil:
            check_status(self.rd_file.get().Seek(position))

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
            cleanup = lambda: stream.close()
        else:
            stream = stream_or_path
            cleanup = lambda: None

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
        buffer.readonly = 1
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL


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

# ----------------------------------------------------------------------
# HDFS IO implementation

_HDFS_PATH_RE = re.compile('hdfs://(.*):(\d+)(.*)')

try:
    # Python 3
    from queue import Queue, Empty as QueueEmpty, Full as QueueFull
except ImportError:
    from Queue import Queue, Empty as QueueEmpty, Full as QueueFull


def have_libhdfs():
    try:
        check_status(HaveLibHdfs())
        return True
    except:
        return False


def have_libhdfs3():
    try:
        check_status(HaveLibHdfs3())
        return True
    except:
        return False


def strip_hdfs_abspath(path):
    m = _HDFS_PATH_RE.match(path)
    if m:
        return m.group(3)
    else:
        return path


cdef class _HdfsClient:
    cdef:
        shared_ptr[CHdfsClient] client

    cdef readonly:
        bint is_open

    def __cinit__(self):
        pass

    def _connect(self, host, port, user, kerb_ticket, driver):
        cdef HdfsConnectionConfig conf

        if host is not None:
            conf.host = tobytes(host)
        conf.port = port
        if user is not None:
            conf.user = tobytes(user)
        if kerb_ticket is not None:
            conf.kerb_ticket = tobytes(kerb_ticket)

        if driver == 'libhdfs':
            check_status(HaveLibHdfs())
            conf.driver = HdfsDriver_LIBHDFS
        else:
            check_status(HaveLibHdfs3())
            conf.driver = HdfsDriver_LIBHDFS3

        with nogil:
            check_status(CHdfsClient.Connect(&conf, &self.client))
        self.is_open = True

    @classmethod
    def connect(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def __dealloc__(self):
        if self.is_open:
            self.close()

    def close(self):
        """
        Disconnect from the HDFS cluster
        """
        self._ensure_client()
        with nogil:
            check_status(self.client.get().Disconnect())
        self.is_open = False

    cdef _ensure_client(self):
        if self.client.get() == NULL:
            raise IOError('HDFS client improperly initialized')
        elif not self.is_open:
            raise IOError('HDFS client is closed')

    def exists(self, path):
        """
        Returns True if the path is known to the cluster, False if it does not
        (or there is an RPC error)
        """
        self._ensure_client()

        cdef c_string c_path = tobytes(path)
        cdef c_bool result
        with nogil:
            result = self.client.get().Exists(c_path)
        return result

    def isdir(self, path):
        cdef HdfsPathInfo info
        self._path_info(path, &info)
        return info.kind == ObjectType_DIRECTORY

    def isfile(self, path):
        cdef HdfsPathInfo info
        self._path_info(path, &info)
        return info.kind == ObjectType_FILE

    cdef _path_info(self, path, HdfsPathInfo* info):
        cdef c_string c_path = tobytes(path)

        with nogil:
            check_status(self.client.get()
                         .GetPathInfo(c_path, info))


    def ls(self, path, bint full_info):
        cdef:
            c_string c_path = tobytes(path)
            vector[HdfsPathInfo] listing
            list results = []
            int i

        self._ensure_client()

        with nogil:
            check_status(self.client.get()
                         .ListDirectory(c_path, &listing))

        cdef const HdfsPathInfo* info
        for i in range(<int> listing.size()):
            info = &listing[i]

            # Try to trim off the hdfs://HOST:PORT piece
            name = strip_hdfs_abspath(frombytes(info.name))

            if full_info:
                kind = ('file' if info.kind == ObjectType_FILE
                        else 'directory')

                results.append({
                    'kind': kind,
                    'name': name,
                    'owner': frombytes(info.owner),
                    'group': frombytes(info.group),
                    'list_modified_time': info.last_modified_time,
                    'list_access_time': info.last_access_time,
                    'size': info.size,
                    'replication': info.replication,
                    'block_size': info.block_size,
                    'permissions': info.permissions
                })
            else:
                results.append(name)

        return results

    def mkdir(self, path):
        """
        Create indicated directory and any necessary parent directories
        """
        self._ensure_client()

        cdef c_string c_path = tobytes(path)
        with nogil:
            check_status(self.client.get()
                         .MakeDirectory(c_path))

    def delete(self, path, bint recursive=False):
        """
        Delete the indicated file or directory

        Parameters
        ----------
        path : string
        recursive : boolean, default False
            If True, also delete child paths for directories
        """
        self._ensure_client()

        cdef c_string c_path = tobytes(path)
        with nogil:
            check_status(self.client.get()
                         .Delete(c_path, recursive))

    def open(self, path, mode='rb', buffer_size=None, replication=None,
             default_block_size=None):
        """
        Parameters
        ----------
        mode : string, 'rb', 'wb', 'ab'
        """
        self._ensure_client()

        cdef HdfsFile out = HdfsFile()

        if mode not in ('rb', 'wb', 'ab'):
            raise Exception("Mode must be 'rb' (read), "
                            "'wb' (write, new file), or 'ab' (append)")

        cdef c_string c_path = tobytes(path)
        cdef c_bool append = False

        # 0 in libhdfs means "use the default"
        cdef int32_t c_buffer_size = buffer_size or 0
        cdef int16_t c_replication = replication or 0
        cdef int64_t c_default_block_size = default_block_size or 0

        cdef shared_ptr[HdfsOutputStream] wr_handle
        cdef shared_ptr[HdfsReadableFile] rd_handle

        if mode in ('wb', 'ab'):
            if mode == 'ab':
                append = True

            with nogil:
                check_status(
                    self.client.get()
                    .OpenWriteable(c_path, append, c_buffer_size,
                                   c_replication, c_default_block_size,
                                   &wr_handle))

            out.wr_file = <shared_ptr[OutputStream]> wr_handle

            out.is_readable = False
            out.is_writeable = 1
        else:
            with nogil:
                check_status(self.client.get()
                             .OpenReadable(c_path, &rd_handle))

            out.rd_file = <shared_ptr[RandomAccessFile]> rd_handle
            out.is_readable = True
            out.is_writeable = 0

        if c_buffer_size == 0:
            c_buffer_size = 2 ** 16

        out.mode = mode
        out.buffer_size = c_buffer_size
        out.parent = _HdfsFileNanny(self, out)
        out.is_open = True
        out.own_file = True

        return out

    def download(self, path, stream, buffer_size=None):
        with self.open(path, 'rb') as f:
            f.download(stream, buffer_size=buffer_size)

    def upload(self, path, stream, buffer_size=None):
        """
        Upload file-like object to HDFS path
        """
        with self.open(path, 'wb') as f:
            f.upload(stream, buffer_size=buffer_size)


# ARROW-404: Helper class to ensure that files are closed before the
# client. During deallocation of the extension class, the attributes are
# decref'd which can cause the client to get closed first if the file has the
# last remaining reference
cdef class _HdfsFileNanny:
    cdef:
        object client
        object file_handle_ref

    def __cinit__(self, client, file_handle):
        import weakref
        self.client = client
        self.file_handle_ref = weakref.ref(file_handle)

    def __dealloc__(self):
        fh = self.file_handle_ref()
        if fh:
            fh.close()
        # avoid cyclic GC
        self.file_handle_ref = None
        self.client = None


cdef class HdfsFile(NativeFile):
    cdef readonly:
        int32_t buffer_size
        object mode
        object parent

    cdef object __weakref__

    def __dealloc__(self):
        self.parent = None

# ----------------------------------------------------------------------
# File and stream readers and writers

cdef class _RecordBatchWriter:
    cdef:
        shared_ptr[CRecordBatchWriter] writer
        shared_ptr[OutputStream] sink
        bint closed

    def __cinit__(self):
        self.closed = True

    def __dealloc__(self):
        if not self.closed:
            self.close()

    def _open(self, sink, Schema schema):
        cdef:
            shared_ptr[CRecordBatchStreamWriter] writer

        get_writer(sink, &self.sink)

        with nogil:
            check_status(
                CRecordBatchStreamWriter.Open(self.sink.get(),
                                              schema.sp_schema,
                                              &writer))

        self.writer = <shared_ptr[CRecordBatchWriter]> writer
        self.closed = False

    def write_batch(self, RecordBatch batch):
        with nogil:
            check_status(self.writer.get()
                         .WriteRecordBatch(deref(batch.batch)))

    def close(self):
        with nogil:
            check_status(self.writer.get().Close())
        self.closed = True


cdef class _RecordBatchReader:
    cdef:
        shared_ptr[CRecordBatchReader] reader

    cdef readonly:
        Schema schema

    def __cinit__(self):
        pass

    def _open(self, source):
        cdef:
            shared_ptr[RandomAccessFile] file_handle
            shared_ptr[InputStream] in_stream
            shared_ptr[CRecordBatchStreamReader] reader

        get_reader(source, &file_handle)
        in_stream = <shared_ptr[InputStream]> file_handle

        with nogil:
            check_status(CRecordBatchStreamReader.Open(in_stream, &reader))

        self.reader = <shared_ptr[CRecordBatchReader]> reader
        self.schema = pyarrow_wrap_schema(self.reader.get().schema())

    def get_next_batch(self):
        """
        Read next RecordBatch from the stream. Raises StopIteration at end of
        stream
        """
        cdef shared_ptr[CRecordBatch] batch

        with nogil:
            check_status(self.reader.get().GetNextRecordBatch(&batch))

        if batch.get() == NULL:
            raise StopIteration

        return pyarrow_wrap_batch(batch)

    def read_all(self):
        """
        Read all record batches as a pyarrow.Table
        """
        cdef:
            vector[shared_ptr[CRecordBatch]] batches
            shared_ptr[CRecordBatch] batch
            shared_ptr[CTable] table

        with nogil:
            while True:
                check_status(self.reader.get().GetNextRecordBatch(&batch))
                if batch.get() == NULL:
                    break
                batches.push_back(batch)

            check_status(CTable.FromRecordBatches(batches, &table))

        return pyarrow_wrap_table(table)


cdef class _RecordBatchFileWriter(_RecordBatchWriter):

    def _open(self, sink, Schema schema):
        cdef shared_ptr[CRecordBatchFileWriter] writer
        get_writer(sink, &self.sink)

        with nogil:
            check_status(
                CRecordBatchFileWriter.Open(self.sink.get(), schema.sp_schema,
                                      &writer))

        # Cast to base class, because has same interface
        self.writer = <shared_ptr[CRecordBatchWriter]> writer
        self.closed = False


cdef class _RecordBatchFileReader:
    cdef:
        shared_ptr[CRecordBatchFileReader] reader

    cdef readonly:
        Schema schema

    def __cinit__(self):
        pass

    def _open(self, source, footer_offset=None):
        cdef shared_ptr[RandomAccessFile] reader
        get_reader(source, &reader)

        cdef int64_t offset = 0
        if footer_offset is not None:
            offset = footer_offset

        with nogil:
            if offset != 0:
                check_status(CRecordBatchFileReader.Open2(
                    reader, offset, &self.reader))
            else:
                check_status(CRecordBatchFileReader.Open(reader, &self.reader))

        self.schema = pyarrow_wrap_schema(self.reader.get().schema())

    property num_record_batches:

        def __get__(self):
            return self.reader.get().num_record_batches()

    def get_batch(self, int i):
        cdef shared_ptr[CRecordBatch] batch

        if i < 0 or i >= self.num_record_batches:
            raise ValueError('Batch number {0} out of range'.format(i))

        with nogil:
            check_status(self.reader.get().GetRecordBatch(i, &batch))

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
                check_status(self.reader.get().GetRecordBatch(i, &batches[i]))
            check_status(CTable.FromRecordBatches(batches, &table))

        return pyarrow_wrap_table(table)


#----------------------------------------------------------------------
# Implement legacy Feather file format


class FeatherError(Exception):
    pass


cdef class FeatherWriter:
    cdef:
        unique_ptr[CFeatherWriter] writer

    cdef public:
        int64_t num_rows

    def __cinit__(self):
        self.num_rows = -1

    def open(self, object dest):
        cdef shared_ptr[OutputStream] sink
        get_writer(dest, &sink)

        with nogil:
            check_status(CFeatherWriter.Open(sink, &self.writer))

    def close(self):
        if self.num_rows < 0:
            self.num_rows = 0
        self.writer.get().SetNumRows(self.num_rows)
        check_status(self.writer.get().Finalize())

    def write_array(self, object name, object col, object mask=None):
        cdef Array arr

        if self.num_rows >= 0:
            if len(col) != self.num_rows:
                raise ValueError('prior column had a different number of rows')
        else:
            self.num_rows = len(col)

        if isinstance(col, Array):
            arr = col
        else:
            arr = Array.from_pandas(col, mask=mask)

        cdef c_string c_name = tobytes(name)

        with nogil:
            check_status(
                self.writer.get().Append(c_name, deref(arr.sp_array)))


cdef class FeatherReader:
    cdef:
        unique_ptr[CFeatherReader] reader

    def __cinit__(self):
        pass

    def open(self, source):
        cdef shared_ptr[RandomAccessFile] reader
        get_reader(source, &reader)

        with nogil:
            check_status(CFeatherReader.Open(reader, &self.reader))

    property num_rows:

        def __get__(self):
            return self.reader.get().num_rows()

    property num_columns:

        def __get__(self):
            return self.reader.get().num_columns()

    def get_column_name(self, int i):
        cdef c_string name = self.reader.get().GetColumnName(i)
        return frombytes(name)

    def get_column(self, int i):
        if i < 0 or i >= self.num_columns:
            raise IndexError(i)

        cdef shared_ptr[CColumn] sp_column
        with nogil:
            check_status(self.reader.get()
                         .GetColumn(i, &sp_column))

        cdef Column col = Column()
        col.init(sp_column)
        return col


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

    dest._assert_writeable()

    with nogil:
        check_status(
            WriteTensor(deref(tensor.tp), dest.wr_file.get(),
                        &metadata_length, &body_length))

    return metadata_length + body_length


def read_tensor(NativeFile source):
    """
    Read pyarrow.Tensor from pyarrow.NativeFile object from current
    position. If the file source supports zero copy (e.g. a memory map), then
    this operation does not allocate any memory

    Parameters
    ----------
    source : pyarrow.NativeFile

    Returns
    -------
    tensor : Tensor
    """
    cdef:
        shared_ptr[CTensor] sp_tensor

    source._assert_readable()

    cdef int64_t offset = source.tell()
    with nogil:
        check_status(ReadTensor(offset, source.rd_file.get(), &sp_tensor))

    return pyarrow_wrap_tensor(sp_tensor)
