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

# Cython wrappers for IO interfaces defined in arrow/io

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libc.stdlib cimport malloc, free

from pyarrow.includes.libarrow cimport *
cimport pyarrow.includes.pyarrow as pyarrow
from pyarrow.includes.libarrow_io cimport *

from pyarrow.compat import frombytes, tobytes
from pyarrow.error cimport check_status

cimport cpython as cp

import re
import sys
import threading


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

    def close(self):
        if self.is_open:
            with nogil:
                if self.is_readonly:
                    check_status(self.rd_file.get().Close())
                else:
                    check_status(self.wr_file.get().Close())
        self.is_open = False

    cdef read_handle(self, shared_ptr[ReadableFileInterface]* file):
        self._assert_readable()
        file[0] = <shared_ptr[ReadableFileInterface]> self.rd_file

    cdef write_handle(self, shared_ptr[OutputStream]* file):
        self._assert_writeable()
        file[0] = <shared_ptr[OutputStream]> self.wr_file

    def _assert_readable(self):
        if not self.is_readonly:
            raise IOError("only valid on readonly files")

        if not self.is_open:
            raise IOError("file not open")

    def _assert_writeable(self):
        if self.is_readonly:
            raise IOError("only valid on writeonly files")

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
            if self.is_readonly:
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
        Write bytes-like (unicode, encoded to UTF-8) to file
        """
        self._assert_writeable()

        data = tobytes(data)

        cdef const uint8_t* buf = <const uint8_t*> cp.PyBytes_AS_STRING(data)
        cdef int64_t bufsize = len(data)
        with nogil:
            check_status(self.wr_file.get().Write(buf, bufsize))

    def read(self, int nbytes):
        cdef:
            int64_t bytes_read = 0
            uint8_t* buf
            shared_ptr[CBuffer] out

        self._assert_readable()

        with nogil:
            check_status(self.rd_file.get().ReadB(nbytes, &out))

        result = cp.PyBytes_FromStringAndSize(
            <const char*>out.get().data(), out.get().size())

        return result


# ----------------------------------------------------------------------
# Python file-like objects

cdef class PythonFileInterface(NativeFile):
    cdef:
        object handle

    def __cinit__(self, handle, mode='w'):
        self.handle = handle

        if mode.startswith('w'):
            self.wr_file.reset(new pyarrow.PyOutputStream(handle))
            self.is_readonly = 0
        elif mode.startswith('r'):
            self.rd_file.reset(new pyarrow.PyReadableFile(handle))
            self.is_readonly = 1
        else:
            raise ValueError('Invalid file mode: {0}'.format(mode))

        self.is_open = True


cdef class BytesReader(NativeFile):
    cdef:
        object obj

    def __cinit__(self, obj):
        if not isinstance(obj, bytes):
            raise ValueError('Must pass bytes object')

        self.obj = obj
        self.is_readonly = 1
        self.is_open = True

        self.rd_file.reset(new pyarrow.PyBytesReader(obj))

# ----------------------------------------------------------------------
# Arrow buffers


cdef class Buffer:

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CBuffer]& buffer):
        self.buffer = buffer

    def __len__(self):
        return self.size

    property size:

        def __get__(self):
            return self.buffer.get().size()

    def __getitem__(self, key):
        # TODO(wesm): buffer slicing
        raise NotImplementedError

    def to_pybytes(self):
        return cp.PyBytes_FromStringAndSize(
            <const char*>self.buffer.get().data(),
            self.buffer.get().size())


cdef shared_ptr[PoolBuffer] allocate_buffer():
    cdef shared_ptr[PoolBuffer] result
    result.reset(new PoolBuffer(pyarrow.get_memory_pool()))
    return result


cdef class InMemoryOutputStream(NativeFile):

    cdef:
        shared_ptr[PoolBuffer] buffer

    def __cinit__(self):
        self.buffer = allocate_buffer()
        self.wr_file.reset(new BufferOutputStream(
            <shared_ptr[ResizableBuffer]> self.buffer))
        self.is_readonly = 0
        self.is_open = True

    def get_result(self):
        cdef Buffer result = Buffer()

        check_status(self.wr_file.get().Close())
        result.init(<shared_ptr[CBuffer]> self.buffer)

        self.is_open = False
        return result


cdef class BufferReader(NativeFile):
    cdef:
        Buffer buffer

    def __cinit__(self, Buffer buffer):
        self.buffer = buffer
        self.rd_file.reset(new CBufferReader(buffer.buffer.get().data(),
                                             buffer.buffer.get().size()))
        self.is_readonly = 1
        self.is_open = True


def buffer_from_bytes(object obj):
    """
    Construct an Arrow buffer from a Python bytes object
    """
    if not isinstance(obj, bytes):
        raise ValueError('Must pass bytes object')

    cdef shared_ptr[CBuffer] buf
    buf.reset(new pyarrow.PyBytesBuffer(obj))

    cdef Buffer result = Buffer()
    result.init(buf)
    return result

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
        check_status(ConnectLibHdfs())
        return True
    except:
        return False


def strip_hdfs_abspath(path):
    m = _HDFS_PATH_RE.match(path)
    if m:
        return m.group(3)
    else:
        return path


cdef class HdfsClient:
    cdef:
        shared_ptr[CHdfsClient] client

    cdef readonly:
        bint is_open

    def __cinit__(self):
        self.is_open = False

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

    @classmethod
    def connect(cls, host="default", port=0, user=None, kerb_ticket=None):
        """
        Connect to an HDFS cluster. All parameters are optional and should
        only be set if the defaults need to be overridden.

        Authentication should be automatic if the HDFS cluster uses Kerberos.
        However, if a username is specified, then the ticket cache will likely
        be required.

        Parameters
        ----------
        host : NameNode. Set to "default" for fs.defaultFS from core-site.xml.
        port : NameNode's port. Set to 0 for default or logical (HA) nodes.
        user : Username when connecting to HDFS; None implies login user.
        kerb_ticket : Path to Kerberos ticket cache.

        Notes
        -----
        The first time you call this method, it will take longer than usual due
        to JNI spin-up time.

        Returns
        -------
        client : HDFSClient
        """
        cdef:
            HdfsClient out = HdfsClient()
            HdfsConnectionConfig conf

        if host is not None:
            conf.host = tobytes(host)
        conf.port = port
        if user is not None:
            conf.user = tobytes(user)
        if kerb_ticket is not None:
            conf.kerb_ticket = tobytes(kerb_ticket)

        with nogil:
            check_status(CHdfsClient.Connect(&conf, &out.client))
        out.is_open = True

        return out

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

    def ls(self, path, bint full_info=True):
        """
        Retrieve directory contents and metadata, if requested.

        Parameters
        ----------
        path : HDFS path
        full_info : boolean, default True
            If False, only return list of paths

        Returns
        -------
        result : list of dicts (full_info=True) or strings (full_info=False)
        """
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
                         .CreateDirectory(c_path))

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

            out.is_readonly = False
        else:
            with nogil:
                check_status(self.client.get()
                             .OpenReadable(c_path, &rd_handle))

            out.rd_file = <shared_ptr[ReadableFileInterface]> rd_handle
            out.is_readonly = True

        if c_buffer_size == 0:
            c_buffer_size = 2 ** 16

        out.mode = mode
        out.buffer_size = c_buffer_size
        out.parent = _HdfsFileNanny(self, out)
        out.is_open = True
        out.own_file = True

        return out

    def upload(self, path, stream, buffer_size=2**16):
        """
        Upload file-like object to HDFS path
        """
        write_queue = Queue(50)

        with self.open(path, 'wb') as f:
            done = False
            exc_info = None
            def bg_write():
                try:
                    while not done or write_queue.qsize() > 0:
                        try:
                            buf = write_queue.get(timeout=0.01)
                        except QueueEmpty:
                            continue

                        f.write(buf)

                except Exception as e:
                    exc_info = sys.exc_info()

            writer_thread = threading.Thread(target=bg_write)
            writer_thread.start()

            try:
                while True:
                    buf = stream.read(buffer_size)
                    if not buf:
                        break

                    write_queue.put_nowait(buf)
            finally:
                done = True

            writer_thread.join()
            if exc_info is not None:
                raise exc_info[0], exc_info[1], exc_info[2]

    def download(self, path, stream, buffer_size=None):
        with self.open(path, 'rb', buffer_size=buffer_size) as f:
            f.download(stream)


# ----------------------------------------------------------------------
# Specialization for HDFS

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

    def read(self, int nbytes):
        """
        Read indicated number of bytes from the file, up to EOF
        """
        cdef:
            int64_t bytes_read = 0
            uint8_t* buf

        self._assert_readable()

        # This isn't ideal -- PyBytes_FromStringAndSize copies the data from
        # the passed buffer, so it's hard for us to avoid doubling the memory
        buf = <uint8_t*> malloc(nbytes)
        if buf == NULL:
            raise MemoryError("Failed to allocate {0} bytes".format(nbytes))

        cdef int64_t total_bytes = 0

        cdef int rpc_chunksize = min(self.buffer_size, nbytes)

        try:
            with nogil:
                while total_bytes < nbytes:
                    check_status(self.rd_file.get()
                                 .Read(rpc_chunksize, &bytes_read,
                                       buf + total_bytes))

                    total_bytes += bytes_read

                    # EOF
                    if bytes_read == 0:
                        break
            result = cp.PyBytes_FromStringAndSize(<const char*>buf,
                                                  total_bytes)
        finally:
            free(buf)

        return result

    def download(self, stream_or_path):
        """
        Read file completely to local path (rather than reading completely into
        memory). First seeks to the beginning of the file.
        """
        cdef:
            int64_t bytes_read = 0
            uint8_t* buf
        self._assert_readable()

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
        buf = <uint8_t*> malloc(self.buffer_size)
        if buf == NULL:
            raise MemoryError("Failed to allocate {0} bytes"
                              .format(self.buffer_size))

        writer_thread.start()

        cdef int64_t total_bytes = 0

        try:
            while True:
                with nogil:
                    check_status(self.rd_file.get()
                                 .Read(self.buffer_size, &bytes_read, buf))

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
