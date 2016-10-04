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
from pyarrow.error cimport check_cstatus

cimport cpython as cp

import re
import sys
import threading

_HDFS_PATH_RE = re.compile('hdfs://(.*):(\d+)(.*)')

try:
    # Python 3
    from queue import Queue, Empty as QueueEmpty, Full as QueueFull
except ImportError:
    from Queue import Queue, Empty as QueueEmpty, Full as QueueFull


def have_libhdfs():
    try:
        check_cstatus(ConnectLibHdfs())
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
        object host
        int port
        object user
        bint is_open

    def __cinit__(self):
        self.is_open = False

    def __dealloc__(self):
        if self.is_open:
            self.close()

    def close(self):
        self._ensure_client()
        with nogil:
            check_cstatus(self.client.get().Disconnect())
        self.is_open = False

    cdef _ensure_client(self):
        if self.client.get() == NULL:
            raise IOError('HDFS client improperly initialized')
        elif not self.is_open:
            raise IOError('HDFS client is closed')

    @classmethod
    def connect(cls, host, port, user):
        """

        Parameters
        ----------
        host :
        port :
        user :

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

        conf.host = tobytes(host)
        conf.port = port
        conf.user = tobytes(user)

        with nogil:
            check_cstatus(
                CHdfsClient.Connect(&conf, &out.client))
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
            check_cstatus(self.client.get()
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
            check_cstatus(self.client.get()
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
            check_cstatus(self.client.get()
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
                check_cstatus(
                    self.client.get()
                    .OpenWriteable(c_path, append, c_buffer_size,
                                   c_replication, c_default_block_size,
                                   &wr_handle))

            out.wr_file = <shared_ptr[OutputStream]> wr_handle

            out.is_readonly = False
        else:
            with nogil:
                check_cstatus(self.client.get()
                              .OpenReadable(c_path, &rd_handle))

            out.rd_file = <shared_ptr[ReadableFileInterface]> rd_handle
            out.is_readonly = True

        if c_buffer_size == 0:
            c_buffer_size = 2 ** 16

        out.mode = mode
        out.buffer_size = c_buffer_size
        out.parent = self
        out.is_open = True

        return out

    def upload(self, path, stream, buffer_size=2**16):
        """
        Upload file-like object to HDFS path
        """
        write_queue = Queue(50)

        f = self.open(path, 'wb')

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
        f = self.open(path, 'rb', buffer_size=buffer_size)
        f.download(stream)


cdef class NativeFile:

    def __cinit__(self):
        self.is_open = False

    def __dealloc__(self):
        if self.is_open:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def close(self):
        if self.is_open:
            with nogil:
                if self.is_readonly:
                    check_cstatus(self.rd_file.get().Close())
                else:
                    check_cstatus(self.wr_file.get().Close())
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

    def _assert_writeable(self):
        if self.is_readonly:
            raise IOError("only valid on writeonly files")

    def size(self):
        cdef int64_t size
        self._assert_readable()
        with nogil:
            check_cstatus(self.rd_file.get().GetSize(&size))
        return size

    def tell(self):
        cdef int64_t position
        with nogil:
            if self.is_readonly:
                check_cstatus(self.rd_file.get().Tell(&position))
            else:
                check_cstatus(self.wr_file.get().Tell(&position))
        return position

    def seek(self, int64_t position):
        self._assert_readable()
        with nogil:
            check_cstatus(self.rd_file.get().Seek(position))

    def write(self, data):
        """
        Write bytes-like (unicode, encoded to UTF-8) to file
        """
        self._assert_writeable()

        data = tobytes(data)

        cdef const uint8_t* buf = <const uint8_t*> cp.PyBytes_AS_STRING(data)
        cdef int64_t bufsize = len(data)
        with nogil:
            check_cstatus(self.wr_file.get().Write(buf, bufsize))

    def read(self, int nbytes):
        cdef:
            int64_t bytes_read = 0
            uint8_t* buf
            shared_ptr[Buffer] out

        self._assert_readable()

        with nogil:
            check_cstatus(self.rd_file.get()
                          .ReadB(nbytes, &out))

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
# Specialization for HDFS


cdef class HdfsFile(NativeFile):
    cdef readonly:
        int32_t buffer_size
        object mode
        object parent

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
                    check_cstatus(self.rd_file.get()
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
                    check_cstatus(self.rd_file.get()
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
