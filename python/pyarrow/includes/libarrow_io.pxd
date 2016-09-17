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

# distutils: language = c++

from pyarrow.includes.common cimport *

cdef extern from "arrow/io/interfaces.h" namespace "arrow::io" nogil:
    enum FileMode" arrow::io::FileMode::type":
        FileMode_READ" arrow::io::FileMode::READ"
        FileMode_WRITE" arrow::io::FileMode::WRITE"
        FileMode_READWRITE" arrow::io::FileMode::READWRITE"

    enum ObjectType" arrow::io::ObjectType::type":
        ObjectType_FILE" arrow::io::ObjectType::FILE"
        ObjectType_DIRECTORY" arrow::io::ObjectType::DIRECTORY"

    cdef cppclass FileInterface:
        CStatus Close()
        CStatus Tell(int64_t* position)
        FileMode mode()

    cdef cppclass Readable:
        CStatus Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out)

    cdef cppclass Seekable:
        CStatus Seek(int64_t position)

    cdef cppclass Writeable:
        CStatus Write(const uint8_t* data, int64_t nbytes)

    cdef cppclass OutputStream(FileInterface, Writeable):
        pass

    cdef cppclass InputStream(FileInterface, Readable):
        pass

    cdef cppclass ReadableFileInterface(InputStream, Seekable):
        CStatus GetSize(int64_t* size)

        CStatus ReadAt(int64_t position, int64_t nbytes,
                       int64_t* bytes_read, uint8_t* buffer)
        CStatus ReadAt(int64_t position, int64_t nbytes,
                       int64_t* bytes_read, shared_ptr[Buffer]* out)

    cdef cppclass WriteableFileInterface(OutputStream, Seekable):
        CStatus WriteAt(int64_t position, const uint8_t* data,
                        int64_t nbytes)

    cdef cppclass ReadWriteFileInterface(ReadableFileInterface,
                                         WriteableFileInterface):
        pass


cdef extern from "arrow/io/hdfs.h" namespace "arrow::io" nogil:
    CStatus ConnectLibHdfs()

    cdef cppclass HdfsConnectionConfig:
        c_string host
        int port
        c_string user

    cdef cppclass HdfsPathInfo:
        ObjectType kind;
        c_string name
        c_string owner
        c_string group
        int32_t last_modified_time
        int32_t last_access_time
        int64_t size
        int16_t replication
        int64_t block_size
        int16_t permissions

    cdef cppclass HdfsReadableFile(ReadableFileInterface):
        pass

    cdef cppclass HdfsOutputStream(OutputStream):
        pass

    cdef cppclass CHdfsClient" arrow::io::HdfsClient":
        @staticmethod
        CStatus Connect(const HdfsConnectionConfig* config,
                        shared_ptr[CHdfsClient]* client)

        CStatus CreateDirectory(const c_string& path)

        CStatus Delete(const c_string& path, c_bool recursive)

        CStatus Disconnect()

        c_bool Exists(const c_string& path)

        CStatus GetCapacity(int64_t* nbytes)
        CStatus GetUsed(int64_t* nbytes)

        CStatus ListDirectory(const c_string& path,
                              vector[HdfsPathInfo]* listing)

        CStatus Rename(const c_string& src, const c_string& dst)

        CStatus OpenReadable(const c_string& path,
                             shared_ptr[HdfsReadableFile]* handle)

        CStatus OpenWriteable(const c_string& path, c_bool append,
                              int32_t buffer_size, int16_t replication,
                              int64_t default_block_size,
                              shared_ptr[HdfsOutputStream]* handle)
