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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from cython.operator cimport dereference as deref
from libcpp.vector cimport vector as std_vector
from libcpp.utility cimport move
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status, _Weakrefable,
                          MemoryPool, maybe_unbox_memory_pool,
                          Schema, pyarrow_wrap_schema,
                          pyarrow_wrap_batch,
                          RecordBatch,
                          pyarrow_wrap_table,
                          pyarrow_unwrap_schema,
                          pyarrow_unwrap_table,
                          get_reader,
                          get_writer)

cdef dict _compression_kinds = {'none': _CompressionKind_NONE,
                                'zlib': _CompressionKind_ZLIB, 'snappy': _CompressionKind_SNAPPY,
                                'lz0': _CompressionKind_LZO, 'lz4': _CompressionKind_LZ4,
                                'zstd': _CompressionKind_ZSTD}


cdef CompressionKind _convert_compression_kind(object compression_kind):
    if compression_kind not in _compression_kinds.keys():
        raise ValueError('Unknown compression_kind')
    else:
        return _compression_kinds[compression_kind]

cdef compression_kind_from_enum(CompressionKind compression_kind_):
    return {
        _CompressionKind_NONE: 'NONE',
        _CompressionKind_ZLIB: 'ZLIB',
        _CompressionKind_SNAPPY: 'SNAPPY',
        _CompressionKind_LZO: 'LZO',
        _CompressionKind_LZ4: 'LZ4',
        _CompressionKind_ZSTD: 'ZSTD',
    }.get(type_, 'UNKNOWN')

cdef compression_strategy_from_enum(CompressionStrategy compression_strategy_):
    return {
        _CompressionStrategy_SPEED: 'SPEED',
        _CompressionStrategy_COMPRESSION: 'COMPRESSION',
    }.get(type_, 'UNKNOWN')

cdef rle_version_from_enum(RleVersion rle_version_):
    return {
        _RleVersion_1: '1',
        _RleVersion_2: '2',
    }.get(type_, 'UNKNOWN')

cdef bloom_filter_version_from_enum(BloomFilterVersion bloom_filter_version_):
    return {
        _BloomFilterVersion_ORIGINAL: 'ORIGINAL',
        _BloomFilterVersion_UTF8: 'UTF8',
        _BloomFilterVersion_FUTURE: 'FUTURE',
    }.get(type_, 'UNKNOWN')

cdef class ORCReader(_Weakrefable):
    cdef:
        object source
        CMemoryPool* allocator
        unique_ptr[ORCFileReader] reader

    def __cinit__(self, MemoryPool memory_pool=None):
        self.allocator = maybe_unbox_memory_pool(memory_pool)

    def open(self, object source, c_bool use_memory_map=True):
        cdef:
            shared_ptr[CRandomAccessFile] rd_handle

        self.source = source

        get_reader(source, use_memory_map, &rd_handle)
        with nogil:
            check_status(ORCFileReader.Open(rd_handle, self.allocator,
                                            &self.reader))

    def schema(self):
        """
        The arrow schema for this file.

        Returns
        -------
        schema : pyarrow.Schema
        """
        cdef:
            shared_ptr[CSchema] sp_arrow_schema

        with nogil:
            check_status(deref(self.reader).ReadSchema(&sp_arrow_schema))

        return pyarrow_wrap_schema(sp_arrow_schema)

    def nrows(self):
        return deref(self.reader).NumberOfRows()

    def nstripes(self):
        return deref(self.reader).NumberOfStripes()

    def read_stripe(self, n, include_indices=None):
        cdef:
            shared_ptr[CRecordBatch] sp_record_batch
            RecordBatch batch
            int64_t stripe
            std_vector[int] indices

        stripe = n

        if include_indices is None:
            with nogil:
                (check_status(deref(self.reader)
                              .ReadStripe(stripe, &sp_record_batch)))
        else:
            indices = include_indices
            with nogil:
                (check_status(deref(self.reader)
                              .ReadStripe(stripe, indices, &sp_record_batch)))

        return pyarrow_wrap_batch(sp_record_batch)

    def read(self, include_indices=None):
        cdef:
            shared_ptr[CTable] sp_table
            std_vector[int] indices

        if include_indices is None:
            with nogil:
                check_status(deref(self.reader).Read(&sp_table))
        else:
            indices = include_indices
            with nogil:
                check_status(deref(self.reader).Read(indices, &sp_table))

        return pyarrow_wrap_table(sp_table)

cdef class ORCWriter(_Weakrefable):
    cdef:
        object source
        unique_ptr[ORCFileWriter] writer

    def open(self, object source):
        cdef:
            shared_ptr[COutputStream] rd_handle
        self.source = source
        get_writer(source, &rd_handle)
        with nogil:
            self.writer = move(GetResultValue[unique_ptr[ORCFileWriter]](
                ORCFileWriter.Open(deref(rd_handle))))

    def write(self, object table):
        cdef:
            shared_ptr[CTable] sp_table
        sp_table = pyarrow_unwrap_table(table)
        with nogil:
            check_status(deref(self.writer).Write(deref(sp_table)))

    def close(self):
        with nogil:
            check_status(deref(self.writer).Close())
