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

from cython.operator cimport dereference as deref
from libcpp.vector cimport vector as std_vector
from libcpp.utility cimport move
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (check_status, _Weakrefable,
                          MemoryPool, maybe_unbox_memory_pool,
                          Schema, pyarrow_wrap_schema,
                          KeyValueMetadata,
                          pyarrow_wrap_batch,
                          RecordBatch,
                          Table,
                          pyarrow_wrap_table,
                          pyarrow_unwrap_schema,
                          pyarrow_wrap_metadata,
                          pyarrow_unwrap_table,
                          get_reader,
                          get_writer)
from pyarrow.lib import tobytes

cdef compression_kind_from_enum(CompressionKind compression_kind_):
    return {
        _CompressionKind_NONE: 'NONE',
        _CompressionKind_ZLIB: 'ZLIB',
        _CompressionKind_SNAPPY: 'SNAPPY',
        _CompressionKind_LZO: 'LZO',
        _CompressionKind_LZ4: 'LZ4',
        _CompressionKind_ZSTD: 'ZSTD',
    }.get(type_, 'UNKNOWN')

cdef CompressionKind compression_kind_from_name(name):
    name = name.upper()
    if name == 'ZLIB':
        return _CompressionKind_ZLIB
    elif name == 'SNAPPY':
        return _CompressionKind_SNAPPY
    elif name == 'LZO':
        return _CompressionKind_LZO
    elif name == 'LZ4':
        return _CompressionKind_LZ4
    elif name == 'ZSTD':
        return _CompressionKind_ZSTD
    else:
        return _CompressionKind_None

cdef compression_strategy_from_enum(CompressionStrategy compression_strategy_):
    return {
        _CompressionStrategy_SPEED: 'SPEED',
        _CompressionStrategy_COMPRESSION: 'COMPRESSION',
    }.get(type_, 'UNKNOWN')

cdef CompressionStrategy compression_strategy_from_name(name):
    name = name.upper()
    # SPEED is the default value in the ORC C++ implementaton
    if name == 'COMPRESSION':
        return _CompressionStrategy_COMPRESSION
    else:
        return _CompressionStrategy_SPEED

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

cdef file_version_from_class(FileVersion file_version_):
    cdef object file_version = file_version_.ToString()
    return file_version

cdef class ORCWriterOptions(_Weakrefable):
    cdef:
        unique_ptr[WriterOptions] options
    
    def __cinit__(self):
        self.options = unique_ptr[WriterOptions](WriterOptions())
    
    def set_stripe_size(self, size):
        deref(self.options).set_stripe_size(size)
    
    def get_stripe_size(self):
        return deref(self.options).stripe_size()

    def set_compression_block_size(self, size):
        deref(self.options).set_compression_block_size(size)
    
    def get_compression_block_size(self):
        return deref(self.options).compression_block_size()

    def set_row_index_stride(self, stride):
        deref(self.options).set_row_index_stride(stride)
    
    def get_row_index_stride(self):
        return deref(self.options).row_index_stride()
    
    def set_dictionary_key_size_threshold(self, val):
        deref(self.options).set_dictionary_key_size_threshold(val)
    
    def get_dictionary_key_size_threshold(self):
        return deref(self.options).dictionary_key_size_threshold()
        
    def set_file_version(self, file_version):
        cdef:
            FileVersion c_file_version
            uint32_t c_major, c_minor
            object major, minor
        major, minor = str(file_version).split('.')
        c_major = major
        c_minor = minor
        deref(self.options).set_file_version(FileVersion(c_major, c_minor))

    def get_file_version(self):
        return file_version_from_class(deref(self.options).file_version())
    
    def set_compression(self, comp):
        return deref(self.options).set_compression(
            compression_kind_from_name(comp))

    def get_compression(self):
        return compression_kind_from_enum(deref(self.options).compression())

    def set_compression_strategy(self, strategy):
        return deref(self.options).set_compression_strategy(
            compression_strategy_from_name(strategy))

    def get_compression_strategy(self):
        return compression_strategy_from_enum(
            deref(self.options).compression_strategy())

    def get_aligned_bitpacking(self):
        return deref(self.options).aligned_bitpacking()

    def set_padding_tolerance(self, tolerance):
        deref(self.options).set_padding_tolerance(tolerance)

    def get_padding_tolerance(self):
        return deref(self.options).padding_tolerance(tolerance)

    def get_rle_version(self):
        return rle_version_from_enum(deref(self.options).rle_version())

    def get_enable_index(self):
        return deref(self.options).enable_index()
    
    def get_enable_dictionary(self):
        return deref(self.options).enable_dictionary()

    def set_columns_use_bloom_filter(self, columns):
        deref(self.options).set_columns_use_bloom_filter(columns)

    def is_column_use_bloom_filter(self, column):
        return deref(self.options).is_column_use_bloom_filter(column)

    def get_columns_use_bloom_filter(self):
        return deref(self.options).columns_use_bloom_filter()

    def set_bloom_filter_fpp(self, fpp):
        deref(self.options).set_bloom_filter_fpp(fpp)

    def get_bloom_filter_fpp(self):
        return deref(self.options).bloom_filter_fpp()

    def get_bloom_filter_version(self):
        return bloom_filter_version_from_enum(
            deref(self.options).bloom_filter_version())


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
            self.reader = move(GetResultValue(
                ORCFileReader.Open(rd_handle, self.allocator)
            ))

    def metadata(self):
        """
        The arrow metadata for this file.

        Returns
        -------
        metadata : pyarrow.KeyValueMetadata
        """
        cdef:
            shared_ptr[const CKeyValueMetadata] sp_arrow_metadata

        with nogil:
            sp_arrow_metadata = GetResultValue(
                deref(self.reader).ReadMetadata()
            )

        return pyarrow_wrap_metadata(sp_arrow_metadata)

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
            sp_arrow_schema = GetResultValue(deref(self.reader).ReadSchema())

        return pyarrow_wrap_schema(sp_arrow_schema)

    def nrows(self):
        return deref(self.reader).NumberOfRows()

    def nstripes(self):
        return deref(self.reader).NumberOfStripes()

    def read_stripe(self, n, columns=None):
        cdef:
            shared_ptr[CRecordBatch] sp_record_batch
            RecordBatch batch
            int64_t stripe
            std_vector[c_string] c_names

        stripe = n

        if columns is None:
            with nogil:
                sp_record_batch = GetResultValue(
                    deref(self.reader).ReadStripe(stripe)
                )
        else:
            c_names = [tobytes(name) for name in columns]
            with nogil:
                sp_record_batch = GetResultValue(
                    deref(self.reader).ReadStripe(stripe, c_names)
                )

        return pyarrow_wrap_batch(sp_record_batch)

    def read(self, columns=None):
        cdef:
            shared_ptr[CTable] sp_table
            std_vector[c_string] c_names

        if columns is None:
            with nogil:
                sp_table = GetResultValue(deref(self.reader).Read())
        else:
            c_names = [tobytes(name) for name in columns]
            with nogil:
                sp_table = GetResultValue(deref(self.reader).Read(c_names))

        return pyarrow_wrap_table(sp_table)

cdef class ORCWriter(_Weakrefable):
    cdef:
        object source
        unique_ptr[ORCFileWriter] writer
        shared_ptr[COutputStream] rd_handle

    def open(self, object source):
        self.source = source
        get_writer(source, &self.rd_handle)
        with nogil:
            self.writer = move(GetResultValue[unique_ptr[ORCFileWriter]](
                ORCFileWriter.Open(self.rd_handle.get())))

    def write(self, Table table):
        cdef:
            shared_ptr[CTable] sp_table
        sp_table = pyarrow_unwrap_table(table)
        with nogil:
            check_status(deref(self.writer).Write(deref(sp_table)))

    def close(self):
        with nogil:
            check_status(deref(self.writer).Close())
