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
# cython: language_level = 3

from libcpp cimport bool as c_bool
from libc.string cimport const_char
from libcpp.vector cimport vector as std_vector
from libcpp.set cimport set as std_set
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport (CArray, CSchema, CStatus,
                                        CResult, CTable, CMemoryPool,
                                        CKeyValueMetadata,
                                        CRecordBatch,
                                        CTable,
                                        CRandomAccessFile, COutputStream,
                                        TimeUnit)

cdef extern from "arrow/adapters/orc/adapter_options.h" \
        namespace "arrow::adapters::orc" nogil:
    enum CompressionKind" arrow::adapters::orc::CompressionKind":
        _CompressionKind_NONE \
            " arrow::adapters::orc::CompressionKind::CompressionKind_NONE"
        _CompressionKind_ZLIB \
            " arrow::adapters::orc::CompressionKind::CompressionKind_ZLIB"
        _CompressionKind_SNAPPY \
            " arrow::adapters::orc::CompressionKind::CompressionKind_SNAPPY"
        _CompressionKind_LZO \
            " arrow::adapters::orc::CompressionKind::CompressionKind_LZO"
        _CompressionKind_LZ4 \
            " arrow::adapters::orc::CompressionKind::CompressionKind_LZ4"
        _CompressionKind_ZSTD \
            " arrow::adapters::orc::CompressionKind::CompressionKind_ZSTD"
        _CompressionKind_MAX \
            " arrow::adapters::orc::CompressionKind::CompressionKind_MAX"

    enum CompressionStrategy" arrow::adapters::orc::CompressionStrategy":
        _CompressionStrategy_SPEED \
            " arrow::adapters::orc::CompressionStrategy"\
            "::CompressionStrategy_SPEED"
        _CompressionStrategy_COMPRESSION \
            " arrow::adapters::orc::CompressionStrategy"\
            "::CompressionStrategy_COMPRESSION"

    enum RleVersion" arrow::adapters::orc::RleVersion":
        _RleVersion_1" arrow::adapters::orc::RleVersion::RleVersion_1"
        _RleVersion_2" arrow::adapters::orc::RleVersion::RleVersion_2"

    enum BloomFilterVersion" arrow::adapters::orc::BloomFilterVersion":
        _BloomFilterVersion_ORIGINAL \
            " arrow::adapters::orc::BloomFilterVersion::ORIGINAL"
        _BloomFilterVersion_UTF8 \
            " arrow::adapters::orc::BloomFilterVersion::UTF8"
        _BloomFilterVersion_FUTURE \
            " arrow::adapters::orc::BloomFilterVersion::FUTURE"

    cdef cppclass FileVersion" arrow::adapters::orc::FileVersion":
        FileVersion(uint32_t major, uint32_t minor)
        uint32_t major()
        uint32_t minor()
        c_string ToString()

    cdef cppclass WriterOptions" arrow::adapters::orc::WriterOptions":
        WriterOptions()
        WriterOptions& set_stripe_size(uint64_t size)
        uint64_t stripe_size()
        WriterOptions& set_compression_block_size(uint64_t size)
        uint64_t compression_block_size()
        WriterOptions& set_row_index_stride(uint64_t stride)
        uint64_t row_index_stride()
        WriterOptions& set_dictionary_key_size_threshold(double val)
        double dictionary_key_size_threshold()
        WriterOptions& set_file_version(const FileVersion& version)
        FileVersion file_version()
        WriterOptions& set_compression(CompressionKind comp)
        CompressionKind compression()
        WriterOptions& set_compression_strategy(CompressionStrategy strategy)
        CompressionStrategy compression_strategy()
        c_bool aligned_bitpacking()
        WriterOptions& set_padding_tolerance(double tolerance)
        double padding_tolerance()
        RleVersion rle_version()
        c_bool enable_index()
        c_bool enable_dictionary()
        WriterOptions& set_columns_use_bloom_filter(const std_set[uint64_t]& columns)
        c_bool is_column_use_bloom_filter(uint64_t column)
        std_set[uint64_t] columns_use_bloom_filter()
        WriterOptions& set_bloom_filter_fpp(double fpp)
        double bloom_filter_fpp()
        BloomFilterVersion bloom_filter_version()


cdef extern from "arrow/adapters/orc/adapter.h" \
        namespace "arrow::adapters::orc" nogil:

    cdef cppclass ORCFileReader:
        @staticmethod
        CStatus Open(const shared_ptr[CRandomAccessFile]& file,
                     CMemoryPool* pool,
                     unique_ptr[ORCFileReader]* reader)

        CStatus ReadSchema(shared_ptr[CSchema]* out)

        CStatus ReadStripe(int64_t stripe, shared_ptr[CRecordBatch]* out)
        CStatus ReadStripe(int64_t stripe, std_vector[int],
                           shared_ptr[CRecordBatch]* out)

        CStatus Read(shared_ptr[CTable]* out)
        CStatus Read(std_vector[int], shared_ptr[CTable]* out)

        int64_t NumberOfStripes()

        int64_t NumberOfRows()

    cdef cppclass ORCFileWriter:
        @staticmethod
        CResult[unique_ptr[ORCFileWriter]] Open(COutputStream* output_stream)

        CStatus Write(const CTable& table)

        CStatus Close()
