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

cdef extern from "arrow/adapters/orc/adapter_common.h" \
        namespace "arrow::adapters::orc" nogil:
    enum CompressionKind" arrow::adapters::orc::CompressionKind":
        _CompressionKind_NONE" arrow::adapters::orc::CompressionKind::CompressionKind_NONE"
        _CompressionKind_ZLIB" arrow::adapters::orc::CompressionKind::CompressionKind_ZLIB"
        _CompressionKind_SNAPPY" arrow::adapters::orc::CompressionKind::CompressionKind_SNAPPY"
        _CompressionKind_LZO" arrow::adapters::orc::CompressionKind::CompressionKind_LZO"
        _CompressionKind_LZ4" arrow::adapters::orc::CompressionKind::CompressionKind_LZ4"
        _CompressionKind_ZSTD" arrow::adapters::orc::CompressionKind::CompressionKind_ZSTD"
        _CompressionKind_MAX" arrow::adapters::orc::CompressionKind::CompressionKind_MAX"

    enum CompressionStrategy" arrow::adapters::orc::CompressionStrategy":
        _CompressionStrategy_SPEED" arrow::adapters::orc::CompressionStrategy::CompressionStrategy_SPEED"
        _CompressionStrategy_COMPRESSION" arrow::adapters::orc::CompressionStrategy::CompressionStrategy_COMPRESSION"

    enum RleVersion" arrow::adapters::orc::RleVersion":
        _RleVersion_1" arrow::adapters::orc::RleVersion::RleVersion_1"
        _RleVersion_2" arrow::adapters::orc::RleVersion::RleVersion_2"

    enum BloomFilterVersion" arrow::adapters::orc::BloomFilterVersion":
        _BloomFilterVersion_ORIGINAL" arrow::adapters::orc::BloomFilterVersion::ORIGINAL"
        _BloomFilterVersion_UTF8" arrow::adapters::orc::BloomFilterVersion::UTF8"
        _BloomFilterVersion_FUTURE" arrow::adapters::orc::BloomFilterVersion::FUTURE"

    enum WriterId" arrow::adapters::orc::WriterId":
        _WriterId_ORC_JAVA_WRITER" arrow::adapters::orc::WriterId::ORC_JAVA_WRITER"
        _WriterId_ORC_CPP_WRITER" arrow::adapters::orc::WriterId::ORC_CPP_WRITER"
        _WriterId_PRESTO_WRITER" arrow::adapters::orc::WriterId::PRESTO_WRITER"
        _WriterId_SCRITCHLEY_GO" arrow::adapters::orc::WriterId::SCRITCHLEY_GO"
        _WriterId_TRINO_WRITER" arrow::adapters::orc::WriterId::TRINO_WRITER"
        _WriterId_UNKNOWN_WRITER" arrow::adapters::orc::WriterId::UNKNOWN_WRITER"

    enum WriterVersion" arrow::adapters::orc::WriterVersion":
        _WriterVersion_ORIGINAL" arrow::adapters::orc::WriterVersion::WriterVersion_ORIGINAL"
        _WriterVersion_HIVE_8732" arrow::adapters::orc::WriterVersion::WriterVersion_HIVE_8732"
        _WriterVersion_HIVE_4243" arrow::adapters::orc::WriterVersion::WriterVersion_HIVE_4243"
        _WriterVersion_HIVE_12055" arrow::adapters::orc::WriterVersion::WriterVersion_HIVE_12055"
        _WriterVersion_HIVE_13083" arrow::adapters::orc::WriterVersion::WriterVersion_HIVE_13083"
        _WriterVersion_ORC_101" arrow::adapters::orc::WriterVersion::WriterVersion_ORC_101"
        _WriterVersion_ORC_135" arrow::adapters::orc::WriterVersion::WriterVersion_ORC_135"
        _WriterVersion_ORC_517" arrow::adapters::orc::WriterVersion::WriterVersion_ORC_517"
        _WriterVersion_ORC_203" arrow::adapters::orc::WriterVersion::WriterVersion_ORC_203"
        _WriterVersion_ORC_14" arrow::adapters::orc::WriterVersion::WriterVersion_ORC_14"
        _WriterVersion_MAX" arrow::adapters::orc::WriterVersion::WriterVersion_MAX"

    cdef cppclass FileVersion" arrow::adapters::orc::FileVersion":
        FileVersion(uint32_t major, uint32_t minor)
        uint32_t major()
        uint32_t minor()
        c_string ToString()


cdef extern from "arrow/adapters/orc/adapter_options.h" \
        namespace "arrow::adapters::orc" nogil:
    cdef cppclass WriteOptions" arrow::adapters::orc::WriteOptions":
        WriteOptions()
        WriteOptions& SetBatchSize(uint64_t size)
        uint64_t GetBatchSize()
        WriteOptions& SetStripeSize(uint64_t size)
        uint64_t GetStripeSize()
        WriteOptions& SetCompressionBlockSize(uint64_t size)
        uint64_t GetCompressionBlockSize()
        WriteOptions& SetRowIndexStride(uint64_t stride)
        uint64_t GetRowIndexStride()
        WriteOptions& SetDictionaryKeySizeThreshold(double val)
        double GetDictionaryKeySizeThreshold()
        WriteOptions& SetFileVersion(const FileVersion& version)
        FileVersion GetFileVersion()
        WriteOptions& SetCompression(CompressionKind comp)
        CompressionKind GetCompression()
        WriteOptions& SetCompressionStrategy(CompressionStrategy strategy)
        CompressionStrategy GetCompressionStrategy()
        c_bool GetAlignedBitpacking()
        WriteOptions& SetPaddingTolerance(double tolerance)
        double GetPaddingTolerance()
        RleVersion GetRleVersion()
        c_bool GetEnableIndex()
        c_bool GetEnableDictionary()
        WriteOptions& SetColumnsUseBloomFilter(const std_set[uint64_t]& columns)
        c_bool IsColumnUseBloomFilter(uint64_t column)
        WriteOptions& SetBloomFilterFpp(double fpp)
        double GetBloomFilterFpp()
        BloomFilterVersion GetBloomFilterVersion()


cdef extern from "arrow/adapters/orc/adapter.h" \
        namespace "arrow::adapters::orc" nogil:

    cdef cppclass ORCFileReader:
        @staticmethod
        CResult[unique_ptr[ORCFileReader]] Open(
            const shared_ptr[CRandomAccessFile]& file,
            CMemoryPool* pool)

        CResult[shared_ptr[const CKeyValueMetadata]] ReadMetadata()

        CResult[shared_ptr[CSchema]] ReadSchema()

        CResult[shared_ptr[CRecordBatch]] ReadStripe(int64_t stripe)
        CResult[shared_ptr[CRecordBatch]] ReadStripe(
            int64_t stripe, std_vector[c_string])

        CResult[shared_ptr[CTable]] Read()
        CResult[shared_ptr[CTable]] Read(std_vector[c_string])

        int64_t NumberOfStripes()

        int64_t NumberOfRows()

        FileVersion GetFileVersion()

        c_string GetSoftwareVersion()

        CompressionKind GetCompression()

        uint64_t GetCompressionSize()

        uint64_t GetRowIndexStride()

        WriterId GetWriterId()

        uint32_t GetWriterIdValue()

        WriterVersion GetWriterVersion()

        uint64_t GetNumberOfStripeStatistics()

        uint64_t GetContentLength()

        uint64_t GetStripeStatisticsLength()

        uint64_t GetFileFooterLength()

        uint64_t GetFilePostscriptLength()

        uint64_t GetFileLength()

        c_string GetSerializedFileTail()

    cdef cppclass ORCFileWriter:
        @staticmethod
        CResult[unique_ptr[ORCFileWriter]] Open(
            COutputStream* output_stream, const WriteOptions& writer_options)

        CStatus Write(const CTable& table)

        CStatus Close()
