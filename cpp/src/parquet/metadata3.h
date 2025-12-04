// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "arrow/result.h"
#include "flatbuffers/flatbuffers.h"
#include "generated/parquet3_generated.h"
#include "generated/parquet_types.h"
#include "parquet/thrift_internal.h"

namespace parquet {

// Convert to flatbuffer representation of the footer metadata.
// Return false if the schema is not supported by the FlatBuffer format.
bool ToFlatbuffer(format::FileMetaData* md, std::string* flatbuf);

// The flatbuffer in `from` must be valid (such as one retured by `ToFlatbuffer`).
format::FileMetaData FromFlatbuffer(const format3::FileMetaData* md);


// Append/extract the flatbuffer from the footer as a thrift extension:
// https://github.com/apache/parquet-format/blob/master/BinaryProtocolExtensions.md.
//
// `flatbuf` is the flatbuffer representation of the footer metadata.
// `thrift` is the buffer containing the thrift representation of the footer metadata as its suffix.
//
// Returns the number of bytes added.
//
// The extension itself is as follows:
//
// +-------------------+------------+--------------------------------------+----------------+---------+--------------------------------+------+
// | compress(flatbuf) | compressor | crc(compress(flatbuf) .. compressor) | compressed_len | raw_len | crc(compressed_len .. raw_len) | UUID |
// +-------------------+------------+--------------------------------------+----------------+---------+--------------------------------+------+
//
// flatbuf: the flatbuffer representation of the footer metadata.
// compressor: the compression scheme applied to the flatbuf.
// compress(x): x compressed with the specified compressor.
// crc(x): the crc32 checksum of x.
// y .. x: concatenation of the bytes of y and x.
// UUID: a 16-byte unique identifier.
//
// All integers (lengths, crc) are stored in little-endian.

// Append a flatbuffer as an extended field to Thrift-serialized metadata.
// The flatbuffer is compressed with LZ4, packed with checksums and metadata,
// then appended as a Thrift binary field (ID 32767) followed by a stop field.
void AppendFlatbuffer(std::string flatbuffer, std::string* thrift);

// Extract flatbuffer from a Parquet file buffer.
// Returns the size of the flatbuffer if found (and writes to out_flatbuffer),
// returns 0 if no flatbuffer extension is present, or returns the required
// buffer size if the input buffer is too small.
::arrow::Result<size_t> ExtractFlatbuffer(std::shared_ptr<Buffer> buf, std::string* out_flatbuffer);

}  // using namespace parquet

