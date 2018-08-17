#!/usr/bin/env python
# Copyright 2015 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os


# For readability purposes we define the error codes and messages at the top of the
# file. New codes and messages must be added here. Old error messages MUST NEVER BE
# DELETED, but can be renamed. The tuple layout for a new entry is: error code enum name,
# numeric error code, format string of the message.
#
# TODO Add support for SQL Error Codes
#      https://msdn.microsoft.com/en-us/library/ms714687%28v=vs.85%29.aspx
error_codes = (
  ("OK", 0, ""),

  ("UNUSED", 1, "<UNUSED>"),

  ("GENERAL", 2, "$0"),

  ("CANCELLED", 3, "$0"),

  ("ANALYSIS_ERROR", 4, "$0"),

  ("NOT_IMPLEMENTED_ERROR", 5, "$0"),

  ("RUNTIME_ERROR", 6, "$0"),

  ("MEM_LIMIT_EXCEEDED", 7, "$0"),

  ("INTERNAL_ERROR", 8, "$0"),

  ("RECOVERABLE_ERROR", 9, "$0"),

  ("PARQUET_MULTIPLE_BLOCKS", 10,
   "Parquet files should not be split into multiple hdfs-blocks. file=$0"),

  ("PARQUET_COLUMN_METADATA_INVALID", 11,
   "Column metadata states there are $0 values, but read $1 values from column $2. "
   "file=$3"),

  ("PARQUET_HEADER_PAGE_SIZE_EXCEEDED", 12, "(unused)"),

  ("PARQUET_HEADER_EOF", 13,
    "ParquetScanner: reached EOF while deserializing data page header. file=$0"),

  ("PARQUET_GROUP_ROW_COUNT_ERROR", 14,
    "Metadata states that in group $0($1) there are $2 rows, but $3 rows were read."),

  ("PARQUET_GROUP_ROW_COUNT_OVERFLOW", 15, "(unused)"),

  ("PARQUET_MISSING_PRECISION", 16,
   "File '$0' column '$1' does not have the decimal precision set."),

  ("PARQUET_WRONG_PRECISION", 17,
    "File '$0' column '$1' has a precision that does not match the table metadata "
    " precision. File metadata precision: $2, table metadata precision: $3."),

  ("PARQUET_BAD_CONVERTED_TYPE", 18,
   "File '$0' column '$1' does not have converted type set to DECIMAL"),

  ("PARQUET_INCOMPATIBLE_DECIMAL", 19,
   "File '$0' column '$1' contains decimal data but the table metadata has type $2"),

  ("SEQUENCE_SCANNER_PARSE_ERROR", 20,
   "Problem parsing file $0 at $1$2"),

  ("SNAPPY_DECOMPRESS_INVALID_BLOCK_SIZE", 21,
   "Decompressor: block size is too big.  Data is likely corrupt. Size: $0"),

  ("SNAPPY_DECOMPRESS_INVALID_COMPRESSED_LENGTH", 22,
   "Decompressor: invalid compressed length.  Data is likely corrupt."),

  ("SNAPPY_DECOMPRESS_UNCOMPRESSED_LENGTH_FAILED", 23,
   "Snappy: GetUncompressedLength failed"),

  ("SNAPPY_DECOMPRESS_RAW_UNCOMPRESS_FAILED", 24,
   "SnappyBlock: RawUncompress failed"),

  ("SNAPPY_DECOMPRESS_DECOMPRESS_SIZE_INCORRECT", 25,
   "Snappy: Decompressed size is not correct."),

  ("HDFS_SCAN_NODE_UNKNOWN_DISK", 26, "Unknown disk id.  "
   "This will negatively affect performance. "
   "Check your hdfs settings to enable block location metadata."),

  ("FRAGMENT_EXECUTOR", 27, "Reserved resource size ($0) is larger than "
    "query mem limit ($1), and will be restricted to $1. Configure the reservation "
    "size by setting RM_INITIAL_MEM."),

  ("PARTITIONED_HASH_JOIN_MAX_PARTITION_DEPTH", 28,
   "Cannot perform join at hash join node with id $0."
   " The input data was partitioned the maximum number of $1 times."
   " This could mean there is significant skew in the data or the memory limit is"
   " set too low."),

  ("PARTITIONED_AGG_MAX_PARTITION_DEPTH", 29,
   "Cannot perform aggregation at hash aggregation node with id $0."
   " The input data was partitioned the maximum number of $1 times."
   " This could mean there is significant skew in the data or the memory limit is"
   " set too low."),

  ("MISSING_BUILTIN", 30, "Builtin '$0' with symbol '$1' does not exist. "
   "Verify that all your impalads are the same version."),

  ("RPC_GENERAL_ERROR", 31, "RPC Error: $0"),
  ("RPC_TIMEOUT", 32, "RPC timed out"),

  ("UDF_VERIFY_FAILED", 33,
   "Failed to verify function $0 from LLVM module $1, see log for more details."),

  ("PARQUET_CORRUPT_VALUE", 34, "File $0 corrupt. RLE level data bytes = $1"),

  ("AVRO_DECIMAL_RESOLUTION_ERROR", 35, "Column '$0' has conflicting Avro decimal types. "
   "Table schema $1: $2, file schema $1: $3"),

  ("AVRO_DECIMAL_METADATA_MISMATCH", 36, "Column '$0' has conflicting Avro decimal types. "
   "Declared $1: $2, $1 in table's Avro schema: $3"),

  ("AVRO_SCHEMA_RESOLUTION_ERROR", 37, "Unresolvable types for column '$0': "
   "table type: $1, file type: $2"),

  ("AVRO_SCHEMA_METADATA_MISMATCH", 38, "Unresolvable types for column '$0': "
   "declared column type: $1, table's Avro schema type: $2"),

  ("AVRO_UNSUPPORTED_DEFAULT_VALUE", 39, "Field $0 is missing from file and default "
   "values of type $1 are not yet supported."),

  ("AVRO_MISSING_FIELD", 40, "Inconsistent table metadata. Mismatch between column "
   "definition and Avro schema: cannot read field $0 because there are only $1 fields."),

  ("AVRO_MISSING_DEFAULT", 41,
   "Field $0 is missing from file and does not have a default value."),

  ("AVRO_NULLABILITY_MISMATCH", 42,
   "Field $0 is nullable in the file schema but not the table schema."),

  ("AVRO_NOT_A_RECORD", 43,
   "Inconsistent table metadata. Field $0 is not a record in the Avro schema."),

  ("PARQUET_DEF_LEVEL_ERROR", 44, "Could not read definition level, even though metadata"
   " states there are $0 values remaining in data page. file=$1"),

  ("PARQUET_NUM_COL_VALS_ERROR", 45, "Mismatched number of values in column index $0 "
   "($1 vs. $2). file=$3"),

  ("PARQUET_DICT_DECODE_FAILURE", 46, "Failed to decode dictionary-encoded value. "
   "file=$0"),

  ("SSL_PASSWORD_CMD_FAILED", 47,
   "SSL private-key password command ('$0') failed with error: $1"),

  ("SSL_CERTIFICATE_PATH_BLANK", 48, "The SSL certificate path is blank"),
  ("SSL_PRIVATE_KEY_PATH_BLANK", 49, "The SSL private key path is blank"),

  ("SSL_CERTIFICATE_NOT_FOUND", 50, "The SSL certificate file does not exist at path $0"),
  ("SSL_PRIVATE_KEY_NOT_FOUND", 51, "The SSL private key file does not exist at path $0"),

  ("SSL_SOCKET_CREATION_FAILED", 52, "SSL socket creation failed: $0"),

  ("MEM_ALLOC_FAILED", 53, "Memory allocation of $0 bytes failed"),

  ("PARQUET_REP_LEVEL_ERROR", 54, "Could not read repetition level, even though metadata"
   " states there are $0 values remaining in data page. file=$1"),

  ("PARQUET_UNRECOGNIZED_SCHEMA", 55, "File '$0' has an incompatible Parquet schema for "
   "column '$1'. Column type: $2, Parquet schema:\\n$3"),

  ("COLLECTION_ALLOC_FAILED", 56, "Failed to allocate buffer for collection '$0'."),

  ("TMP_DEVICE_BLACKLISTED", 57,
    "Temporary device for directory $0 is blacklisted from a previous error and cannot "
    "be used."),

  ("TMP_FILE_BLACKLISTED", 58,
    "Temporary file $0 is blacklisted from a previous error and cannot be expanded."),

  ("RPC_CLIENT_CONNECT_FAILURE", 59,
    "RPC client failed to connect: $0"),

  ("STALE_METADATA_FILE_TOO_SHORT", 60, "Metadata for file '$0' appears stale. "
   "Try running \\\"refresh $1\\\" to reload the file metadata."),

  ("PARQUET_BAD_VERSION_NUMBER", 61, "File '$0' has an invalid version number: $1\\n"
   "This could be due to stale metadata. Try running \\\"refresh $2\\\"."),

  ("SCANNER_INCOMPLETE_READ", 62, "Tried to read $0 bytes but could only read $1 bytes. "
   "This may indicate data file corruption. (file $2, byte offset: $3)"),

  ("SCANNER_INVALID_READ", 63, "Invalid read of $0 bytes. This may indicate data file "
   "corruption. (file $1, byte offset: $2)"),

  ("AVRO_BAD_VERSION_HEADER", 64, "File '$0' has an invalid version header: $1\\n"
   "Make sure the file is an Avro data file."),

  ("UDF_MEM_LIMIT_EXCEEDED", 65, "$0's allocations exceeded memory limits."),

  ("BTS_BLOCK_OVERFLOW", 66, "Cannot process row that is bigger than the IO size "
   "(row_size=$0, null_indicators_size=$1). To run this query, increase the IO size "
   "(--read_size option)."),

  ("COMPRESSED_FILE_MULTIPLE_BLOCKS", 67,
   "For better performance, snappy-, gzip-, and bzip-compressed files "
   "should not be split into multiple HDFS blocks. file=$0 offset $1"),

  ("COMPRESSED_FILE_BLOCK_CORRUPTED", 68,
   "$0 Data error, likely data corrupted in this block."),

  ("COMPRESSED_FILE_DECOMPRESSOR_ERROR", 69, "$0 Decompressor error at $1, code=$2"),

  ("COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS", 70,
   "Decompression failed to make progress, but end of input is not reached. "
   "File appears corrupted. file=$0"),

  ("COMPRESSED_FILE_TRUNCATED", 71,
   "Unexpected end of compressed file. File may be truncated. file=$0")
)

# Verifies the uniqueness of the error constants and numeric error codes.
# Numeric codes must start from 0, be in order and have no gaps
def check_duplicates(codes):
  constants = {}
  next_num_code = 0
  for row in codes:
    if row[0] in constants:
      print("Constant %s already used, please check definition of '%s'!" % \
            (row[0], constants[row[0]]))
      exit(1)
    if row[1] != next_num_code:
      print("Numeric error codes must start from 0, be in order, and not have any gaps: "
            "got %d, expected %d" % (row[1], next_num_code))
      exit(1)
    next_num_code += 1
    constants[row[0]] = row[2]

preamble = """
// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// THIS FILE IS AUTO GENERATED BY generated_error_codes.py DO NOT MODIFY
// IT BY HAND.
//

namespace cpp impala
namespace java com.cloudera.impala.thrift

"""
# The script will always generate the file, CMake will take care of running it only if
# necessary.
target_file = os.path.join(sys.argv[1], "ErrorCodes.thrift")

# Check uniqueness of error constants and numeric codes
check_duplicates(error_codes)

fid = open(target_file, "w+")
try:
  fid.write(preamble)
  fid.write("""\nenum TErrorCode {\n""")
  fid.write(",\n".join(map(lambda x: "  %s = %d" % (x[0], x[1]), error_codes)))
  fid.write("\n}")
  fid.write("\n")
  fid.write("const list<string> TErrorMessage = [\n")
  fid.write(",\n".join(map(lambda x: "  // %s\n  \"%s\"" %(x[0], x[2]), error_codes)))
  fid.write("\n]")
finally:
  fid.close()

print("%s created." % target_file)
