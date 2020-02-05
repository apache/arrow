Parquet C++ 1.5.0
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-979] - [C++] Limit size of min, max or disable stats for long binary types
    * [PARQUET-1071] - [C++] parquet::arrow::FileWriter::Close is not idempotent
    * [PARQUET-1349] - [C++] PARQUET_RPATH_ORIGIN is not picked by the build
    * [PARQUET-1334] - [C++] memory_map parameter seems missleading in parquet file opener
    * [PARQUET-1333] - [C++] Reading of files with dictionary size 0 fails on Windows with bad_alloc
    * [PARQUET-1283] - [C++] FormatStatValue appends trailing space to string and int96
    * [PARQUET-1270] - [C++] Executable tools do not get installed
    * [PARQUET-1272] - [C++] ScanFileContents reports wrong row count for nested columns
    * [PARQUET-1268] - [C++] Conversion of Arrow null list columns fails
    * [PARQUET-1255] - [C++] Exceptions thrown in some tests
    * [PARQUET-1358] - [C++] index_page_offset should be unset as it is not supported.
    * [PARQUET-1357] - [C++] FormatStatValue truncates binary statistics on zero character
    * [PARQUET-1319] - [C++] Pass BISON_EXECUTABLE to Thrift EP for MacOS
    * [PARQUET-1313] - [C++] Compilation failure with VS2017
    * [PARQUET-1315] - [C++] ColumnChunkMetaData.has_dictionary_page() should return bool, not int64_t
    * [PARQUET-1307] - [C++] memory-test fails with latest Arrow
    * [PARQUET-1274] - [Python] SegFault in pyarrow.parquet.write_table with specific options
    * [PARQUET-1209] - locally defined symbol ... imported in function ..
    * [PARQUET-1245] - [C++] Segfault when writing Arrow table with duplicate columns
    * [PARQUET-1273] - [Python] Error writing to partitioned Parquet dataset
    * [PARQUET-1384] - [C++] Clang compiler warnings in bloom_filter-test.cc

## Improvement
    * [PARQUET-1348] - [C++] Allow Arrow FileWriter To Write FileMetaData
    * [PARQUET-1346] - [C++] Protect against null values data in empty Arrow array
    * [PARQUET-1340] - [C++] Fix Travis Ci valgrind errors related to std::random_device
    * [PARQUET-1323] - [C++] Fix compiler warnings with clang-6.0
    * [PARQUET-1279] - Use ASSERT_NO_FATAIL_FAILURE in C++ unit tests
    * [PARQUET-1262] - [C++] Use the same BOOST_ROOT and Boost_NAMESPACE for Thrift
    * [PARQUET-1267] - replace "unsafe" std::equal by std::memcmp
    * [PARQUET-1360] - [C++] Minor API + style changes follow up to PARQUET-1348
    * [PARQUET-1166] - [API Proposal] Add GetRecordBatchReader in parquet/arrow/reader.h
    * [PARQUET-1378] - [c++] Allow RowGroups with zero rows to be written
    * [PARQUET-1256] - [C++] Add --print-key-value-metadata option to parquet_reader tool
    * [PARQUET-1276] - [C++] Reduce the amount of memory used for writing null decimal values

## New Feature
    * [PARQUET-1392] - [C++] Supply row group indices to parquet::arrow::FileReader::ReadTable

## Sub-task
    * [PARQUET-1227] - Thrift crypto metadata structures
    * [PARQUET-1332] - [C++] Add bloom filter utility class

## Task
    * [PARQUET-1350] - [C++] Use abstract ResizableBuffer instead of concrete PoolBuffer
    * [PARQUET-1366] - [C++] Streamline use of Arrow bit-util.h
    * [PARQUET-1308] - [C++] parquet::arrow should use thread pool, not ParallelFor
    * [PARQUET-1382] - [C++] Prepare for arrow::test namespace removal
    * [PARQUET-1372] - [C++] Add an API to allow writing RowGroups based on their size rather than num_rows


Parquet C++ 1.4.0
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-1193] - [CPP] Implement ColumnOrder to support min_value and max_value
    * [PARQUET-1180] - C++: Fix behaviour of num_children element of primitive nodes
    * [PARQUET-1146] - C++: Add macOS-compatible sha512sum call to release verify script
    * [PARQUET-1167] - [C++] FieldToNode function should return a status when throwing an exception
    * [PARQUET-1175] - [C++] Fix usage of deprecated Arrow API
    * [PARQUET-1113] - [C++] Incorporate fix from ARROW-1601 on bitmap read path
    * [PARQUET-1111] - dev/release/verify-release-candidate has stale help
    * [PARQUET-1109] - C++: Update release verification script to SHA512
    * [PARQUET-1179] - [C++] Support Apache Thrift 0.11
    * [PARQUET-1226] - [C++] Fix new build warnings with clang 5.0
    * [PARQUET-1233] - [CPP ]Enable option to switch between stl classes and boost classes for thrift header
    * [PARQUET-1205] - Fix msvc static build
    * [PARQUET-1210] - [C++] Boost 1.66 compilation fails on Windows on linkage stage

## Improvement
    * [PARQUET-1092] - [C++] Write Arrow tables with chunked columns
    * [PARQUET-1086] - [C++] Remove usage of arrow/util/compiler-util.h after 1.3.0 release
    * [PARQUET-1097] - [C++] Account for Arrow API deprecation in ARROW-1511
    * [PARQUET-1150] - C++: Hide statically linked boost symbols
    * [PARQUET-1151] - [C++] Add build options / configuration to use static runtime libraries with MSVC
    * [PARQUET-1147] - [C++] Account for API deprecation / change in ARROW-1671
    * [PARQUET-1162] - C++: Update dev/README after migration to Gitbox
    * [PARQUET-1165] - [C++] Pin clang-format version to 4.0
    * [PARQUET-1164] - [C++] Follow API changes in ARROW-1808
    * [PARQUET-1177] - [C++] Add more extensive compiler warnings when using Clang
    * [PARQUET-1110] - [C++] Release verification script for Windows
    * [PARQUET-859] - [C++] Flatten parquet/file directory
    * [PARQUET-1220] - [C++] Don't build Thrift examples and tutorials in the ExternalProject
    * [PARQUET-1219] - [C++] Update release-candidate script links to gitbox
    * [PARQUET-1196] - [C++] Provide a parquet_arrow example project incl. CMake setup
    * [PARQUET-1200] - [C++] Support reading a single Arrow column from a Parquet file

## New Feature
    * [PARQUET-1095] - [C++] Read and write Arrow decimal values
    * [PARQUET-970] - Add Add Lz4 and Zstd compression codecs

## Task
    * [PARQUET-1221] - [C++] Extend release README
    * [PARQUET-1225] - NaN values may lead to incorrect filtering under certain circumstances


Parquet C++ 1.3.1
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-1105] - [CPP] Remove libboost_system dependency
    * [PARQUET-1138] - [C++] Fix compilation with Arrow 0.7.1
    * [PARQUET-1123] - [C++] Update parquet-cpp to use Arrow's AssertArraysEqual
    * [PARQUET-1121] - C++: DictionaryArrays of NullType cannot be written
    * [PARQUET-1139] - Add license to cmake_modules/parquet-cppConfig.cmake.in

## Improvement
    * [PARQUET-1140] - [C++] Fail on RAT errors in CI
    * [PARQUET-1070] - Add CPack support to the build


Parquet C++ 1.3.0
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-1098] - [C++] Install new header in parquet/util
    * [PARQUET-1085] - [C++] Backwards compatibility from macro cleanup in transitive dependencies in ARROW-1452
    * [PARQUET-1074] - [C++] Switch to long key ids in KEYs file
    * [PARQUET-1075] - C++: Coverage upload is broken
    * [PARQUET-1088] - [CPP] remove parquet_version.h from version control since it gets auto generated
    * [PARQUET-1002] - [C++] Compute statistics based on Logical Types
    * [PARQUET-1100] - [C++] Reading repeated types should decode number of records rather than number of values
    * [PARQUET-1090] - [C++] Fix int32 overflow in Arrow table writer, add max row group size property
    * [PARQUET-1108] - [C++] Fix Int96 comparators

## Improvement
    * [PARQUET-1104] - [C++] Upgrade to Apache Arrow 0.7.0 RC0
    * [PARQUET-1072] - [C++] Add ARROW_NO_DEPRECATED_API to CI to check for deprecated API use
    * [PARQUET-1096] - C++: Update sha{1, 256, 512} checksums per latest ASF release policy
    * [PARQUET-1079] - [C++] Account for Arrow API change in ARROW-1335
    * [PARQUET-1087] - [C++] Add wrapper for ScanFileContents in parquet::arrow that catches exceptions
    * [PARQUET-1093] - C++: Improve Arrow level generation error message
    * [PARQUET-1094] - C++: Add benchmark for boolean Arrow column I/O
    * [PARQUET-1083] - [C++] Refactor core logic in parquet-scan.cc so that it can be used as a library function for benchmarking
    * [PARQUET-1037] - Allow final RowGroup to be unfilled

## New Feature
    * [PARQUET-1078] - [C++] Add Arrow writer option to coerce timestamps to milliseconds or microseconds
    * [PARQUET-929] - [C++] Handle arrow::DictionaryArray when writing Arrow data


Parquet C++ 1.2.0
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-1029] - [C++] TypedColumnReader/TypeColumnWriter symbols are no longer being exported
    * [PARQUET-997] - Fix override compiler warnings
    * [PARQUET-1033] - Mismatched Read and Write
    * [PARQUET-1007] - [C++ ] Update parquet.thrift from https://github.com/apache/parquet-format
    * [PARQUET-1039] - PARQUET-911 Breaks Arrow
    * [PARQUET-1038] - Key value metadata should be nullptr if not set
    * [PARQUET-1018] - [C++] parquet.dll has runtime dependencies on one or more libraries in the build toolchain
    * [PARQUET-1003] - [C++] Modify DEFAULT_CREATED_BY value for every new release version
    * [PARQUET-1004] - CPP Building fails on windows
    * [PARQUET-1040] - Missing writer method implementations
    * [PARQUET-1054] - [C++] Account for Arrow API changes in ARROW-1199
    * [PARQUET-1042] - C++: Compilation breaks on GCC 4.8
    * [PARQUET-1048] - [C++] Static linking of libarrow is no longer supported
    * [PARQUET-1013] - Fix ZLIB_INCLUDE_DIR
    * [PARQUET-998] - C++: Release script is not usable
    * [PARQUET-1023] - [C++] Brotli libraries are not being statically linked on Windows
    * [PARQUET-1000] - [C++] Do not build thirdparty Arrow with /WX on MSVC
    * [PARQUET-1052] - [C++] add_compiler_export_flags() throws warning with CMake >= 3.3
    * [PARQUET-1069] - C++: ./dev/release/verify-release-candidate is broken due to missing Arrow dependencies

## Improvement
    * [PARQUET-996] - Improve MSVC build - ThirdpartyToolchain - Arrow
    * [PARQUET-911] - C++: Support nested structs in parquet_arrow
    * [PARQUET-986] - Improve MSVC build - ThirdpartyToolchain - Thrift
    * [PARQUET-864] - [C++] Consolidate non-Parquet-specific bit utility code into Apache Arrow
    * [PARQUET-1043] - [C++] Raise minimum supported CMake version to 3.2
    * [PARQUET-1016] - Upgrade thirdparty Arrow to 0.4.0
    * [PARQUET-858] - [C++] Flatten parquet/column directory, consolidate related code
    * [PARQUET-978] - [C++] Minimizing footer reads for small(ish) metadata
    * [PARQUET-991] - [C++] Fix compiler warnings on MSVC and build with /WX in Appveyor
    * [PARQUET-863] - [C++] Move SIMD, CPU info, hashing, and other generic utilities into Apache Arrow
    * [PARQUET-1053] - Fix unused result warnings due to unchecked Statuses
    * [PARQUET-1067] - C++: Update arrow hash to 0.5.0
    * [PARQUET-1041] - C++: Support Arrow's NullArray
    * [PARQUET-1008] - Update TypedColumnReader::ReadBatch method to accept batch_size as int64_t
    * [PARQUET-1044] - [C++] Use compression libraries from Apache Arrow
    * [PARQUET-999] - Improve MSVC build - Enable PARQUET_BUILD_BENCHMARKS
    * [PARQUET-967] - [C++] Combine libparquet/libparquet_arrow libraries
    * [PARQUET-1045] - [C++] Refactor to account for computational utility code migration in ARROW-1154

## New Feature
    * [PARQUET-1035] - Write Int96 from Arrow Timestamp(ns)

## Task
    * [PARQUET-994] - C++: release-candidate script should not push to master
    * [PARQUET-902] - [C++] Move compressor interfaces into Apache Arrow

## Test
    * [PARQUET-706] - [C++] Create test case that uses libparquet as a 3rd party library


Parquet C++ 1.1.0
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-898] - [C++] Change Travis CI OS X image to Xcode 6.4 and fix our thirdparty build
    * [PARQUET-976] - [C++] Pass unit test suite with MSVC, build in Appveyor
    * [PARQUET-963] - [C++] Disallow reading struct types in Arrow reader for now
    * [PARQUET-959] - [C++] Arrow thirdparty build fails on multiarch systems
    * [PARQUET-962] - [C++] GTEST_MAIN_STATIC_LIB is not defined in FindGTest.cmake
    * [PARQUET-958] - [C++] Print Parquet metadata in JSON format
    * [PARQUET-956] - C++: BUILD_BYPRODUCTS not specified anymore for gtest
    * [PARQUET-948] - [C++] Account for API changes in ARROW-782
    * [PARQUET-947] - [C++] Refactor to account for ARROW-795 Arrow core library consolidation
    * [PARQUET-965] - [C++] FIXED_LEN_BYTE_ARRAY types are unhandled in the Arrow reader
    * [PARQUET-949] - [C++] Arrow version pinning seems to not be working properly
    * [PARQUET-955] - [C++] pkg_check_modules will override $ARROW_HOME if it is set in the environment
    * [PARQUET-945] - [C++] Thrift static libraries are not used with recent patch
    * [PARQUET-943] - [C++] Overflow build error on x86
    * [PARQUET-938] - [C++] There is a typo in cmake_modules/FindSnappy.cmake comment
    * [PARQUET-936] - [C++] parquet::arrow::WriteTable can enter infinite loop if chunk_size is 0
    * [PARQUET-981] - Repair usage of *_HOME 3rd party dependencies environment variables during Windows build
    * [PARQUET-992] - [C++] parquet/compression.h leaks zlib.h
    * [PARQUET-987] - [C++] Fix regressions caused by PARQUET-981
    * [PARQUET-933] - [C++] Account for Arrow Table API changes coming in ARROW-728
    * [PARQUET-915] - Support Arrow Time Types in Schema
    * [PARQUET-914] - [C++] Throw more informative exception when user writes too many values to a column in a row group
    * [PARQUET-923] - [C++] Account for Time metadata changes in ARROW-686
    * [PARQUET-918] - FromParquetSchema API crashes on nested schemas
    * [PARQUET-925] - [C++] FindArrow.cmake sets the wrong library path after ARROW-648
    * [PARQUET-932] - [c++] Add option to build parquet library with minimal dependency
    * [PARQUET-919] - [C++] Account for API changes in ARROW-683
    * [PARQUET-995] - [C++] Int96 reader in parquet_arrow uses size of Int96Type instead of Int96

## Improvement
    * [PARQUET-508] - Add ParquetFilePrinter
    * [PARQUET-595] - Add API for key-value metadata
    * [PARQUET-897] - [C++] Only use designated public headers from libarrow
    * [PARQUET-679] - [C++] Build and unit tests support for MSVC on Windows
    * [PARQUET-977] - Improve MSVC build
    * [PARQUET-957] - [C++] Add optional $PARQUET_BUILD_TOOLCHAIN environment variable option for configuring build environment
    * [PARQUET-961] - [C++] Strip debug symbols from libparquet libraries in release builds by default
    * [PARQUET-954] - C++: Use Brolti 0.6 release
    * [PARQUET-953] - [C++] Change arrow::FileWriter API to be initialized from a Schema, and provide for writing multiple tables
    * [PARQUET-941] - [C++] Stop needless Boost static library detection for CentOS 7 support
    * [PARQUET-942] - [C++] Fix wrong variabe use in FindSnappy
    * [PARQUET-939] - [C++] Support Thrift_HOME CMake variable like FindSnappy does as Snappy_HOME
    * [PARQUET-940] - [C++] Fix Arrow library path detection
    * [PARQUET-937] - [C++] Support CMake < 3.4 again for Arrow detection
    * [PARQUET-935] - [C++] Set shared library version for .deb packages
    * [PARQUET-934] - [C++] Support multiarch on Debian
    * [PARQUET-984] - C++: Add abi and so version to pkg-config
    * [PARQUET-983] - C++: Update Thirdparty hash to Arrow 0.3.0
    * [PARQUET-989] - [C++] Link dynamically to libarrow in toolchain build, set LD_LIBRARY_PATH
    * [PARQUET-988] - [C++] Add Linux toolchain-based build to Travis CI
    * [PARQUET-928] - [C++] Support pkg-config
    * [PARQUET-927] - [C++] Specify shared library version of Apache Arrow
    * [PARQUET-931] - [C++] Add option to pin thirdparty Arrow version used in ExternalProject
    * [PARQUET-926] - [C++] Use pkg-config to find Apache Arrow
    * [PARQUET-917] - C++: Build parquet_arrow by default
    * [PARQUET-910] - C++: Support TIME logical type in parquet_arrow
    * [PARQUET-909] - [CPP]: Reduce buffer allocations (mallocs) on critical path

## New Feature
    * [PARQUET-853] - [C++] Add option to link with shared boost libraries when building Arrow in the thirdparty toolchain
    * [PARQUET-946] - [C++] Refactoring in parquet::arrow::FileReader to be able to read a single row group
    * [PARQUET-930] - [C++] Account for all Arrow date/time types


Parquet C++ 1.0.0
--------------------------------------------------------------------------------
## Bug
    * [PARQUET-455] - Fix compiler warnings on OS X / Clang
    * [PARQUET-558] - Support ZSH in build scripts
    * [PARQUET-720] - Parquet-cpp fails to link when included in multiple TUs
    * [PARQUET-718] - Reading boolean pages written by parquet-cpp fails
    * [PARQUET-640] - [C++] Force the use of gcc 4.9 in conda builds
    * [PARQUET-643] - Add const modifier to schema pointer reference in ParquetFileWriter
    * [PARQUET-672] - [C++] Build testing conda artifacts in debug mode
    * [PARQUET-661] - [C++] Do not assume that perl is found in /usr/bin
    * [PARQUET-659] - [C++] Instantiated template visibility is broken on clang / OS X
    * [PARQUET-657] - [C++] Don't define DISALLOW_COPY_AND_ASSIGN if already defined
    * [PARQUET-656] - [C++] Revert PARQUET-653
    * [PARQUET-676] - MAX_VALUES_PER_LITERAL_RUN causes RLE encoding failure
    * [PARQUET-614] - C++: Remove unneeded LZ4-related code
    * [PARQUET-604] - Install writer.h headers
    * [PARQUET-621] - C++: Uninitialised DecimalMetadata is read
    * [PARQUET-620] - C++: Duplicate calls to ParquetFileWriter::Close cause duplicate metdata writes
    * [PARQUET-599] - ColumnWriter::RleEncodeLevels' size estimation might be wrong
    * [PARQUET-617] - C++: Enable conda build to work on systems with non-default C++ toolchains
    * [PARQUET-627] - Ensure that thrift headers are generated before source compilation
    * [PARQUET-745] - TypedRowGroupStatistics fails to PlainDecode min and max in ByteArrayType
    * [PARQUET-738] - Update arrow version that also supports newer Xcode
    * [PARQUET-747] - [C++] TypedRowGroupStatistics are not being exported in libparquet.so
    * [PARQUET-711] - Use metadata builders in parquet writer
    * [PARQUET-732] - Building a subset of dependencies does not work
    * [PARQUET-760] - On switching from dictionary to the fallback encoding, an incorrect encoding is set
    * [PARQUET-691] - [C++] Write ColumnChunk metadata after each column chunk in the file
    * [PARQUET-797] - [C++] Update for API changes in ARROW-418
    * [PARQUET-837] - [C++] SerializedFile::ParseMetaData uses Seek, followed by Read, and could have race conditions
    * [PARQUET-827] - [C++] Incorporate addition of arrow::MemoryPool::Reallocate
    * [PARQUET-502] - Scanner segfaults when its batch size is smaller than the number of rows
    * [PARQUET-469] - Roll back Thrift bindings to 0.9.0
    * [PARQUET-889] - Fix compilation when PARQUET_USE_SSE is on
    * [PARQUET-888] - C++ Memory leak in RowGroupSerializer
    * [PARQUET-819] - C++: Trying to install non-existing parquet/arrow/utils.h
    * [PARQUET-736] - XCode 8.0 breaks builds
    * [PARQUET-505] - Column reader: automatically handle large data pages
    * [PARQUET-615] - C++: Building static or shared libparquet should not be mutually exclusive
    * [PARQUET-658] - ColumnReader has no virtual destructor
    * [PARQUET-799] - concurrent usage of the file reader API
    * [PARQUET-513] - Valgrind errors are not failing the Travis CI build
    * [PARQUET-841] - [C++] Writing wrong format version when using ParquetVersion::PARQUET_1_0
    * [PARQUET-742] - Add missing license headers
    * [PARQUET-741] - compression_buffer_ is reused although it shouldn't
    * [PARQUET-700] - C++: Disable dictionary encoding for boolean columns
    * [PARQUET-662] - [C++] ParquetException must be explicitly exported in dynamic libraries
    * [PARQUET-704] - [C++] scan-all.h is not being installed
    * [PARQUET-865] - C++: Pass all CXXFLAGS to Thrift ExternalProject
    * [PARQUET-875] - [C++] Fix coveralls build given changes to thirdparty build procedure
    * [PARQUET-709] - [C++] Fix conda dev binary builds
    * [PARQUET-638] - [C++] Revert static linking of libstdc++ in conda builds until symbol visibility addressed
    * [PARQUET-606] - Travis coverage is broken
    * [PARQUET-880] - [CPP] Prevent destructors from throwing
    * [PARQUET-886] - [C++] Revise build documentation and requirements in README.md
    * [PARQUET-900] - C++: Fix NOTICE / LICENSE issues
    * [PARQUET-885] - [C++] Do not search for Thrift in default system paths
    * [PARQUET-879] - C++: ExternalProject compilation for Thrift fails on older CMake versions
    * [PARQUET-635] - [C++] Statically link libstdc++ on Linux in conda recipe
    * [PARQUET-710] - Remove unneeded private member variables from RowGroupReader ABI
    * [PARQUET-766] - C++: Expose ParquetFileReader through Arrow reader as const
    * [PARQUET-876] - C++: Correct snapshot version
    * [PARQUET-821] - [C++] zlib download link is broken
    * [PARQUET-818] - [C++] Refactor library to share IO, Buffer, and memory management abstractions with Apache Arrow
    * [PARQUET-537] - LocalFileSource leaks resources
    * [PARQUET-764] - [CPP] Parquet Writer does not write Boolean values correctly
    * [PARQUET-812] - [C++] Failure reading BYTE_ARRAY data from file in parquet-compatibility project
    * [PARQUET-759] - Cannot store columns consisting of empty strings
    * [PARQUET-846] - [CPP] CpuInfo::Init() is not thread safe
    * [PARQUET-694] - C++: Revert default data page size back to 1M
    * [PARQUET-842] - [C++] Impala rejects DOUBLE columns if decimal metadata is set
    * [PARQUET-708] - [C++] RleEncoder does not account for "worst case scenario" in MaxBufferSize for bit_width > 1
    * [PARQUET-639] - Do not export DCHECK in public headers
    * [PARQUET-828] - [C++] "version" field set improperly in file metadata
    * [PARQUET-891] - [C++] Do not search for Snappy in default system paths
    * [PARQUET-626] - Fix builds due to unavailable llvm.org apt mirror
    * [PARQUET-629] - RowGroupSerializer should only close itself once
    * [PARQUET-472] - Clean up InputStream ownership semantics in ColumnReader
    * [PARQUET-739] - Rle-decoding uses static buffer that is shared accross threads
    * [PARQUET-561] - ParquetFileReader::Contents PIMPL missing a virtual destructor
    * [PARQUET-892] - [C++] Clean up link library targets in CMake files
    * [PARQUET-454] - Address inconsistencies in boolean decoding
    * [PARQUET-816] - [C++] Failure decoding sample dict-encoded file from parquet-compatibility project
    * [PARQUET-565] - Use PATH instead of DIRECTORY in get_filename_component to support CMake<2.8.12
    * [PARQUET-446] - Hide thrift dependency in parquet-cpp
    * [PARQUET-843] - [C++] Impala unable to read files created by parquet-cpp
    * [PARQUET-555] - Dictionary page metadata handling inconsistencies
    * [PARQUET-908] - Fix for PARQUET-890 introduces undefined symbol in libparquet_arrow.so
    * [PARQUET-793] - [CPP] Do not return incorrect statistics
    * [PARQUET-887] - C++: Fix issues in release scripts arise in RC1

## Improvement
    * [PARQUET-277] - Remove boost dependency
    * [PARQUET-500] - Enable coveralls.io for apache/parquet-cpp
    * [PARQUET-497] - Decouple Parquet physical file structure from FileReader class
    * [PARQUET-597] - Add data rates to benchmark output
    * [PARQUET-522] - #include cleanup with include-what-you-use
    * [PARQUET-515] - Add "Reset" to LevelEncoder and LevelDecoder
    * [PARQUET-514] - Automate coveralls.io updates in Travis CI
    * [PARQUET-551] - Handle compiler warnings due to disabled DCHECKs in release builds
    * [PARQUET-559] - Enable InputStream as a source to the ParquetFileReader
    * [PARQUET-562] - Simplified ZSH support in build scripts
    * [PARQUET-538] - Improve ColumnReader Tests
    * [PARQUET-541] - Portable build scripts
    * [PARQUET-724] - Test more advanced properties setting
    * [PARQUET-641] - Instantiate stringstream only if needed in SerializedPageReader::NextPage
    * [PARQUET-636] - Expose selection for different encodings
    * [PARQUET-603] - Implement missing information in schema descriptor
    * [PARQUET-610] - Print ColumnMetaData for each RowGroup
    * [PARQUET-600] - Add benchmarks for RLE-Level encoding
    * [PARQUET-592] - Support compressed writes
    * [PARQUET-593] - Add API for writing Page statistics
    * [PARQUET-589] - Implement Chunked InMemoryInputStream for better memory usage
    * [PARQUET-587] - Implement BufferReader::Read(int64_t,uint8_t*)
    * [PARQUET-616] - C++: WriteBatch should accept const arrays
    * [PARQUET-630] - C++: Support link flags for older CMake versions
    * [PARQUET-634] - Consistent private linking of dependencies
    * [PARQUET-633] - Add version to WriterProperties
    * [PARQUET-625] - Improve RLE read performance
    * [PARQUET-737] - Use absolute namespace in macros
    * [PARQUET-762] - C++: Use optimistic allocation instead of Arrow Builders
    * [PARQUET-773] - C++: Check licenses with RAT in CI
    * [PARQUET-687] - C++: Switch to PLAIN encoding if dictionary grows too large
    * [PARQUET-784] - C++: Reference Spark, Kudu and FrameOfReference in LICENSE
    * [PARQUET-809] - [C++] Add API to determine if two files' schemas are compatible
    * [PARQUET-778] - Standardize the schema output to match the parquet-mr format
    * [PARQUET-463] - Add DCHECK* macros for assertions in debug builds
    * [PARQUET-471] - Use the same environment setup script for Travis CI as local sandbox development
    * [PARQUET-449] - Update to latest parquet.thrift
    * [PARQUET-496] - Fix cpplint configuration to be more restrictive
    * [PARQUET-468] - Add a cmake option to generate the Parquet thrift headers with the thriftc in the environment
    * [PARQUET-482] - Organize src code file structure to have a very clear folder with public headers.
    * [PARQUET-591] - Page size estimation during writes
    * [PARQUET-518] - Review usages of size_t and unsigned integers generally per Google style guide
    * [PARQUET-533] - Simplify RandomAccessSource API to combine Seek/Read
    * [PARQUET-767] - Add release scripts for parquet-cpp
    * [PARQUET-699] - Update parquet.thrift from https://github.com/apache/parquet-format
    * [PARQUET-653] - [C++] Re-enable -static-libstdc++ in dev artifact builds
    * [PARQUET-763] - C++: Expose ParquetFileReader through Arrow reader
    * [PARQUET-857] - [C++] Flatten parquet/encodings directory
    * [PARQUET-862] - Provide defaut cache size values if CPU info probing is not available
    * [PARQUET-689] - C++: Compress DataPages eagerly
    * [PARQUET-874] - [C++] Use default memory allocator from Arrow
    * [PARQUET-267] - Detach thirdparty code from build configuration.
    * [PARQUET-418] - Add a utility to print contents of a Parquet file to stdout
    * [PARQUET-519] - Disable compiler warning supressions and fix all DEBUG build warnings
    * [PARQUET-447] - Add Debug and Release build types and associated compiler flags
    * [PARQUET-868] - C++: Build snappy with optimizations
    * [PARQUET-894] - Fix compilation warning
    * [PARQUET-883] - C++: Support non-standard gcc version strings
    * [PARQUET-607] - Public Writer header
    * [PARQUET-731] - [CPP] Add API to return metadata size and Skip reading values
    * [PARQUET-628] - Link thrift privately
    * [PARQUET-877] - C++: Update Arrow Hash, update Version in metadata.
    * [PARQUET-547] - Refactor most templates to use DataType structs rather than the Type::type enum
    * [PARQUET-882] - [CPP] Improve Application Version parsing
    * [PARQUET-448] - Add cmake option to skip building the unit tests
    * [PARQUET-721] - Performance benchmarks for reading into Arrow structures
    * [PARQUET-820] - C++: Decoders should directly emit arrays with spacing for null entries
    * [PARQUET-813] - C++: Build dependencies using CMake External project
    * [PARQUET-488] - Add SSE-related cmake options to manage compiler flags
    * [PARQUET-564] - Add option to run unit tests with valgrind --tool=memcheck
    * [PARQUET-572] - Rename parquet_cpp namespace to parquet
    * [PARQUET-829] - C++: Make use of ARROW-469
    * [PARQUET-501] - Add an OutputStream abstraction (capable of memory allocation) for Encoder public API
    * [PARQUET-744] - Clarifications on build instructions
    * [PARQUET-520] - Add version of LocalFileSource that uses memory-mapping for zero-copy reads
    * [PARQUET-556] - Extend RowGroupStatistics to include "min" "max" statistics
    * [PARQUET-671] - Improve performance of RLE/bit-packed decoding in parquet-cpp
    * [PARQUET-681] - Add tool to scan a parquet file

## New Feature
    * [PARQUET-499] - Complete PlainEncoder implementation for all primitive types and test end to end
    * [PARQUET-439] - Conform all copyright headers to ASF requirements
    * [PARQUET-436] - Implement ParquetFileWriter class entry point for generating new Parquet files
    * [PARQUET-435] - Provide vectorized ColumnReader interface
    * [PARQUET-438] - Update RLE encoder/decoder modules from Impala upstream changes and adapt unit tests
    * [PARQUET-512] - Add optional google/benchmark 3rd-party dependency for performance testing
    * [PARQUET-566] - Add method to retrieve the full column path
    * [PARQUET-613] - C++: Add conda packaging recipe
    * [PARQUET-605] - Expose schema node in ColumnDescriptor
    * [PARQUET-619] - C++: Add OutputStream for local files
    * [PARQUET-583] - Implement Parquet to Thrift schema conversion
    * [PARQUET-582] - Conversion functions for Parquet enums to Thrift enums
    * [PARQUET-728] - [C++] Bring parquet::arrow up to date with API changes in arrow::io
    * [PARQUET-752] - [C++] Conform parquet_arrow to upstream API changes
    * [PARQUET-788] - [C++] Reference Impala / Apache Impala (incubating) in LICENSE
    * [PARQUET-808] - [C++] Add API to read file given externally-provided FileMetadata
    * [PARQUET-807] - [C++] Add API to read file metadata only from a file handle
    * [PARQUET-805] - C++: Read Int96 into Arrow Timestamp(ns)
    * [PARQUET-836] - [C++] Add column selection to parquet::arrow::FileReader
    * [PARQUET-835] - [C++] Add option to parquet::arrow to read columns in parallel using a thread pool
    * [PARQUET-830] - [C++] Add additional configuration options to parquet::arrow::OpenFIle
    * [PARQUET-769] - C++: Add support for Brotli Compression
    * [PARQUET-489] - Add visibility macros to be used for public and internal APIs of libparquet
    * [PARQUET-542] - Support memory allocation from external memory
    * [PARQUET-844] - [C++] Consolidate encodings, schema, and compression subdirectories into fewer files
    * [PARQUET-848] - [C++] Consolidate libparquet_thrift subcomponent
    * [PARQUET-646] - [C++] Enable easier 3rd-party toolchain clang builds on Linux
    * [PARQUET-598] - [C++] Test writing all primitive data types
    * [PARQUET-442] - Convert flat SchemaElement vector to implied nested schema data structure
    * [PARQUET-867] - [C++] Support writing sliced Arrow arrays
    * [PARQUET-456] - Add zlib codec support
    * [PARQUET-834] - C++: Support r/w of arrow::ListArray
    * [PARQUET-485] - Decouple data page delimiting from column reader / scanner classes, create test fixtures
    * [PARQUET-434] - Add a ParquetFileReader class to encapsulate some low-level details of interacting with Parquet files
    * [PARQUET-666] - PLAIN_DICTIONARY write support
    * [PARQUET-437] - Incorporate googletest thirdparty dependency and add cmake tools (ADD_PARQUET_TEST) to simplify adding new unit tests
    * [PARQUET-866] - [C++] Account for API changes in ARROW-33
    * [PARQUET-545] - Improve API to support Decimal type
    * [PARQUET-579] - Add API for writing Column statistics
    * [PARQUET-494] - Implement PLAIN_DICTIONARY encoding and decoding
    * [PARQUET-618] - C++: Automatically upload conda build artifacts on commits to master
    * [PARQUET-833] - C++: Provide API to write spaced arrays (e.g. Arrow)
    * [PARQUET-903] - C++: Add option to set RPATH to ORIGIN
    * [PARQUET-451] - Add a RowGroup reader interface class
    * [PARQUET-785] - C++: List conversion for Arrow Schemas
    * [PARQUET-712] - C++: Read into Arrow memory
    * [PARQUET-890] - C++: Support I/O of DATE columns in parquet_arrow
    * [PARQUET-782] - C++: Support writing to Arrow sinks
    * [PARQUET-849] - [C++] Upgrade default Thrift in thirdparty toolchain to 0.9.3 or 0.10
    * [PARQUET-573] - C++: Create a public API for reading and writing file metadata

## Task
    * [PARQUET-814] - C++: Remove Conda recipes
    * [PARQUET-503] - Re-enable parquet 2.0 encodings
    * [PARQUET-169] - Parquet-cpp: Implement support for bulk reading and writing repetition/definition levels.
    * [PARQUET-878] - C++: Remove setup_build_env from rc-verification script
    * [PARQUET-881] - C++: Update Arrow hash to 0.2.0-rc2
    * [PARQUET-771] - C++: Sync KEYS file
    * [PARQUET-901] - C++: Publish RCs in apache-parquet-VERSION in SVN

## Test
    * [PARQUET-525] - Test coverage for malformed file failure modes on the read path
    * [PARQUET-703] - [C++] Validate num_values metadata for columns with nulls
    * [PARQUET-507] - Improve runtime of rle-test.cc
    * [PARQUET-549] - Add scanner and column reader tests for dictionary data pages
    * [PARQUET-457] - Add compressed data page unit tests
