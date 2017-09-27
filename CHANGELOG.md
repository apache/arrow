<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Arrow 0.7.1 (27 September 2017)

## Bug

* ARROW-1497 - [Java] JsonFileReader doesn't set value count for some vectors
* ARROW-1500 - [C++] Result of ftruncate ignored in MemoryMappedFile::Create
* ARROW-1536 - [C++] Do not transitively depend on libboost\_system
* ARROW-1542 - [C++] Windows release verification script should not modify conda environment
* ARROW-1544 - [JS] Export Vector type definitions
* ARROW-1545 - Int64Builder should not need int64() as arg
* ARROW-1550 - [Python] Fix flaky test on Windows 
* ARROW-1554 - [Python] Document that pip wheels depend on MSVC14 runtime
* ARROW-1557 - [PYTHON] pyarrow.Table.from\_arrays doesn't validate names length
* ARROW-1591 - C++: Xcode 9 is not correctly detected
* ARROW-1595 - [Python] Fix package dependency issues causing build failures
* ARROW-1601 - [C++] READ\_NEXT\_BITSET reads one byte past the last byte on last iteration
* ARROW-1606 - Python: Windows wheels don't include .lib files.
* ARROW-1610 - C++/Python: Only call python-prefix if the default PYTHON\_LIBRARY is not present
* ARROW-1611 - Crash in BitmapReader when length is zero

## Improvement

* ARROW-1537 - [C++] Support building with full path install\_name on macOS
* ARROW-1546 - [GLib] Support GLib 2.40 again
* ARROW-1578 - [C++/Python] Run lint checks in Travis CI to fail for linting issues as early as possible
* ARROW-1608 - Support Release verification script on macOS
* ARROW-1612 - [GLib] add how to install for mac os to README

## New Feature

* ARROW-1548 - [GLib] Support build append in builder
* ARROW-1592 - [GLib] Add GArrowUIntArrayBuilder

## Test

* ARROW-1529 - [GLib] Fix failure on macOS on Travis CI

## Wish

* ARROW-559 - Script to easily verify release in all languages

# Apache Arrow 0.7.0 (12 September 2017)

## Bug

* ARROW-1302 - C++: ${MAKE} variable not set sometimes on older MacOS installations
* ARROW-1354 - [Python] Segfault in Table.from\_pandas with Mixed-Type Categories
* ARROW-1357 - [Python] Data corruption in reading multi-file parquet dataset
* ARROW-1363 - [C++] IPC writer sends buffer layout for dictionary rather than indices
* ARROW-1365 - [Python] Remove usage of removed jemalloc\_memory\_pool in Python API docs
* ARROW-1373 - [Java] Implement get<type>Buffer() methods at the ValueVector interface
* ARROW-1375 - [C++] Visual Studio 2017 Appveyor builds failing
* ARROW-1379 - [Java] maven dependency issues - both unused and undeclared
* ARROW-1407 - Dictionaries can only hold a maximum of 4096 indices
* ARROW-1411 - [Python] Booleans in Float Columns cause Segfault
* ARROW-1414 - [GLib] Cast after status check
* ARROW-1421 - [Python] pyarrow.serialize cannot serialize a Python dict input
* ARROW-1426 - [Website] The title element of the top page is empty
* ARROW-1429 - [Python] Error loading parquet file with \_metadata from HDFS
* ARROW-1430 - [Python] flake8 warnings are not failing CI builds
* ARROW-1434 - [C++/Python] pyarrow.Array.from\_pandas does not support datetime64[D] arrays
* ARROW-1435 - [Python] PyArrow not propagating timezone information from Parquet to Python
* ARROW-1439 - [Packaging] Automate updating RPM in RPM build
* ARROW-1443 - [Java] Bug on ArrowBuf.setBytes with unsliced ByteBuffers
* ARROW-1444 - BitVector.splitAndTransfer copies last byte incorrectly 
* ARROW-1446 - Python: Writing more than 2^31 rows from pandas dataframe causes row count overflow error
* ARROW-1450 - [Python] Raise proper error if custom serialization handler fails
* ARROW-1452 - [C++] Make UNUSED macro name more unique so it does not conflict with thirdparty projects
* ARROW-1453 - [Python] Implement WriteTensor for non-contiguous tensors
* ARROW-1458 - [Python] Document that HadoopFileSystem.mkdir with create\_parents=False has no effect
* ARROW-1459 - [Python] PyArrow fails to load partitioned parquet files with non-primitive types
* ARROW-1461 - [C++] Disable builds using LLVM apt packages temporarily
* ARROW-1467 - [JAVA]: Fix reset() and allocateNew() in Nullable Value Vectors template
* ARROW-1490 - [Java] Allow Travis CI failures for JDK9 for now
* ARROW-1493 - [C++] Flush the output stream at the end of each PrettyPrint function
* ARROW-1495 - [C++] Store shared\_ptr to boxed arrays in RecordBatch
* ARROW-1507 - [C++] arrow/compute/api.h can't be used without arrow/array.h
* ARROW-1512 - [Docs] NumericArray has no member named 'raw\_data'
* ARROW-1514 - [C++] Fix a typo in document
* ARROW-1527 - Fix Travis JDK9 build
* ARROW-1531 - [C++] Return ToBytes by value from Decimal128
* ARROW-1532 - [Python] Referencing an Empty Schema causes a SegFault
* ARROW-407 - BitVector.copyFromSafe() should re-allocate if necessary instead of returning false
* ARROW-801 - [JAVA] Provide direct access to underlying buffer memory addresses in consistent way without generating garbage or large amount indirections

## Improvement

* ARROW-1307 - [Python] Add pandas serialization section + Feather API to Sphinx docs
* ARROW-1317 - [Python] Add function to set Hadoop CLASSPATH 
* ARROW-1331 - [Java] Refactor tests
* ARROW-1339 - [C++] Use boost::filesystem for handling of platform-specific file path encodings
* ARROW-1344 - [C++] Calling BufferOutputStream::Write after calling Finish crashes
* ARROW-1348 - [C++/Python] Add release verification script for Windows
* ARROW-1351 - Automate updating CHANGELOG.md as part of release scripts
* ARROW-1352 - [Integration] Improve print formatting for producer, consumer line
* ARROW-1355 - Make arrow buildable with java9
* ARROW-1356 - [Website] Add new committers
* ARROW-1358 - Update source release scripts to account for new SHA checksum policy
* ARROW-1359 - [Python] Add Parquet writer option to normalize field names for use in Spark
* ARROW-1366 - [Python] Add instructions for starting the Plasma store when installing pyarrow from wheels
* ARROW-1372 - [Plasma] Support for storing data in huge pages
* ARROW-1376 - [C++] RecordBatchStreamReader::Open API is inconsistent with writer
* ARROW-1381 - [Python] Improve performance of SerializedPyObject.to\_buffer
* ARROW-1383 - [C++] Support std::vector<bool> in builder vector appends
* ARROW-1384 - [C++] Add convenience function for serializing a record batch to an IPC message 
* ARROW-1386 - [C++] Unpin CMake version in MSVC build toolchain
* ARROW-1395 - [C++] Remove APIs deprecated as of 0.5.0 and later versions
* ARROW-1397 - [Packaging] Use Docker instead of Vagrant
* ARROW-1401 - [C++] Add extra debugging context to failures in RETURN\_NOT\_OK in debug builds
* ARROW-1402 - [C++] Possibly deprecate public APIs that use MutableBuffer
* ARROW-1404 - [Packaging] Build .deb and .rpm on Travis CI
* ARROW-1405 - [Python] Add logging option for verbose memory allocations
* ARROW-1406 - [Python] Harden user API for generating serialized schema and record batch messages as memoryview-compatible objects
* ARROW-1408 - [C++] Refactor and make IPC read / write APIs more consistent, add appropriate deprecations
* ARROW-1410 - Plasma object store occasionally pauses for a long time
* ARROW-1412 - [Plasma] Add higher level API for putting and getting Python objects
* ARROW-1413 - [C++] Add include-what-you-use configuration
* ARROW-1416 - [Format] Clarify example array in memory layout documentation
* ARROW-1418 - [Python] Introduce SerializationContext to register custom serialization callbacks
* ARROW-1419 - [GLib] Suppress sign-conversion warning on Clang
* ARROW-1427 - [GLib] Add a link to readme of Arrow GLib
* ARROW-1428 - [C++] Append steps to clone source code to README.mb
* ARROW-1432 - [C++] Build bundled jemalloc functions with private prefix
* ARROW-1433 - [C++] Simplify implementation of Array::Slice
* ARROW-1438 - [Plasma] Pull SerializationContext through PlasmaClient put and get
* ARROW-1441 - [Site] Add Ruby to Flexible section
* ARROW-1442 - [Website] Add pointer to nightly conda packages on /install
* ARROW-1447 - [C++] Round of include-what-you-use include cleanups
* ARROW-1448 - [Packaging] Support uploading built .deb and .rpm to Bintray
* ARROW-1449 - Implement Decimal using only Int128
* ARROW-1451 - [C++] Create arrow/io/api.h
* ARROW-1460 - [C++] Upgrade clang-format used to LLVM 4.0
* ARROW-1466 - [C++] Support DecimalArray in arrow::PrettyPrint
* ARROW-1468 - [C++] Append to PrimitiveBuilder from std::vector<CTYPE>
* ARROW-1480 - [Python] Improve performance of serializing sets
* ARROW-1494 - [C++] Document that shared\_ptr returned by RecordBatch::column needs to be retained
* ARROW-1499 - [Python] Consider adding option to parquet.write\_table that sets options for maximum Spark compatibility
* ARROW-1505 - [GLib] Simplify arguments check
* ARROW-1506 - [C++] Support pkg-config for compute modules
* ARROW-1508 - C++: Add support for FixedSizeBinaryType in DictionaryBuilder
* ARROW-1511 - [C++] Deprecate arrow::MakePrimitiveArray
* ARROW-1513 - C++: Add cast from Dictionary to plain arrays
* ARROW-1515 - [GLib] Detect version directly
* ARROW-1516 - [GLib] Update document
* ARROW-1517 - Remove unnecessary temporary in DecimalUtil::ToString function
* ARROW-1519 - [C++] Move DecimalUtil functions to methods on the Int128 class
* ARROW-1528 - [GLib] Resolve include dependency
* ARROW-1530 - [C++] Install arrow/util/parallel.h
* ARROW-594 - [Python] Provide interface to write pyarrow.Table to a stream
* ARROW-786 - [Format] In-memory format for 128-bit Decimals, handling of sign bit
* ARROW-837 - [Python] Expose buffer allocation, FixedSizeBufferWriter
* ARROW-941 - [Docs] Improve "cold start" integration testing instructions

## New Feature

* ARROW-1156 - [Python] pyarrow.Array.from\_pandas should take a type parameter
* ARROW-1238 - [Java] Add JSON read/write support for decimals for integration tests
* ARROW-1364 - [C++] IPC reader and writer specialized for GPU device memory
* ARROW-1377 - [Python] Add function to assist with benchmarking Parquet scan performance
* ARROW-1387 - [C++] Set up GPU leaf library build toolchain
* ARROW-1392 - [C++] Implement reader and writer IO interfaces for GPU buffers
* ARROW-1396 - [C++] Add PrettyPrint function for Schemas, which also outputs any dictionaries
* ARROW-1399 - [C++] Add CUDA build version in a public header to help prevent ABI conflicts
* ARROW-1400 - [Python] Ability to create partitions when writing to Parquet
* ARROW-1415 - [GLib] Support date32 and date64
* ARROW-1417 - [Python] Allow more generic filesystem objects to be passed to ParquetDataset
* ARROW-1462 - [GLib] Support time array
* ARROW-1479 - [JS] Expand JavaScript implementation
* ARROW-1481 - [C++] Expose type casts as generic callable object that can write into pre-allocated memory
* ARROW-1504 - [GLib] Support timestamp
* ARROW-1510 - [C++] Support cast
* ARROW-229 - [C++] Implement safe casts for primitive types
* ARROW-592 - [C++] Provide .deb and .rpm packages
* ARROW-695 - Integration tests for Decimal types
* ARROW-696 - [C++] Add JSON read/write support for decimals for integration tests
* ARROW-759 - [Python] Implement a transient list serialization function that can handle a mix of scalars, lists, ndarrays, dicts
* ARROW-989 - [Python] Write pyarrow.Table to FileWriter or StreamWriter

## Test

* ARROW-1390 - [Python] Extend tests for python serialization

# Apache Arrow 0.6.0 (14 August 2017)

## Bug

* ARROW-1192 - [JAVA] Improve splitAndTransfer performance for List and Union vectors
* ARROW-1195 - [C++] CpuInfo doesn't get cache size on Windows
* ARROW-1204 - [C++] lz4 ExternalProject fails in Visual Studio 2015
* ARROW-1225 - [Python] pyarrow.array does not attempt to convert bytes to UTF8 when passed a StringType
* ARROW-1237 - [JAVA] Expose the ability to set lastSet
* ARROW-1239 - issue with current version of git-commit-id-plugin
* ARROW-1240 - security: upgrade logback to address CVE-2017-5929
* ARROW-1242 - [Java] security - upgrade Jackson to mitigate 3 CVE vulnerabilities
* ARROW-1245 - [Integration] Java Integration Tests Disabled
* ARROW-1248 - [Python] C linkage warnings in Clang with public Cython API
* ARROW-1249 - [JAVA] Expose the fillEmpties function from Nullable<Varlength>Vector.mutator
* ARROW-1263 - [C++] CpuInfo should be able to get CPU features on Windows
* ARROW-1265 - [Plasma] Plasma store memory leak warnings in Python test suite
* ARROW-1267 - [Java] Handle zero length case in BitVector.splitAndTransfer
* ARROW-1269 - [Packaging] Add Windows wheel build scripts from ARROW-1068 to arrow-dist
* ARROW-1275 - [C++] Default static library prefix for Snappy should be "\_static"
* ARROW-1276 - Cannot serializer empty DataFrame to parquet
* ARROW-1283 - [Java] VectorSchemaRoot should be able to be closed() more than once
* ARROW-1285 - PYTHON: NotImplemented exception creates empty parquet file
* ARROW-1287 - [Python] Emulate "whence" argument of seek in NativeFile
* ARROW-1290 - [C++] Use array capacity doubling in arrow::BufferBuilder
* ARROW-1291 - [Python] `pa.RecordBatch.from_pandas` doesn't accept DataFrame with numeric column names
* ARROW-1294 - [C++] New Appveyor build failures
* ARROW-1296 - [Java] templates/FixValueVectors reset() method doesn't set allocationSizeInBytes correctly
* ARROW-1300 - [JAVA] Fix ListVector Tests
* ARROW-1306 - [Python] Encoding? issue with error reporting for `parquet.read_table`
* ARROW-1308 - [C++] ld tries to link `arrow_static` even when -DARROW_BUILD_STATIC=off
* ARROW-1309 - [Python] Error inferring List type in `Array.from_pandas` when inner values are all None
* ARROW-1310 - [JAVA] Revert ARROW-886
* ARROW-1312 - [C++] Set default value to `ARROW_JEMALLOC` to OFF until ARROW-1282 is resolved
* ARROW-1326 - [Python] Fix Sphinx build in Travis CI
* ARROW-1327 - [Python] Failing to release GIL in `MemoryMappedFile._open` causes deadlock
* ARROW-1328 - [Python] `pyarrow.Table.from_pandas` option `timestamps_to_ms` changes column values
* ARROW-1330 - [Plasma] Turn on plasma tests on manylinux1
* ARROW-1335 - [C++] `PrimitiveArray::raw_values` has inconsistent semantics re: offsets compared with subclasses
* ARROW-1338 - [Python] Investigate non-deterministic core dump on Python 2.7, Travis CI builds
* ARROW-1340 - [Java] NullableMapVector field doesn't maintain metadata
* ARROW-1342 - [Python] Support strided array of lists
* ARROW-1343 - [Format/Java/C++] Ensuring encapsulated stream / IPC message sizes are always a multiple of 8
* ARROW-1350 - [C++] Include Plasma source tree in source distribution
* ARROW-187 - [C++] Decide on how pedantic we want to be about exceptions
* ARROW-276 - [JAVA] Nullable Value Vectors should extend BaseValueVector instead of BaseDataValueVector
* ARROW-573 - [Python/C++] Support ordered dictionaries data, pandas Categorical
* ARROW-884 - [C++] Exclude internal classes from documentation
* ARROW-932 - [Python] Fix compiler warnings on MSVC
* ARROW-968 - [Python] RecordBatch [i:j] syntax is incomplete

## Improvement

* ARROW-1093 - [Python] Fail Python builds if flake8 yields warnings
* ARROW-1121 - [C++] Improve error message when opening OS file fails
* ARROW-1140 - [C++] Allow optional build of plasma
* ARROW-1149 - [Plasma] Create Cython client library for Plasma
* ARROW-1173 - [Plasma] Blog post for Plasma
* ARROW-1211 - [C++] Consider making `default_memory_pool()` the default for builder classes
* ARROW-1213 - [Python] Enable s3fs to be used with ParquetDataset and reader/writer functions
* ARROW-1219 - [C++] Use more vanilla Google C++ formatting
* ARROW-1224 - [Format] Clarify language around buffer padding and alignment in IPC
* ARROW-1230 - [Plasma] Install libraries and headers
* ARROW-1243 - [Java] security: upgrade all libraries to latest stable versions
* ARROW-1251 - [Python/C++] Revise build documentation to account for latest build toolchain
* ARROW-1253 - [C++] Use pre-built toolchain libraries where prudent to speed up CI builds
* ARROW-1255 - [Plasma] Check plasma flatbuffer messages with the flatbuffer verifier
* ARROW-1257 - [Plasma] Plasma documentation
* ARROW-1258 - [C++] Suppress dlmalloc warnings on Clang
* ARROW-1259 - [Plasma] Speed up Plasma tests
* ARROW-1260 - [Plasma] Use factory method to create Python PlasmaClient
* ARROW-1264 - [Plasma] Don't exit the Python interpreter if the plasma client can't connect to the store
* ARROW-1274 - [C++] `add_compiler_export_flags()` throws warning with CMake >= 3.3
* ARROW-1288 - Clean up many ASF license headers
* ARROW-1289 - [Python] Add `PYARROW_BUILD_PLASMA` option like Parquet
* ARROW-1301 - [C++/Python] Add remaining supported libhdfs UNIX-like filesystem APIs
* ARROW-1303 - [C++] Support downloading Boost
* ARROW-1315 - [GLib] Status check of arrow::ArrayBuilder::Finish() is missing
* ARROW-1323 - [GLib] Add `garrow_boolean_array_get_values()`
* ARROW-1333 - [Plasma] Sorting example for DataFrames in plasma
* ARROW-1334 - [C++] Instantiate arrow::Table from vector of Array objects (instead of Columns)

## New Feature

* ARROW-1076 - [Python] Handle nanosecond timestamps more gracefully when writing to Parquet format
* ARROW-1104 - Integrate in-memory object store from Ray
* ARROW-1246 - [Format] Add Map logical type to metadata
* ARROW-1268 - [Website] Blog post on Arrow integration with Spark
* ARROW-1281 - [C++/Python] Add Docker setup for running HDFS tests and other tests we may not run in Travis CI
* ARROW-1305 - [GLib] Add GArrowIntArrayBuilder
* ARROW-1336 - [C++] Add arrow::schema factory function
* ARROW-439 - [Python] Add option in `to_pandas` conversions to yield Categorical from String/Binary arrays
* ARROW-622 - [Python] Investigate alternatives to `timestamps_to_ms` argument in pandas conversion

## Task

* ARROW-1270 - [Packaging] Add Python wheel build scripts for macOS to arrow-dist
* ARROW-1272 - [Python] Add script to arrow-dist to generate and upload manylinux1 Python wheels
* ARROW-1273 - [Python] Add convenience functions for reading only Parquet metadata or effective Arrow schema from a particular Parquet file
* ARROW-1297 - 0.6.0 Release
* ARROW-1304 - [Java] Fix checkstyle checks warning

## Test

* ARROW-1241 - [C++] Visual Studio 2017 Appveyor build job

# Apache Arrow 0.5.0 (23 July 2017)

## Bug

* ARROW-1074 - `from_pandas` doesnt convert ndarray to list
* ARROW-1079 - [Python] Empty "private" directories should be ignored by Parquet interface
* ARROW-1081 - C++: arrow::test::TestBase::MakePrimitive doesn't fill `null_bitmap`
* ARROW-1096 - [C++] Memory mapping file over 4GB fails on Windows
* ARROW-1097 - Reading tensor needs file to be opened in writeable mode
* ARROW-1098 - Document Error?
* ARROW-1101 - UnionListWriter is not implementing all methods on interface ScalarWriter
* ARROW-1103 - [Python] Utilize pandas metadata from common `_metadata` Parquet file if it exists
* ARROW-1107 - [JAVA] NullableMapVector getField() should return nullable type
* ARROW-1108 - Check if ArrowBuf is empty buffer in getActualConsumedMemory() and getPossibleConsumedMemory()
* ARROW-1109 - [JAVA] transferOwnership fails when readerIndex is not 0
* ARROW-1110 - [JAVA] make union vector naming consistent
* ARROW-1111 - [JAVA] Make aligning buffers optional, and allow -1 for unknown null count
* ARROW-1112 - [JAVA] Set lastSet for VarLength and List vectors when loading
* ARROW-1113 - [C++] gflags EP build gets triggered (as a no-op) on subsequent calls to make or ninja build
* ARROW-1115 - [C++] Use absolute path for ccache
* ARROW-1117 - [Docs] Minor issues in GLib README
* ARROW-1124 - [Python] pyarrow needs to depend on numpy>=1.10 (not 1.9)
* ARROW-1125 - Python: `Table.from_pandas` doesn't work anymore on partial schemas
* ARROW-1128 - [Docs] command to build a wheel is not properly rendered
* ARROW-1129 - [C++] Fix Linux toolchain build regression from ARROW-742
* ARROW-1131 - Python: Parquet unit tests are always skipped
* ARROW-1132 - [Python] Unable to write pandas DataFrame w/MultiIndex containing duplicate values to parquet
* ARROW-1136 - [C++/Python] Segfault on empty stream
* ARROW-1138 - Travis: Use OpenJDK7 instead of OracleJDK7
* ARROW-1139 - [C++] dlmalloc doesn't allow arrow to be built with clang 4 or gcc 7.1.1
* ARROW-1141 - on import get libjemalloc.so.2: cannot allocate memory in static TLS block
* ARROW-1143 - C++: Fix comparison of NullArray
* ARROW-1144 - [C++] Remove unused variable
* ARROW-1150 - [C++] AdaptiveIntBuilder compiler warning on MSVC
* ARROW-1152 - [Cython] `read_tensor` should work with a readable file
* ARROW-1155 - segmentation fault when run pa.Int16Value()
* ARROW-1157 - C++/Python: Decimal templates are not correctly exported on OSX
* ARROW-1159 - [C++] Static data members cannot be accessed from inline functions in Arrow headers by thirdparty users
* ARROW-1162 - Transfer Between Empty Lists Should Not Invoke Callback
* ARROW-1166 - Errors in Struct type's example and missing reference in Layout.md
* ARROW-1167 - [Python] Create chunked BinaryArray in `Table.from_pandas` when a column's data exceeds 2GB
* ARROW-1168 - [Python] pandas metadata may contain "mixed" data types
* ARROW-1169 - C++: jemalloc externalproject doesn't build with CMake's ninja generator
* ARROW-1170 - C++: `ARROW_JEMALLOC=OFF` breaks linking on unittest
* ARROW-1174 - [GLib] Investigate root cause of ListArray glib test failure
* ARROW-1177 - [C++] Detect int32 overflow in ListBuilder::Append
* ARROW-1179 - C++: Add missing virtual destructors
* ARROW-1180 - [GLib] `garrow_tensor_get_dimension_name()` returns invalid address
* ARROW-1181 - [Python] Parquet test fail if not enabled
* ARROW-1182 - C++: Specify `BUILD_BYPRODUCTS` for zlib and zstd
* ARROW-1186 - [C++] Enable option to build arrow with minimal dependencies needed to build Parquet library
* ARROW-1188 - Segfault when trying to serialize a DataFrame with Null-only Categorical Column
* ARROW-1190 - VectorLoader corrupts vectors with duplicate names
* ARROW-1191 - [JAVA] Implement getField() method for the complex readers
* ARROW-1194 - Getting record batch size with `pa.get_record_batch_size` returns a size that is too small for pandas DataFrame.
* ARROW-1197 - [GLib] `record_batch.hpp` Inclusion is missing
* ARROW-1200 - [C++] DictionaryBuilder should use signed integers for indices
* ARROW-1201 - [Python] Incomplete Python types cause a core dump when repr-ing
* ARROW-1203 - [C++] Disallow BinaryBuilder to append byte strings larger than the maximum value of `int32_t`
* ARROW-1205 - C++: Reference to type objects in ArrayLoader may cause segmentation faults.
* ARROW-1206 - [C++] Enable MSVC builds to work with some compression library support disabled
* ARROW-1208 - [C++] Toolchain build with ZSTD library from conda-forge failure
* ARROW-1215 - [Python] Class methods in API reference
* ARROW-1216 - Numpy arrays cannot be created from Arrow Buffers on Python 2
* ARROW-1218 - Arrow doesn't compile if all compression libraries are deactivated
* ARROW-1222 - [Python] pyarrow.array returns NullArray for array of unsupported Python objects
* ARROW-1223 - [GLib] Fix function name that returns wrapped object
* ARROW-1235 - [C++] macOS linker failure with operator<< and std::ostream
* ARROW-1236 - Library paths in exported pkg-config file are incorrect
* ARROW-601 - Some logical types not supported when loading Parquet
* ARROW-784 - Cleaning up thirdparty toolchain support in Arrow on Windows
* ARROW-992 - [Python] In place development builds do not have a `__version__`

## Improvement

* ARROW-1041 - [Python] Support `read_pandas` on a directory of Parquet files
* ARROW-1100 - [Python] Add "mode" property to NativeFile instances
* ARROW-1102 - Make MessageSerializer.serializeMessage() public
* ARROW-1120 - [Python] Write support for int96
* ARROW-1137 - Python: Ensure Pandas roundtrip of all-None column
* ARROW-1148 - [C++] Raise minimum CMake version to 3.2
* ARROW-1151 - [C++] Add gcc branch prediction to status check macro
* ARROW-1160 - C++: Implement DictionaryBuilder
* ARROW-1165 - [C++] Refactor PythonDecimalToArrowDecimal to not use templates
* ARROW-1185 - [C++] Clean up arrow::Status implementation, add `warn_unused_result` attribute for clang
* ARROW-1187 - Serialize a DataFrame with None column
* ARROW-1193 - [C++] Support pkg-config for `arrow_python.so`
* ARROW-1196 - [C++] Appveyor separate jobs for Debug/Release builds from sources; Build with conda toolchain; Build with NMake Makefiles Generator
* ARROW-1199 - [C++] Introduce mutable POD struct for generic array data
* ARROW-1202 - Remove semicolons from status macros
* ARROW-1217 - [GLib] Add GInputStream based arrow::io::RandomAccessFile
* ARROW-1220 - [C++] Standartize usage of `*_HOME` cmake script variables for 3rd party libs
* ARROW-1221 - [C++] Pin clang-format version
* ARROW-1229 - [GLib] Follow Reader API change (get -> read)
* ARROW-742 - Handling exceptions during execution of `std::wstring_convert`
* ARROW-834 - [Python] Support creating Arrow arrays from Python iterables
* ARROW-915 - Struct Array reads limited support
* ARROW-935 - [Java] Build Javadoc in Travis CI
* ARROW-960 - [Python] Add source build guide for macOS + Homebrew
* ARROW-962 - [Python] Add schema attribute to FileReader
* ARROW-966 - [Python] `pyarrow.list_` should also accept Field instance
* ARROW-978 - [Python] Use sphinx-bootstrap-theme for Sphinx documentation

## New Feature

* ARROW-1048 - Allow user `LD_LIBRARY_PATH` to be used with source release script
* ARROW-1073 - C++: Adapative integer builder
* ARROW-1095 - [Website] Add Arrow icon asset
* ARROW-111 - [C++] Add static analyzer to tool chain to verify checking of Status returns
* ARROW-1122 - [Website] Guest blog post on Arrow + ODBC from turbodbc
* ARROW-1123 - C++: Make jemalloc the default allocator
* ARROW-1135 - Upgrade Travis CI clang builds to use LLVM 4.0
* ARROW-1142 - [C++] Move over compression library toolchain from parquet-cpp
* ARROW-1145 - [GLib] Add `get_values()`
* ARROW-1154 - [C++] Migrate more computational utility code from parquet-cpp
* ARROW-1183 - [Python] Implement time type conversions in `to_pandas`
* ARROW-1198 - Python: Add public C++ API to unwrap PyArrow object
* ARROW-1212 - [GLib] Add `garrow_binary_array_get_offsets_buffer()`
* ARROW-1214 - [Python] Add classes / functions to enable stream message components to be handled outside of the stream reader class
* ARROW-1227 - [GLib] Support GOutputStream
* ARROW-460 - [C++] Implement JSON round trip for DictionaryArray
* ARROW-462 - [C++] Implement in-memory conversions between non-nested primitive types and DictionaryArray equivalent
* ARROW-575 - Python: Auto-detect nested lists and nested numpy arrays in Pandas
* ARROW-597 - [Python] Add convenience function to yield DataFrame from any object that a StreamReader or FileReader can read from
* ARROW-599 - [C++] Add LZ4 codec to 3rd-party toolchain
* ARROW-600 - [C++] Add ZSTD codec to 3rd-party toolchain
* ARROW-692 - Java<->C++ Integration tests for dictionary-encoded vectors
* ARROW-693 - [Java] Add JSON support for dictionary vectors

## Task

* ARROW-1052 - Arrow 0.5.0 release

## Test

* ARROW-1228 - [GLib] Test file name should be the same name as target class
* ARROW-1233 - [C++] Validate cmake script resolving of 3rd party linked libs from correct location in toolchain build

# Apache Arrow 0.4.1 (9 June 2017)

## Bug

* ARROW-1039 - Python: `pyarrow.Filesystem.read_parquet` causing error if nthreads>1
* ARROW-1050 - [C++] Export arrow::ValidateArray
* ARROW-1051 - [Python] If pyarrow.parquet fails to import due to a shared library ABI conflict, the `test_parquet.py` tests silently do not run
* ARROW-1056 - [Python] Parquet+HDFS test failure due to writing pandas index
* ARROW-1057 - Fix cmake warning and msvc debug asserts
* ARROW-1062 - [GLib] Examples use old API
* ARROW-1066 - remove warning on feather for pandas >= 0.20.1
* ARROW-1070 - [C++] Feather files for date/time types should be written with the physical types
* ARROW-1075 - [GLib] Build error on macOS
* ARROW-1085 - [java] Follow up on template cleanup. Missing method for IntervalYear
* ARROW-1086 - [Python] pyarrow 0.4.0 on pypi is missing pxd files
* ARROW-1088 - [Python] `test_unicode_filename` test fails when unicode filenames aren't supported by system
* ARROW-1090 - [Python] `build_ext` usability
* ARROW-1091 - Decimal scale and precision are flipped
* ARROW-1092 - More Decimal and scale flipped follow-up
* ARROW-1094 - [C++] Incomplete buffer reads in arrow::io::ReadableFile should exactly truncate returned buffer
* ARROW-424 - [C++] Threadsafety in arrow/io/hdfs.h

## Improvement

* ARROW-1020 - [Format] Add additional language to Schema.fbs to clarify naive vs. localized Timestamp values
* ARROW-1034 - [Python] Enable creation of binary wheels on Windows / MSVC
* ARROW-1049 - [java] vector template cleanup
* ARROW-1063 - [Website] Blog post and website updates for 0.4.0 release
* ARROW-1078 - [Python] Account for PARQUET-967
* ARROW-1080 - C++: Add tutorial about converting to/from row-wise representation
* ARROW-897 - [GLib] Build arrow-glib as a separate build in the Travis CI build matrix
* ARROW-986 - [Format] Update IPC.md to account for dictionary batches
* ARROW-990 - [JS] Add tslint support for linting TypeScript

## Task

* ARROW-1068 - [Python] Create external repo with appveyor.yml configured for building Python wheel installers
* ARROW-1069 - Add instructions for publishing maven artifacts
* ARROW-1084 - Implementations of BufferAllocator should handle Netty's OutOfDirectMemoryError

## Test

* ARROW-1060 - [Python] Add unit test for ARROW-1053
* ARROW-1082 - [GLib] Add CI on macOS

# Apache Arrow 0.4.0 (22 May 2017)

## Bug

* ARROW-1003 - [C++] Hdfs and java dlls fail to load when built for Windows with MSVC
* ARROW-1004 - ArrowInvalid: Invalid: Python object of type float is not None and is not a string, bool, or date object
* ARROW-1017 - Python: `Table.to_pandas` leaks memory
* ARROW-1023 - Python: Fix bundling of arrow-cpp for macOS
* ARROW-1033 - [Python] pytest discovers `scripts/test_leak.py`
* ARROW-1046 - [Python] Conform DataFrame metadata to pandas spec
* ARROW-1053 - [Python] Memory leak with RecordBatchFileReader
* ARROW-1054 - [Python] Test suite fails on pandas 0.19.2
* ARROW-1061 - [C++] Harden decimal parsing against invalid strings
* ARROW-1064 - ModuleNotFoundError: No module named 'pyarrow._parquet'
* ARROW-813 - [Python] setup.py sdist must also bundle dependent cmake modules
* ARROW-824 - Date and Time Vectors should reflect timezone-less semantics
* ARROW-856 - CmakeError by Unknown compiler.
* ARROW-881 - [Python] Reconstruct Pandas DataFrame indexes using `custom_metadata`
* ARROW-909 - libjemalloc.so.2: cannot open shared object file:
* ARROW-939 - Fix division by zero for zero-dimensional Tensors
* ARROW-940 - [JS] Generate multiple sets of artifacts
* ARROW-944 - Python: Compat broken for pandas==0.18.1
* ARROW-948 - [GLib] Update C++ header file list
* ARROW-952 - Compilation error on macOS with clang-802.0.42
* ARROW-958 - [Python] Conda build guide still needs `ARROW_HOME`, `PARQUET_HOME`
* ARROW-979 - [Python] Fix `setuptools_scm` version when release tag is not in the master timeline
* ARROW-991 - [Python] `PyArray_SimpleNew` should not be used with `NPY_DATETIME`
* ARROW-995 - [Website] 0.3 release announce has a typo in reference
* ARROW-998 - [Doc] File format documents incorrect schema location

## Improvement

* ARROW-1000 - [GLib] Move install document to Website
* ARROW-1001 - [GLib] Unify writer files
* ARROW-1002 - [C++] It is not necessary to add padding after the magic header in the FileWriter implementation
* ARROW-1010 - [Website] Only show English posts in /blog/
* ARROW-1016 - Python: Include C++ headers (optionally) in wheels
* ARROW-1022 - [Python] Add nthreads option to Feather read method
* ARROW-1024 - Python: Update build time numpy version to 1.10.1
* ARROW-1025 - [Website] Improve changelog on website
* ARROW-1027 - [Python] Allow negative indexing in fields/columns on pyarrow Table and Schema objects
* ARROW-1028 - [Python] Documentation updates after ARROW-1008
* ARROW-1029 - [Python] Fix --with-parquet build on Windows, add unit tests to Appveyor
* ARROW-1030 - Python: Account for library versioning in parquet-cpp
* ARROW-1037 - [GLib] Follow reader name change
* ARROW-1038 - [GLib] Follow writer name change
* ARROW-1040 - [GLib] Follow tensor IO
* ARROW-182 - [C++] Remove Array::Validate virtual function and make a separate method
* ARROW-376 - Python: Convert non-range Pandas indices (optionally) to Arrow
* ARROW-532 - [Python] Expand pyarrow.parquet documentation for 0.3 release
* ARROW-579 - Python: Provide redistributable pyarrow wheels on OSX
* ARROW-891 - [Python] Expand Windows build instructions to not require looking at separate C++ docs
* ARROW-899 - [Docs] Add CHANGELOG for 0.3.0
* ARROW-901 - [Python] Write FixedSizeBinary to Parquet
* ARROW-913 - [Python] Only link jemalloc to the Cython extension where it's needed
* ARROW-923 - [Docs] Generate Changelog for website with JIRA links
* ARROW-929 - Move KEYS file to SVN, remove from git
* ARROW-943 - [GLib] Support running unit tests with source archive
* ARROW-945 - [GLib] Add a Lua example to show Torch integration
* ARROW-946 - [GLib] Use "new" instead of "open" for constructor name
* ARROW-947 - [Python] Improve execution time of manylinux1 build
* ARROW-953 - Use cmake / curl from conda-forge in CI builds
* ARROW-954 - Make it possible to compile Arrow with header-only boost
* ARROW-961 - [Python] Rename InMemoryOutputStream to BufferOutputStream
* ARROW-970 - [Python] Accidentally calling pyarrow.Table() should not segfault process
* ARROW-982 - [Website] Improve website front copy to highlight serialization efficiency benefits
* ARROW-984 - [GLib] Add Go examples
* ARROW-985 - [GLib] Update package information
* ARROW-988 - [JS] Add entry to Travis CI matrix
* ARROW-993 - [GLib] Add missing error checks in Go examples
* ARROW-996 - [Website] Add 0.3 release announce in Japanese

## New Feature

* ARROW-1008 - [C++] Define abstract interface for stream iteration
* ARROW-1011 - [Format] Clarify requirements around buffer padding in validity bitmaps
* ARROW-1014 - 0.4.0 release
* ARROW-1031 - [GLib] Support pretty print
* ARROW-1044 - [GLib] Support Feather
* ARROW-29 - C++: Add re2 as optional 3rd-party toolchain dependency
* ARROW-446 - [Python] Document NativeFile interfaces, HDFS client in Sphinx
* ARROW-482 - [Java] Provide API access to `custom_metadata` Field attribute in IPC setting
* ARROW-596 - [Python] Add convenience function to convert pandas.DataFrame to pyarrow.Buffer containing a file or stream representation
* ARROW-714 - [C++] Add `import_pyarrow` C API in the style of NumPy for thirdparty C++ users
* ARROW-819 - [Python] Define public Cython API
* ARROW-872 - [JS] Read streaming format
* ARROW-873 - [JS] Implement fixed width list type
* ARROW-874 - [JS] Read dictionary-encoded vectors
* ARROW-963 - [GLib] Add equal
* ARROW-967 - [GLib] Support initializing array with buffer
* ARROW-977 - [java] Add Timezone aware timestamp vectors

## Task

* ARROW-1015 - [Java] Implement schema-level metadata
* ARROW-629 - [JS] Add unit test suite
* ARROW-956 - remove pandas pre-0.20.0 compat
* ARROW-957 - [Doc] Add HDFS and Windows documents to doxygen output
* ARROW-997 - [Java] Implement transfer in FixedSizeListVector

# Apache Arrow 0.3.0 (5 May 2017)

## Bug

* ARROW-109 - [C++] Investigate recursive data types limit in flatbuffers
* ARROW-208 - Add checkstyle policy to java project
* ARROW-347 - Add method to pass CallBack when creating a transfer pair
* ARROW-413 - DATE type is not specified clearly
* ARROW-431 - [Python] Review GIL release and acquisition in `to_pandas` conversion
* ARROW-443 - [Python] Support for converting from strided pandas data in `Table.from_pandas`
* ARROW-451 - [C++] Override DataType::Equals for other types with additional metadata
* ARROW-454 - pojo.Field doesn't implement hashCode()
* ARROW-526 - [Format] Update IPC.md to account for File format changes and Streaming format
* ARROW-565 - [C++] Examine "Field::dictionary" member
* ARROW-570 - Determine Java tools JAR location from project metadata
* ARROW-584 - [C++] Fix compiler warnings exposed with -Wconversion
* ARROW-588 - [C++] Fix compiler warnings on 32-bit platforms
* ARROW-595 - [Python] StreamReader.schema returns None
* ARROW-604 - Python: boxed Field instances are missing the reference to DataType
* ARROW-613 - [JS] Implement random-access file format
* ARROW-617 - Time type is not specified clearly
* ARROW-619 - Python: Fix typos in setup.py args and `LD_LIBRARY_PATH`
* ARROW-623 - segfault with `__repr__` of empty Field
* ARROW-624 - [C++] Restore MakePrimitiveArray function
* ARROW-627 - [C++] Compatibility macros for exported extern template class declarations
* ARROW-628 - [Python] Install nomkl metapackage when building parquet-cpp for faster Travis builds
* ARROW-630 - [C++] IPC unloading for BooleanArray does not account for offset
* ARROW-636 - [C++] Add Boost / other system requirements to C++ README
* ARROW-639 - [C++] Invalid offset in slices
* ARROW-642 - [Java] Remove temporary file in java/tools
* ARROW-644 - Python: Cython should be a setup-only requirement
* ARROW-652 - Remove trailing f in merge script output
* ARROW-654 - [C++] Support timezone metadata in file/stream formats
* ARROW-668 - [Python] Convert nanosecond timestamps to pandas.Timestamp when converting from TimestampValue
* ARROW-671 - [GLib] License file isn't installed
* ARROW-673 - [Java] Support additional Time metadata
* ARROW-677 - [java] Fix checkstyle jcl-over-slf4j conflict issue
* ARROW-678 - [GLib] Fix dependenciesfff
* ARROW-680 - [C++] Multiarch support impacts user-supplied install prefix
* ARROW-682 - Add self-validation checks in integration tests
* ARROW-683 - [C++] Support date32 (DateUnit::DAY) in IPC metadata, rename date to date64
* ARROW-686 - [C++] Account for time metadata changes, add time32 and time64 types
* ARROW-689 - [GLib] Install header files and documents to wrong directories
* ARROW-691 - [Java] Encode dictionary Int type in message format
* ARROW-697 - [Java] Raise appropriate exceptions when encountering large (> `INT32_MAX`) record batches
* ARROW-699 - [C++] Arrow dynamic libraries are missed on run of unit tests on Windows
* ARROW-702 - Fix BitVector.copyFromSafe to reAllocate instead of returning false
* ARROW-703 - Fix issue where setValueCount(0) doesn’t work in the case that we’ve shipped vectors across the wire
* ARROW-704 - Fix bad import caused by conflicting changes
* ARROW-709 - [C++] Restore type comparator for DecimalType
* ARROW-713 - [C++] Fix linking issue with ipc benchmark
* ARROW-715 - Python: Explicit pandas import makes it a hard requirement
* ARROW-716 - error building arrow/python
* ARROW-720 - [java] arrow should not have a dependency on slf4j bridges in compile
* ARROW-723 - Arrow freezes on write if `chunk_size=0`
* ARROW-726 - [C++] PyBuffer dtor may segfault if constructor passed an object not exporting buffer protocol
* ARROW-732 - Schema comparison bugs in struct and union types
* ARROW-736 - [Python] Mixed-type object DataFrame columns should not silently coerce to an Arrow type by default
* ARROW-738 - [Python] Fix manylinux1 packaging
* ARROW-739 - Parallel build fails non-deterministically.
* ARROW-740 - FileReader fails for large objects
* ARROW-747 - [C++] Fix spurious warning caused by passing dl to `add_dependencies`
* ARROW-749 - [Python] Delete incomplete binary files when writing fails
* ARROW-753 - [Python] Unit tests in arrow/python fail to link on some OS X platforms
* ARROW-756 - [C++] Do not pass -fPIC when compiling with MSVC
* ARROW-757 - [C++] MSVC build fails on googletest when using NMake
* ARROW-762 - Kerberos Problem with PyArrow
* ARROW-776 - [GLib] Cast type is wrong
* ARROW-777 - [Java] Resolve getObject behavior per changes / discussion in ARROW-729
* ARROW-778 - Modify merge tool to work on Windows
* ARROW-781 - [Python/C++] Increase reference count for base object?
* ARROW-783 - Integration tests fail for length-0 record batch
* ARROW-787 - [GLib] Fix compilation errors caused by ARROW-758
* ARROW-793 - [GLib] Wrong indent
* ARROW-794 - [C++] Check whether data is contiguous in ipc::WriteTensor
* ARROW-797 - [Python] Add updated pyarrow. public API listing in Sphinx docs
* ARROW-800 - [C++] Boost headers being transitively included in pyarrow
* ARROW-805 - listing empty HDFS directory returns an error instead of returning empty list
* ARROW-809 - C++: Writing sliced record batch to IPC writes the entire array
* ARROW-812 - Pip install pyarrow on mac failed.
* ARROW-817 - [C++] Fix incorrect code comment from ARROW-722
* ARROW-821 - [Python] Extra file `_table_api.h` generated during Python build process
* ARROW-822 - [Python] StreamWriter fails to open with socket as sink
* ARROW-826 - Compilation error on Mac with `-DARROW_PYTHON=on`
* ARROW-829 - Python: Parquet: Dictionary encoding is deactivated if column-wise compression was selected
* ARROW-830 - Python: jemalloc is not anymore publicly exposed
* ARROW-839 - [C++] Portable alternative to `PyDate_to_ms` function
* ARROW-847 - C++: `BUILD_BYPRODUCTS` not specified anymore for gtest
* ARROW-852 - Python: Also set Arrow Library PATHS when detection was done through pkg-config
* ARROW-853 - [Python] It is no longer necessary to modify the RPATH of the Cython extensions on many environments
* ARROW-858 - Remove dependency on boost regex
* ARROW-866 - [Python] Error from file object destructor
* ARROW-867 - [Python] Miscellaneous pyarrow MSVC fixes
* ARROW-875 - Nullable variable length vector fillEmpties() fills an extra value
* ARROW-879 - compat with pandas 0.20.0
* ARROW-882 - [C++] On Windows statically built lib file overwrites lib file of shared build
* ARROW-886 - VariableLengthVectors don't reAlloc offsets
* ARROW-887 - [format] For backward compatibility, new unit fields must have default values matching previous implied unit
* ARROW-888 - BitVector transfer() does not transfer ownership
* ARROW-895 - Nullable variable length vector lastSet not set correctly
* ARROW-900 - [Python] UnboundLocalError in ParquetDatasetPiece
* ARROW-903 - [GLib] Remove a needless "."
* ARROW-914 - [C++/Python] Fix Decimal ToBytes
* ARROW-922 - Allow Flatbuffers and RapidJSON to be used locally on Windows
* ARROW-928 - Update CMAKE script to detect unsupported msvc compilers versions
* ARROW-933 - [Python] `arrow_python` bindings have debug print statement
* ARROW-934 - [GLib] Glib sources missing from result of 02-source.sh
* ARROW-936 - Fix release README
* ARROW-938 - Fix Apache Rat errors from source release build

## Improvement

* ARROW-316 - Finalize Date type
* ARROW-542 - [Java] Implement dictionaries in stream/file encoding
* ARROW-563 - C++: Support non-standard gcc version strings
* ARROW-566 - Python: Deterministic position of libarrow in manylinux1 wheels
* ARROW-569 - [C++] Set version for .pc
* ARROW-577 - [C++] Refactor StreamWriter and FileWriter to have private implementations
* ARROW-580 - C++: Also provide `jemalloc_X` targets if only a static or shared version is found
* ARROW-582 - [Java] Add Date/Time Support to JSON File
* ARROW-589 - C++: Use system provided shared jemalloc if static is unavailable
* ARROW-593 - [C++] Rename ReadableFileInterface to RandomAccessFile
* ARROW-612 - [Java] Field toString should show nullable flag status
* ARROW-615 - Move ByteArrayReadableSeekableByteChannel to vector.util package
* ARROW-631 - [GLib] Import C API (C++ API wrapper) based on GLib from https://github.com/kou/arrow-glib
* ARROW-646 - Cache miniconda packages
* ARROW-647 - [C++] Don't require Boost static libraries to support CentOS 7
* ARROW-648 - [C++] Support multiarch on Debian
* ARROW-650 - [GLib] Follow eadableFileInterface -> RnadomAccessFile change
* ARROW-651 - [C++] Set shared library version for .deb packages
* ARROW-655 - Implement DecimalArray
* ARROW-662 - [Format] Factor Flatbuffer schema metadata into a Schema.fbs
* ARROW-664 - Make C++ Arrow serialization deterministic
* ARROW-674 - [Java] Support additional Timestamp timezone metadata
* ARROW-675 - [GLib] Update package metadata
* ARROW-676 - [java] move from MinorType to FieldType in ValueVectors to carry all the relevant type bits
* ARROW-679 - [Format] Change RecordBatch and Field length members from int to long
* ARROW-681 - [C++] Build Arrow on Windows with dynamically linked boost
* ARROW-684 - Python: More informative message when parquet-cpp but not parquet-arrow is available
* ARROW-688 - [C++] Use `CMAKE_INSTALL_INCLUDEDIR` for consistency
* ARROW-690 - Only send JIRA updates to issues@arrow.apache.org
* ARROW-700 - Add headroom interface for allocator.
* ARROW-706 - [GLib] Add package install document
* ARROW-707 - Python: All none-Pandas column should be converted to NullArray
* ARROW-708 - [C++] Some IPC code simplification, perf analysis
* ARROW-712 - [C++] Implement Array::Accept as inline visitor
* ARROW-719 - [GLib] Support prepared source archive release
* ARROW-724 - Add "How to Contribute" section to README
* ARROW-725 - [Format] Constant length list type
* ARROW-727 - [Python] Write memoryview-compatible objects in NativeFile.write with zero copy
* ARROW-728 - [C++/Python] Add arrow::Table function for removing a column
* ARROW-731 - [C++] Add shared library related versions to .pc
* ARROW-741 - [Python] Add Python 3.6 to Travis CI
* ARROW-743 - [C++] Consolidate unit tests for code in array.h
* ARROW-744 - [GLib] Re-add an assertion to `garrow_table_new()` test
* ARROW-745 - [C++] Allow use of system cpplint
* ARROW-746 - [GLib] Add `garrow_array_get_data_type()`
* ARROW-751 - [Python] Rename all Cython extensions to "private" status with leading underscore
* ARROW-752 - [Python] Construct pyarrow.DictionaryArray from boxed pyarrow array objects
* ARROW-754 - [GLib] Add `garrow_array_is_null()`
* ARROW-755 - [GLib] Add `garrow_array_get_value_type()`
* ARROW-758 - [C++] Fix compiler warnings on MSVC x64
* ARROW-761 - [Python] Add function to compute the total size of tensor payloads, including metadata and padding
* ARROW-763 - C++: Use `python-config` to find libpythonX.X.dylib
* ARROW-765 - [Python] Make generic ArrowException subclass value error
* ARROW-769 - [GLib] Support building without installed Arrow C++
* ARROW-770 - [C++] Move clang-tidy/format config files back to C++ source tree
* ARROW-774 - [GLib] Remove needless LICENSE.txt copy
* ARROW-775 - [Java] add simple constructors to value vectors
* ARROW-779 - [C++/Python] Raise exception if old metadata encountered
* ARROW-782 - [C++] Change struct to class for objects that meet the criteria in the Google style guide
* ARROW-788 - Possible nondeterminism in Tensor serialization code
* ARROW-795 - [C++] Combine `libarrow/libarrow_io/libarrow_ipc`
* ARROW-802 - [GLib] Add read examples
* ARROW-803 - [GLib] Update package repository URL
* ARROW-804 - [GLib] Update build document
* ARROW-806 - [GLib] Support add/remove a column from table
* ARROW-807 - [GLib] Update "Since" tag
* ARROW-808 - [GLib] Remove needless ignore entries
* ARROW-810 - [GLib] Remove io/ipc prefix
* ARROW-811 - [GLib] Add GArrowBuffer
* ARROW-815 - [Java] Allow for expanding underlying buffer size after allocation
* ARROW-816 - [C++] Use conda packages for RapidJSON, Flatbuffers to speed up builds
* ARROW-818 - [Python] Review public pyarrow. API completeness and update docs
* ARROW-820 - [C++] Build dependencies for Parquet library without arrow support
* ARROW-825 - [Python] Generalize `pyarrow.from_pylist` to accept any object implementing the PySequence protocol
* ARROW-827 - [Python] Variety of Parquet improvements to support Dask integration
* ARROW-828 - [CPP] Document new requirement (libboost-regex-dev) in README.md
* ARROW-832 - [C++] Upgrade thirdparty gtest to 1.8.0
* ARROW-833 - [Python] "Quickstart" build / environment setup guide for Python developers
* ARROW-841 - [Python] Add pyarrow build to Appveyor
* ARROW-844 - [Format] Revise format/README.md to reflect progress reaching a more complete specification
* ARROW-845 - [Python] Sync FindArrow.cmake changes from parquet-cpp
* ARROW-846 - [GLib] Add GArrowTensor, GArrowInt8Tensor and GArrowUInt8Tensor
* ARROW-848 - [Python] Improvements / fixes to conda quickstart guide
* ARROW-849 - [C++] Add optional `$ARROW_BUILD_TOOLCHAIN` environment variable option for configuring build environment
* ARROW-857 - [Python] Automate publishing Python documentation to arrow-site
* ARROW-860 - [C++] Decide if typed Tensor subclasses are worthwhile
* ARROW-861 - [Python] Move DEVELOPMENT.md to Sphinx docs
* ARROW-862 - [Python] Improve source build instructions in README
* ARROW-863 - [GLib] Use GBytes to implement zero-copy
* ARROW-864 - [GLib] Unify Array files
* ARROW-868 - [GLib] Use GBytes to reduce copy
* ARROW-871 - [GLib] Unify DataType files
* ARROW-876 - [GLib] Unify ArrayBuffer files
* ARROW-877 - [GLib] Add `garrow_array_get_null_bitmap()`
* ARROW-878 - [GLib] Add `garrow_binary_array_get_buffer()`
* ARROW-892 - [GLib] Fix GArrowTensor document
* ARROW-893 - Add GLib document to Web site
* ARROW-894 - [GLib] Add GArrowPoolBuffer
* ARROW-896 - [Docs] Add Jekyll plugin for including rendered Jupyter notebooks on website
* ARROW-898 - [C++] Expand metadata support to field level, provide for sharing instances of KeyValueMetadata
* ARROW-904 - [GLib] Simplify error check codes
* ARROW-907 - C++: Convenience construct Table from schema and arrays
* ARROW-908 - [GLib] Unify OutputStream files
* ARROW-910 - [C++] Write 0-length EOS indicator at end of stream
* ARROW-916 - [GLib] Add GArrowBufferOutputStream
* ARROW-917 - [GLib] Add GArrowBufferReader
* ARROW-918 - [GLib] Use GArrowBuffer for read
* ARROW-919 - [GLib] Use "id" to get type enum value from GArrowDataType
* ARROW-920 - [GLib] Add Lua examples
* ARROW-925 - [GLib] Fix GArrowBufferReader test
* ARROW-930 - javadoc generation fails with java 8
* ARROW-931 - [GLib] Reconstruct input stream

## New Feature

* ARROW-231 - C++: Add typed Resize to PoolBuffer
* ARROW-281 - [C++] IPC/RPC support on Win32 platforms
* ARROW-341 - [Python] Making libpyarrow available to third parties
* ARROW-452 - [C++/Python] Merge "Feather" file format implementation
* ARROW-459 - [C++] Implement IPC round trip for DictionaryArray, dictionaries shared across record batches
* ARROW-483 - [C++/Python] Provide access to `custom_metadata` Field attribute in IPC setting
* ARROW-491 - [C++] Add FixedWidthBinary type
* ARROW-493 - [C++] Allow in-memory array over 2^31 -1 elements but require splitting at IPC / RPC boundaries
* ARROW-502 - [C++/Python] Add MemoryPool implementation that logs allocation activity to std::cout
* ARROW-510 - Add integration tests for date and time types
* ARROW-520 - [C++] Add STL-compliant allocator that hooks into an arrow::MemoryPool
* ARROW-528 - [Python] Support `_metadata` or `_common_metadata` files when reading Parquet directories
* ARROW-534 - [C++] Add IPC tests for date/time types
* ARROW-539 - [Python] Support reading Parquet datasets with standard partition directory schemes
* ARROW-550 - [Format] Add a TensorMessage type
* ARROW-552 - [Python] Add scalar value support for Dictionary type
* ARROW-557 - [Python] Explicitly opt in to HDFS unit tests
* ARROW-568 - [C++] Add default implementations for TypeVisitor, ArrayVisitor methods that return NotImplemented
* ARROW-574 - Python: Add support for nested Python lists in Pandas conversion
* ARROW-576 - [C++] Complete round trip Union file/stream IPC tests
* ARROW-578 - [C++] Add CMake option to add custom $CXXFLAGS
* ARROW-598 - [Python] Add support for converting pyarrow.Buffer to a memoryview with zero copy
* ARROW-603 - [C++] Add RecordBatch::Validate method that at least checks that schema matches the array metadata
* ARROW-605 - [C++] Refactor generic ArrayLoader class, support work for Feather merge
* ARROW-606 - [C++] Upgrade to flatbuffers 1.6.0
* ARROW-608 - [Format] Days since epoch date type
* ARROW-610 - [C++] Win32 compatibility in file.cc
* ARROW-616 - [C++] Remove -g flag in release builds
* ARROW-618 - [Python] Implement support for DatetimeTZ custom type from pandas
* ARROW-620 - [C++] Add date/time support to JSON reader/writer for integration testing
* ARROW-621 - [C++] Implement an "inline visitor" template that enables visitor-pattern-like code without virtual function dispatch
* ARROW-625 - [C++] Add time unit to TimeType::ToString
* ARROW-626 - [Python] Enable pyarrow.BufferReader to read from any Python object implementing the buffer/memoryview protocol
* ARROW-632 - [Python] Add support for FixedWidthBinary type
* ARROW-635 - [C++] Add JSON read/write support for FixedWidthBinary
* ARROW-637 - [Format] Add time zone metadata to Timestamp type
* ARROW-656 - [C++] Implement IO interface that can read and write to a fixed-size mutable buffer
* ARROW-657 - [Python] Write and read tensors (with zero copy) into shared memory
* ARROW-658 - [C++] Implement in-memory arrow::Tensor objects
* ARROW-659 - [C++] Add multithreaded memcpy implementation (for hardware where it helps)
* ARROW-660 - [C++] Restore function that can read a complete encapsulated record batch message
* ARROW-661 - [C++] Add a Flatbuffer metadata type that supports array data over 2^31 - 1 elements
* ARROW-663 - [Java] Support additional Time metadata + vector value accessors
* ARROW-669 - [Python] Attach proper tzinfo when computing boxed scalars for TimestampArray
* ARROW-687 - [C++] Build and run full test suite in Appveyor
* ARROW-698 - [C++] Add options to StreamWriter/FileWriter to permit large record batches
* ARROW-701 - [Java] Support additional Date metadata
* ARROW-710 - [Python] Enable Feather APIs to read and write using Python file-like objects
* ARROW-717 - [C++] IPC zero-copy round trips for arrow::Tensor
* ARROW-718 - [Python] Expose arrow::Tensor with conversions to/from NumPy arrays
* ARROW-722 - [Python] pandas conversions for new date and time types/metadata
* ARROW-729 - [Java] Add vector type for 32-bit date as days since UNIX epoch
* ARROW-733 - [C++/Format] Change name of Fixed Width Binary to Fixed Size Binary for consistency
* ARROW-734 - [Python] Support for pyarrow on Windows / MSVC
* ARROW-735 - [C++] Developer instruction document for MSVC on Windows
* ARROW-737 - [C++] Support obtaining mutable slices of mutable buffers
* ARROW-768 - [Java] Change the "boxed" object representation of date and time types
* ARROW-771 - [Python] Add APIs for reading individual Parquet row groups
* ARROW-773 - [C++] Add function to create arrow::Table with column appended to existing table
* ARROW-865 - [Python] Verify Parquet roundtrips for new date/time types
* ARROW-880 - [GLib] Add `garrow_primitive_array_get_buffer()`
* ARROW-890 - [GLib] Add GArrowMutableBuffer
* ARROW-926 - Update KEYS to include wesm

## Task

* ARROW-52 - Set up project blog
* ARROW-670 - Arrow 0.3 release
* ARROW-672 - [Format] Bump metadata version for 0.3 release
* ARROW-748 - [Python] Pin runtime library versions in conda-forge packages to force upgrades
* ARROW-798 - [Docs] Publish Format Markdown documents somehow on arrow.apache.org
* ARROW-869 - [JS] Rename directory to js/
* ARROW-95 - Scaffold Main Documentation using asciidoc
* ARROW-98 - Java: API documentation

## Test

* ARROW-836 - Test for timedelta compat with pandas
* ARROW-927 - C++/Python: Add manylinux1 builds to Travis matrix

# Apache Arrow 0.2.0 (15 February 2017)

## Bug

* ARROW-112 - [C++]  Style fix for constants/enums
* ARROW-202 - [C++] Integrate with appveyor ci for windows support and get arrow building on windows
* ARROW-220 - [C++] Build conda artifacts in a build environment with better cross-linux ABI compatibility
* ARROW-224 - [C++] Address static linking of boost dependencies
* ARROW-230 - Python: Do not name modules like native ones (i.e. rename pyarrow.io)
* ARROW-239 - [Python] HdfsFile.read called with no arguments should read remainder of file
* ARROW-261 - [C++] Refactor BinaryArray/StringArray classes to not inherit from ListArray
* ARROW-275 - Add tests for UnionVector in Arrow File
* ARROW-294 - [C++] Do not use fopen / fclose / etc. methods for memory mapped file implementation
* ARROW-322 - [C++] Do not build HDFS IO interface optionally
* ARROW-323 - [Python] Opt-in to PyArrow parquet build rather than skipping silently on failure
* ARROW-334 - [Python] OS X rpath issues on some configurations
* ARROW-337 - UnionListWriter.list() is doing more than it should, this can cause data corruption
* ARROW-339 - Make `merge_arrow_pr` script work with Python 3
* ARROW-340 - [C++] Opening a writeable file on disk that already exists does not truncate to zero
* ARROW-342 - Set Python version on release
* ARROW-345 - libhdfs integration doesn't work for Mac
* ARROW-346 - Python API Documentation
* ARROW-348 - [Python] CMake build type should be configurable on the command line
* ARROW-349 - Six is missing as a requirement in the python setup.py
* ARROW-351 - Time type has no unit
* ARROW-354 - Connot compare an array of empty strings to another
* ARROW-357 - Default Parquet `chunk_size` of 64k is too small
* ARROW-358 - [C++] libhdfs can be in non-standard locations in some Hadoop distributions
* ARROW-362 - Python: Calling `to_pandas` on a table read from Parquet leaks memory
* ARROW-371 - Python: Table with null timestamp becomes float in pandas
* ARROW-375 - columns parameter in `parquet.read_table()` raises KeyError for valid column
* ARROW-384 - Align Java and C++ RecordBatch data and metadata layout
* ARROW-386 - [Java] Respect case of struct / map field names
* ARROW-387 - [C++] arrow::io::BufferReader does not permit shared memory ownership in zero-copy reads
* ARROW-390 - C++: CMake fails on json-integration-test with `ARROW_BUILD_TESTS=OFF`
* ARROW-392 - Fix string/binary integration tests
* ARROW-393 - [JAVA] JSON file reader fails to set the buffer size on String data vector
* ARROW-395 - Arrow file format writes record batches in reverse order.
* ARROW-398 - [Java] Java file format requires bitmaps of all 1's to be written when there are no nulls
* ARROW-399 - [Java] ListVector.loadFieldBuffers ignores the ArrowFieldNode length metadata
* ARROW-400 - [Java] ArrowWriter writes length 0 for Struct types
* ARROW-401 - [Java] Floating point vectors should do an approximate comparison in integration tests
* ARROW-402 - [Java] "refCnt gone negative" error in integration tests
* ARROW-403 - [JAVA] UnionVector: Creating a transfer pair doesn't transfer the schema to destination vector
* ARROW-404 - [Python] Closing an HdfsClient while there are still open file handles results in a crash
* ARROW-405 - [C++] Be less stringent about finding include/hdfs.h in `HADOOP_HOME`
* ARROW-406 - [C++] Large HDFS reads must utilize the set file buffer size when making RPCs
* ARROW-408 - [C++/Python] Remove defunct conda recipes
* ARROW-414 - [Java] "Buffer too large to resize to ..." error
* ARROW-420 - Align Date implementation between Java and C++
* ARROW-421 - [Python] Zero-copy buffers read by pyarrow::PyBytesReader must retain a reference to the parent PyBytes to avoid premature garbage collection issues
* ARROW-422 - C++: IPC should depend on `rapidjson_ep` if RapidJSON is vendored
* ARROW-429 - git-archive SHA-256 checksums are changing
* ARROW-433 - [Python] Date conversion is locale-dependent
* ARROW-434 - Segfaults and encoding issues in Python Parquet reads
* ARROW-435 - C++: Spelling mistake in `if(RAPIDJSON_VENDORED)`
* ARROW-437 - [C++] clang compiler warnings from overridden virtual functions
* ARROW-445 - C++: `arrow_ipc` is built before `arrow/ipc/Message_generated.h` was generated
* ARROW-447 - Python: Align scalar/pylist string encoding with pandas' one.
* ARROW-455 - [C++] BufferOutputStream dtor does not call Close()
* ARROW-469 - C++: Add option so that resize doesn't decrease the capacity
* ARROW-481 - [Python] Fix Python 2.7 regression in patch for PARQUET-472
* ARROW-486 - [C++] arrow::io::MemoryMappedFile can't be casted to arrow::io::FileInterface
* ARROW-487 - Python: ConvertTableToPandas segfaults if ObjectBlock::Write fails
* ARROW-494 - [C++] When MemoryMappedFile is destructed, memory is unmapped even if buffer referecnes still exist
* ARROW-499 - Update file serialization to use streaming serialization format
* ARROW-505 - [C++] Fix compiler warnings in release mode
* ARROW-511 - [Python] List[T] conversions not implemented for single arrays
* ARROW-513 - [C++] Fix Appveyor build
* ARROW-519 - [C++] Missing vtable in libarrow.dylib on Xcode 6.4
* ARROW-523 - Python: Account for changes in PARQUET-834
* ARROW-533 - [C++] arrow::TimestampArray / TimeArray has a broken constructor
* ARROW-535 - [Python] Add type mapping for `NPY_LONGLONG`
* ARROW-537 - [C++] StringArray/BinaryArray comparisons may be incorrect when values with non-zero length are null
* ARROW-540 - [C++] Fix build in aftermath of ARROW-33
* ARROW-543 - C++: Lazily computed `null_counts` counts number of non-null entries
* ARROW-544 - [C++] ArrayLoader::LoadBinary fails for length-0 arrays
* ARROW-545 - [Python] Ignore files without .parq or .parquet prefix when reading directory of files
* ARROW-548 - [Python] Add nthreads option to `pyarrow.Filesystem.read_parquet`
* ARROW-551 - C++: Construction of Column with nullptr Array segfaults
* ARROW-556 - [Integration] Can not run Integration tests if different cpp build path
* ARROW-561 - Update java & python dependencies to improve downstream packaging experience

## Improvement

* ARROW-189 - C++: Use ExternalProject to build thirdparty dependencies
* ARROW-191 - Python: Provide infrastructure for manylinux1 wheels
* ARROW-328 - [C++] Return `shared_ptr` by value instead of const-ref?
* ARROW-330 - [C++] CMake functions to simplify shared / static library configuration
* ARROW-333 - Make writers update their internal schema even when no data is written.
* ARROW-335 - Improve Type apis and toString() by encapsulating flatbuffers better
* ARROW-336 - Run Apache Rat in Travis builds
* ARROW-338 - [C++] Refactor IPC vector "loading" and "unloading" to be based on cleaner visitor pattern
* ARROW-350 - Add Kerberos support to HDFS shim
* ARROW-355 - Add tests for serialising arrays of empty strings to Parquet
* ARROW-356 - Add documentation about reading Parquet
* ARROW-360 - C++: Add method to shrink PoolBuffer using realloc
* ARROW-361 - Python: Support reading a column-selection from Parquet files
* ARROW-365 - Python: Provide `Array.to_pandas()`
* ARROW-366 - [java] implement Dictionary vector
* ARROW-374 - Python: clarify unicode vs. binary in API
* ARROW-379 - Python: Use `setuptools_scm`/`setuptools_scm_git_archive` to provide the version number
* ARROW-380 - [Java] optimize null count when serializing vectors.
* ARROW-382 - Python: Extend API documentation
* ARROW-396 - Python: Add pyarrow.schema.Schema.equals
* ARROW-409 - Python: Change `pyarrow.Table.dataframe_from_batches` API to create Table instead
* ARROW-411 - [Java] Move Intergration.compare and Intergration.compareSchemas to a public utils class
* ARROW-423 - C++: Define `BUILD_BYPRODUCTS` in external project to support non-make CMake generators
* ARROW-425 - Python: Expose a C function to convert arrow::Table to pyarrow.Table
* ARROW-426 - Python: Conversion from pyarrow.Array to a Python list
* ARROW-430 - Python: Better version handling
* ARROW-432 - [Python] Avoid unnecessary memory copy in `to_pandas` conversion by using low-level pandas internals APIs
* ARROW-450 - Python: Fixes for PARQUET-818
* ARROW-457 - Python: Better control over memory pool
* ARROW-458 - Python: Expose jemalloc MemoryPool
* ARROW-463 - C++: Support jemalloc 4.x
* ARROW-466 - C++: ExternalProject for jemalloc
* ARROW-468 - Python: Conversion of nested data in pd.DataFrames to/from Arrow structures
* ARROW-474 - Create an Arrow streaming file fomat
* ARROW-479 - Python: Test for expected schema in Pandas conversion
* ARROW-485 - [Java] Users are required to initialize VariableLengthVectors.offsetVector before calling VariableLengthVectors.mutator.getSafe
* ARROW-490 - Python: Update manylinux1 build scripts
* ARROW-524 - [java] provide apis to access nested vectors and buffers
* ARROW-525 - Python: Add more documentation to the package
* ARROW-529 - Python: Add jemalloc and Python 3.6 to manylinux1 build
* ARROW-546 - Python: Account for changes in PARQUET-867
* ARROW-553 - C++: Faster valid bitmap building

## New Feature

* ARROW-108 - [C++] Add IPC round trip for union types
* ARROW-221 - Add switch for writing Parquet 1.0 compatible logical types
* ARROW-227 - [C++/Python] Hook `arrow_io` generic reader / writer interface into `arrow_parquet`
* ARROW-228 - [Python] Create an Arrow-cpp-compatible interface for reading bytes from Python file-like objects
* ARROW-243 - [C++] Add "driver" option to HdfsClient to choose between libhdfs and libhdfs3 at runtime
* ARROW-303 - [C++] Also build static libraries for leaf libraries
* ARROW-312 - [Python] Provide Python API to read/write the Arrow IPC file format
* ARROW-317 - [C++] Implement zero-copy Slice method on arrow::Buffer that retains reference to parent
* ARROW-33 - C++: Implement zero-copy array slicing
* ARROW-332 - [Python] Add helper function to convert RecordBatch to pandas.DataFrame
* ARROW-363 - Set up Java/C++ integration test harness
* ARROW-369 - [Python] Add ability to convert multiple record batches at once to pandas
* ARROW-373 - [C++] Implement C++ version of JSON file format for testing
* ARROW-377 - Python: Add support for conversion of Pandas.Categorical
* ARROW-381 - [C++] Simplify primitive array type builders to use a default type singleton
* ARROW-383 - [C++] Implement C++ version of ARROW-367 integration test validator
* ARROW-389 - Python: Write Parquet files to pyarrow.io.NativeFile objects
* ARROW-394 - Add integration tests for boolean, list, struct, and other basic types
* ARROW-410 - [C++] Add Flush method to arrow::io::OutputStream
* ARROW-415 - C++: Add Equals implementation to compare Tables
* ARROW-416 - C++: Add Equals implementation to compare Columns
* ARROW-417 - C++: Add Equals implementation to compare ChunkedArrays
* ARROW-418 - [C++] Consolidate array container and builder code, remove arrow/types
* ARROW-419 - [C++] Promote util/{status.h, buffer.h, memory-pool.h} to top level of arrow/ source directory
* ARROW-427 - [C++] Implement dictionary-encoded array container
* ARROW-428 - [Python] Deserialize from Arrow record batches to pandas in parallel using a thread pool
* ARROW-438 - [Python] Concatenate Table instances with equal schemas
* ARROW-440 - [C++] Support pkg-config
* ARROW-441 - [Python] Expose Arrow's file and memory map classes as NativeFile subclasses
* ARROW-442 - [Python] Add public Python API to inspect Parquet file metadata
* ARROW-444 - [Python] Avoid unnecessary memory copies from use of `PyBytes_*` C APIs
* ARROW-449 - Python: Conversion from pyarrow.{Table,RecordBatch} to a Python dict
* ARROW-456 - C++: Add jemalloc based MemoryPool
* ARROW-461 - [Python] Implement conversion between arrow::DictionaryArray and pandas.Categorical
* ARROW-467 - [Python] Run parquet-cpp unit tests in Travis CI
* ARROW-470 - [Python] Add "FileSystem" abstraction to access directories of files in a uniform way
* ARROW-471 - [Python] Enable ParquetFile to pass down separately-obtained file metadata
* ARROW-472 - [Python] Expose parquet::{SchemaDescriptor, ColumnDescriptor}::Equals
* ARROW-475 - [Python] High level support for reading directories of Parquet files (as a single Arrow table) from supported file system interfaces
* ARROW-476 - [Integration] Add integration tests for Binary / Varbytes type
* ARROW-477 - [Java] Add support for second/microsecond/nanosecond timestamps in-memory and in IPC/JSON layer
* ARROW-478 - [Python] Accept a PyBytes object in the pyarrow.io.BufferReader ctor
* ARROW-484 - Add more detail about what of technology can be found in the Arrow implementations to README
* ARROW-495 - [C++] Add C++ implementation of streaming serialized format
* ARROW-497 - [Java] Integration test harness for streaming format
* ARROW-498 - [C++] Integration test harness for streaming format
* ARROW-503 - [Python] Interface to streaming binary format
* ARROW-508 - [C++] Make file/memory-mapped file interfaces threadsafe
* ARROW-509 - [Python] Add support for PARQUET-835 (parallel column reads)
* ARROW-512 - C++: Add method to check for primitive types
* ARROW-514 - [Python] Accept pyarrow.io.Buffer as input to StreamReader, FileReader classes
* ARROW-515 - [Python] Add StreamReader/FileReader methods that read all record batches as a Table
* ARROW-521 - [C++/Python] Track peak memory use in default MemoryPool
* ARROW-531 - Python: Document jemalloc, extend Pandas section, add Getting Involved
* ARROW-538 - [C++] Set up AddressSanitizer (ASAN) builds
* ARROW-547 - [Python] Expose Array::Slice and RecordBatch::Slice
* ARROW-81 - [Format] Add a Category logical type (distinct from dictionary-encoding)

## Task

* ARROW-268 - [C++] Flesh out union implementation to have all required methods for IPC
* ARROW-327 - [Python] Remove conda builds from Travis CI processes
* ARROW-353 - Arrow release 0.2
* ARROW-359 - Need to document `ARROW_LIBHDFS_DIR`
* ARROW-367 - [java] converter csv/json <=> Arrow file format for Integration tests
* ARROW-368 - Document use of `LD_LIBRARY_PATH` when using Python
* ARROW-372 - Create JSON arrow file format for integration tests
* ARROW-506 - Implement Arrow Echo server for integration testing
* ARROW-527 - clean drill-module.conf file
* ARROW-558 - Add KEYS files
* ARROW-96 - C++: API documentation using Doxygen
* ARROW-97 - Python: API documentation via sphinx-apidoc

# Apache Arrow 0.1.0 (7 October 2016)

## Bug

* ARROW-103 - Missing patterns from .gitignore
* ARROW-104 - Update Layout.md based on discussion on the mailing list
* ARROW-105 - Unit tests fail if assertions are disabled
* ARROW-113 - TestValueVector test fails if cannot allocate 2GB of memory
* ARROW-16 - Building cpp issues on XCode 7.2.1
* ARROW-17 - Set some vector fields to default access level for Drill compatibility
* ARROW-18 - Fix bug with decimal precision and scale
* ARROW-185 - [C++] Make sure alignment and memory padding conform to spec
* ARROW-188 - Python: Add numpy as install requirement
* ARROW-193 - For the instruction, typos "int his" should be "in this"
* ARROW-194 - C++: Allow read-only memory mapped source
* ARROW-200 - [Python] Convert Values String looks like it has incorrect error handling
* ARROW-209 - [C++] Broken builds: llvm.org apt repos are unavailable
* ARROW-210 - [C++] Tidy up the type system a little bit
* ARROW-211 - Several typos/errors in Layout.md examples
* ARROW-217 - Fix Travis w.r.t conda 4.1.0 changes
* ARROW-219 - [C++] Passed `CMAKE_CXX_FLAGS` are being dropped, fix compiler warnings
* ARROW-223 - Do not link against libpython
* ARROW-225 - [C++/Python] master Travis CI build is broken
* ARROW-244 - [C++] Some global APIs of IPC module should be visible to the outside
* ARROW-246 - [Java] UnionVector doesn't call allocateNew() when creating it's vectorType
* ARROW-247 - [C++] Missing explicit destructor in RowBatchReader causes an incomplete type error
* ARROW-250 - Fix for ARROW-246 may cause memory leaks
* ARROW-259 - Use flatbuffer fields in java implementation
* ARROW-265 - Negative decimal values have wrong padding
* ARROW-266 - [C++] Fix the broken build
* ARROW-274 - Make the MapVector nullable
* ARROW-278 - [Format] Struct type name consistency in implementations and metadata
* ARROW-283 - [C++] Update `arrow_parquet` to account for API changes in PARQUET-573
* ARROW-284 - [C++] Triage builds by disabling Arrow-Parquet module
* ARROW-287 - [java] Make nullable vectors use a BitVecor instead of UInt1Vector for bits
* ARROW-297 - Fix Arrow pom for release
* ARROW-304 - NullableMapReaderImpl.isSet() always returns true
* ARROW-308 - UnionListWriter.setPosition() should not call startList()
* ARROW-309 - Types.getMinorTypeForArrowType() does not work for Union type
* ARROW-313 - XCode 8.0 breaks builds
* ARROW-314 - JSONScalar is unnecessary and unused.
* ARROW-320 - ComplexCopier.copy(FieldReader, FieldWriter) should not start a list if reader is not set
* ARROW-321 - Fix Arrow licences
* ARROW-36 - Remove fixVersions from patch tool (until we have them)
* ARROW-46 - Port DRILL-4410 to Arrow
* ARROW-5 - Error when run maven install
* ARROW-51 - Move ValueVector test from Drill project
* ARROW-55 - Python: fix legacy Python (2.7) tests and add to Travis CI
* ARROW-62 - Format: Are the nulls bits 0 or 1 for null values?
* ARROW-63 - C++: ctest fails if Python 3 is the active Python interpreter
* ARROW-65 - Python: FindPythonLibsNew does not work in a virtualenv
* ARROW-69 - Change permissions for assignable users
* ARROW-72 - FindParquet searches for non-existent header
* ARROW-75 - C++: Fix handling of empty strings
* ARROW-77 - C++: conform null bit interpretation to match ARROW-62
* ARROW-80 - Segmentation fault on len(Array) for empty arrays
* ARROW-88 - C++: Refactor given PARQUET-572
* ARROW-93 - XCode 7.3 breaks builds
* ARROW-94 - Expand list example to clarify null vs empty list

## Improvement

* ARROW-10 - Fix mismatch of javadoc names and method parameters
* ARROW-15 - Fix a naming typo for memory.AllocationManager.AllocationOutcome
* ARROW-190 - Python: Provide installable sdist builds
* ARROW-199 - [C++] Refine third party dependency
* ARROW-206 - [C++] Expose an equality API for arrays that compares a range of slots on two arrays
* ARROW-212 - [C++] Clarify the fact that PrimitiveArray is now abstract class
* ARROW-213 - Exposing static arrow build
* ARROW-218 - Add option to use GitHub API token via environment variable when merging PRs
* ARROW-234 - [C++] Build with libhdfs support in `arrow_io` in conda builds
* ARROW-238 - C++: InternalMemoryPool::Free() should throw an error when there is insufficient allocated memory
* ARROW-245 - [Format] Clarify Arrow's relationship with big endian platforms
* ARROW-252 - Add implementation guidelines to the documentation
* ARROW-253 - Int types should only have width of 8*2^n (8, 16, 32, 64)
* ARROW-254 - Remove Bit type as it is redundant with boolean
* ARROW-255 - Finalize Dictionary representation
* ARROW-256 - Add versioning to the arrow spec.
* ARROW-257 - Add a typeids Vector to Union type
* ARROW-264 - Create an Arrow File format
* ARROW-270 - [Format] Define more generic Interval logical type
* ARROW-271 - Update Field structure to be more explicit
* ARROW-279 - rename vector module to arrow-vector for consistency
* ARROW-280 - [C++] Consolidate file and shared memory IO interfaces
* ARROW-285 - Allow for custom flatc compiler
* ARROW-286 - Build thirdparty dependencies in parallel
* ARROW-289 - Install test-util.h
* ARROW-290 - Specialize alloc() in ArrowBuf
* ARROW-292 - [Java] Upgrade Netty to 4.041
* ARROW-299 - Use absolute namespace in macros
* ARROW-305 - Add compression and `use_dictionary` options to Parquet interface
* ARROW-306 - Add option to pass cmake arguments via environment variable
* ARROW-315 - Finalize timestamp type
* ARROW-319 - Add canonical Arrow Schema json representation
* ARROW-324 - Update arrow metadata diagram
* ARROW-325 - make TestArrowFile not dependent on timezone
* ARROW-50 - C++: Enable library builds for 3rd-party users without having to build thirdparty googletest
* ARROW-54 - Python: rename package to "pyarrow"
* ARROW-64 - Add zsh support to C++ build scripts
* ARROW-66 - Maybe some missing steps in installation guide
* ARROW-68 - Update `setup_build_env` and third-party script to be more userfriendly
* ARROW-71 - C++: Add script to run clang-tidy on codebase
* ARROW-73 - Support CMake 2.8
* ARROW-78 - C++: Add constructor for DecimalType
* ARROW-79 - Python: Add benchmarks
* ARROW-8 - Set up Travis CI
* ARROW-85 - C++: memcmp can be avoided in Equal when comparing with the same Buffer
* ARROW-86 - Python: Implement zero-copy Arrow-to-Pandas conversion
* ARROW-87 - Implement Decimal schema conversion for all ways supported in Parquet
* ARROW-89 - Python: Add benchmarks for Arrow<->Pandas conversion
* ARROW-9 - Rename some unchanged "Drill" to "Arrow"
* ARROW-91 - C++: First draft of an adapter class for parquet-cpp's ParquetFileReader that produces Arrow table/row batch objects

## New Feature

* ARROW-100 - [C++] Computing RowBatch size
* ARROW-106 - Add IPC round trip for string types (string, char, varchar, binary)
* ARROW-107 - [C++] add ipc round trip for struct types
* ARROW-13 - Add PR merge tool similar to that used in Parquet
* ARROW-19 - C++: Externalize memory allocations and add a MemoryPool abstract interface to builder classes
* ARROW-197 - [Python] Add conda dev recipe for pyarrow
* ARROW-2 - Post Simple Website
* ARROW-20 - C++: Add null count member to Array containers, remove nullable member
* ARROW-201 - C++: Initial ParquetWriter implementation
* ARROW-203 - Python: Basic filename based Parquet read/write
* ARROW-204 - [Python] Automate uploading conda build artifacts for libarrow and pyarrow
* ARROW-21 - C++: Add in-memory schema metadata container
* ARROW-214 - C++: Add String support to Parquet I/O
* ARROW-215 - C++: Support other integer types in Parquet I/O
* ARROW-22 - C++: Add schema adapter routines for converting flat Parquet schemas to in-memory Arrow schemas
* ARROW-222 - [C++] Create prototype file-like interface to HDFS (via libhdfs) and begin defining more general IO interface for Arrow data adapters
* ARROW-23 - C++: Add logical "Column" container for chunked data
* ARROW-233 - [C++] Add visibility defines for limiting shared library symbol visibility
* ARROW-236 - [Python] Enable Parquet read/write to work with HDFS file objects
* ARROW-237 - [C++] Create Arrow specializations of Parquet allocator and read interfaces
* ARROW-24 - C++: Add logical "Table" container
* ARROW-242 - C++/Python: Support Timestamp Data Type
* ARROW-26 - C++: Add developer instructions for building parquet-cpp integration
* ARROW-262 - [Format] Add a new format document for metadata and logical types for messaging and IPC / on-wire/file representations
* ARROW-267 - [C++] C++ implementation of file-like layout for RPC / IPC
* ARROW-28 - C++: Add google/benchmark to the 3rd-party build toolchain
* ARROW-293 - [C++] Implementations of IO interfaces for operating system files
* ARROW-296 - [C++] Remove `arrow_parquet` C++ module and related parts of build system
* ARROW-3 - Post Initial Arrow Format Spec
* ARROW-30 - Python: pandas/NumPy to/from Arrow conversion routines
* ARROW-301 - [Format] Add some form of user field metadata to IPC schemas
* ARROW-302 - [Python] Add support to use the Arrow file format with file-like objects
* ARROW-31 - Python: basic PyList <-> Arrow marshaling code
* ARROW-318 - [Python] Revise README to reflect current state of project
* ARROW-37 - C++: Represent boolean array data in bit-packed form
* ARROW-4 - Initial Arrow CPP Implementation
* ARROW-42 - Python: Add to Travis CI build
* ARROW-43 - Python: Add rudimentary console `__repr__` for array types
* ARROW-44 - Python: Implement basic object model for scalar values (i.e. results of `arrow_arr[i]`)
* ARROW-48 - Python: Add Schema object wrapper
* ARROW-49 - Python: Add Column and Table wrapper interface
* ARROW-53 - Python: Fix RPATH and add source installation instructions
* ARROW-56 - Format: Specify LSB bit ordering in bit arrays
* ARROW-57 - Format: Draft data headers IDL for data interchange
* ARROW-58 - Format: Draft type metadata ("schemas") IDL
* ARROW-59 - Python: Boolean data support for builtin data structures
* ARROW-60 - C++: Struct type builder API
* ARROW-67 - C++: Draft type metadata conversion to/from IPC representation
* ARROW-7 - Add Python library build toolchain
* ARROW-70 - C++: Add "lite" DCHECK macros used in parquet-cpp
* ARROW-76 - Revise format document to include null count, defer non-nullable arrays to the domain of metadata
* ARROW-82 - C++: Implement IPC exchange for List types
* ARROW-90 - Apache Arrow cpp code does not support power architecture
* ARROW-92 - C++: Arrow to Parquet Schema conversion

## Task

* ARROW-1 - Import Initial Codebase
* ARROW-101 - Fix java warnings emitted by java compiler
* ARROW-102 - travis-ci support for java project
* ARROW-11 - Mirror JIRA activity to dev@arrow.apache.org
* ARROW-14 - Add JIRA components
* ARROW-251 - [C++] Expose APIs for getting code and message of the status
* ARROW-272 - Arrow release 0.1
* ARROW-298 - create release scripts
* ARROW-35 - Add a short call-to-action / how-to-get-involved to the main README.md

## Test

* ARROW-260 - TestValueVector.testFixedVectorReallocation and testVariableVectorReallocation are flaky
* ARROW-83 - Add basic test infrastructure for DecimalType


