<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Apache Arrow 0.3.0 (5 May 2017)

## Bug

* ARROW-109 - [C++] Investigate recursive data types limit in flatbuffers
* ARROW-208 - Add checkstyle policy to java project
* ARROW-347 - Add method to pass CallBack when creating a transfer pair
* ARROW-413 - DATE type is not specified clearly
* ARROW-431 - [Python] Review GIL release and acquisition in to_pandas conversion
* ARROW-443 - [Python] Support for converting from strided pandas data in Table.from_pandas
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
* ARROW-619 - Python: Fix typos in setup.py args and LD_LIBRARY_PATH
* ARROW-623 - segfault with __repr__ of empty Field
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
* ARROW-697 - [Java] Raise appropriate exceptions when encountering large (> INT32_MAX) record batches
* ARROW-699 - [C++] Arrow dynamic libraries are missed on run of unit tests on Windows
* ARROW-702 - Fix BitVector.copyFromSafe to reAllocate instead of returning false
* ARROW-703 - Fix issue where setValueCount(0) doesn’t work in the case that we’ve shipped vectors across the wire
* ARROW-704 - Fix bad import caused by conflicting changes
* ARROW-709 - [C++] Restore type comparator for DecimalType
* ARROW-713 - [C++] Fix linking issue with ipc benchmark
* ARROW-715 - Python: Explicit pandas import makes it a hard requirement
* ARROW-716 - error building arrow/python
* ARROW-720 - [java] arrow should not have a dependency on slf4j bridges in compile
* ARROW-723 - Arrow freezes on write if chunk_size=0
* ARROW-726 - [C++] PyBuffer dtor may segfault if constructor passed an object not exporting buffer protocol
* ARROW-732 - Schema comparison bugs in struct and union types
* ARROW-736 - [Python] Mixed-type object DataFrame columns should not silently coerce to an Arrow type by default
* ARROW-738 - [Python] Fix manylinux1 packaging
* ARROW-739 - Parallel build fails non-deterministically.
* ARROW-740 - FileReader fails for large objects
* ARROW-747 - [C++] Fix spurious warning caused by passing dl to add_dependencies
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
* ARROW-821 - [Python] Extra file _table_api.h generated during Python build process
* ARROW-822 - [Python] StreamWriter fails to open with socket as sink
* ARROW-826 - Compilation error on Mac with -DARROW_PYTHON=on
* ARROW-829 - Python: Parquet: Dictionary encoding is deactivated if column-wise compression was selected
* ARROW-830 - Python: jemalloc is not anymore publicly exposed
* ARROW-839 - [C++] Portable alternative to PyDate_to_ms function
* ARROW-847 - C++: BUILD_BYPRODUCTS not specified anymore for gtest
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
* ARROW-933 - [Python] arrow_python bindings have debug print statement
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
* ARROW-580 - C++: Also provide jemalloc_X targets if only a static or shared version is found
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
* ARROW-688 - [C++] Use CMAKE_INSTALL_INCLUDEDIR for consistency
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
* ARROW-744 - [GLib] Re-add an assertion to garrow_table_new() test
* ARROW-745 - [C++] Allow use of system cpplint
* ARROW-746 - [GLib] Add garrow_array_get_data_type()
* ARROW-751 - [Python] Rename all Cython extensions to "private" status with leading underscore
* ARROW-752 - [Python] Construct pyarrow.DictionaryArray from boxed pyarrow array objects
* ARROW-754 - [GLib] Add garrow_array_is_null()
* ARROW-755 - [GLib] Add garrow_array_get_value_type()
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
* ARROW-795 - [C++] Combine libarrow/libarrow_io/libarrow_ipc
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
* ARROW-825 - [Python] Generalize pyarrow.from_pylist to accept any object implementing the PySequence protocol
* ARROW-827 - [Python] Variety of Parquet improvements to support Dask integration
* ARROW-828 - [CPP] Document new requirement (libboost-regex-dev) in README.md
* ARROW-832 - [C++] Upgrade thirdparty gtest to 1.8.0
* ARROW-833 - [Python] "Quickstart" build / environment setup guide for Python developers
* ARROW-841 - [Python] Add pyarrow build to Appveyor
* ARROW-844 - [Format] Revise format/README.md to reflect progress reaching a more complete specification
* ARROW-845 - [Python] Sync FindArrow.cmake changes from parquet-cpp
* ARROW-846 - [GLib] Add GArrowTensor, GArrowInt8Tensor and GArrowUInt8Tensor
* ARROW-848 - [Python] Improvements / fixes to conda quickstart guide
* ARROW-849 - [C++] Add optional $ARROW_BUILD_TOOLCHAIN environment variable option for configuring build environment
* ARROW-857 - [Python] Automate publishing Python documentation to arrow-site
* ARROW-860 - [C++] Decide if typed Tensor subclasses are worthwhile
* ARROW-861 - [Python] Move DEVELOPMENT.md to Sphinx docs
* ARROW-862 - [Python] Improve source build instructions in README
* ARROW-863 - [GLib] Use GBytes to implement zero-copy
* ARROW-864 - [GLib] Unify Array files
* ARROW-868 - [GLib] Use GBytes to reduce copy
* ARROW-871 - [GLib] Unify DataType files
* ARROW-876 - [GLib] Unify ArrayBuffer files
* ARROW-877 - [GLib] Add garrow_array_get_null_bitmap()
* ARROW-878 - [GLib] Add garrow_binary_array_get_buffer()
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
* ARROW-483 - [C++/Python] Provide access to "custom_metadata" Field attribute in IPC setting
* ARROW-491 - [C++] Add FixedWidthBinary type
* ARROW-493 - [C++] Allow in-memory array over 2^31 -1 elements but require splitting at IPC / RPC boundaries
* ARROW-502 - [C++/Python] Add MemoryPool implementation that logs allocation activity to std::cout
* ARROW-510 - Add integration tests for date and time types
* ARROW-520 - [C++] Add STL-compliant allocator that hooks into an arrow::MemoryPool
* ARROW-528 - [Python] Support _metadata or _common_metadata files when reading Parquet directories
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
* ARROW-880 - [GLib] Add garrow_primitive_array_get_buffer()
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
* ARROW-339 - Make merge_arrow_pr script work with Python 3
* ARROW-340 - [C++] Opening a writeable file on disk that already exists does not truncate to zero
* ARROW-342 - Set Python version on release
* ARROW-345 - libhdfs integration doesn't work for Mac
* ARROW-346 - Python API Documentation
* ARROW-348 - [Python] CMake build type should be configurable on the command line
* ARROW-349 - Six is missing as a requirement in the python setup.py
* ARROW-351 - Time type has no unit
* ARROW-354 - Connot compare an array of empty strings to another
* ARROW-357 - Default Parquet chunk_size of 64k is too small
* ARROW-358 - [C++] libhdfs can be in non-standard locations in some Hadoop distributions
* ARROW-362 - Python: Calling to_pandas on a table read from Parquet leaks memory
* ARROW-371 - Python: Table with null timestamp becomes float in pandas
* ARROW-375 - columns parameter in parquet.read_table() raises KeyError for valid column
* ARROW-384 - Align Java and C++ RecordBatch data and metadata layout
* ARROW-386 - [Java] Respect case of struct / map field names
* ARROW-387 - [C++] arrow::io::BufferReader does not permit shared memory ownership in zero-copy reads
* ARROW-390 - C++: CMake fails on json-integration-test with ARROW_BUILD_TESTS=OFF
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
* ARROW-405 - [C++] Be less stringent about finding include/hdfs.h in HADOOP_HOME
* ARROW-406 - [C++] Large HDFS reads must utilize the set file buffer size when making RPCs
* ARROW-408 - [C++/Python] Remove defunct conda recipes
* ARROW-414 - [Java] "Buffer too large to resize to ..." error
* ARROW-420 - Align Date implementation between Java and C++
* ARROW-421 - [Python] Zero-copy buffers read by pyarrow::PyBytesReader must retain a reference to the parent PyBytes to avoid premature garbage collection issues
* ARROW-422 - C++: IPC should depend on rapidjson_ep if RapidJSON is vendored
* ARROW-429 - git-archive SHA-256 checksums are changing
* ARROW-433 - [Python] Date conversion is locale-dependent
* ARROW-434 - Segfaults and encoding issues in Python Parquet reads
* ARROW-435 - C++: Spelling mistake in if(RAPIDJSON_VENDORED)
* ARROW-437 - [C++] clang compiler warnings from overridden virtual functions
* ARROW-445 - C++: arrow_ipc is built before arrow/ipc/Message_generated.h was generated
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
* ARROW-535 - [Python] Add type mapping for NPY_LONGLONG
* ARROW-537 - [C++] StringArray/BinaryArray comparisons may be incorrect when values with non-zero length are null
* ARROW-540 - [C++] Fix build in aftermath of ARROW-33
* ARROW-543 - C++: Lazily computed null_counts counts number of non-null entries
* ARROW-544 - [C++] ArrayLoader::LoadBinary fails for length-0 arrays
* ARROW-545 - [Python] Ignore files without .parq or .parquet prefix when reading directory of files
* ARROW-548 - [Python] Add nthreads option to pyarrow.Filesystem.read_parquet
* ARROW-551 - C++: Construction of Column with nullptr Array segfaults
* ARROW-556 - [Integration] Can not run Integration tests if different cpp build path
* ARROW-561 - Update java & python dependencies to improve downstream packaging experience

## Improvement

* ARROW-189 - C++: Use ExternalProject to build thirdparty dependencies
* ARROW-191 - Python: Provide infrastructure for manylinux1 wheels
* ARROW-328 - [C++] Return shared_ptr by value instead of const-ref?
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
* ARROW-365 - Python: Provide Array.to_pandas()
* ARROW-366 - [java] implement Dictionary vector
* ARROW-374 - Python: clarify unicode vs. binary in API
* ARROW-379 - Python: Use setuptools_scm/setuptools_scm_git_archive to provide the version number
* ARROW-380 - [Java] optimize null count when serializing vectors.
* ARROW-382 - Python: Extend API documentation
* ARROW-396 - Python: Add pyarrow.schema.Schema.equals
* ARROW-409 - Python: Change pyarrow.Table.dataframe_from_batches API to create Table instead
* ARROW-411 - [Java] Move Intergration.compare and Intergration.compareSchemas to a public utils class
* ARROW-423 - C++: Define BUILD_BYPRODUCTS in external project to support non-make CMake generators
* ARROW-425 - Python: Expose a C function to convert arrow::Table to pyarrow.Table
* ARROW-426 - Python: Conversion from pyarrow.Array to a Python list
* ARROW-430 - Python: Better version handling
* ARROW-432 - [Python] Avoid unnecessary memory copy in to_pandas conversion by using low-level pandas internals APIs
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
* ARROW-227 - [C++/Python] Hook arrow_io generic reader / writer interface into arrow_parquet
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
* ARROW-444 - [Python] Avoid unnecessary memory copies from use of PyBytes_* C APIs
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
* ARROW-359 - Need to document ARROW_LIBHDFS_DIR
* ARROW-367 - [java] converter csv/json <=> Arrow file format for Integration tests
* ARROW-368 - Document use of LD_LIBRARY_PATH when using Python
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
* ARROW-219 - [C++] Passed CMAKE_CXX_FLAGS are being dropped, fix compiler warnings
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
* ARROW-283 - [C++] Update arrow_parquet to account for API changes in PARQUET-573
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
* ARROW-234 - [C++] Build with libhdfs support in arrow_io in conda builds
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
* ARROW-305 - Add compression and use_dictionary options to Parquet interface
* ARROW-306 - Add option to pass cmake arguments via environment variable
* ARROW-315 - Finalize timestamp type
* ARROW-319 - Add canonical Arrow Schema json representation
* ARROW-324 - Update arrow metadata diagram
* ARROW-325 - make TestArrowFile not dependent on timezone
* ARROW-50 - C++: Enable library builds for 3rd-party users without having to build thirdparty googletest
* ARROW-54 - Python: rename package to "pyarrow"
* ARROW-64 - Add zsh support to C++ build scripts
* ARROW-66 - Maybe some missing steps in installation guide
* ARROW-68 - Update setup_build_env and third-party script to be more userfriendly
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
* ARROW-296 - [C++] Remove arrow_parquet C++ module and related parts of build system
* ARROW-3 - Post Initial Arrow Format Spec
* ARROW-30 - Python: pandas/NumPy to/from Arrow conversion routines
* ARROW-301 - [Format] Add some form of user field metadata to IPC schemas
* ARROW-302 - [Python] Add support to use the Arrow file format with file-like objects
* ARROW-31 - Python: basic PyList <-> Arrow marshaling code
* ARROW-318 - [Python] Revise README to reflect current state of project
* ARROW-37 - C++: Represent boolean array data in bit-packed form
* ARROW-4 - Initial Arrow CPP Implementation
* ARROW-42 - Python: Add to Travis CI build
* ARROW-43 - Python: Add rudimentary console __repr__ for array types
* ARROW-44 - Python: Implement basic object model for scalar values (i.e. results of arrow_arr[i])
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
