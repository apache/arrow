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

# Apache Arrow 0.17.0 (16 April 2020)

## Bug

* ARROW-1907 - [C++/Python] Feather format cannot accommodate string columns containing more than a total of 2GB of data
* ARROW-2255 - [Developer][Integration] Serialize schema- and field-level custom metadata in integration test JSON format
* ARROW-2587 - [Python] Unable to write StructArrays with multiple children to parquet
* ARROW-3004 - [Documentation] Builds docs for master rather than a pinned commit
* ARROW-3543 - [R] Better support for timestamp format and time zones in R
* ARROW-5265 - [Python/CI] Add integration test with kartothek
* ARROW-5473 - [C++] Build failure on googletest\_ep on Windows when using Ninja
* ARROW-5981 - [C++] DictionaryBuilder<T> initialization with Array can fail silently
* ARROW-6528 - [C++] Spurious Flight test failures (port allocation failure)
* ARROW-6738 - [Java] Fix problems with current union comparison logic
* ARROW-6757 - [Python] Creating csv.ParseOptions() causes "Windows fatal exception: access violation" with Visual Studio 2017
* ARROW-6871 - [Java] Enhance TransferPair related parameters check and tests
* ARROW-6872 - [C++][Python] Empty table with dictionary-columns raises ArrowNotImplementedError
* ARROW-6895 - [C++][Parquet] parquet::arrow::ColumnReader: ByteArrayDictionaryRecordReader repeats returned values when calling \`NextBatch()\`
* ARROW-7008 - [Python] pyarrow.chunked\_array([array]) fails on array with all-None buffers
* ARROW-7049 - [C++] warnings building on mingw-w64
* ARROW-7301 - [Java] Sql type DATE should correspond to DateDayVector
* ARROW-7335 - [C++][Gandiva] Add castBIGINT, extractDay interval\_day functions in Gandiva
* ARROW-7390 - [C++][Dataset] Concurrency race in Projector::Project 
* ARROW-7405 - [Java] ListVector isEmpty API is incorrect
* ARROW-7466 - [CI][Java] Fix gandiva-jar-osx nightly build failure
* ARROW-7467 - [Java] ComplexCopier does incorrect copy for Map nullable info
* ARROW-7520 - [R] Writing many batches causes a crash
* ARROW-7546 - [Java] Use new implementation to concat vectors values in batch
* ARROW-7624 - [Rust] Soundness issues via \`Buffer\` methods
* ARROW-7628 - [Python] Better document some read\_csv corner cases
* ARROW-7631 - [C++][Gandiva] return zero if there is an overflow while converting a decimal to a lower precision/scale
* ARROW-7680 - [C++][Dataset] Partition discovery is not working with windows path
* ARROW-7701 - [C++] [CI] Flight test error on macOS
* ARROW-7713 - [Java] TastLeak was put at the wrong location
* ARROW-7722 - [Java][FlightRPC] Memory leak 
* ARROW-7734 - [C++] Segfault when comparing status with and without detail
* ARROW-7740 - [C++] Array internals corruption in StructArray::Flatten
* ARROW-7755 - [Python] Windows wheel cannot be installed on Python 3.8
* ARROW-7758 - [Python] Wrong conversion of timestamps that are out of bounds for pandas (eg 0000-01-01)
* ARROW-7760 - [Release] Fix verify-release-candidate.sh since pip3 seems to no longer be in miniconda
* ARROW-7762 - [Python] Exceptions in ParquetWriter get ignored
* ARROW-7766 - [Python][Packaging] Windows py38 wheels are built with wrong ABI tag
* ARROW-7772 - [R][C++][Dataset] Unable to filter on date32 object with date64 scalar
* ARROW-7775 - [Rust] Don't let safe code arbitrarily transmute readers and writers
* ARROW-7777 - [Go] StructBuilder/ListBuilder index out of range panic
* ARROW-7780 - [Release] Fix Windows wheel RC verification script given lack of "m" ABI tag in Python 3.8
* ARROW-7781 - [C++][Dataset] Filtering on a non-existent column gives a segfault
* ARROW-7783 - [C++] ARROW\_DATASET should enable ARROW\_COMPUTE
* ARROW-7785 - [C++] sparse\_tensor.cc is extremely slow to compile
* ARROW-7786 - [R] Wire up check\_metadata in Table.Equals method
* ARROW-7789 - [R] Can't initialize arrow objects when R.oo package is loaded
* ARROW-7792 - [R] read\_\* functions should close connection to file
* ARROW-7793 - [Java] If there is a leak the base allocator should release the excess memory to parent before throwing exception
* ARROW-7794 - [Rust] cargo publish fails for arrow-flight due to relative path to Flight.proto
* ARROW-7797 - [Release][Rust] Fix arrow-flight's version in datafusion crate
* ARROW-7802 - [C++] Support for LargeBinary and LargeString in the hash kernel
* ARROW-7806 - [Python] Implement to\_pandas for lists of LargeBinary/String
* ARROW-7807 - [R] Installation on RHEL 7 Cannot call io\_\_\_MemoryMappedFile\_\_Open()
* ARROW-7809 - [R] vignette does not run on Win 10 nor ubuntu
* ARROW-7813 - [Rust] Fix undefined behaviour and and remove unsafe
* ARROW-7815 - [C++] Fix crashes on corrupt IPC input (OSS-Fuzz)
* ARROW-7827 - [Python] conda-forge pyarrow package does not have s3 enabled
* ARROW-7832 - [R] Patches to 0.16.0 release
* ARROW-7836 - [Rust] "allocate\_aligned"/"reallocate" need to initialize memory to avoid UB
* ARROW-7837 - [Java] bug in BaseVariableWidthVector.copyFromSafe results with an index out of bounds exception
* ARROW-7838 - [C++] Installed plasma-store-server fails finding Boost
* ARROW-7841 - [C++] HADOOP\_HOME doesn't work to find libhdfs.so
* ARROW-7844 - [R] array\_to\_vector is not thread safe
* ARROW-7848 - Add doc for MapType
* ARROW-7852 - [Python] 0.16.0 wheels not compatible with older numpy
* ARROW-7861 - [C++][Parquet] Add fuzz regression corpus for parquet reader
* ARROW-7884 - [C++][Python] Crash in pq.read\_table()
* ARROW-7887 - [Rust] Filter kernel does not support temporal types
* ARROW-7889 - [Rust] Datafusion CLI does not support registering Parquet files
* ARROW-7899 - [Integration][Java] null type integration test
* ARROW-7908 - [R] Can't install package without setting LIBARROW\_DOWNLOAD=true
* ARROW-7922 - [CI][Crossbow] Nightly macOS wheel builds fail (brew bundle edition)
* ARROW-7923 - [CI][Crossbow] macOS autobrew fails on homebrew-versions
* ARROW-7926 - [Developer] "archery lint" target is not ergonomic for running a single check like IWYU
* ARROW-7928 - [Python] Example of flight server and client not working
* ARROW-7931 - [C++] Fix crash on corrupt Map array input (OSS-Fuzz)
* ARROW-7936 - [Python] FileSystem.from\_uri test fails on python 3.5
* ARROW-7940 - [C++] Unable to generate cmake build with settings other than default
* ARROW-7956 - [Python] Memory leak in pyarrow functions .ipc.serialize\_pandas/deserialize\_pandas
* ARROW-7958 - [Java] Update Avro to version 1.9.2
* ARROW-7962 - [R][Dataset] Followup to "Consolidate Source and Dataset classes"
* ARROW-7968 - [C++] orc\_ep build fails on 64-bit Raspbian
* ARROW-7973 - [Developer][C++] ResourceWarnings in run\_cpplint.py 
* ARROW-7974 - [Developer][C++] ResourceWarning in "make check-format"
* ARROW-7975 - [C++] Do not include padding bytes in "Buffer" IPC metadata accounting
* ARROW-7978 - [Developer] GitHub Actions "lint" task is running include-what-you-use and failing
* ARROW-7980 - [Python] Deserialization with pyarrow fails for certain Timestamp-based data frame
* ARROW-7981 - [C++][Dataset] Fails to compile on gcc 5.4
* ARROW-7985 - [C++] ListBuilder.Finish fails if underlying value builder is empty and .Reserve'd
* ARROW-7990 - [C++][Developer] Add "archery lint" option for running "iwyu.sh all"
* ARROW-7992 - [C++] MSVC warning causing Appveyor failure in sort\_to\_indices.cc
* ARROW-7996 - [Python] Error serializing empty pandas DataFrame with pyarrow
* ARROW-7997 - [Python] Schema equals method with inconsistent docs in pyarrow
* ARROW-7999 - [C++] Fix crash on corrupt Map array input (OSS-Fuzz)
* ARROW-8000 - [C++] gcc 4.8 build failures
* ARROW-8003 - [C++] -DBZip2\_SOURCE=BUNDLED fails when building with clang
* ARROW-8006 - [C++] Unsafe arrow dictionary recovered from parquet
* ARROW-8007 - [Python] Remove unused and defunct assert\_get\_object\_equal in plasma tests
* ARROW-8008 - [C++/Python] Framework Python is preferred even though not the activated one
* ARROW-8009 - [Java] Fix the hash code methods for BitVector
* ARROW-8011 - [C++] Some buffers not resized when reading from Parquet
* ARROW-8013 - [Python][Packaging] Fix manylinux wheels
* ARROW-8021 - [Python] Appveyor does not appear to be including pandas in test runs
* ARROW-8036 - [C++] Compilation failure with gtest 1.10.0
* ARROW-8042 - [Python] pyarrow.ChunkedArray docstring is incorrect regarding zero-length ChunkedArray having no chunks
* ARROW-8057 - [Python] Don't check Schema metadata in \_\_eq\_\_ and \_\_ne\_\_
* ARROW-8070 - [C++] Cast segfaults on unsupported cast from list<binary> to utf8
* ARROW-8071 - [GLib] Build error with configure
* ARROW-8088 - [C++][Dataset] Partition columns with specified dictionary type result in all nulls
* ARROW-8091 - [CI][Crossbow] Fix nightly homebrew and R failures
* ARROW-8092 - [CI][Crossbow] OSX wheels fail on bundled bzip2
* ARROW-8094 - [CI][Crossbow] Nightly valgrind test fails
* ARROW-8095 - [CI][Crossbow] Nightly turbodbc job fails
* ARROW-8098 - [go] Checkptr Failures on Go 1.14
* ARROW-8101 - [FlightRPC][Java] Can't read/write only an empty null array
* ARROW-8102 - [Dev] Crossbow's version detection doesn't work in the comment bot's scenario
* ARROW-8105 - [Python] pyarrow.array segfaults when passed masked array with shrunken mask
* ARROW-8106 - [Python] Builds on master broken by pandas 1.0.2 release
* ARROW-8110 - [C#] BuildArrays fails if NestedType is included
* ARROW-8112 - [FlightRPC][C++] Some status codes don't round-trip through gRPC
* ARROW-8119 - [Dev] Make Yaml optional dependency for archery
* ARROW-8122 - [Python] Empty numpy arrays with shape cannot be deserialized
* ARROW-8125 - [C++] "arrow-tests" target broken with ninja build
* ARROW-8127 - [C++] [Parquet] Incorrect column chunk metadata for multipage batch writes
* ARROW-8128 - [C#] NestedType children serialized on wrong length
* ARROW-8132 - [C++] arrow-s3fs-test failing on master
* ARROW-8133 - [CI] Github Actions sometimes fail to checkout Arrow
* ARROW-8136 - [C++][Python] Creating dataset from relative path no longer working
* ARROW-8139 - [C++] FileSystem enum causes attributes warning
* ARROW-8142 - [C++] Casting a chunked array with 0 chunks critical failure
* ARROW-8144 - [CI] Cmake 3.2 nightly build fails
* ARROW-8159 - [Python] pyarrow.Schema.from\_pandas doesn't support ExtensionDtype
* ARROW-8166 - [C++] AVX512 intrinsics fail to compile with clang-8 on Ubuntu 18.04
* ARROW-8176 - [FlightRPC][Integration] Have Flight services bind to port 0 in integration
* ARROW-8186 - [Python] Dataset expression != returns bool instead of expression for invalid value
* ARROW-8188 - [R] Adapt to latest checks in R-devel
* ARROW-8193 - [C++] arrow-future-test fails to compile on gcc 4.8
* ARROW-8197 - [Rust] DataFusion "create\_physical\_plan" returns incorrect schema?
* ARROW-8206 - [R] Minor fix for backwards compatibility on Linux installation
* ARROW-8209 - [Python] Accessing duplicate column of Table by name gives wrong error
* ARROW-8213 - [Python][Dataset] Opening a dataset with a local incorrect path gives confusing error message
* ARROW-8216 - [R][C++][Dataset] Filtering returns all-missing rows where the filtering column is missing
* ARROW-8217 - [R][C++] Fix crashing test in test-dataset.R on 32-bit Windows from ARROW-7979
* ARROW-8219 - [Rust] sqlparser crate needs to be bumped to version 0.2.5
* ARROW-8233 - [CI] Build timeouts on "AMD64 Windows MinGW 64 GLib & Ruby "
* ARROW-8237 - [Python] Review Developer build instructions for conda and non-conda users
* ARROW-8238 - [C++][Compute] Failed to build compute tests on windows with msvc2015
* ARROW-8239 - [Java] fix param checks in splitAndTransfer method
* ARROW-8245 - [Python][Parquet] Skip hidden directories when reading partitioned parquet files
* ARROW-8255 - [Rust] [DataFusion] COUNT(\*) results in confusing error
* ARROW-8259 - [Rust] [DataFusion] ProjectionPushDownRule does not rewrite LIMIT
* ARROW-8268 - [Ruby] Test failure due to lack of built ZSTD support
* ARROW-8269 - [Python] Failure in "nopandas" build in test\_parquet\_row\_group\_fragments
* ARROW-8270 - [Python][Flight] Example Flight server with TLS's certificate and key is not working
* ARROW-8272 - [CI][Python] Test failure on Ubuntu 16.04
* ARROW-8274 - [C++] Use LZ4 frame format for "LZ4" compression in IPC write
* ARROW-8276 - [C++][Dataset] Scanning a Fragment does not take into account the partition columns
* ARROW-8280 - [C++] MinGW builds failing due to CARES-related toolchain issue
* ARROW-8286 - [Python] Creating dataset from pathlib results in UnionDataset instead of FileSystemDataset
* ARROW-8298 - [C++][CI] MinGW builds fail building grpc
* ARROW-8303 - [Python] Fix test failure caused by non-deterministic dict key ordering on Python 3.5
* ARROW-8304 - [Flight][Python] Flight client with TLS root certificate is reporting error on do\_get()
* ARROW-8305 - [Java] ExtensionTypeVector should make sure underlyingVector not null
* ARROW-8310 - [C++] Minio's exceptions not recognized by IsConnectError()
* ARROW-8315 - [Python][Dataset] Don't rely on ordered dict keys in test\_dataset.py
* ARROW-8323 - [C++] Pin gRPC at v1.27 to avoid compilation error in its headers
* ARROW-8327 - [FlightRPC][Java] gRPC trailers may be null
* ARROW-8331 - [C++] arrow-compute-filter-benchmark fails to compile
* ARROW-8333 - [C++][CI] Always compile benchmarks in some C++ CI entry 
* ARROW-8334 - [C++] [Gandiva] Missing DATE32 in LLVM Types / Simple D32 Compute Functions
* ARROW-8342 - [Python] dask and kartothek integration tests are failing
* ARROW-8345 - [Python] feather.read\_table should not require pandas
* ARROW-8346 - [CI][Ruby] GLib/Ruby macOS build fails on zlib
* ARROW-8349 - [CI][NIGHTLY:gandiva-jar-osx] Use latest pygit2
* ARROW-8353 - [C++] is\_nullable maybe not initialized in parquet writer
* ARROW-8354 - [R] Fix segfault in Table to Array conversion
* ARROW-8357 - [Rust] [DataFusion] Dockerfile for CLI is missing format dir
* ARROW-8358 - [C++] Fix -Wrange-loop-construct warnings in clang-11 
* ARROW-8365 - [C++] Error when writing files to S3 larger than 5 GB
* ARROW-8366 - [Rust] Need to revert recent arrow-flight build change
* ARROW-8369 - [CI] Fix crossbow wildcard groups
* ARROW-8373 - [GLib] Problems resolving gobject-introspection, arrow in Meson builds
* ARROW-8380 - [RUST] StringDictionaryBuilder not publicly exported from arrow::array
* ARROW-8384 - [C++][Python] arrow/filesystem/hdfs.h and Python wrapper does not have an option for setting a path to a Kerberos ticket
* ARROW-8386 - [Python] pyarrow.jvm raises error for empty Arrays
* ARROW-8388 - [C++] GCC 4.8 fails to move on return
* ARROW-8397 - [C++] Fail to compile aggregate\_test.cc on Ubuntu 16.04
* ARROW-8406 - [Python] test\_fs fails when run from a different drive on Windows
* ARROW-8410 - [C++] CMake fails on aarch64 systems that do not support -march=armv8-a+crc+crypto
* ARROW-8414 - [Python] Non-deterministic row order failure in test\_parquet.py
* ARROW-8415 - [C++][Packaging] fix gandiva linux job
* ARROW-8416 - [Python] Provide a "feather" alias in the dataset API
* ARROW-8420 - [C++] CMake fails to configure on armv7l platform (e.g. Raspberry Pi 3)
* ARROW-8427 - [C++][Dataset] Do not ignore file paths with underscore/dot when full path was specified
* ARROW-8428 - [C++][NIGHTLY:gandiva-jar-trusty] GCC 4.8 failures in C++ unit tests
* ARROW-8429 - [C++] Fix Buffer::CopySlice on 0-sized buffer
* ARROW-8432 - [Python][CI] Failure to download Hadoop
* ARROW-8437 - [C++] Remove std::move return value from MakeRandomNullBitmap test utility
* ARROW-8438 - [C++] arrow-io-memory-benchmark crashes
* ARROW-8439 - [Python] Filesystem docs are outdated
* ARROW-8441 - [C++] Fix crashes on invalid input (OSS-Fuzz)
* ARROW-8442 - [Python] NullType.to\_pandas\_dtype inconsisent with dtype returned in to\_pandas/to\_numpy
* ARROW-8460 - [Packaging][deb] Ubuntu Focal build is failed
* ARROW-8465 - [Packaging][Python] Windows py35 wheel build fails because of boost
* ARROW-8466 - [Packaging] The python unittests are not running in the windows wheel builds
* ARROW-8468 - [Document] Fix the incorrect null bits description
* ARROW-8469 - [Dev] Fix nightly docker tests on azure
* PARQUET-1780 - [C++] Set ColumnMetadata.encoding\_stats field
* PARQUET-1788 - [C++] ColumnWriter has undefined behavior when writing arrow chunks
* PARQUET-1797 - [C++] Fix fuzzing errors
* PARQUET-1799 - [C++] Stream API: Relax schema checking when reading
* PARQUET-1810 - [C++] Fix undefined behaviour on invalid enum values (OSS-Fuzz)
* PARQUET-1813 - [C++] Remove logging statement in unit test
* PARQUET-1819 - [C++] Fix crashes on corrupt IPC input (OSS-Fuzz)
* PARQUET-1823 - [C++] Invalid RowGroup returned when reading with parquet::arrow::FileReader->RowGroup(i)->Column(j)
* PARQUET-1829 - [C++] Fix crashes on invalid input (OSS-Fuzz)
* PARQUET-1831 - [C++] Fix crashes on invalid input (OSS-Fuzz)
* PARQUET-1835 - [C++] Fix crashes on invalid input (OSS-Fuzz)

## Improvement

* ARROW-1581 - [Packaging] Tooling to make nightly wheels available for install
* ARROW-1582 - [Python] Set up + document nightly conda builds for macOS
* ARROW-2447 - [C++] Create a device abstraction
* ARROW-4120 - [Python] Define process for testing procedures that check for no macro-level memory leaks
* ARROW-4304 - [Rust] Enhance documentation for arrow
* ARROW-4482 - [Website] Add blog archive page
* ARROW-4815 - [Rust] [DataFusion] Add support for \* in SQL projection
* ARROW-5357 - [Rust] Add capacity field in Buffer
* ARROW-5405 - [Documentation] Move integration testing documentation to Sphinx docs, add instructions for JavaScript
* ARROW-5497 - [Release] Build and publish R/Java/JS docs
* ARROW-5501 - [R] Reorganize read/write file/stream functions
* ARROW-5510 - [Format] Feather V2 based on Arrow IPC file format, with compression support
* ARROW-5563 - [Format] Update integration test JSON format documentation
* ARROW-5585 - [Go] rename arrow.TypeEquals into arrow.TypeEqual
* ARROW-6165 - [Integration] Use multiprocessing to run integration tests on multiple CPU cores
* ARROW-6176 - [Python] Allow to subclass ExtensionArray to attach to custom extension type
* ARROW-6393 - [C++]Add EqualOptions support in SparseTensor::Equals
* ARROW-6479 - [C++] inline errors from external projects' build logs
* ARROW-6510 - [Python][Filesystem] Expose nanosecond resolution mtime
* ARROW-6724 - [C++] Add simpler static ctor for BufferOutputStream than the current Create function
* ARROW-6821 - [C++][Parquet] Do not require Thrift compiler when building (but still require library)
* ARROW-6823 - [C++][Python][R] Support metadata in the feather format?
* ARROW-6841 - [C++] Upgrade to LLVM 8
* ARROW-6875 - [FlightRPC] Implement Criteria for ListFlights RPC / list\_flights method
* ARROW-6915 - [Developer] Do not overwrite minor release version with merge script, even if not specified by committer
* ARROW-6996 - [Python] Expose boolean filter kernel on Table
* ARROW-7044 - [Release] Create a post release script for the home-brew formulas
* ARROW-7063 - [C++] Schema print method prints too much metadata
* ARROW-7091 - [C++] Move all factories to type\_fwd.h
* ARROW-7119 - [C++][CI] Use scripts/util\_coredump.sh to show automatic backtraces
* ARROW-7202 - [R][CI] Improve rwinlib building on CI to stop re-downloading dependencies
* ARROW-7222 - [Python][Release] Wipe any existing generated Python API documentation when updating website
* ARROW-7256 - [C++] Remove ARROW\_MEMORY\_POOL\_DEFAULT macro
* ARROW-7332 - [C++][Parquet] Explicitly catch status exceptions in PARQUET\_CATCH\_NOT\_OK
* ARROW-7336 - [C++] Implement MinMax options to not skip nulls
* ARROW-7338 - [C++] Improve InMemoryDataSource to support generator instead of static list
* ARROW-7365 - [Python] Support FixedSizeList type in conversion to numpy/pandas
* ARROW-7400 - [Java] Avoids the worst case for quick sort
* ARROW-7462 - [C++] Add CpuInfo detection for Arm64 Architecture
* ARROW-7491 - [Java] Improve the performance of aligning
* ARROW-7524 - [C++][CI] Build parquet support in the VS2019 GitHub Actions job
* ARROW-7530 - [Developer] Do not include list of commits from PR in squashed summary message
* ARROW-7547 - [C++] [Python] [Dataset] Additional reader options in ParquetFileFormat
* ARROW-7608 - [C++][Dataset] Expose more informational properties
* ARROW-7625 - [GLib] Parquet GLib and Red Parquet (Ruby) do not allow specifying compression type
* ARROW-7641 - [R] Make dataset vignette have executable code
* ARROW-7662 - [R] Support creating ListArray from R list
* ARROW-7664 - [C++] Extract localfs default from FileSystemFromUri
* ARROW-7675 - [R][CI] Move Windows CI from Appveyor to GHA
* ARROW-7679 - [R] Cleaner interface for creating UnionDataset
* ARROW-7684 - [Rust] Provide example of Flight server for DataFusion
* ARROW-7685 - [Developer] Add support for GitHub Actions to Crossbow
* ARROW-7708 - [Release] Include PARQUET commits from git changelog in release changelogs
* ARROW-7720 - [C++][Python] Add check\_metadata argument to Table.equals
* ARROW-7725 - [C++] Add infrastructure for unity builds and precompiled headers
* ARROW-7726 - [CI] [C++] Use boost binaries on Windows GHA build
* ARROW-7733 - [Developer] Install locally a new enough version of Go for release verification script
* ARROW-7735 - [Release] conda-forge channel is missing for verifying wheels
* ARROW-7736 - [Release] Binary verification sometimes fails with transient error
* ARROW-7745 - [Doc] [C++] Update Parquet documentation
* ARROW-7749 - [C++] Link some more tests together
* ARROW-7750 - [Release] Make the source release verification script restartable
* ARROW-7751 - [Release] macOS wheel verification also needs arrow-testing
* ARROW-7752 - [Release] Enable and test dataset in the verification script
* ARROW-7754 - [C++] Result<T> is slow
* ARROW-7761 - [C++] Add S3 support to fs::FileSystemFromUri
* ARROW-7764 - [C++] Builders allocate a null bitmap buffer even if there is no nulls
* ARROW-7771 - [Developer] Use ARROW\_TMPDIR environment variable in the verification scripts instead of TMPDIR
* ARROW-7787 - [Rust] Add collect to Table API
* ARROW-7790 - [Website] Update how to install Linux packages
* ARROW-7795 - [Rust - DataFusion] Support boolean negation (NOT)
* ARROW-7796 - [R] write\_\* functions should invisibly return their inputs
* ARROW-7799 - [R][CI] Remove flatbuffers from homebrew formulae
* ARROW-7804 - [C++][R] Compile error on macOS 10.11
* ARROW-7817 - [CI] macOS R autobrew nightly failed on installing dependency from source
* ARROW-7824 - [C++][Dataset] Provide Dataset writing to IPC format
* ARROW-7828 - [Release] Remove SSH keys for internal use
* ARROW-7829 - [R] Test R bindings on clang
* ARROW-7834 - [Release] Post release task for updating the documentations
* ARROW-7839 - [Python][Dataset] Add IPC format to python bindings
* ARROW-7858 - [C++][Python] Support casting an Extension type to its storage type
* ARROW-7859 - [R] Minor patches for CRAN submission 0.16.0.2
* ARROW-7862 - [R] Linux installation should run quieter by default
* ARROW-7863 - [C++][Python][CI] Ensure running HDFS related tests
* ARROW-7864 - [R] Make sure bundled installation works even if there are system packages
* ARROW-7865 - [R] Test builds on latest Linux versions
* ARROW-7872 - [Python] Support conversion of list-of-struct in Array/Table.to\_pandas
* ARROW-7874 - [Python][Archery] Validate docstrings with numpydoc
* ARROW-7876 - [R] Installation fails in the documentation generation image
* ARROW-7879 - [C++][Doc] Add doc for the Device API
* ARROW-7880 - [CI][R] R sanitizer job is not really working
* ARROW-7881 - [C++] Fix pedantic warnings
* ARROW-7886 - [C++][Dataset] Consolidate Source and Dataset
* ARROW-7888 - [Python] Allow using a more modern version of jpype in pyarrow.jvm
* ARROW-7891 - [C++] RecordBatch->Equals should also have a check\_metadata argument
* ARROW-7892 - [Python] Expose FilesystemSource.format attribute
* ARROW-7895 - [Python] Remove more python 2.7 cruft
* ARROW-7896 - [C++] Refactor from #include guards to #pragma once
* ARROW-7904 - [C++] Decide about Field/Schema metadata printing parameters and how much to show by default
* ARROW-7907 - [Python] Conversion to pandas of empty table with timestamp type aborts
* ARROW-7912 - [Format] C data interface
* ARROW-7913 - [C++][Python][R] C++ implementation of C data interface
* ARROW-7915 - [CI] [Python] Run tests with Python development mode enabled
* ARROW-7916 - [C++][Dataset] Project IPC record batches to materialized fields
* ARROW-7917 - [CMake] FindPythonInterp should check for python3
* ARROW-7919 - [R] install\_arrow() should conda install if appropriate
* ARROW-7920 - [R] Fill in some missing input validation
* ARROW-7927 - [C++] Fix 'cpu\_info.cc' compilation warning
* ARROW-7929 - [C++] CMake target names differ from upstream provided names
* ARROW-7932 - [Rust] [Parquet] Implement array reader for temporal types
* ARROW-7935 - [Java] Remove Netty dependency for BufferAllocator and ReferenceManager
* ARROW-7937 - [Python][Packaging] Remove boost from the macos wheels
* ARROW-7941 - [Rust] [DataFusion] Logical plan should support unresolved column references
* ARROW-7947 - [Rust] [Flight] [DataFusion] Implement example for get\_schema
* ARROW-7949 - [Developer] Update to '.gitignore' to not track user specific  'cpp/Brewfile.lock.json' file
* ARROW-7951 - [Python][Parquet] Expose BYTE\_STREAM\_SPLIT to pyarrow
* ARROW-7959 - [Ruby] Add support for Ruby 2.3 again
* ARROW-7963 - [C++][Python][Dataset] Expose listing fragments
* ARROW-7965 - [Python] Refine higher level dataset API
* ARROW-7966 - [Integration][Flight][C++] Client should verify each batch independently
* ARROW-7969 - [Packaging] Use cURL to upload artifacts
* ARROW-7970 - [Packaging][Python] Use system boost to build the macos wheels
* ARROW-7977 - [C++] Rename fs::FileStats to fs::FileInfo
* ARROW-7983 - [CI][R] Nightly builds should be more verbose when they fail
* ARROW-7984 - [R] Check for valid inputs in more places
* ARROW-7987 - [CI][R] Fix for verbose nightly builds
* ARROW-7988 - [R] Fix on.exit calls in reticulate bindings
* ARROW-7991 - [C++][Plasma] Allow option for evicting if full when creating an object
* ARROW-7998 - [C++][Plasma] Make Seal requests synchronous
* ARROW-8005 - [Website] Review and adjust any usages of Apache dist system from website / tools
* ARROW-8014 - [C++] Provide CMake targets to test only within a given label
* ARROW-8016 - [Developer] Fix deprecation warning in PR merge tool
* ARROW-8024 - [R] Bindings for BinaryType and FixedBinaryType
* ARROW-8026 - [Python] Support memoryview in addition to string value types for constructing string and binary type arrays
* ARROW-8027 - [Developer][Integration] Add integration tests for duplicate field names
* ARROW-8028 - [Go] Allow duplicate field names in schemas and nested types
* ARROW-8030 - [C++][Plasma] Fix inconsistent comment style
* ARROW-8044 - [CI][NIGHTLY:gandiva-jar-osx] pygit2 needs libgit2 v1.0.x
* ARROW-8055 - [GLib][Ruby] Add some metadata bindings to GArrowSchema
* ARROW-8058 - [C++][Python][Dataset] Provide an option to toggle validation and schema inference in FileSystemDatasetFactoryOptions
* ARROW-8059 - [Python] Make FileSystem objects serializable
* ARROW-8060 - [Python] Make dataset Expression objects serializable
* ARROW-8061 - [C++][Dataset] Ability to specify granularity of ParquetFileFragment (support row groups)
* ARROW-8063 - [Python] Add user guide documentation for Datasets API
* ARROW-8064 - [Dev] Implement Comment bot via Github actions
* ARROW-8072 - [C++][Plasma] Add const constraint when parsing data
* ARROW-8077 - [Python] Add wheel build script and Crossbow configuration for Windows on Python 3.5
* ARROW-8079 - [Python] Implement a wrapper for KeyValueMetadata, duck-typing dict where relevant
* ARROW-8080 - [C++] Add AVX512 build option
* ARROW-8082 - [Java][Plasma] Add JNI list() interface
* ARROW-8083 - [GLib] Add support for Peek() to GIOInputStream
* ARROW-8087 - [C++][Dataset] Order of keys with HivePartitioning is lost in resulting schema
* ARROW-8097 - [Dev] Comment bot's crossbow command acts on the master branch
* ARROW-8104 - [C++] Don't install bundled Thrift
* ARROW-8107 - [Packaging][APT] Use HTTPS for LLVM APT repository for Debian GNU/Linux stretch
* ARROW-8109 - [Packaging][APT] Drop support for Ubuntu Disco
* ARROW-8117 - [Rust] [Datafusion] Allow CAST from number to timestamp
* ARROW-8118 - [R] dim method for FileSystemDataset
* ARROW-8120 - [Packaging][APT] Add support for Ubuntu Focal
* ARROW-8123 - [Rust] [DataFusion] Create LogicalPlanBuilder
* ARROW-8126 - [C++][Compute] Add Top-K kernel benchmark
* ARROW-8129 - [C++][Compute] Refine compare sorting kernel
* ARROW-8140 - [Developer] Follow NullType -> NullField change
* ARROW-8141 - [C++] Optimize BM\_PlainDecodingBoolean performance using AVX512 Intrinsics API
* ARROW-8146 - [C++] Add per-filesystem facility to sanitize a path
* ARROW-8150 - [Rust] Allow writing custom FileMetaData k/v pairs
* ARROW-8151 - [Benchmarking][Dataset] Benchmark Parquet read performance with S3File
* ARROW-8153 - [Packaging] Update the conda feedstock files and upload artifacts to Anaconda
* ARROW-8158 - [Java] Getting length of data buffer and base variable width vector
* ARROW-8164 - [C++][Dataset] Let datasets be viewable with non-identical schema
* ARROW-8165 - [Packaging] Make nightly wheels available on a PyPI server
* ARROW-8167 - [CI] Add support for skipping builds with skip pattern in pull request title
* ARROW-8168 - [Java][Plasma] Improve Java Plasma client off-heap memory usage
* ARROW-8177 - [rust] Make schema\_to\_fb\_offset public because it is very useful!
* ARROW-8178 - [C++] Upgrade to Flatbuffers 1.12
* ARROW-8179 - [R] Windows build script tweaking for nightly packaging on GHA
* ARROW-8182 - [Packaging] Increment the version number detected from the latest git tag
* ARROW-8184 - [Packaging] Use arrow-nightlies organization name on Anaconda and Gemfury to host the nightlies
* ARROW-8185 - [Packaging] Document the available nightly wheels and conda packages
* ARROW-8187 - [R] Make test assertions robust to i18n
* ARROW-8191 - [Packaging][APT] Fix cmake removal in Debian GNU/Linux Stretch
* ARROW-8192 - [C++] script for unpack avx512 intrinsics code
* ARROW-8198 - [C++] Diffing should handle null arrays
* ARROW-8200 - [GLib] Rename garrow\_file\_system\_target\_info{,s}() to ...\_file\_info{,s}()
* ARROW-8203 - [C#] "dotnet pack" is failed
* ARROW-8204 - [Rust] [DataFusion] Add support for aliased expressions in SQL
* ARROW-8207 - [Packaging][wheel] Use LLVM 8 in manylinux2010 and manylinux2014
* ARROW-8215 - [CI][GLib] Meson install fails in the macOS build
* ARROW-8218 - [C++] Parallelize decompression at field level in experimental IPC compression code
* ARROW-8220 - [Python] Make dataset FileFormat objects serializable
* ARROW-8222 - [C++] Use bcp to make a slim boost for bundled build
* ARROW-8224 - [C++] Remove APIs deprecated prior to 0.16.0
* ARROW-8225 - [Rust] IPC reader must respect continuation markers
* ARROW-8227 - [C++] Refine SIMD feature definitions
* ARROW-8231 - [Rust] Parse key\_value\_metadata from parquet FileMetaData into arrow schema metadata
* ARROW-8232 - [Python] Deprecate pa.open\_file and pa.open\_stream in favor of pa.ipc.open\_file/open\_stream
* ARROW-8241 - [Rust] Add convenience methods to Schema
* ARROW-8242 - [C++] Flight fails to compile on GCC 4.8
* ARROW-8243 - [Rust] [DataFusion] Fix inconsistent API in LogicalPlanBuilder
* ARROW-8246 - [C++] Add -Wa,-mbig-obj when compiling with MinGW to avoid linking errors
* ARROW-8247 - [Python] Expose Parquet writing "engine" setting in pyarrow.parquet.write\_table
* ARROW-8249 - [Rust] [DataFusion] Make Table and LogicalPlanBuilder APIs more consistent
* ARROW-8252 - [CI][Ruby] Add Ubuntu 20.04
* ARROW-8256 - [Rust] [DataFusion] Update CLI documentation for 0.17.0 release
* ARROW-8264 - [Rust] [DataFusion] Create utility for printing record batches
* ARROW-8266 - [C++] Add backup mirrors for external project source downloads
* ARROW-8267 - [CI][GLib] Failed to build on Ubuntu 16.04
* ARROW-8271 - [Packaging] Allow wheel upload failures to gemfury
* ARROW-8275 - [Python][Docs] Review Feather + IPC file documentation per "Feather V2" changes
* ARROW-8277 - [Python] RecordBatch interface improvements
* ARROW-8279 - [C++] Do not export symbols from Codec implementations, remove need for PIMPL pattern
* ARROW-8288 - [Python] Expose with\_ modifiers on DataType
* ARROW-8290 - [Python][Dataset] Improve ergonomy of the FileSystemDataset constructor
* ARROW-8291 - [Packaging] Conda nightly builds can't locate Numpy
* ARROW-8295 - [C++][Dataset] IpcFileFormat should expliclity push down column projection
* ARROW-8300 - [R] Documentation and changelog updates for 0.17
* ARROW-8307 - [Python] Expose use\_memory\_map option in pyarrow.feather APIs
* ARROW-8308 - [Rust] [Flight] Implement DoExchange on examples
* ARROW-8309 - [CI] C++/Java/Rust workflows should trigger on changes to Flight.proto
* ARROW-8311 - [C++] Add push style stream format reader
* ARROW-8316 - [CI] Set docker-compose to use docker-cli instead of docker-py for building images
* ARROW-8319 - [CI] Install thrift compiler in the debian build
* ARROW-8320 - [Documentation][Format] Clarify (lack of) alignment requirements in C data interface
* ARROW-8321 - [CI] Use bundled thrift in Fedora 30 build
* ARROW-8325 - [R][CI] Stop including boost in R windows bundle
* ARROW-8332 - [C++] Require Thrift compiler to use system libthrift for Parquet build
* ARROW-8335 - [Release] Add crossbow jobs to run release verification
* ARROW-8336 - [Packaging][deb] Use libthrift-dev on Debian 10 and Ubuntu 19.10 or later
* ARROW-8341 - [Packaging][deb] Fail to build by no disk space
* ARROW-8356 - [Developer] Support \* wildcards with "crossbow submit" via GitHub actions
* ARROW-8367 - [C++] Deprecate Buffer::FromString(..., pool) 
* ARROW-8368 - [Format] In C interface, clarify resource management for consumers needing only a subset of child fields in ArrowArray
* ARROW-8371 - [Crossbow] Implement and exercise sanity checks for tasks.yml 
* ARROW-8375 - [CI][R] Make Windows tests more verbose in case of segfault
* ARROW-8387 - [Rust] Make schema\_to\_fb public
* ARROW-8389 - [Integration] Run tests in parallel
* ARROW-8396 - [Rust] Remove libc from dependencies
* ARROW-8398 - [Python] Remove deprecation warnings originating from python tests
* ARROW-8401 - [C++] Add AVX2/AVX512 version of ByteStreamSplitDecode/ByteStreamSplitEncode
* ARROW-8403 - [C++] Add ToString() to ChunkedArray, Table and RecordBatch
* ARROW-8407 - [Rust] Add rustdoc for Dictionary type
* ARROW-8408 - [Python] Add memory\_map= toggle to pyarrow.feather.read\_feather
* ARROW-8409 - [R] Add arrow::cpu\_count, arrow::set\_cpu\_count wrapper functions a la Python
* ARROW-8433 - [R] Add feather alias for ipc format in dataset API
* ARROW-8449 - [R] Use CMAKE\_UNITY\_BUILD everywhere
* ARROW-8450 - [Integration][C++] Implement large list/binary/utf8 integration
* ARROW-8457 - [C++] bridge test does not take care of endianness
* ARROW-8461 - [Packaging][deb] Use zstd package for Ubuntu Xenial
* ARROW-8467 - [C++] Test cases using ArrayFromJSON assume only a little-endian platform
* PARQUET-1663 - [C++] Provide API to check the presence of complex data types
* PARQUET-1785 - [C++] Improve code reusability in encoding-test.cc
* PARQUET-1786 - [C++] Use simd to improve BYTE\_STREAM\_SPLIT decoding performance
* PARQUET-1825 - [C++] Fix compilation error in column\_io\_benchmark.cc
* PARQUET-1828 - Add a SSE2 path for the ByteStreamSplit encoder implementation
* PARQUET-1840 - [C++] DecodeSpaced copies more values then necessary

## New Feature

* ARROW-1560 - [C++] Kernel implementations for "match" function
* ARROW-1571 - [C++] Implement argsort kernels (sort indices) for integers using O(n) counting sort
* ARROW-1636 - [Format] Integration tests for null type
* ARROW-3410 - [C++][Dataset] Streaming CSV reader interface for memory-constrainted environments
* ARROW-3750 - [R] Pass various wrapped Arrow objects created in Python into R with zero copy via reticulate
* ARROW-4226 - [Format][C++] Add CSF sparse tensor support
* ARROW-4428 - [R] Feature flags for R build
* ARROW-590 - [Integration] Add integration tests for Union types
* ARROW-5949 - [Rust] Implement DictionaryArray
* ARROW-6837 - [C++/Python] access File Footer custom\_metadata
* ARROW-6947 - [Rust] [DataFusion] Add support for scalar UDFs
* ARROW-7048 - [Java] Support for combining multiple vectors under VectorSchemaRoot
* ARROW-7073 - [Java] Support concating vectors values in batch
* ARROW-7080 - [Python][Parquet][C++] Expose parquet field\_id in Schema objects
* ARROW-7201 - [GLib][Gandiva] Add support for BooleanNode
* ARROW-7412 - [C++][Dataset] Ensure that dataset code is robust to schemas with duplicate field names
* ARROW-7419 - [Python] Support SparseCSCMatrix
* ARROW-7427 - [Python] Support SparseCSFTensor
* ARROW-7428 - [Format][C++] Add serialization for CSF sparse tensors
* ARROW-7587 - [C++][Compute] Add Top-k kernel
* ARROW-7616 - [Java] Support comparing value ranges for dense union vector
* ARROW-7742 - [GLib] Add support for MapArray
* ARROW-7833 - [R] Make install\_arrow() actually install arrow
* ARROW-7921 - [Go] Add Reset method to various components and clean up comments
* ARROW-7971 - [Rust] Create rowcount utility
* ARROW-7986 - [Python] pa.Array.from\_pandas cannot convert pandas.Series containing pyspark.ml.linalg.SparseVector
* ARROW-8018 - [C++][Parquet]Parquet Modular Encryption
* ARROW-8103 - [R] Make default Linux build more minimal
* ARROW-8294 - [Format][Flight] Add DoExchange RPC to Flight protocol
* ARROW-8299 - [C++] Reusable "optional ParallelFor" function for optional use of multithreading
* ARROW-8343 - [GLib] Add GArrowRecordBatchIterator
* ARROW-8351 - [R][CI] Store the Rtools-built Arrow C++ library as a build artifact
* ARROW-8352 - [R] Add install\_pyarrow()
* ARROW-8376 - [R] Add experimental interface to ScanTask/RecordBatch iterators
* ARROW-8390 - [R] Expose schema unification features
* PARQUET-1716 - [C++] Add support for BYTE\_STREAM\_SPLIT encoding
* PARQUET-458 - [C++] Implement support for DataPageV2

## Sub-task

* ARROW-6666 - [Rust] [DataFusion] Implement string literal expression
* ARROW-7233 - [C++] Add Result<T> APIs to IPC module
* ARROW-7330 - [C++] Add Result<T> to APIs to arrow/gpu
* ARROW-7444 - [GLib] Add LocalFileSystem support
* ARROW-7741 - [C++][Parquet] Incorporate new level generation logic in parquet write path with a flag to revert back to old logic
* ARROW-7788 - [C++] Add schema conversion support for map type
* ARROW-7819 - [C++][Gandiva] Add DumpIR to Filter/Projector classes
* ARROW-7943 - [C++][Parquet] Add a new level builder capable of handling nested data
* ARROW-7979 - [C++] Implement experimental buffer compression in IPC messages
* ARROW-8039 - [Python][Dataset] Support using dataset API in pyarrow.parquet with a minimal ParquetDataset shim
* ARROW-8292 - [Python][Dataset] Passthrough schema to Factory.finish() in dataset() function
* ARROW-8347 - [C++] Add Result<T> APIs to Array methods
* ARROW-8361 - [C++] Add Result<T> APIs to Buffer methods and functions
* ARROW-8370 - [C++] Add Result<T> to type / schema APIs
* ARROW-8372 - [C++] Add Result<T> to table / record batch APIs

## Task

* ARROW-3054 - [Packaging] Tooling to enable nightly conda packages to be updated to some anaconda.org channel
* ARROW-5757 - [Python] Stop supporting Python 2.7
* ARROW-7515 - [C++] Rename nonexistent and non\_existent to not\_found
* ARROW-7534 - [Java] Create a new java/contrib module
* ARROW-7615 - [CI][Gandiva] Ensure that the gandiva\_jni library has only a whitelisted set of shared dependencies as part of Travis CI job
* ARROW-7691 - [C++] Verify missing fields when walking Flatbuffers data
* ARROW-7712 - [CI][Crossbow] Fix or delete fuzzit jobs
* ARROW-7729 - [Python][CI] Pin pandas version to 0.25 in the dask integration test
* ARROW-7739 - [GLib] Use placement new to initialize shared\_ptr object in private structs
* ARROW-7774 - [Packaging][Python] Update macos and windows wheel filenames
* ARROW-7812 - [Packaging][Python] Upgrade LLVM in manylinux1 docker image
* ARROW-7846 - [Python][Dev] Remove last dependencies on six
* ARROW-7849 - [Packaging][Python] Remove the remaining py27 crossbow wheel tasks from the nightlies
* ARROW-7868 - [Crossbow] Reduce GitHub API query parallelism 
* ARROW-7869 - [Python] Boost::system and boost::filesystem not necessary anymore in Python wheels
* ARROW-7877 - [Packaging] Fix crossbow deployment to github artifacts
* ARROW-7882 - [C++][Gandiva] Optimise like function for substring pattern
* ARROW-7897 - [Packaging] Temporarily disable artifact uploading until we fix the deployment issues
* ARROW-7898 - [Python] Reduce the number docstring violations using numpydoc
* ARROW-7930 - [Python][CI] Test jpype integration in CI
* ARROW-7934 - [C++] Fix UriEscape for empty string
* ARROW-7993 - [Java] Support decimal type in ComplexCopier
* ARROW-8086 - [Java] Support writing decimal from big endian byte array in UnionListWriter
* ARROW-8096 - [C++][Gandiva] Create null node of Interval type 
* ARROW-8124 - [Rust] Update library dependencies
* ARROW-8130 - [C++][Gandiva] Fix Dex visitor in llvm\_generator to handle interval type
* ARROW-8181 - [Java][FlightRPC] Expose transport error metadata
* ARROW-8183 - [c++][FlightRPC] Expose transport error metadata
* ARROW-8195 - [CI] Remove Boost download step in Github Actions
* ARROW-8322 - [CI] Fix C# workflow file syntax
* ARROW-8329 - [Documentation][C++] Undocumented FilterOptions argument in Filter kernel
* ARROW-8330 - [Documentation] The post release script generates the documentation with a development version
* ARROW-8362 - [Crossbow] Ensure that the locally generated version is used in the docker tasks
* ARROW-8393 - [C++][Gandiva] Make gandiva function registry case-insensitive
* ARROW-8412 - [C++][Gandiva] Fix gandiva date\_diff function definitions
* ARROW-8444 - [Documentation] Fix spelling errors across the codebase
* ARROW-8458 - [C++] Prefer the original mirrors for the bundled thirdparty dependencies
* ARROW-8463 - [CI] Balance the nightly test builds between CircleCI, Azure and Github
* PARQUET-1770 - [C++][CI] Add fuzz target for reading Parquet files

## Test

* ARROW-7791 - [C++][Parquet] Fix building error "cannot bind lvalue"
* ARROW-7857 - [Python] Failing test with pandas master for extension type conversion
* ARROW-8326 - [C++] Don't use deprecated TYPED\_TEST\_CASE

## Wish

* ARROW-5742 - [CI] Add daily / weekly Valgrind build
* ARROW-7890 - [C++] Add Promise / Future implementation
* ARROW-7982 - [C++] Let ArrayDataVisitor accept void-returning functions
* ARROW-7994 - [CI][C++] Move AppVeyor MinGW builds to GitHub Actions
* ARROW-7995 - [C++] IO: coalescing and caching read ranges
* ARROW-8145 - [C++] Rename GetTargetInfos
* ARROW-8194 - [CI] Github Actions Windows job should run tests in parallel
* ARROW-8244 - [Python][Parquet] Add \`write\_to\_dataset\` option to populate the "file\_path" metadata fields
* PARQUET-1806 - [C++] [CI] Improve fuzzing seed corpus

# Apache Arrow 0.16.0 (30 January 2020)

## Bug

* ARROW-3783 - [R] Incorrect collection of float type
* ARROW-3962 - [Go] Support null values while reading a CSV file.
* ARROW-4998 - [R] R package fails to install on OSX
* ARROW-5575 - [C++] arrowConfig.cmake includes uninstalled targets
* ARROW-5655 - [Python] Table.from\_pydict/from\_arrays not using types in specified schema correctly
* ARROW-5680 - [Rust] datafusion group-by tests depends on result set order
* ARROW-6157 - [Python][C++] UnionArray with invalid data passes validation / leads to segfaults
* ARROW-6195 - [C++] CMake fails with file not found error while bundling thrift if python is not installed
* ARROW-6429 - [CI][Crossbow] Nightly spark integration job fails
* ARROW-6445 - [CI][Crossbow] Nightly Gandiva jar trusty job fails
* ARROW-6567 - [Rust] [DataFusion] SQL aggregate query execution assume grouping expressions precede aggregate expressions
* ARROW-6581 - [C++] Fix fuzzit job submission
* ARROW-6704 - [C++] Cast from timestamp to higher resolution does not check out of bounds timestamps
* ARROW-6708 - [C++] "cannot find -lboost\_filesystem\_static"
* ARROW-6728 - [C#] Support reading and writing Date32 and Date64 arrays
* ARROW-6736 - [Rust] [DataFusion] Aggregate expressions get evaluated repeatedly
* ARROW-6740 - [Python] Unable to delete closed MemoryMappedFile on Windows
* ARROW-6745 - [Rust] Fix a variety of typos
* ARROW-6749 - [Python] Conversion of non-ns timestamp array to numpy gives wrong values
* ARROW-6750 - [Python] Silence S3 error logs by default
* ARROW-6761 - [Rust] Travis CI builds not respecting rust-toolchain
* ARROW-6762 - [C++] JSON reader segfaults on newline
* ARROW-6785 - [JS] Remove superfluous child assignment
* ARROW-6786 - [C++] arrow-dataset-file-parquet-test is slow
* ARROW-6795 - [C#] Reading large Arrow files in C# results in an exception
* ARROW-6801 - [Rust] Arrow source release tarball is missing benchmarks
* ARROW-6806 - [C++] Segfault deserializing ListArray containing null/empty list
* ARROW-6808 - [Ruby] Ensure requiring suitable MSYS2 package
* ARROW-6809 - [RUBY] Gem does not install on macOS due to glib2 3.3.7 compilation failure
* ARROW-6812 - [Java] Remove Dremio Corp. from License Header
* ARROW-6813 - [Ruby] Arrow::Table.load with headers=true leads to exception in Arrow 0.15
* ARROW-6820 - [C++] [Doc] [Format] Map specification and implementation inconsistent
* ARROW-6834 - [C++] Pin gtest to 1.8.1 to triage failing Appveyor / MSVC build
* ARROW-6835 - [Archery][CMake] Restore ARROW\_LINT\_ONLY
* ARROW-6842 - [Website] Jekyll error building website
* ARROW-6844 - [C++][Parquet][Python] List<scalar type> columns read broken with 0.15.0
* ARROW-6857 - [Python][C++] Segfault for dictionary\_encode on empty chunked\_array (edge case)
* ARROW-6859 - [CI][Nightly] Disable docker layer caching for CircleCI tasks
* ARROW-6860 - [Python] Only link libarrow\_flight.so to pyarrow.\_flight
* ARROW-6861 - [Python] arrow-0.15.0 reading arrow-0.14.1-output Parquet dictionary column: Failure reading column: IOError: Arrow error: Invalid: Resize cannot downsize
* ARROW-6864 - [C++] bz2 / zstd tests not enabled
* ARROW-6867 - [FlightRPC][Java] Flight server can hang JVM on shutdown
* ARROW-6868 - [Go] slicing Struct array does not slice child fields
* ARROW-6869 - [C++] Dictionary "delta" building logic in builder\_dict.h produces invalid arrays
* ARROW-6873 - [Python] Stale CColumn reference break Cython cimport pyarrow
* ARROW-6874 - [Python] Memory leak in Table.to\_pandas() when conversion to object dtype
* ARROW-6876 - [Python] Reading parquet file with many columns becomes slow for 0.15.0
* ARROW-6877 - [C++] Boost not found from the correct environment
* ARROW-6878 - [Python] pa.array() does not handle list of dicts with bytes keys correctly under python3
* ARROW-6882 - [Python] cannot create a chunked\_array from dictionary\_encoding result
* ARROW-6886 - [C++] arrow::io header nvcc compiler warnings
* ARROW-6898 - [Java] Fix potential memory leak in ArrowWriter and several test classes
* ARROW-6899 - [Python] to\_pandas() not implemented on list<dictionary<values=string, indices=int32>
* ARROW-6901 - [Rust][Parquet] SerializedFileWriter writes total\_num\_rows as zero
* ARROW-6903 - [Python] Wheels broken after ARROW-6860 changes
* ARROW-6905 - [Packaging][OSX] Nightly builds on MacOS are failing because of brew compile timeouts
* ARROW-6910 - [Python] pyarrow.parquet.read\_table(...) takes up lots of memory which is not released until program exits
* ARROW-6913 - [R] Potential bug in compute.cc
* ARROW-6914 - [CI] docker-clang-format nightly failing
* ARROW-6922 - [Python] Pandas master build is failing (MultiIndex.levels change)
* ARROW-6925 - [C++] Arrow fails to buld on MacOS 10.13.6 using brew gcc 7 and 8
* ARROW-6929 - [C++] ValidateArray is out of sync with the ListArray IPC specification
* ARROW-6937 - [Packaging][Python] Fix conda linux and OSX wheel nightly builds
* ARROW-6938 - [Python] Windows wheel depends on zstd.dll and libbz2.dll, which are not bundled
* ARROW-6948 - [Rust] [Parquet] Fix bool array support in arrow reader.
* ARROW-6957 - [CI][Crossbow] Nightly R with sanitizers build fails installing dependencies
* ARROW-6962 - [C++] [CI] Stop compiling with -Weverything
* ARROW-6966 - [Go] 32bit memset is null
* ARROW-6977 - [C++] Only enable jemalloc background\_thread if feature is supported
* ARROW-6983 - [C++] Threaded task group crashes sometimes
* ARROW-6989 - [Python][C++] Assert is triggered when decimal type inference occurs on a value with out of range precision
* ARROW-6992 - [C++]: Undefined Behavior sanitizer build option fails with GCC
* ARROW-6999 - [Python] KeyError: '\_\_index\_level\_0\_\_' passing Table.from\_pandas its own schema
* ARROW-7013 - [C++] arrow-dataset pkgconfig is incomplete
* ARROW-7020 - [Java] Fix the bugs when calculating vector hash code
* ARROW-7021 - [Java] UnionFixedSizeListWriter decimal type should check writer index
* ARROW-7022 - [Python] \_\_arrow\_array\_\_ does not work for ExtensionTypes in Table.from\_pandas
* ARROW-7023 - [Python] pa.array does not use "from\_pandas" semantics for pd.Index
* ARROW-7024 - [CI][R] Update R dependencies for Conda build
* ARROW-7027 - [Python] pa.table(..) returns instead of raises error if passing invalid object
* ARROW-7033 - [C++] Error in./configure step for jemalloc when building on OSX 10.14.6
* ARROW-7045 - [R] Factor type not preserved in Parquet roundtrip
* ARROW-7050 - [R] Fix compiler warnings in R bindings
* ARROW-7056 - [Python] Test errors without S3
* ARROW-7059 - [Python] Reading parquet file with many columns is much slower in 0.15.x versus 0.14.x
* ARROW-7074 - [C++] ASSERT\_OK\_AND\_ASSIGN crashes when failing
* ARROW-7077 - [C++] Unsupported Dict->T cast crashes instead of returning error
* ARROW-7087 - [Python] Table Metadata disappear when we write a partitioned dataset
* ARROW-7097 - [Rust][CI] Builds failing due to rust nightly formatting
* ARROW-7100 - [C++] libjvm.so not found on ubuntu 19.04 with openjdk-11
* ARROW-7105 - [CI][Crossbow] Nightly homebrew-cpp job fails
* ARROW-7106 - [Java] Fix the problem that flight perf test hangs endlessly
* ARROW-7117 - [C++][CI] Fix the hanging C++ tests in Windows 2019
* ARROW-7128 - [CI] Fedora cron jobs are failing because of wrong fedora version
* ARROW-7133 - [CI] Allow GH Actions to run on all branches
* ARROW-7142 - [C++] Compile error with GCC 5.4.0
* ARROW-7152 - [Java] Delete useless class DiffFunction
* ARROW-7157 - [R] Add validation, helpful error message to Object$new()
* ARROW-7158 - [C++][Visual Studio]Build config Error on non English Version visual studio.
* ARROW-7163 - [Doc] Fix double-and typos
* ARROW-7164 - [CI] Dev cron github action is failing every 15 minutes
* ARROW-7167 - [CI][Python] Add nightly tests for older pandas versions to Github Actions
* ARROW-7168 - [Python] pa.array() doesn't respect specified dictionary type
* ARROW-7170 - [C++] Bundled ORC fails linking
* ARROW-7180 - [CI] Java builds are not triggered on the master branch
* ARROW-7181 - [Python][Nightly] Wheel builds could NOT find ArrowPython
* ARROW-7183 - [CI][Crossbow] Re-skip r-sanitizer nightly tests
* ARROW-7187 - [C++][Doc] doxygen broken on master because of @
* ARROW-7188 - [C++][Doc] doxygen broken on master: missing param implicit\_casts
* ARROW-7194 - [Rust] CSV Writer causing recursion errors
* ARROW-7199 - [Java] ConcurrentModificationException in BaseAllocator::getChildAllocators
* ARROW-7200 - [C++][Flight] Running Arrow Flight benchmark on two hosts doesn't work
* ARROW-7212 - "go test -bench=8192 -run=. ./math" fails
* ARROW-7214 - [Python] unpickling a pyarrow table with dictionary fields crashes
* ARROW-7225 - [C++] \`\*std::move(Result<T>)\` calls T copy constructor
* ARROW-7249 - [CI] Release test fails in master due to new arrow-flight Rust crate
* ARROW-7250 - [C++] Undefined symbols for StringToFloatConverter::Impl with clang 4.x
* ARROW-7253 - [CI] Fix master failure with release test
* ARROW-7254 - BaseVariableWidthVector#setSafe appears to make value offsets inconsistent
* ARROW-7264 - [Java] RangeEqualsVisitor type check is not correct
* ARROW-7266 - [Python] dictionary\_encode() of a slice gives wrong result
* ARROW-7271 - [C++][Flight] Use the single parameter version of SetTotalBytesLimit
* ARROW-7281 - [C++] AdaptiveIntBuilder::length() does not consider pending\_pos\_.
* ARROW-7282 - [Python] IO functions should raise FileNotFoundError when appropriate
* ARROW-7291 - [Dev] Fix FORMAT\_DIR in update-flatbuffers.sh
* ARROW-7294 - [Python] converted\_type\_name\_from\_enum(): Incorrect name for INT\_64
* ARROW-7295 - [R] Fix bad test that causes failure on R < 3.5
* ARROW-7298 - [C++] cpp/thirdparty/download-dependencies.sh is broken
* ARROW-7314 - [Python] Compiler warning in pyarrow
* ARROW-7318 - [C#] TimestampArray serialization failure
* ARROW-7320 - [C++] Target arrow-type-benchmark failed to be built on bullx Linux
* ARROW-7327 - [CI] Failing C GLib and R buildbot builders
* ARROW-7328 - [CI] GitHub Actions should trigger on changes to GitHub Actions configuration
* ARROW-7341 - [CI] Unbreak nightly Conda R job
* ARROW-7343 - [Java] Memory leak in Flight DoGet when client cancels
* ARROW-7349 - [C++] Fix the bug of parsing string hex values
* ARROW-7353 - [C++] Disable -Wmissing-braces when building with clang
* ARROW-7354 - [C++] TestHadoopFileSystem::ThreadSafety fails with sigabort
* ARROW-7355 - [CI] Environment variables are defined twice for the fuzzit builds
* ARROW-7358 - [CI] [Dev] [C++] ccache disabled on conda-python-hdfs
* ARROW-7359 - [C++][Gandiva] Don't throw error for locate function with start position exceeding string length, return 0 instead
* ARROW-7360 - [R] Can't use dplyr filter() with variables defined in parent scope
* ARROW-7361 - [Rust] Build directory is not passed to ci/scripts/rust\_test.sh
* ARROW-7362 - [Python] ListArray.flatten() should take care of slicing offsets
* ARROW-7374 - [Dev] [C++] cuda-cpp docker image fails compiling Arrow
* ARROW-7381 - [C++][Packaging] Iterator change broke manylinux1 wheels
* ARROW-7386 - [C#] Array offset does not work properly
* ARROW-7388 - [Python] Skip HDFS tests if libhdfs cannot be located
* ARROW-7389 - [Python][Packaging] Remove pyarrow.s3fs import check from the recipe
* ARROW-7393 - [Plasma] Fix plasma executable name in build for Java
* ARROW-7395 - [C++] Logical "or" with constants is a Clang warning
* ARROW-7397 - [C++] Json white space length detection error
* ARROW-7404 - [C++][Gandiva] Fix utf8 char length error on Arm64
* ARROW-7406 - [Java] NonNullableStructVector#hashCode should pass hasher to child vectors
* ARROW-7407 - [Python] Failed to install pyarrow 0.15.1 on Python 3.8
* ARROW-7408 - [C++] Reference benchmarks fail compiling
* ARROW-7435 - Security issue: ValidateOffsets() does not prevent buffer over-read
* ARROW-7436 - [Archery] Fix benchmark default configuration
* ARROW-7437 - [Java] ReadChannel#readFully does not set writer index correctly
* ARROW-7442 - [Ruby] Specifying column type as time causes segmentation fault
* ARROW-7447 - [Java] ComplexCopier does incorrect copy in some cases
* ARROW-7450 - [CI][C++] test-ubuntu-18.04-cpp-static failing with linking error in arrow-io-hdfs-test
* ARROW-7458 - [GLib] incorrect build dependency in Makefile
* ARROW-7471 - [Python] Cython flake8 failures
* ARROW-7472 - [Java] Fix some incorrect behavior in UnionListWriter
* ARROW-7478 - [Rust] [DataFusion] Group by expression ignored unless paired with aggregate expression
* ARROW-7492 - [CI][Crossbow] Nightly homebrew-cpp job fails on Python installation
* ARROW-7497 - [Python] Test asserts: pandas.util.testing is deprecated, use pandas.testing instead
* ARROW-7500 - [C++][Dataset] regex\_error in hive partition on centos7 and opensuse42
* ARROW-7503 - [Rust] Rust builds are failing on master
* ARROW-7506 - [Java] JMH benchmarks should be called from main methods
* ARROW-7508 - [C#] DateTime32 Reading is Broken
* ARROW-7510 - [C++] Array::null\_count() is not thread-compatible
* ARROW-7516 - [C#] .NET Benchmarks are broken
* ARROW-7518 - [Python] Use PYARROW\_WITH\_HDFS when building wheels, conda packages
* ARROW-7528 - [Python] The pandas.datetime class (import of datetime.datetime) and pandas.np are deprecated
* ARROW-7535 - [C++] ASAN failure in validation
* ARROW-7543 - [R] arrow::write\_parquet() code examples do not work
* ARROW-7551 - [FlightRPC][C++] Flight test on macOS fails due to Homebrew gRPC
* ARROW-7552 - [C++] TestSlowInputStream is flaky
* ARROW-7554 - [C++] Unknown CMake command "externalproject\_add".
* ARROW-7559 - [Rust] Possibly incorrect index check assertion in StringArray and BinaryArray
* ARROW-7561 - [Doc][Python] fix conda environment command
* ARROW-7582 - [Rust][Flight] Unable to compile arrow.flight.protocol.rs
* ARROW-7583 - [C++][Flight] Auth handler tests fragile on Windows
* ARROW-7591 - [Python] DictionaryArray.to\_numpy returns dict of parts instead of numpy array
* ARROW-7592 - [C++] Fix crashes on corrupt IPC input
* ARROW-7593 - [CI][Python] Python datasets failing on master / not run on CI
* ARROW-7595 - [R][CI] R appveyor job fails due to pacman compression change
* ARROW-7596 - [Python] Only apply zero-copy DataFrame block optimizations when split\_blocks=True
* ARROW-7599 - [Java] Fix build break due to change in RangeEqualsVisitor
* ARROW-7603 - [CI][Crossbow] Nightly centos 8 job fails
* ARROW-7611 - [Packaging][Python] Artifacts patterns for wheel are wrong
* ARROW-7612 - [Packaging][Python] Artifact paths for Conda on WIndows are wrong
* ARROW-7614 - [Python] Slow performance in test\_parquet.py::test\_set\_data\_page\_size
* ARROW-7618 - [C++] Fix crashes or undefined behaviour on corrupt IPC input
* ARROW-7620 - [Rust] Windows builds failing due to flatbuffer compile error
* ARROW-7621 - [Doc] Doc build fails
* ARROW-7634 - [Python] Dataset tests failing on Windows to parse file path
* ARROW-7638 - [Python] Segfault when inspecting dataset.Source with invalid file/partitioning
* ARROW-7639 - [R] Cannot convert Dictionary Array to R when values aren't strings
* ARROW-7640 - [C++][Dataset] segfault when reading compressed Parquet files if build didn't include support for codec
* ARROW-7647 - [C++] JSON reader fails to read arrays with few values
* ARROW-7650 - [C++] Dataset tests not built on Windows
* ARROW-7651 - [CI][Crossbow] Nightly macOS wheel builds fail
* ARROW-7652 - [Python][Dataset] Insert implicit cast in ScannerBuilder.filter
* ARROW-7661 - [Python] Non-optimal CSV chunking when no newline at end
* ARROW-7689 - [C++] Sporadic Flight test crash on macOS
* ARROW-7690 - [R] Cannot write parquet to OutputStream
* ARROW-7693 - [CI] Fix test-conda-python-3.7-spark-master nightly errors
* ARROW-7709 - [Python] Conversion from Table Column to Pandas loses name for Timestamps
* ARROW-7714 - [Release] Variable expansion is missing
* ARROW-7718 - [Release] Fix auto-retry in the binary release script
* ARROW-7723 - [Python] StructArray  timestamp type with timezone to\_pandas convert error
* PARQUET-1692 - [C++] LogicalType::FromThrift error on Centos 7 RPM
* PARQUET-1693 - [C++] Build examples don't account for CMAKE compression feature flags
* PARQUET-1702 - [C++] Make BufferedRowGroupWriter compatible with parquet encryption
* PARQUET-1706 - [C++] Wrong dictionary\_page\_offset when writing only data pages via BufferedPageWriter
* PARQUET-1709 - [C++] Avoid unnecessary temporary std::shared\_ptr copies
* PARQUET-1720 - [C++] Parquet JSONPrint not showing version correctly
* PARQUET-1747 - [C++] Access to ColumnChunkMetaData fails when encryption is on
* PARQUET-1766 - [C++] parquet NaN/null double statistics can result in endless loop
* PARQUET-1772 - [C++] ParquetFileWriter: Data overwritten when output stream opened in append mode

## Improvement

* ARROW-1175 - [Java] Implement/test dictionary-encoded subfields
* ARROW-1456 - [Python] Run s3fs unit tests in Travis CI
* ARROW-2428 - [Python] Add API to map Arrow types (including extension types) to pandas ExtensionArray instances for to\_pandas conversions
* ARROW-2602 - [Packaging] Automate build of development docker containers
* ARROW-2863 - [Python] Add context manager APIs to RecordBatch\*Writer/Reader classes
* ARROW-3789 - [Python] Enable calling object in Table.to\_pandas to "self-destruct" for improved memory use
* ARROW-4208 - [CI/Python] Have automatized tests for S3
* ARROW-4748 - [Rust] [DataFusion] GROUP BY performance could be optimized
* ARROW-4930 - [Python] Remove LIBDIR assumptions in Python build
* ARROW-5277 - [C#] MemoryAllocator.Allocate(length: 0) should not return null
* ARROW-5333 - [C++] Fit build option summary into narrower console
* ARROW-5454 - [C++] Implement Take on ChunkedArray for DataFrame use
* ARROW-5508 - [C++] Create reusable Iterator<T> interface
* ARROW-5523 - [Python] [Packaging] Use HTTPS consistently for downloading dependencies
* ARROW-5801 - [CI] Dockerize (add to docker-compose) all Travis CI Linux tasks
* ARROW-5802 - [CI] Dockerize "lint" Travis CI job
* ARROW-5809 - [Rust] Dockerize (add to docker-compose) Rust Travis CI build
* ARROW-5831 - [Release] Migrate and improve binary release verification script
* ARROW-5855 - [Python] Add support for Duration type
* ARROW-5859 - [Python] Support ExtentionType on conversion to numpy/pandas
* ARROW-6003 - [C++] Better input validation and error messaging in CSV reader
* ARROW-6074 - [FlightRPC] Implement middleware
* ARROW-6112 - [Java] Update APIs to support 64-bit address space
* ARROW-6251 - [Developer] Add PR merge tool to apache/arrow-site
* ARROW-6274 - [Rust] [DataFusion] Add support for writing results to CSV
* ARROW-6277 - [C++][Parquet] Support reading/writing other Parquet primitive types to DictionaryArray
* ARROW-6321 - [Python] Ability to create ExtensionBlock on conversion to pandas
* ARROW-6352 - [Java] Add implementation of DenseUnionVector.
* ARROW-6405 - [Python] Add std::move wrapper for use in Cython
* ARROW-6452 - [Java] Override ValueVector toString() method
* ARROW-6463 - [C++][Python] Rename arrow::fs::Selector to FileSelector
* ARROW-6466 - [Developer] Refactor integration/integration\_test.py into a proper Python package
* ARROW-6473 - [Format] Clarify dictionary encoding edge cases
* ARROW-6503 - [C++] Add an argument of memory pool object to SparseTensorConverter
* ARROW-6508 - [C++] Add Tensor and SparseTensor factory function with validations
* ARROW-6515 - [C++] Clean type\_traits.h definitions
* ARROW-6578 - [C++] Casting int64 to string columns
* ARROW-6598 - [Java] Sort the code for ApproxEqualsVisitor
* ARROW-6608 - [C++] Make default for ARROW\_HDFS to be OFF
* ARROW-6610 - [C++] Add ARROW\_FILESYSTEM=ON/OFF CMake configuration flag
* ARROW-6631 - [C++] Do not build with any compression library dependencies by default
* ARROW-6633 - [C++] Do not require double-conversion for default build
* ARROW-6634 - [C++] Do not require flatbuffers or flatbuffers\_ep to build
* ARROW-6635 - [C++] Do not require glog for default build
* ARROW-6636 - [C++] Do not build C++ command line utilities by default
* ARROW-6637 - [C++] Zero-dependency default core build
* ARROW-6646 - [Go] Amend NullType IPC implementation to append no buffers in RecordBatch message
* ARROW-6685 - [C++/Python] S3 FileStat object's base\_path and type depends on trailing slash
* ARROW-6686 - [CI] Pull and push docker images to speed up the nightly builds
* ARROW-6688 - [Packaging] Include s3 support in the conda packages
* ARROW-6700 - [Rust] [DataFusion] Use new parquet arrow reader
* ARROW-6707 - [Java] Improve the performance of JDBC adapters by using nullable information
* ARROW-6710 - [Java] Add JDBC adapter test to cover cases which contains some null values
* ARROW-6711 - [C++] Consolidate Filter and Expression classes
* ARROW-6722 - [Java] Provide a uniform way to get vector name
* ARROW-6729 - [C++] StlStringBuffer constructor is not zero-copy
* ARROW-6732 - [Java] Implement quick sort in a non-recursive way to avoid stack overflow
* ARROW-6741 - [Release] Update changelog.py to use APACHE\_ prefixed JIRA\_USERNAME and JIRA\_PASSWORD environment variables
* ARROW-6742 - [C++] Remove usage of boost::filesystem::path from arrow/io/hdfs\_internal.cc
* ARROW-6754 - [C++] Merge arrow/allocator.h and arrow/stl.h, or rename allocator.h
* ARROW-6758 - [Release] Install ephemeral node/npm/npx in release verification script
* ARROW-6764 - [C++] Add readahead iterator
* ARROW-6767 - [JS] lazily bind batches in scan/scanReverse
* ARROW-6770 - [CI][Travis] Download Minio quietly
* ARROW-6777 - [GLib][CI] Unpin gobject-introspection gem
* ARROW-6778 - [C++] Support DurationType in Cast kernel
* ARROW-6782 - [C++] Build minimal core Arrow libraries without any Boost headers
* ARROW-6784 - [C++][R] Move filter and take code from Rcpp to C++ library
* ARROW-6787 - [CI] Decommission "C++ with clang 7 and system packages" Travis CI job
* ARROW-6788 - [CI] Migrate Travis CI lint job to GitHub Actions
* ARROW-6789 - [Python] Automatically box bytes/buffer-like values yielded from \`FlightServerBase.do\_action\` in Result values
* ARROW-6790 - [Release] Automatically disable integration test cases in release verification
* ARROW-6793 - [R] Arrow C++ binary packaging for Linux
* ARROW-6797 - [Release] Use a separately cloned arrow-site repository in the website post release script
* ARROW-6802 - [Packaging][deb][RPM] Update qemu-user-static package URL
* ARROW-6804 - [CI] [Rust] Migrate Travis Rust job to Github Actions
* ARROW-6810 - [Website] Add docs for R package 0.15 release
* ARROW-6811 - [R] Assorted post-0.15 release cleanups
* ARROW-6822 - [Website] merge\_pr.py is published
* ARROW-6824 - [Plasma] Support batched create and seal requests for small objects
* ARROW-6825 - [C++] Rework CSV reader IO around readahead iterator
* ARROW-6831 - [R] Update R macOS/Windows builds for change in cmake compression defaults
* ARROW-6832 - [R] Implement Codec::IsAvailable
* ARROW-6833 - [R][CI] Add crossbow job for full R autobrew macOS build
* ARROW-6843 - [Website] Disable deploy on pull request
* ARROW-6847 - [C++] Add a range\_expression interface to Iterator<>
* ARROW-6852 - [C++] memory-benchmark build failed on Arm64
* ARROW-6862 - [Developer] Check pull request title
* ARROW-6865 - [Java] Improve the performance of comparing an ArrowBuf against a byte array
* ARROW-6866 - [Java] Improve the performance of calculating hash code for struct vector
* ARROW-6881 - [Rust] Remove "array\_ops" in favor of the "compute" sub-module
* ARROW-6884 - [Python][Flight] Make server-side RPC exceptions more friendly?
* ARROW-6887 - [Java] Create prose documentation for using ValueVectors
* ARROW-6889 - [Java] ComplexCopier enable FixedSizeList type & fix RangeEualsVisitor StackOverFlow
* ARROW-6902 - [C++] Add String\*/Binary\* support for Compare kernels
* ARROW-6904 - [Python] Implement MapArray and MapType
* ARROW-6907 - [C++][Plasma] Allow Plasma store to batch notifications to clients
* ARROW-6916 - [Developer] Alphabetize task names in nightly Crossbow report
* ARROW-6918 - [R] Make docker-compose setup faster
* ARROW-6919 - [Python] Expose more builders in Cython
* ARROW-6920 - [Python] create manylinux wheels for python3.8
* ARROW-6926 - [Python] Support \_\_sizeof\_\_ protocol for Python objects
* ARROW-6927 - [C++] Add gRPC version check
* ARROW-6932 - [Java] incorrect log on known extension type
* ARROW-6936 - [Python] Improve error message when object of wrong type is given
* ARROW-6942 - [Developer] Add support for Parquet in pull request check by GitHub Actions
* ARROW-6943 - [Website] Translate Apache Arrow Flight introduction to Japanese
* ARROW-6951 - [C++][Dataset] Ensure column projection is passed to ParquetDataFragment
* ARROW-6952 - [C++][Dataset] Ensure expression filter is passed ParquetDataFragment
* ARROW-6960 - [R] Add support for more compression codecs in Windows build
* ARROW-6963 - [Packaging][Wheel][OSX] Use crossbow's command to deploy artifacts from travis builds
* ARROW-6964 - [C++][Dataset] Expose a nested parallel option for Scanner::ToTable
* ARROW-6965 - [C++][Dataset] Optionally expose partition keys as materialized columns
* ARROW-6967 - [C++] Add filter expressions for IN, IS\_VALID
* ARROW-6969 - [C++][Dataset] ParquetScanTask eagerly load file
* ARROW-6970 - [Packaging][RPM] Add support for CentOS 8
* ARROW-6973 - [C++][ThreadPool] Use perfect forwarding in Submit
* ARROW-6980 - [R] dplyr backend for RecordBatch/Table
* ARROW-6987 - [CI] Travis OSX failing to install sdk headers
* ARROW-6991 - [Packaging][deb] Add support for Ubuntu 19.10
* ARROW-6994 - [C++] Research jemalloc memory page reclamation configuration on macOS when background\_thread option is unavailable
* ARROW-6997 - [Packaging] Add support for RHEL
* ARROW-7004 - [Plasma] Make it possible to bump up object in LRU cache
* ARROW-7006 - [Rust] Bump flatbuffers version to avoid vulnerability
* ARROW-7007 - [C++] Enable mmap option for LocalFs
* ARROW-7019 - [Java] Improve the performance of loading validity buffers
* ARROW-7026 - [Java] Remove assertions in MessageSerializer/vector/writer/reader
* ARROW-7031 - [Python] Expose the offsets of a ListArray in python
* ARROW-7032 - [Release] Run the python unit tests in the release verification script
* ARROW-7034 - [CI][Crossbow] Skip known nightly failures
* ARROW-7035 - [R] Default arguments are unclear in write\_parquet docs
* ARROW-7039 - [Python] Typecheck expects pandas to be installed
* ARROW-7054 - [Docs] Add option to override displayed docs version with an environment variable
* ARROW-7057 - [C++] Add API to parse URI query strings
* ARROW-7058 - [C++] FileSystemDataSourceDiscovery should apply partition schemes relative to the base\_dir of its selector
* ARROW-7062 - [C++] Parquet file parse error messages should include the file name
* ARROW-7066 - [Python] support returning ChunkedArray from \_\_arrow\_array\_\_ ?
* ARROW-7069 - [C++][Dataset] Replace ConstantPartitionScheme with PrefixDictionaryPartitionScheme
* ARROW-7070 - [Packaging][deb] Update package names for 1.0.0
* ARROW-7082 - [Packaging][deb] Add apache-arrow-archive-keyring
* ARROW-7096 - [C++] Add options structs for concatenation-with-promotion and schema unification
* ARROW-7098 - [Java] Improve the performance of comparing two memory blocks
* ARROW-7101 - [CI] Refactor docker-compose setup and use it with GitHub Actions
* ARROW-7107 - [C++][MinGW] Enable Flight on AppVeyor
* ARROW-7113 - [Rust] Buffer should accept memory owned by others
* ARROW-7146 - [R][CI] Various fixes and speedups for the R docker-compose setup
* ARROW-7148 - [C++][Dataset] API cleanup
* ARROW-7166 - [Java] Remove redundant code for Jdbc adapters
* ARROW-7172 - [C++][Dataset] Improve format of Expression::ToString
* ARROW-7176 - [C++] Fix arrow::ipc compiler warning
* ARROW-7178 - [C++] Vendor forward compatible std::optional
* ARROW-7195 - [Ruby] Improve #filter, #take, and #is\_in
* ARROW-7196 - [Ruby] Remove needless BinaryArrayBuilder#append\_values
* ARROW-7197 - [Ruby] Suppress keyword argument related warnings with Ruby 2.7
* ARROW-7204 - [C++][Dataset] In expression should not require exact type match
* ARROW-7206 - [Java] Avoid string concatenation when calling Preconditions#checkArgument
* ARROW-7210 - [C++] Scalar cast should support time-based types
* ARROW-7216 - [Java] Improve the performance of setting/clearing individual bits
* ARROW-7219 - [CI][Python] Install pickle5 in the conda-python docker image for python version 3.6
* ARROW-7246 - [CI][Python] wheel can't be built by SSL\_ST\_INIT error
* ARROW-7247 - [CI][Python] wheel can't be built by wget and OpenSSL error
* ARROW-7248 - [Rust] Automatically Regenerate IPC messages from Flatbuffers
* ARROW-7255 - [CI] Run source release test on pull request
* ARROW-7256 - [C++] Remove ARROW\_MEMORY\_POOL\_DEFAULT option
* ARROW-7257 - [CI] Homebrew formula is failed by openssl formula name update
* ARROW-7258 - [CI] Fuzzit job is failed by nonexistent directory
* ARROW-7260 - [CI] Ubuntu 14.04 test is failed by user defined literal
* ARROW-7261 - [Python] Python support for fixed size list type
* ARROW-7268 - [Rust] Propagate \`custom\_metadata\` field from IPC message
* ARROW-7269 - [C++] Fix arrow::parquet compiler warning
* ARROW-7275 - [Ruby] Add support for Arrow::ListDataType.new(data\_type)
* ARROW-7276 - [Ruby] Add support for building Arrow::ListArray from [[...]]
* ARROW-7277 - [Document] Add discussion about vector lifecycle
* ARROW-7289 - [C#] ListType constructor argument is redundant
* ARROW-7290 - [C#] Implement ListArray Builder
* ARROW-7292 - [C++] [CI] [Dev] Add ASAN / UBSAN CI run
* ARROW-7293 - [Dev] [C++] Persist ccache in docker-compose build volumes
* ARROW-7296 - [Python] Add ORC api documentation
* ARROW-7299 - [GLib] Use Result instead of Status
* ARROW-7303 - [C++] Refactor benchmarks to use new Result APIs
* ARROW-7307 - [CI][GLib] Documentation isn't generated
* ARROW-7309 - [Python] Support HDFS federation viewfs://
* ARROW-7310 - [Python] Expose HDFS implementation for pyarrow.fs
* ARROW-7311 - [Python] Return filesystem and path from URI
* ARROW-7312 - [Rust] ArrowError should implement std::error:Error
* ARROW-7321 - [CI][GLib] Failed to build with GLib warning
* ARROW-7323 - [CI][Rust] Nightly CI is failed by different toolchain
* ARROW-7325 - [Rust] [Parquet] Update to parquet-format 2.6 and thrift 0.12
* ARROW-7329 - [Java] AllocationManager: Allow managing different types of memory other than those are allocated using Netty
* ARROW-7333 - [CI][Rust] Remove duplicated nightly job
* ARROW-7334 - [CI][Python] macOS uses Python 2
* ARROW-7340 - [CI] Prune defunct appveyor build setup
* ARROW-7347 - [C++] Update bundled Boost to 1.71.0
* ARROW-7348 - [Rust] Add api to return references of buffer of null bitmap.
* ARROW-7351 - [Developer] Only suggest cpp-\* fix versions when merging Parquet patches
* ARROW-7367 - [Python] Use np.full instead of np.array.repeat in ParquetDatasetPiece
* ARROW-7368 - [Ruby] Use :arrow\_file and :arrow\_streaming for format name
* ARROW-7370 - [C++] Old Protobuf with AUTO detection is failed
* ARROW-7377 - [C++][Dataset] Simplify parquet column projection
* ARROW-7379 - [C++] Introduce SchemaBuilder companion class and Field::IsCompatibleWith
* ARROW-7380 - [C++][Dataset] Implement DatasetFactory
* ARROW-7382 - [C++][Dataset] Refactor FsDsDiscovery constructors
* ARROW-7387 - [C#] Support ListType Serialization
* ARROW-7392 - [Packaging] Add conda packaging tasks for python 3.8
* ARROW-7398 - [Packaging][Python] Conda builds are failing on macOS
* ARROW-7402 - [C++] Add more information on CUDA error
* ARROW-7403 - [C++][JSON] Enable Rapidjson on Arm64 Neon
* ARROW-7411 - [C++][Flight] Incorrect Arrow Flight benchmark output
* ARROW-7413 - [Python][Dataset] Add tests for PartitionSchemeDiscovery
* ARROW-7414 - [R][Dataset] Implement PartitionSchemeDiscovery
* ARROW-7417 - [C++] Add a docker-compose entry for CUDA 10.1
* ARROW-7418 - [C++] Can't build with g++ 5.4.0 on Ubuntu 16.04
* ARROW-7429 - [Java] Enhance code style checking for Java code (remove consecutive spaces)
* ARROW-7430 - [Python] Add more docstrings to dataset bindings
* ARROW-7431 - [Python] Add dataset API to reference docs
* ARROW-7432 - [Python] Add higher-level datasets functions
* ARROW-7449 - [GLib] Make GObject Introspection optional
* ARROW-7452 - [GLib] Make GArrowTimeDataType abstract
* ARROW-7453 - [Ruby] Add support for Arrow::NullArray#[]
* ARROW-7454 - [Ruby] Add support for saving/loading TSV
* ARROW-7455 - [Ruby] Use Arrow::DataType.resolve for all GArrowDataType input
* ARROW-7456 - [C++] Add support for YYYY-MM-DDThh and YYYY-MM-DDThh:mm timestamp formats
* ARROW-7457 - [Doc] Fix typos
* ARROW-7459 - [Python] Documentation lint is failed
* ARROW-7460 - [Rust] Improve some kernels with autovectorisation
* ARROW-7461 - [Java] Fix typos and spelling
* ARROW-7463 - [Doc] Fix a broken link and typos
* ARROW-7464 - [C++] Refine CpuInfo singleton with std::call\_once
* ARROW-7465 - [C++] Add Arrow memory benchmark for Arm64
* ARROW-7468 - [Python] Fix typos
* ARROW-7469 - [C++] Improve division related bit operations
* ARROW-7470 - [JS] Fix typos
* ARROW-7474 - [Ruby] Save CSV files faster
* ARROW-7477 - [FlightRPC][Java] Flight gRPC service is missing reflection info
* ARROW-7479 - [Rust][Ruby][R] Fix typos
* ARROW-7481 - [C#] Fix typos
* ARROW-7482 - [C++] Fix typos
* ARROW-7484 - [C++][Gandiva] Fix typos
* ARROW-7485 - [C++][Plasma] Fix typos
* ARROW-7487 - [Developer] Fix typos
* ARROW-7488 - [GLib] Fix typos and broken links
* ARROW-7489 - [CI] Fix typos
* ARROW-7493 - [Python] Expose sum kernel in pyarrow.compute and support ChunkedArray inputs
* ARROW-7502 - [Integration] Remove Spark Integration patch that not needed anymore
* ARROW-7514 - [C#] Make GetValueOffset Obsolete
* ARROW-7519 - [Python] Build wheels, conda packages with dataset support
* ARROW-7521 - [Rust] Remove tuple on FixedSizeList datatype
* ARROW-7523 - [Developer] Relax clang-tidy check
* ARROW-7526 - [C++][Compute]: Optimize small integer sorting
* ARROW-7532 - [CI] Unskip brew test after Homebrew fixes it upstream
* ARROW-7537 - [CI][R] Nightly macOS autobrew job should be more verbose if it fails
* ARROW-7538 - Clarify actual and desired size in AllocationManager
* ARROW-7540 - [C++] License files aren't installed
* ARROW-7541 - [GLib] Install license files
* ARROW-7542 - [CI][C++] nproc isn't available on macOS
* ARROW-7550 - [R][CI] Run donttest examples in CI
* ARROW-7557 - [C++][Compute] Validate sorting stability in random test
* ARROW-7558 - [Packaging][deb][RPM] Use the host owner and group for artifacts
* ARROW-7560 - [Rust] Reduce Rc/Refcell usage
* ARROW-7565 - [Website] Add support for download URL redirect
* ARROW-7567 - [Java] Bump Checkstyle from 6.19 to 8.18
* ARROW-7568 - [Java] Bump Apache Avro from 1.9.0 to 1.9.1
* ARROW-7569 - [Python] Add API to map Arrow types to pandas ExtensionDtypes for to\_pandas conversions
* ARROW-7570 - [Java] Fix high severity issues reported by LGTM
* ARROW-7571 - [Java] Correct minimal java version on README
* ARROW-7572 - [Java] Enfore Maven 3.3+ as mentioned in README
* ARROW-7573 - [Rust] Reduce boxing and cleanup
* ARROW-7575 - [R] Linux binary packaging followup
* ARROW-7578 - [R] Add support for datasets with IPC files and with multiple sources
* ARROW-7581 - [R] Documentation/polishing for 0.16 release
* ARROW-7590 - [C++] Managed files in thirdparty/ are ignored
* ARROW-7597 - [C++] Improvements to CMake configuration console summary
* ARROW-7602 - [Archery] Add more build options
* ARROW-7613 - [Rust] Remove redundant \`::\` prefixes
* ARROW-7626 - [Parquet][GLib] Add support for version macros
* ARROW-7635 - [C++] Add pkg-config support for each components
* ARROW-7636 - [Python] Clean-up the pyarrow.dataset.partitioning() API
* ARROW-7644 - Add vcpkg installation instructions
* ARROW-7645 - [Packaging][deb][RPM] arm64 build by crossbow is broken
* ARROW-7659 - [Rust] Reduce Rc usage
* ARROW-7666 - [Packaging][deb] Always use NInja to reduce build time
* ARROW-7667 - [Packaging][deb] ubuntu-eoan is missing in nightly jobs
* ARROW-7668 - [Packaging][RPM] Use NInja if possible to reduce build time
* ARROW-7671 - [Python][Dataset] Add bindings for the DatasetFactory
* ARROW-7674 - Add helpful message for captcha challenge in merge\_arrow\_pr.py
* ARROW-7682 - [Packaging][APT][Yum] Add support for arm64 APT/Yum repositories
* ARROW-7683 - [Packaging] Set 0.16.0 as the next version
* ARROW-7686 - [Packaging][deb][RPM] Include more arrow-\*.pc
* ARROW-7687 - [C++] C++ developer document links in README are broken
* ARROW-7692 - [Rust] Several pattern matches are hard to read
* ARROW-7694 - [Packaging][deb][RPM] Can't build repository packages for RC
* ARROW-7695 - [Release] Update java versions to 0.16-SNAPSHOT
* ARROW-7696 - [Release] Unit test on release branch is failed
* ARROW-7697 - [Release] Add a test for updating Linux packages by 00-prepare.sh
* ARROW-7710 - [Release][C#] .NET download URL is redirected
* ARROW-7711 - [C#] Date32 test depends on system timezone
* ARROW-7715 - [Release][APT] Ignore some arm64 verifications
* ARROW-7716 - [Packaging][APT] Use the "main" component for Ubuntu 19.10
* ARROW-7719 - [Python][Dataset] Table equality check occasionally fails
* ARROW-7724 - [Release][Yum] Ignore some arm64 verifications
* ARROW-7768 - [Rust] Implement Length and TryClone traits for Cursor<Vec<u8>> in reader.rs
* ARROW-8015 - [Python] Build 0.16.0 wheel install for Windows + Python 3.5 and publish to PyPI
* PARQUET-1664 - [C++] Provide API to return metadata string from FileMetadata.
* PARQUET-1678 - [C++] Provide classes for reading/writing using input/output operators
* PARQUET-1688 - [C++] StreamWriter/StreamReader can't be built with g++ 4.8.5 on CentOS 7
* PARQUET-1689 - [C++] Stream API: Allow for columns/rows to be skipped when reading
* PARQUET-1701 - [C++] Stream API: Add support for optional fields
* PARQUET-1704 - [C++] Add re-usable encryption buffer to SerializedPageWriter
* PARQUET-1705 - [C++] Disable shrink-to-fit on the re-usable decryption buffer
* PARQUET-1721 - [C++] Arrow dependency is missing in parquet.pc
* PARQUET-1734 - [C++] Fix typos
* PARQUET-1769 - [C++] Update to parquet-format 2.8.0

## New Feature

* ARROW-1638 - [Java] IPC roundtrip for null type
* ARROW-1900 - [C++] Add kernel functions for determining value range (maximum and minimum) of integer arrays
* ARROW-3408 - [C++] Add option to CSV reader to dictionary encode individual columns or all string / binary columns
* ARROW-3444 - [Python] Table.nbytes attribute
* ARROW-3808 - [R] Implement [.arrow::Array
* ARROW-3813 - [R] lower level construction of Dictionary Arrays
* ARROW-412 - [Format] Handling of buffer padding in the IPC metadata
* ARROW-4223 - [Python] Support scipy.sparse integration
* ARROW-4224 - [Python] Support integration with pydata/sparse library
* ARROW-4225 - [Format][C++] Add CSC sparse matrix support
* ARROW-4722 - [C++] Implement Bitmap class to modularize handling of bitmaps
* ARROW-5971 - [Website] Blog post introducing Arrow Flight
* ARROW-6184 - [Java] Provide hash table based dictionary encoder
* ARROW-6285 - [GLib] Add support for LargeBinary and LargeString types
* ARROW-6286 - [GLib] Add support for LargeList type
* ARROW-6340 - [R] Implements low-level bindings to Dataset classes
* ARROW-6341 - [Python] Implement low-level bindings for Dataset
* ARROW-6378 - [C++][Dataset] Implement TreeDataSource
* ARROW-6394 - [Java] Support conversions between delta vector and partial sum vector
* ARROW-6396 - [C++] Add ResolveNullOptions to Logical kernels
* ARROW-6619 - [Ruby] Add support for building Gandiva::Expression by Arrow::Schema#build\_expression
* ARROW-6624 - [C++] Add SparseTensor.ToTensor() method
* ARROW-6662 - [Java] Implement equals/approxEquals API for VectorSchemaRoot
* ARROW-6672 - [Java] Extract a common interface for dictionary builders
* ARROW-6730 - [CI] Use GitHub Actions for "C++ with clang 7" docker image
* ARROW-6744 - [Rust] Export JsonEqual trait in the array module
* ARROW-6768 - [C++][Dataset] Implement dataset::Scan to Table helper function
* ARROW-6769 - [C++][Dataset] End to End dataset integration test case
* ARROW-6807 - [Java][FlightRPC] Expose gRPC service
* ARROW-6836 - [Format] add a custom\_metadata:[KeyValue] field to the Footer table in File.fbs
* ARROW-6850 - [Java] Jdbc converter support Null type
* ARROW-6853 - [Java] Support vector and dictionary encoder use different hasher for calculating hashCode
* ARROW-6855 - [C++][Python][Flight] Implement Flight middleware
* ARROW-6863 - [Java] Provide parallel searcher
* ARROW-6888 - [Java] Support copy operation for vector value comparators
* ARROW-6891 - [Rust] [Parquet] Add Utf8 support to ArrowReader
* ARROW-6911 - [Java] Provide composite comparator
* ARROW-6930 - [Java] Create utility class for populating vector values used for test purpose only
* ARROW-6933 - [Java] Suppor linear dictionary encoder
* ARROW-6986 - [R] Add basic Expression class
* ARROW-7014 - [Developer] Write script to verify Linux wheels given local environment with conda or virtualenv
* ARROW-7015 - [Developer] Write script to verify macOS wheels given local environment with conda or virtualenv
* ARROW-7016 - [Developer][Python] Write script to verify Windows wheels given local environment with conda
* ARROW-7047 - [C++][Dataset] Filter expressions should not require exact type match
* ARROW-7052 - [C++] Datasets example fails to build with ARROW\_SHARED=OFF
* ARROW-7060 - [R] Post-0.15.1 cleanup
* ARROW-7061 - [C++][Dataset] FileSystemDiscovery with ParquetFileFormat should ignore files that aren't Parquet
* ARROW-7064 - [R] Implement null type
* ARROW-7072 - [Java] Support concating validity bits efficiently
* ARROW-7092 - [R] Add vignette for dplyr and datasets
* ARROW-7093 - [R] Support creating ScalarExpressions for more data types
* ARROW-7095 - [R] Better handling of unsupported filter and mutate expressions in dplyr methods
* ARROW-7103 - [R] Various minor cleanups
* ARROW-7110 - [GLib] Add filter support for GArrowTable, GArrowChunkedArray, and GArrowRecordBatch
* ARROW-7111 - [GLib] Add take support for GArrowTable, GArrowChunkedArray, and GArrowRecordBatch
* ARROW-7171 - [Ruby] Pass Array<Boolean> for Arrow::Table#filter
* ARROW-7185 - [R][Dataset] Add bindings for IN, IS\_VALID expressions
* ARROW-7186 - [R] Add inline comments to document the dplyr code
* ARROW-7192 - [Rust] Implement Flight crate
* ARROW-7211 - [Rust] [Parquet] Support writing to byte buffers
* ARROW-7227 - [Python] Provide wrappers for ConcatenateWithPromotion()
* ARROW-7228 - [Python] Expose RecordBatch.FromStructArray in Python.
* ARROW-7259 - [Java] Support subfield encoder use different hasher
* ARROW-7270 - [Go] preserve CSV reading behaviour, improve memory usage
* ARROW-7344 - [Packaging][Python] Build manylinux2014 wheels
* ARROW-7357 - [Go] migrate from pkg/errors to x/xerrors
* ARROW-7366 - [C++][Dataset] Use PartitionSchemeDiscovery in DataSourceDiscovery
* ARROW-7369 - [GLib] Add garrow\_table\_combine\_chunks
* ARROW-7415 - [C++][Dataset] Implement IpcFormat for sources composed of ipc files
* ARROW-7416 - [R][Nightly] Fix macos-r-autobrew build on R 3.6.2
* ARROW-7658 - [R] Support dplyr filtering on date/time
* ARROW-7670 - [Python][Dataset] Better ergonomics for the filter expressions
* ARROW-772 - [C++] Implement take kernel functions
* ARROW-7743 - [Rust] [Parquet] Support reading timestamp micros
* ARROW-843 - [C++] Implement Schema unification, merging unequal but equivalent schemas
* ARROW-976 - [C++][Python] Provide API for defining and reading Parquet datasets with more ad hoc partition schemes
* PARQUET-1300 - [C++] Parquet modular encryption

## Sub-task

* ARROW-1562 - [C++] Numeric kernel implementations for add (+)
* ARROW-4219 - [Rust] [Parquet] Implement ArrowReader
* ARROW-5181 - [Rust] Create Arrow File reader
* ARROW-5182 - [Rust] Create Arrow File writer
* ARROW-5366 - [Rust] Implement Duration and Interval Arrays
* ARROW-6091 - [Rust] [DataFusion] Implement parallel execution for limit
* ARROW-6283 - [Rust] [DataFusion] Implement operator to write query results to partitioned CSV
* ARROW-6592 - [Java] Add support for skipping decoding of columns/field in Avro converter
* ARROW-6594 - [Java] Support logical type encodings from Avro
* ARROW-6650 - [Rust] [Integration] Create methods to test Arrow files against Integration JSON
* ARROW-6656 - [Rust] [DataFusion] Implement MIN and MAX aggregate expressions
* ARROW-6657 - [Rust] [DataFusion] Implement COUNT aggregate expression
* ARROW-6658 - [Rust] [DataFusion] Implement AVG aggregate expression
* ARROW-6659 - [Rust] [DataFusion] Refactor of HashAggregateExec to support custom merge
* ARROW-6690 - [Rust] [DataFusion] HashAggregate without GROUP BY should use SIMD
* ARROW-6692 - [Rust] [DataFusion] Update examples to use physical query plan
* ARROW-6694 - [Rust] [DataFusion] Update integration tests to use physical plan
* ARROW-6695 - [Rust] [DataFusion] Remove execution of logical plan
* ARROW-6696 - [Rust] [DataFusion] Implement simple math operations in physical query plan
* ARROW-6721 - [JAVA] Avro adapter benchmark only runs once in JMH
* ARROW-6803 - [Rust] [DataFusion] Aggregate queries are slower with new physical query plan
* ARROW-6912 - [Java] Extract a common base class for avro converter consumers
* ARROW-6928 - [Rust] Add FixedSizeList type
* ARROW-6944 - [Rust] Add StringType
* ARROW-7003 - [Format] [Rust] Generate flatbuffers files in build script
* ARROW-7094 - [C++] FileSystemDataSource should use an owning pointer for fs::Filesystem
* ARROW-7147 - [C++][Dataset] Refactor dataset's API to use Result<T>
* ARROW-7193 - [Rust] Create Arrow stream reader
* ARROW-7207 - [Rust] Update Generated Flatbuffer Files
* ARROW-7235 - [C++] Add Result<T> to APIs to arrow/io
* ARROW-7236 - [C++] Add Result<T> to APIs to arrow/csv
* ARROW-7240 - [C++] Add Result<T> to APIs to arrow/util
* ARROW-7274 - [C++] Add Result<T> APIs to Decimal class
* ARROW-7284 - [Java] ensure java implementation meets clarified dictionary spec
* ARROW-7317 - [C++] Migrate Iterator API to Result<T>
* ARROW-7324 - [Rust] Add Timezone to Timestamp
* ARROW-7439 - [C++][Dataset] Remove dataset pointer aliases
* ARROW-7475 - [Rust] Create Arrow Stream writer
* ARROW-7490 - [Java] Avro converter should convert attributes and props to FieldType metadata
* ARROW-7576 - [C++][Dev] Improve fuzzing setup
* ARROW-7577 - [C++][CI] Check fuzzer setup in CI
* ARROW-7600 - [C++][Parquet] Add a basic disabled unit test to excercise nesting functionality
* ARROW-7601 - [Doc] [C++] Update fuzzing documentation

## Task

* ARROW-6367 - [C++][Gandiva] Implement string reverse
* ARROW-6468 - [C++] Remove unused hashing routines
* ARROW-6743 - [C++] Completely remove usage of boost::filesystem (except in hdfs\_internal)
* ARROW-6814 - [C++] Resolve compiler warnings occurred on release build
* ARROW-6949 - [Java] Fix promotable write to handle nullvectors
* ARROW-6961 - [C++][Gandiva] Add lower\_utf8 function in Gandiva
* ARROW-7000 - [C++][Gandiva] Handle empty inputs in string lower, upper functions
* ARROW-7036 - [C++] Version up ORC to avoid compile errors
* ARROW-7037 - [C++ ] Compile error on the combination of protobuf >= 3.9 and clang
* ARROW-7099 - [C++] Disambiguate function calls in csv parser test
* ARROW-7116 - [CI] Use the docker repository provided by apache organisation
* ARROW-7149 - [C++] Remove experimental status on filesystem APIs
* ARROW-7159 - [CI] Run HDFS tests as cron task
* ARROW-7160 - [C++] Update string\_view backport
* ARROW-7161 - [C++] Migrate filesystem layer from Status to Result
* ARROW-7262 - [C++][Gandiva] Implement replace function in Gandiva
* ARROW-7263 - [C++][Gandiva] Implement locate and position functions
* ARROW-7306 - [C++] Add Result-returning version of FileSystemFromUri
* ARROW-7322 - [CI][Python] Fall back to arrowdev dockerhub organization for manylinux images
* ARROW-7346 - [CI] Explicit usage of ccache across the builds
* ARROW-7378 - [C++][Gandiva] Loop vectorization broken in IR optimization
* ARROW-7399 - [C++][Gandiva] Gandiva does not pick runtime cpu features
* ARROW-7410 - [Python] [Doc] Document filesystem APIs
* ARROW-7420 - [C++] Migrate tensor related APIs to Result-returning version
* ARROW-7513 - [JS] Arrow Tutorial: Common data types
* ARROW-7549 - [Java] Reorganize Flight modules to keep top level clean/organized
* ARROW-7623 - [C++] Update generated flatbuffers files
* ARROW-7627 - [C++][Gandiva] Optimize string truncate function
* ARROW-7629 - [C++][CI] Add fuzz regression files to arrow-testing
* ARROW-7630 - [C++][CI] Check fuzz crash regressions in CI
* ARROW-7632 - [C++] [CI] Improve fuzzing seed corpus
* ARROW-7660 - [C++][Gandiva] Optimise castVarchar(string, int) function for single byte characters
* PARQUET-1712 - [C++] Stop using deprecated APIs in examples

## Test

* ARROW-6885 - [Python] Remove superfluous skipped timedelta test
* ARROW-6950 - [C++][Dataset] Add example/benchmark for reading parquet files with dataset
* ARROW-7209 - [Python] tests with pandas master are failing now \_\_from\_arrow\_\_ support landed in pandas
* ARROW-7217 - ARROW-7217: [CI][Python] Use correct python version in Github Actions
* ARROW-7527 - [Python] pandas/feather tests failing on pandas master

## Wish

* ARROW-6625 - [Python] Allow concat\_tables to null or default fill missing columns
* ARROW-6671 - [C++] Sparse tensor naming
* ARROW-6954 - [Python] [CI] Add Python 3.8 to CI matrix
* ARROW-6975 - [C++] Put make\_unique in its own header
* ARROW-6984 - [C++] Update LZ4 to 1.9.2 for CVE-2019-17543
* ARROW-7067 - [CI] Disable code coverage on Travis-CI
* ARROW-7162 - [C++] Cleanup warnings in cmake\_modules/SetupCxxFlags.cmake
* ARROW-7169 - [C++] Vendor uriparser library
* ARROW-7279 - [C++] Rename UnionArray::type\_ids to UnionArray::type\_codes
* ARROW-7498 - [C++][Dataset] Rename DataFragment/DataSource/PartitionScheme
* ARROW-7566 - [CI] Use more recent Miniconda on AppVeyor
* ARROW-7622 - [Format] Mark Tensor and SparseTensor fields required
* ARROW-7648 - [C++] Sanitize local paths on Windows
* ARROW-7665 - [R] linuxLibs.R should build in parallel

# Apache Arrow 0.15.0 (30 September 2019)

## Bug

* ARROW-1184 - [Java] Dictionary.equals is not working correctly
* ARROW-2317 - [Python] fix C linkage warning
* ARROW-2490 - [C++] input stream locking inconsistent
* ARROW-3176 - [Python] Overflow in Date32 column conversion to pandas
* ARROW-3203 - [C++] Build error on Debian Buster
* ARROW-3651 - [Python] Datetimes from non-DateTimeIndex cannot be deserialized
* ARROW-3652 - [Python] CategoricalIndex is lost after reading back
* ARROW-3762 - [C++] Parquet arrow::Table reads error when overflowing capacity of BinaryArray
* ARROW-3933 - [Python] Segfault reading Parquet files from GNOMAD
* ARROW-4187 - [C++] file-benchmark uses <poll.h>
* ARROW-4746 - [C++/Python] PyDataTime\_Date wrongly casted to PyDataTime\_DateTime
* ARROW-4836 - [Python] "Cannot tell() a compressed stream" when using RecordBatchStreamWriter
* ARROW-4848 - [C++] Static libparquet not compiled with -DARROW\_STATIC on Windows
* ARROW-4880 - [Python] python/asv-build.sh is probably broken after CMake refactor
* ARROW-4883 - [Python] read\_csv() returns garbage if given file object in text mode
* ARROW-5028 - [Python][C++] Creating list<string> with pyarrow.array can overflow child builder
* ARROW-5085 - [Python/C++] Conversion of dict encoded null column fails in parquet writing when using RowGroups
* ARROW-5086 - [Python] Space leak in  ParquetFile.read\_row\_group()
* ARROW-5089 - [C++/Python] Writing dictionary encoded columns to parquet is extremely slow when using chunk size
* ARROW-5125 - [Python] Cannot roundtrip extreme dates through pyarrow
* ARROW-5220 - [Python] index / unknown columns in specified schema in Table.from\_pandas
* ARROW-5292 - [C++] Static libraries are built on AppVeyor
* ARROW-5300 - [C++] 0.13 FAILED to build with option -DARROW\_NO\_DEFAULT\_MEMORY\_POOL
* ARROW-5374 - [Python] Misleading error message when calling pyarrow.read\_record\_batch on a complete IPC stream
* ARROW-5414 - [C++] Using "Ninja" build system generator overrides default Release build type on Windows
* ARROW-5450 - [Python] TimestampArray.to\_pylist() fails with OverflowError: Python int too large to convert to C long
* ARROW-5471 - [C++][Gandiva]Array offset is ignored in Gandiva projector
* ARROW-5522 - [Packaging][Documentation] Comments out of date in python/manylinux1/build\_arrow.sh
* ARROW-5560 - [C++][Plasma] Cannot create Plasma object after OutOfMemory error
* ARROW-5562 - [C++][Parquet] parquet writer does not handle negative zero correctly
* ARROW-5630 - [Python][Parquet] Table of nested arrays doesn't round trip
* ARROW-5638 - [C++] cmake fails to generate Xcode project when Gandiva JNI bindings are enabled
* ARROW-5651 - [Python] Incorrect conversion from strided Numpy array when other type is specified
* ARROW-5682 - [Python] from\_pandas conversion casts values to string inconsistently
* ARROW-5731 - [CI] Turbodbc integration tests are failing
* ARROW-5753 - [Rust] Fix test failure in CI code coverage
* ARROW-5772 - [GLib][Plasma][CUDA] Plasma::Client#refer\_object test is failed
* ARROW-5775 - [C++] StructArray : cached boxed fields not thread-safe
* ARROW-5776 - [Gandiva][Crossbow] Revert template to have commit ids.
* ARROW-5790 - [Python] Passing zero-dim numpy array to pa.array causes segfault
* ARROW-5817 - [Python] Use pytest marks for Flight test to avoid silently skipping unit tests due to import failures
* ARROW-5823 - [Rust] CI scripts miss --all-targets cargo argument
* ARROW-5824 - [Gandiva] [C++] Fix decimal null
* ARROW-5836 - [Java][OSX] Flight tests are failing: address already in use
* ARROW-5838 - [C++][Flight][OSX] Building 3rdparty grpc cannot find OpenSSL
* ARROW-5848 - [C++] SO versioning schema after release 1.0.0
* ARROW-5849 - [C++] Compiler warnings on mingw-w64
* ARROW-5851 - [C++] Compilation of reference benchmarks fails
* ARROW-5856 - [Python] linking 3rd party cython modules against pyarrow fails since 0.14.0
* ARROW-5860 - [Java] [Vector] Fix decimal byte setter
* ARROW-5863 - [Python] Segmentation Fault via pytest-runner
* ARROW-5868 - [Python] manylinux2010 wheels have shared library dependency on liblz4
* ARROW-5870 - [C++] Development compile instructions need to include "make"
* ARROW-5873 - [Python] Segmentation fault when comparing schema with None
* ARROW-5874 - [Python] pyarrow 0.14.0 macOS wheels depend on shared libs under /usr/local/opt
* ARROW-5878 - [Python][C++] Parquet reader not forward compatible for timestamps without timezone
* ARROW-5884 - [Java] Fix the get method of StructVector
* ARROW-5886 - [Python][Packaging] Manylinux1/2010 compliance issue with libz
* ARROW-5887 - [C#] ArrowStreamWriter writes FieldNodes in wrong order
* ARROW-5889 - [Python][C++] Parquet backwards compat for timestamps without timezone broken
* ARROW-5894 - [C++] libgandiva.so.14 is exporting libstdc++ symbols
* ARROW-5899 - [Python][Packaging] Bundle uriparser.dll in windows wheels
* ARROW-5910 - [Python] read\_tensor() fails on non-seekable streams
* ARROW-5921 - [C++][Fuzzing] Missing nullptr checks in IPC
* ARROW-5923 - [C++] Fix int96 comment
* ARROW-5925 - [Gandiva][C++] cast decimal to int should round up
* ARROW-5930 - [FlightRPC] [Python] Flight CI tests are failing
* ARROW-5935 - [C++] ArrayBuilders with mutable type are not robustly supported
* ARROW-5946 - [Rust] [DataFusion] Projection push down with aggregate producing incorrect results
* ARROW-5952 - [Python] Segfault when reading empty table with category as pandas dataframe
* ARROW-5959 - [C++][CI] Fuzzit does not know about branch + commit hash
* ARROW-5960 - [C++] Boost dependencies are specified in wrong order
* ARROW-5963 - [R] R Appveyor job does not test changes in the C++ library
* ARROW-5964 - [C++][Gandiva] Cast double to decimal with rounding returns 0
* ARROW-5966 - [Python] Capacity error when converting large UTF32 numpy array to arrow array
* ARROW-5968 - [Java] Remove duplicate Preconditions check in JDBC adapter
* ARROW-5969 - [CI] [R] Lint failures
* ARROW-5973 - [Java] Variable width vectors' get methods should return null when the underlying data is null
* ARROW-5989 - [C++][Python] pyarrow.lib.ArrowIOError: Unable to load libjvm when using openjdk-8
* ARROW-5990 - [Python] RowGroupMetaData.column misses bounds check
* ARROW-5992 - [C++] Array::View fails for string/utf8 as binary
* ARROW-5996 - [Java] Avoid resource leak in flight service
* ARROW-5999 - [C++] Required header files missing when built with -DARROW\_DATASET=OFF
* ARROW-6002 - [C++][Gandiva] TestCastFunctions does not test int64 casting\`
* ARROW-6004 - [C++] CSV reader ignore\_empty\_lines option doesn't handle empty lines
* ARROW-6005 - [C++] parquet::arrow::FileReader::GetRecordBatchReader() does not behave as documented since ARROW-1012
* ARROW-6006 - [C++] Empty IPC streams containing a dictionary are corrupt
* ARROW-6012 - [C++] Fall back on known Apache mirror for Thrift downloads
* ARROW-6015 - [Python] pyarrow wheel:  \`DLL load failed\` when importing on windows
* ARROW-6016 - [Python] pyarrow get\_library\_dirs assertion error
* ARROW-6029 - [R] Improve R docs on how to fix library version mismatch
* ARROW-6032 - [C++] CountSetBits doesn't ensure 64-bit aligned accesses
* ARROW-6038 - [Python] pyarrow.Table.from\_batches produces corrupted table if any of the batches were empty
* ARROW-6040 - [Java] Dictionary entries are required in IPC streams even when empty
* ARROW-6046 - [C++] Slice RecordBatch of String array with offset 0 returns whole batch
* ARROW-6047 - [Rust] Rust nightly 1.38.0 builds failing
* ARROW-6050 - [Java] Update out-of-date java/flight/README.md
* ARROW-6054 - pyarrow.serialize should respect the value of structured dtype of numpy
* ARROW-6058 - [Python][Parquet] Failure when reading Parquet file from S3 with s3fs
* ARROW-6060 - [Python] too large memory cost using pyarrow.parquet.read\_table with use\_threads=True
* ARROW-6061 - [C++] Cannot build libarrow without rapidjson
* ARROW-6066 - [Website] Fix blog post author header
* ARROW-6067 - [Python] Large memory test failures
* ARROW-6068 - [Python] Hypothesis test failure, Add StructType::Make that accepts vector of fields
* ARROW-6073 - [C++] Decimal128Builder is not reset in Finish()
* ARROW-6082 - [Python] create pa.dictionary() type with non-integer indices type crashes
* ARROW-6092 - [C++] Python 2.7: arrow\_python\_test failure
* ARROW-6095 - [C++] Python subproject ignores ARROW\_TEST\_LINKAGE
* ARROW-6108 - [C++] Appveyor Build\_Debug configuration is hanging in C++ unit tests
* ARROW-6116 - [C++][Gandiva] Fix bug in TimedTestFilterAdd2
* ARROW-6117 - [Java] Fix the set method of FixedSizeBinaryVector
* ARROW-6120 - [C++][Gandiva] including some headers causes decimal\_test to fail
* ARROW-6126 - [C++] IPC stream reader handling of empty streams potentially not robust
* ARROW-6132 - [Python] ListArray.from\_arrays does not check validity of input arrays
* ARROW-6135 - [C++] KeyValueMetadata::Equals should not be order-sensitive
* ARROW-6136 - [FlightRPC][Java] Don't double-close response stream
* ARROW-6145 - [Java] UnionVector created by MinorType#getNewVector could not keep field type info properly
* ARROW-6148 - [C++][Packaging]  Improve aarch64 support
* ARROW-6152 - [C++][Parquet] Write arrow::Array directly into parquet::TypedColumnWriter<T>
* ARROW-6153 - [R] Address parquet deprecation warning
* ARROW-6158 - [Python] possible to create StructArray with type that conflicts with child array's types
* ARROW-6159 - [C++] PrettyPrint of arrow::Schema missing identation for first line
* ARROW-6160 - [Java] AbstractStructVector#getPrimitiveVectors fails to work with complex child vectors
* ARROW-6166 - [Go] Slice of slice causes index out of range panic
* ARROW-6167 - [R] macOS binary R packages on CRAN don't have arrow\_available
* ARROW-6170 - [R] "docker-compose build r" is slow
* ARROW-6171 - [R] "docker-compose run r" fails
* ARROW-6174 - [C++] Validate chunks in ChunkedArray::Validate
* ARROW-6175 - [Java] Fix MapVector#getMinorType and extend AbstractContainerVector addOrGet complex vector API
* ARROW-6178 - [Developer] Don't fail in merge script on bad primary author input in multi-author PRs
* ARROW-6182 - [R] Add note to README about r-arrow conda installation
* ARROW-6186 - [Packaging][C++] Plasma headers not included for ubuntu-xenial libplasma-dev debian package
* ARROW-6190 - [C++] Define and declare functions regardless of NDEBUG
* ARROW-6200 - [Java] Method getBufferSizeFor in BaseRepeatedValueVector/ListVector not correct
* ARROW-6202 - [Java] Exception in thread "main" org.apache.arrow.memory.OutOfMemoryException: Unable to allocate buffer of size 4 due to memory limit. Current allocation: 2147483646
* ARROW-6205 - [C++] ARROW\_DEPRECATED warning when including io/interfaces.h from CUDA (.cu) source
* ARROW-6208 - [Java] Correct byte order before comparing in ByteFunctionHelpers
* ARROW-6210 - [Java] remove equals API from ValueVector
* ARROW-6211 - [Java] Remove dependency on RangeEqualsVisitor from ValueVector interface
* ARROW-6214 - [R] Sanitizer errors triggered via R bindings
* ARROW-6215 - [Java] RangeEqualVisitor does not properly compare ZeroVector
* ARROW-6223 - [C++] Configuration error with Anaconda Python 3.7.4
* ARROW-6224 - [Python] remaining usages of the 'data' attribute (from previous Column) cause warnings
* ARROW-6227 - [Python] pyarrow.array() shouldn't coerce np.nan to string
* ARROW-6234 - [Java] ListVector hashCode() is not correct
* ARROW-6241 - [Java] Failures on master
* ARROW-6259 - [C++][CI] Flatbuffers-related failures in CI on macOS
* ARROW-6263 - [Python] RecordBatch.from\_arrays does not check array types against a passed schema
* ARROW-6266 - [Java] Resolve the ambiguous method overload in RangeEqualsVisitor
* ARROW-6268 - Empty buffer should have a valid address
* ARROW-6269 - [C++][Fuzzing] IPC reads do not check decimal precision
* ARROW-6270 - [C++][Fuzzing] IPC reads do not check buffer indices
* ARROW-6290 - [Rust] [DataFusion] sql\_csv example errors when running
* ARROW-6291 - [C++] CMake ignores ARROW\_PARQUET
* ARROW-6301 - [Python] atexit: pyarrow.lib.ArrowKeyError: 'No type extension with name arrow.py\_extension\_type found'
* ARROW-6302 - [Python][Parquet] Reading dictionary type with serialized Arrow schema does not restore "ordered" type property
* ARROW-6309 - [C++] Parquet tests and executables are linked statically
* ARROW-6323 - [R] Expand file paths when passing to readers
* ARROW-6325 - [Python] wrong conversion of DataFrame with boolean values
* ARROW-6330 - [C++] Include missing headers in api.h
* ARROW-6332 - [Java][C++][Gandiva] Handle size of varchar vectors correctly
* ARROW-6339 - [Python][C++] Rowgroup statistics for pd.NaT array ill defined
* ARROW-6343 - [Java] [Vector] Fix allocation helper
* ARROW-6344 - [C++][Gandiva] substring does not handle multibyte characters
* ARROW-6345 - [C++][Python] "ordered" flag seemingly not taken into account when comparing DictionaryType values for equality
* ARROW-6348 - [R] arrow::read\_csv\_arrow namespace error when package not loaded
* ARROW-6354 - [C++] Building without Parquet fails
* ARROW-6363 - [R] segfault in Table\_\_from\_dots with unexpected schema
* ARROW-6364 - [R] Handling unexpected input to time64() et al
* ARROW-6369 - [Python] Support list-of-boolean in Array.to\_pandas conversion
* ARROW-6371 - [Doc] Row to columnar conversion example mentions arrow::Column in comments
* ARROW-6372 - [Rust][Datafusion] Casting from Un-signed to Signed Integers not supported
* ARROW-6376 - [Developer] PR merge script has "master" target ref hard-coded
* ARROW-6387 - [Archery] Errors with make
* ARROW-6392 - [Python][Flight] list\_actions Server RPC is not tested in test\_flight.py, nor is return value validated
* ARROW-6406 - [C++] jemalloc\_ep fails for offline build
* ARROW-6411 - [C++][Parquet] DictEncoderImpl<T>::PutIndicesTyped has bad performance on some systems
* ARROW-6412 - [C++] arrow-flight-test can crash because of port allocation
* ARROW-6418 - [C++] Plasma cmake targets are not exported
* ARROW-6423 - [Python] pyarrow.CompressedOutputStream() never completes with compression='snappy'
* ARROW-6424 - [C++][Fuzzing] Fuzzit nightly is broken
* ARROW-6428 - [CI][Crossbow] Nightly turbodbc job fails
* ARROW-6431 - [Python] Test suite fails without pandas installed
* ARROW-6432 - [CI][Crossbow] Remove alpine crossbow jobs
* ARROW-6433 - [CI][Crossbow] Nightly java docker job fails
* ARROW-6434 - [CI][Crossbow] Nightly HDFS integration job fails
* ARROW-6435 - [CI][Crossbow] Nightly dask integration job fails
* ARROW-6440 - [CI][Crossbow] Nightly ubuntu, debian, and centos package builds fail
* ARROW-6441 - [CI][Crossbow] Nightly Centos 6 job fails
* ARROW-6443 - [CI][Crossbow] Nightly conda osx builds fail
* ARROW-6446 - [OSX][Python][Wheel] Turn off ORC feature in the wheel building scripts
* ARROW-6449 - [R] io "tell()" methods are inconsistently named and untested
* ARROW-6457 - [C++] CMake build locally fails with MSVC 2015 build generator
* ARROW-6461 - [Java] EchoServer can close socket before client has finished reading
* ARROW-6472 - [Java] ValueVector#accept may has potential cast exception
* ARROW-6476 - [Java][CI] Travis java all-jdks job is broken
* ARROW-6478 - [C++] Roll back to jemalloc stable-4 branch until performance issues in 5.2.x addressed
* ARROW-6481 - [Python][C++] Bad performance of read\_csv() with column\_types
* ARROW-6488 - [Python] pyarrow.NULL equals to itself
* ARROW-6492 - [Python] file written with latest fastparquet cannot be read with latest pyarrow
* ARROW-6502 - [GLib][CI] MinGW failure in CI
* ARROW-6506 - [C++] Validation of ExtensionType with nested type fails
* ARROW-6509 - [C++][Gandiva] Re-enable Gandiva JNI tests and fix Travis CI failure
* ARROW-6520 - [Python] Segmentation fault on writing tables with fixed size binary fields
* ARROW-6522 - [Python] Test suite fails with pandas 0.23.4, pytest 3.8.1
* ARROW-6530 - [CI][Crossbow][R] Nightly R job doesn't install all dependencies
* ARROW-6550 - [C++] Filter expressions PR failing manylinux package builds
* ARROW-6552 - [C++] boost::optional in STL test fails compiling in gcc 4.8.2
* ARROW-6560 - [Python] Failures in \*-nopandas integration tests
* ARROW-6561 - [Python] pandas-master integration test failure
* ARROW-6562 - [GLib] Fix wrong sliced data of GArrowBuffer
* ARROW-6564 - [Python] Do not require pandas for invoking Array.\_\_array\_\_
* ARROW-6565 - [Rust] [DataFusion] Intermittent test failure due to temp dir already existing
* ARROW-6568 - [C++][Python][Parquet] pyarrow.parquet crash writing zero-chunk dictionary-type column
* ARROW-6572 - [C++] Reading some Parquet data can return uninitialized memory
* ARROW-6573 - [Python] Segfault when writing to parquet
* ARROW-6576 - [R] Fix sparklyr integration tests
* ARROW-6597 - [Python] Segfault in test\_pandas with Python 2.7
* ARROW-6618 - [Python] Reading a zero-size buffer can segfault
* ARROW-6622 - [C++][R] SubTreeFileSystem path error on Windows
* ARROW-6623 - [CI][Python] Dask docker integration test broken perhaps by statistics-related change
* ARROW-6639 - [Packaging][RPM] Add support for CentOS 7 on aarch64
* ARROW-6640 - [C++] Error when BufferedInputStream Peek more than bytes buffered
* ARROW-6642 - [Python] chained access of ParquetDataset's metadata segfaults
* ARROW-6651 - [R] Fix R conda job
* ARROW-6652 - [Python] to\_pandas conversion removes timezone from type
* ARROW-6660 - [Rust] [DataFusion] Minor docs update for 0.15.0 release
* ARROW-6670 - [CI][R] Fix fix for R nightly jobs
* ARROW-6674 - [Python] Fix or ignore the test warnings
* ARROW-6677 - [FlightRPC][C++] Document using Flight in C++
* ARROW-6678 - [C++] Regression in Parquet file compatibility introduced by ARROW-3246
* ARROW-6679 - [RELEASE] autobrew license in LICENSE.txt is not acceptable
* ARROW-6682 - [C#] Arrow R/C++ hangs reading binary file generated by C#
* ARROW-6687 - [Rust] [DataFusion] Query returns incorrect row count
* ARROW-6701 - [C++][R] Lint failing on R cpp code
* ARROW-6703 - [Packaging][Linux] Restore ARROW\_VERSION environment variable
* ARROW-6705 - [Rust] [DataFusion] README has invalid github URL
* ARROW-6709 - [JAVA] Jdbc adapter currentIndex should increment when value is null
* ARROW-6714 - [R] Fix untested RecordBatchWriter case
* ARROW-6716 - [CI] [Rust] New 1.40.0 nightly causing builds to fail
* ARROW-6751 - [CI] ccache doesn't cache on Travis-CI
* ARROW-6760 - [C++] JSON: improve error message when column changed type
* ARROW-6773 - [C++] Filter kernel returns invalid data when filtering with an Array slice
* PARQUET-1623 - [C++] Invalid memory access with a magic number of records
* PARQUET-1631 - [C++] ParquetInputWrapper::GetSize always returns 0
* PARQUET-1640 - [C++] parquet-encoding-benchmark crashes

## Improvement

* ARROW-1324 - [C++] Support ARROW\_BOOST\_VENDORED on Windows / MSVC
* ARROW-1786 - [Format] List expected on-wire buffer layouts for each kind of Arrow physical type in specification
* ARROW-1789 - [Format] Consolidate specification documents and improve clarity for new implementation authors
* ARROW-2769 - [C++][Python] Deprecate and rename add\_metadata methods
* ARROW-3032 - [Python] Clean up NumPy-related C++ headers
* ARROW-3243 - [C++] Upgrade jemalloc to version 5
* ARROW-3246 - [Python][Parquet] direct reading/writing of pandas categoricals in parquet
* ARROW-3325 - [Python] Support reading Parquet binary/string columns directly as DictionaryArray
* ARROW-3531 - [Python] Deprecate Schema.field\_by\_name in favor of \_\_getitem\_\_
* ARROW-3579 - [Crossbow] Unintuitive error message when remote branch has not been pushed
* ARROW-3643 - [Rust] Optimize \`push\_slice\` of \`BufferBuilder<bool>\`
* ARROW-3710 - [Crossbow][Python] Run nightly tests against pandas master
* ARROW-3772 - [C++] Read Parquet dictionary encoded ColumnChunks directly into an Arrow DictionaryArray
* ARROW-3829 - [Python] Support protocols to extract Arrow objects from third-party classes
* ARROW-3943 - [R] Write vignette for R package
* ARROW-4036 - [C++] Make status codes pluggable
* ARROW-4095 - [C++] Implement optimizations for dictionary unification where dictionaries are prefixes of the unified dictionary
* ARROW-4111 - [Python] Create time types from Python sequences of integers
* ARROW-4220 - [Python] Add buffered input and output stream ASV benchmarks with simulated high latency IO
* ARROW-4398 - [Python] Add benchmarks for Arrow<>Parquet BYTE\_ARRAY serialization (read and write)
* ARROW-4473 - [Website] Add instructions to do a test-deploy of Arrow website and fix bugs
* ARROW-4648 - [C++/Question] Naming/organizational inconsistencies in cpp codebase
* ARROW-4649 - [C++/CI/R] Add (nightly) job that builds \`brew install apache-arrow --HEAD\`
* ARROW-4752 - [Rust] Add explicit SIMD vectorization for the divide kernel
* ARROW-4810 - [Format][C++] Add "LargeList" type with 64-bit offsets
* ARROW-4841 - [C++] Persist CMake options in generated CMake config
* ARROW-5134 - [R][CI] Run nightly tests against multiple R versions
* ARROW-5211 - [Format] Missing documentation under \`Dictionary encoding\` section on MetaData page
* ARROW-5216 - [CI] Add Appveyor badge to README
* ARROW-5307 - [CI][GLib] Enable GTK-Doc
* ARROW-5343 - [C++] Consider using Buffer for transpose maps in DictionaryType::Unify instead of std::vector
* ARROW-5344 - [C++] Use ArrayDataVisitor in implementation of dictionary unpacking in compute/kernels/cast.cc
* ARROW-5358 - [Rust] Implement equality check for ArrayData and Array
* ARROW-5380 - [C++] Fix and enable UBSan for unaligned accesses.
* ARROW-5439 - [Java] Utilize stream EOS in File format
* ARROW-5444 - [Release][Website] After 0.14 release, update what is an "official" release
* ARROW-5458 - [C++] ARMv8 parallel CRC32c computation optimization
* ARROW-5480 - [Python] Pandas categorical type doesn't survive a round-trip through parquet
* ARROW-5494 - [Python] Create FileSystem bindings
* ARROW-5505 - [R] Stop masking base R functions/rethink namespacing
* ARROW-5527 - [C++] HashTable/MemoTable should use Buffer(s)/Builder(s) for heap data
* ARROW-5558 - [C++] Support Array::View on arrays with non-zero offsets
* ARROW-5559 - [C++] Introduce IpcOptions struct object for better API-stability when adding new options
* ARROW-5564 - [C++] Add uriparser to conda-forge
* ARROW-5610 - [Python] Define extension type API in Python to "receive" or "send" a foreign extension type
* ARROW-5646 - [Crossbow][Documentation] Move the user guide to the Sphinx documentation
* ARROW-5681 - [FlightRPC] Wrap gRPC exceptions/statuses
* ARROW-5686 - [R] Review R Windows CI build
* ARROW-5716 - [Developer] Improve merge PR script to acknowledge co-authors
* ARROW-5717 - [Python] Support dictionary unification when converting variable dictionaries to pandas
* ARROW-5722 - [Rust] Implement std::fmt::Debug for ListArray, BinaryArray and StructArray
* ARROW-5734 - [Python] Dispatch to Table.from\_arrays from pyarrow.table factory function
* ARROW-5736 - [Format][C++] Support small bit-width indices in sparse tensor
* ARROW-5741 - [JS] Make numeric vector from functions consistent with TypedArray.from
* ARROW-5743 - [C++] Add CMake option to enable "large memory" unit tests
* ARROW-5746 - [Website] Move website source out of apache/arrow
* ARROW-5747 - [C++] Better column name and header support in CSV reader
* ARROW-5762 - [Integration][JS] Integration Tests for Map Type
* ARROW-5777 - [C++] BasicDecimal128 is a small object it doesn't always make sense to pass by const ref
* ARROW-5778 - [Java] Extract the logic for vector data copying to the super classes
* ARROW-5784 - [Release][GLib] Replace c\_glib/ after running c\_glib/autogen.sh in dev/release/02-source.sh
* ARROW-5786 - [Release] Use arrow-jni profile in dev/release/01-prepare.sh
* ARROW-5788 - [Rust] Use { version = "...", path = "../..." } for arrow and parquet dependencies
* ARROW-5789 - [C++] Small Warning/Linkage cleanups
* ARROW-5798 - [Packaging][deb] Update doc architecture
* ARROW-5800 - [R] Dockerize R Travis CI tests so they can be run anywhere via docker-compose
* ARROW-5803 - [C++] Dockerize C++ with clang 7 Travis CI unit test logic
* ARROW-5812 - [Java] Refactor method name and param type in BaseIntVector
* ARROW-5813 - [C++] Support checking the equality of the different contiguous tensors
* ARROW-5814 - [Java] Implement a <Object, int> HashMap for DictionaryEncoder
* ARROW-5827 - [C++] Require c-ares CMake config
* ARROW-5828 - [C++] Add Protocol Buffers version check
* ARROW-5830 - [C++] Stop using memcmp in TensorEquals
* ARROW-5833 - [C++] Factor out status copying code from cast.cc
* ARROW-5842 - [Java] Revise the semantic of lastSet in ListVector
* ARROW-5843 - [Java] Improve the readability and performance of BitVectorHelper#getNullCount
* ARROW-5853 - [Python] Expose boolean filter kernel on Array
* ARROW-5864 - [Python] simplify cython wrapping of Result
* ARROW-5865 - [Release] Helper script for rebasing open pull requests on master
* ARROW-5866 - [C++] Remove duplicate library in cpp/Brewfile
* ARROW-5876 - [FlightRPC] Implement basic auth across all languages
* ARROW-5877 - [FlightRPC] Fix auth incompatibilities between Python/Java
* ARROW-5880 - [C++] Update arrow parquet writer to use TypedBufferBuilder
* ARROW-5883 - [Java] Support dictionary encoding for List and Struct type
* ARROW-5888 - [Python][C++] Add metadata to store Arrow time zones in Parquet file metadata
* ARROW-5897 - [Java] Remove duplicated logic in MapVector
* ARROW-5900 - [Gandiva] [Java] Decimal precision,scale bounds check
* ARROW-5904 - [Java] [Plasma] Fix compilation of Plasma Java client
* ARROW-5906 - [CI] Set -DARROW\_VERBOSE\_THIRDPARTY\_BUILD=OFF in builds running in Travis CI, maybe all docker-compose builds by default
* ARROW-5908 - [C#] ArrowStreamWriter doesn't align buffers to 8 bytes
* ARROW-5909 - [Java] Optimize ByteFunctionHelpers equals & compare logic
* ARROW-5911 - [Java] Make ListVector and MapVector create reader lazily
* ARROW-5918 - [Java] Add get to BaseIntVector interface
* ARROW-5919 - [R] Add nightly tests for building r-arrow with dependencies from conda-forge
* ARROW-5924 - [C++][Plasma] It is not convenient to release a GPU object
* ARROW-5937 - [Release] Stop parallel binary upload
* ARROW-5938 - [Release] Create branch for adding release note automatically
* ARROW-5939 - [Release] Add support for generating vote email template separately
* ARROW-5940 - [Release] Add support for re-uploading sign/checksum for binary artifacts
* ARROW-5941 - [Release] Avoid re-uploading already uploaded binary artifacts
* ARROW-5943 - [GLib][Gandiva] Add support for function aliases
* ARROW-5947 - [Rust] [DataFusion] Remove serde\_json dependency
* ARROW-5948 - [Rust] [DataFusion] create\_logical\_plan should not call optimizer
* ARROW-5955 - [Plasma] Support setting memory quotas per plasma client for better isolation
* ARROW-5961 - [R] Be able to run R-only tests even without C++ library
* ARROW-5962 - [CI][Python] Do not test manylinux1 wheels in Travis CI
* ARROW-5967 - [Java] DateUtility#timeZoneList is not correct
* ARROW-5976 - [C++] RETURN\_IF\_ERROR(ctx) should be namespaced
* ARROW-5977 - [C++] [Python] Method for read\_csv to limit which columns are read?
* ARROW-5985 - [Developer] Do not suggest setting Fix Version for point releases in dev/merge\_arrow\_pr.py
* ARROW-5986 - [Java] Code cleanup for dictionary encoding
* ARROW-5998 - [Java] Open a document to track the API changes
* ARROW-6000 - [Python] Expose LargeBinaryType and LargeStringType
* ARROW-6017 - [FlightRPC] Allow creating Locations with unknown schemes
* ARROW-6020 - [Java] Refactor ByteFunctionHelper#hash with new added ArrowBufHasher
* ARROW-6021 - [Java] Extract copyFrom and copyFromSafe methods to ValueVector interface
* ARROW-6036 - [GLib] Add support for skip rows and column\_names CSV read option
* ARROW-6037 - [GLib] Add a missing version macro
* ARROW-6041 - [Website] Blog post announcing R package release
* ARROW-6042 - [C++] Implement alternative DictionaryBuilder that always yields int32 indices
* ARROW-6045 - [C++] Benchmark for Parquet float and NaN encoding/decoding
* ARROW-6048 - [C++] Add ChunkedArray::View which calls to Array::View
* ARROW-6049 - [C++] Support using Array::View from compatible dictionary type to another
* ARROW-6063 - [FlightRPC] Implement "half-closed" semantics for DoPut
* ARROW-6065 - [C++] Reorganize parquet/arrow/reader.cc, remove code duplication, improve readability
* ARROW-6070 - [Java] Avoid creating new schema before IPC sending
* ARROW-6077 - [C++][Parquet] Build logical schema tree mapping Arrow fields to Parquet schema levels
* ARROW-6083 - [Java] Refactor Jdbc adapter consume logic
* ARROW-6084 - [Python] Support LargeList
* ARROW-6093 - [Java] reduce branches in algo for first match in VectorRangeSearcher
* ARROW-6096 - [C++] Conditionally depend on boost regex library
* ARROW-6100 - [Rust] Pin to specific Rust nightly release
* ARROW-6104 - [Rust] [DataFusion] Don't allow bare\_trait\_objects
* ARROW-6105 - [C++][Parquet][Python] Add test case showing dictionary-encoded subfields in nested type
* ARROW-6115 - [Python] support LargeList, LargeString, LargeBinary in conversion to pandas
* ARROW-6118 - [Java] Replace google Preconditions with Arrow Preconditions
* ARROW-6121 - [Tools] Improve merge tool cli ergonomic
* ARROW-6125 - [Python] Remove any APIs deprecated prior to 0.14.x
* ARROW-6127 - [Website] Add favicons and meta tags
* ARROW-6128 - [C++] Can't build with g++ 8.3.0 by class-memaccess warning
* ARROW-6130 - [Release] Use 0.15.0 as the next release
* ARROW-6139 - [Documentation][R] Build R docs (pkgdown) site and add to arrow-site
* ARROW-6141 - [C++] Enable memory-mapping a file region that is offset from the beginning of the file
* ARROW-6143 - [Java] Unify the copyFrom and copyFromSafe methods for all vectors
* ARROW-6164 - [Docs][Format] Document project versioning schema and forward/backward compatibility policies
* ARROW-6172 - [Java] Provide benchmarks to set IntVector with different methods
* ARROW-6180 - [C++] Create InputStream that is an isolated reader of a segment of a RandomAccessFile
* ARROW-6181 - [R] Only allow R package to install without libarrow on linux
* ARROW-6187 - [C++] fallback to storage type when writing ExtensionType to Parquet
* ARROW-6192 - [GLib] Use the same SO version as C++
* ARROW-6194 - [Java] Add non-static approach in DictionaryEncoder making it easy to extend and reuse
* ARROW-6206 - [Java][Docs] Document environment variables/java properties
* ARROW-6209 - [Java] Extract set null method to the base class for fixed width vectors
* ARROW-6216 - [C++] Allow user to select the compression level
* ARROW-6219 - [Java] Add API for JDBC adapter that can convert less then the full result set at a time.
* ARROW-6225 - [Website] Update arrow-site/README and any other places to point website contributors in right direction
* ARROW-6230 - [R] Reading in Parquet files are 20x slower than reading fst files in R
* ARROW-6231 - [C++][Python] Consider assigning default column names when reading CSV file and header\_rows=0
* ARROW-6232 - [C++] Rename Argsort kernel to SortToIndices
* ARROW-6237 - [R] Add option to set CXXFLAGS when compiling R package with $ARROW\_R\_CXXFLAGS
* ARROW-6240 - [Ruby] Arrow::Decimal128Array returns BigDecimal
* ARROW-6246 - [Website] Add link to R documentation site
* ARROW-6249 - [Java] Remove useless class ByteArrayWrapper
* ARROW-6252 - [Python] Add pyarrow.Array.diff method that exposes arrow::Diff
* ARROW-6253 - [Python] Expose "enable\_buffered\_stream" option from parquet::ReaderProperties in pyarrow.parquet.read\_table
* ARROW-6258 - [R] Add macOS build scripts
* ARROW-6260 - [Website] Use deploy key on Travis to build and push to asf-site
* ARROW-6262 - [Developer] Show JIRA issue before merging
* ARROW-6264 - [Java] There is no need to consider byte order in ArrowBufHasher
* ARROW-6267 - [Ruby] Add Arrow::Time for Arrow::Time{32,64}DataType value
* ARROW-6271 - [Rust] [DataFusion] Add example for running SQL against Parquet
* ARROW-6272 - [Rust] [DataFusion] Add register\_parquet convenience method to ExecutionContext
* ARROW-6279 - [Python] Add Table.slice method or allow slices in \_\_getitem\_\_
* ARROW-6284 - [C++] Allow references in std::tuple when converting tuple to arrow array
* ARROW-6289 - [Java] Add empty() in UnionVector to create instance
* ARROW-6294 - [C++] Use hyphen for plasma-store-server executable
* ARROW-6296 - [Java] Cleanup JDBC interfaces and eliminate one memcopy for binary/varchar fields
* ARROW-6297 - [Java] Compare ArrowBufPointers by unsinged integers
* ARROW-6303 - [Rust] Add a feature to disable SIMD
* ARROW-6304 - [Java] Add description to each maven artifact
* ARROW-6311 - [Java] Make ApproxEqualsVisitor accept DiffFunction to make it more flexible
* ARROW-6313 - [Format] Tracking for ensuring flatbuffer serialized values are aligned in stream/files.
* ARROW-6319 - [C++] Extract the core of NumericTensor<T>::Value as Tensor::Value<T>
* ARROW-6328 - Click.option-s should have help text
* ARROW-6329 - [Format] Add 4-byte "stream continuation" to IPC message format to align Flatbuffers
* ARROW-6331 - [Java] Incorporate ErrorProne into the java build
* ARROW-6334 - [Java] Improve the dictionary builder API to return the position of the value in the dictionary
* ARROW-6335 - [Java] Improve the performance of DictionaryHashTable
* ARROW-6336 - [Python] Clarify pyarrow.serialize/deserialize docstrings viz-a-viz relationship with Arrow IPC protocol
* ARROW-6337 - [R] as\_tibble in R API is a misnomer
* ARROW-6338 - [R] Type function names don't match type names
* ARROW-6342 - [Python] Add pyarrow.record\_batch factory function with same basic API / semantics as pyarrow.table
* ARROW-6350 - [Ruby] Remove Arrow::Struct and use Hash instead
* ARROW-6351 - [Ruby] Improve Arrow#values performance
* ARROW-6353 - [Python] Allow user to select compression level in pyarrow.parquet.write\_table
* ARROW-6355 - [Java] Make range equal visitor reusable
* ARROW-6357 - [C++] S3: allow for background writes
* ARROW-6358 - [C++] FileSystem::DeleteDir should make it optional to delete the directory itself
* ARROW-6360 - [R] Update support for compression
* ARROW-6362 - [C++] S3: more flexible credential options
* ARROW-6365 - [R] Should be able to coerce numeric to integer with schema
* ARROW-6366 - [Java] Make field vectors final explicitly
* ARROW-6368 - [C++] Add RecordBatch projection functionality
* ARROW-6373 - [C++] Make FixedWidthBinaryBuilder consistent with other primitive fixed width builders
* ARROW-6375 - [C++] Extend ConversionTraits to allow efficiently appending list values in STL API
* ARROW-6379 - [C++] Do not append any buffers when serializing NullType for IPC
* ARROW-6381 - [C++] BufferOutputStream::Write is slow for many small writes
* ARROW-6384 - [C++] Bump dependencies
* ARROW-6391 - [Python][Flight] Add built-in methods on FlightServerBase to start server and wait for it to be available
* ARROW-6402 - [C++] Suppress sign-compare warning with g++ 9.2.1
* ARROW-6403 - [Python] Expose FileReader::ReadRowGroups() to Python
* ARROW-6408 - [Rust] Use "if cfg!" pattern in SIMD kernel implementations
* ARROW-6413 - [R] Support autogenerating column names
* ARROW-6415 - [R] Remove usage of R CMD config CXXCPP
* ARROW-6416 - [Python] Confusing API & documentation regarding chunksizes
* ARROW-6426 - [FlightRPC] Expose gRPC configuration knobs in Flight
* ARROW-6447 - [C++] Builds with ARROW\_JEMALLOC=ON wait until jemalloc\_ep is complete before building any libarrow .cc files
* ARROW-6450 - [C++] Use 2x reallocation strategy in arrow::BufferBuilder instead of 1.5x
* ARROW-6451 - [Format] Add clarifications to Columnar.rst about the contents of "null" slots in Varbinary or List arrays
* ARROW-6453 - [C++] More informative error messages from S3
* ARROW-6454 - [Developer] Add LLVM license to LICENSE.txt due to binary redistribution in packages
* ARROW-6458 - [Java] Remove value boxing/unboxing for ApproxEqualsVisitor
* ARROW-6462 - [C++] Can't build with bundled double-conversion on CentOS 6 x86\_64
* ARROW-6465 - [Python] Improve Windows build instructions
* ARROW-6475 - [C++] Don't try to dictionary encode dictionary arrays
* ARROW-6477 - [Packaging][Crossbow] Use Azure Pipelines to build linux packages
* ARROW-6484 - [Java] Enable create indexType for DictionaryEncoding according to dictionary value count
* ARROW-6487 - [Rust] [DataFusion] Create test utils module
* ARROW-6489 - [Developer][Documentation] Fix merge script and readme
* ARROW-6494 - [C++][Dataset] Implement basic PartitionScheme
* ARROW-6504 - [Python][Packaging] Add mimalloc to conda packages for better performance
* ARROW-6505 - [Website] Add new committers
* ARROW-6518 - [Packaging][Python] Flight failing in OSX Python wheel builds
* ARROW-6524 - [Developer][Packaging] Nightly build report's subject should contain Arrow
* ARROW-6526 - [C++] Poison data in PoolBuffer destructor
* ARROW-6527 - [C++] Add OutputStream::Write() variant taking an owned buffer
* ARROW-6531 - [Python] Add detach() method to buffered streams
* ARROW-6532 - [R] Write parquet files with compression
* ARROW-6533 - [R] Compression codec should take a "level"
* ARROW-6534 - [Java] Fix typos and spelling
* ARROW-6540 - [R] Add Validate() methods
* ARROW-6541 - [Format][C++] Use two-part EOS and amend Format documentation
* ARROW-6542 - [R] Add View() method to array types
* ARROW-6544 - [R] Documentation/polishing for 0.15 release
* ARROW-6545 - [Go] Update Go IPC writer to use two-part EOS per mailing list discussion
* ARROW-6546 - [C++] Add missing FlatBuffers source dependency
* ARROW-6556 - [Python] Prepare for pandas release without SparseDataFrame
* ARROW-6557 - [Python] Always return pandas.Series from Array/ChunkedArray.to\_pandas, propagate field names to Series from RecordBatch, Table
* ARROW-6558 - [C++] Refactor Iterator to a type erased handle
* ARROW-6559 - [Developer][C++] Add "archery" option to specify system toolchain for C++ builds
* ARROW-6569 - [Website] Add support for auto deployment by GitHub Actions
* ARROW-6570 - [Python] Use MemoryPool to allocate memory for NumPy arrays in to\_pandas calls
* ARROW-6584 - [Python][Wheel] Bundle zlib again with the windows wheels
* ARROW-6588 - [C++] Suppress class-memaccess warning with g++ 9.2.1
* ARROW-6589 - [C++] Support BinaryType in MakeArrayOfNull
* ARROW-6590 - [C++] Do not require ARROW\_JSON=ON when ARROW\_IPC=ON
* ARROW-6591 - [R] Ignore .Rhistory files in source control
* ARROW-6605 - [C++] Add recursion depth control to fs::Selector
* ARROW-6606 - [C++] Construct tree structure from std::vector<fs::FileStats>
* ARROW-6609 - [C++] Add minimal build Dockerfile example
* ARROW-6621 - [Rust][DataFusion] Examples for DataFusion are not executed in CI
* ARROW-6629 - [Doc][C++] Document the FileSystem API
* ARROW-6630 - [Doc][C++] Document the file readers (CSV, JSON, Parquet, etc.)
* ARROW-6644 - [JS] Amend NullType IPC protocol to append no buffers
* ARROW-6647 - [C++] Can't build with g++ 4.8.5 on CentOS 7 by member initializer for shared\_ptr
* ARROW-6648 - [Go] Expose the bitutil package
* ARROW-6649 - [R] print() methods for Table, RecordBatch, etc.
* ARROW-6653 - [Developer] Add support for auto JIRA link on pull request
* ARROW-6655 - [Python] Filesystem bindings for S3
* ARROW-6664 - [C++] Add option to build without SSE4.2
* ARROW-6667 - [Python] Avoid Reference Cycles in pyarrow.parquet
* ARROW-6683 - [Python] Add unit tests that validate cross-compatibility with pyarrow.parquet when fastparquet is installed
* ARROW-6735 - [C++] Suppress sign-compare warning with g++ 9.2.1
* ARROW-6755 - [Release] Improvements to Windows release verification script
* ARROW-6771 - [Packaging][Python] Missing pytest dependency from conda and wheel builds
* PARQUET-1468 - [C++] Consolidate RecordReader, ColumnReader code paths

## New Feature

* ARROW-1561 - [C++] Kernel implementations for "isin" (set containment)
* ARROW-1566 - [C++] Implement non-materializing sort kernels
* ARROW-1741 - [C++] Comparison function for DictionaryArray to determine if indices are "compatible"
* ARROW-3204 - [R] Enable package to be made available on CRAN
* ARROW-3777 - [C++] Implement a mock "high latency" filesystem
* ARROW-3817 - [R] $ method for RecordBatch
* ARROW-453 - [C++] Add filesystem implementation for Amazon S3
* ARROW-517 - [C++] Verbose Array::Equals
* ARROW-5351 - [Rust] Add support for take kernel functions
* ARROW-5588 - [C++] Better support for building UnionArrays
* ARROW-5594 - [C++] add support for UnionArrays to Take and Filter
* ARROW-5719 - [Java] Support in-place vector sorting
* ARROW-5792 - [Rust] [Parquet] A visitor trait for parquet types.
* ARROW-5832 - [Java] Support search operations for vector data
* ARROW-5834 - [Java] Apply new hash map in DictionaryEncoder
* ARROW-5835 - [Java] Support Dictionary Encoding for binary type
* ARROW-5844 - [Java] Support comparison & sort for more numeric types
* ARROW-5862 - [Java] Provide dictionary builder
* ARROW-5881 - [Java] Provide functionalities to efficiently determine if a validity buffer has completely 1 bits/0 bits
* ARROW-5892 - [C++][Gandiva] Support function aliases
* ARROW-5893 - [C++] Remove arrow::Column class from C++ library
* ARROW-5898 - [Java] Provide functionality to efficiently compute hash code for arbitrary memory segment
* ARROW-5901 - [Rust] Implement PartialEq to compare array and json values
* ARROW-5902 - [Java] Implement hash table and equals & hashCode API for dictionary encoding
* ARROW-5917 - [Java] Redesign the dictionary encoder
* ARROW-5920 - [Java] Support sort & compare for all variable width vectors
* ARROW-5945 - [Rust] [DataFusion] Table trait should support building complete queries
* ARROW-5970 - [Java] Provide pointer to Arrow buffer
* ARROW-5974 - [Python][C++] Enable CSV reader to read from concatenated gzip stream
* ARROW-5979 - [FlightRPC] Expose (de)serialization of protocol types
* ARROW-5997 - [Java] Support dictionary encoding for Union type
* ARROW-6013 - [Java] Support range searcher
* ARROW-6022 - [Java] Support equals API in ValueVector to compare two vectors equal
* ARROW-6024 - [Java] Provide more hash algorithms
* ARROW-6030 - [Java] Efficiently compute hash code for ArrowBufPointer
* ARROW-6031 - [Java] Support iterating a vector by ArrowBufPointer
* ARROW-6039 - [GLib] Add garrow\_array\_filter()
* ARROW-6053 - [Python] RecordBatchStreamReader::Open2 cdef type signature doesn't match C++
* ARROW-6079 - [Java] Implement/test UnionFixedSizeListWriter for FixedSizeListVector
* ARROW-6080 - [Java] Support compare and search operation for BaseRepeatedValueVector
* ARROW-6113 - [Java] Support vector deduplicate function
* ARROW-6138 - [C++] Add a basic (single RecordBatch) implementation of Dataset
* ARROW-6155 - [Java] Extract a super interface for vectors whose elements reside in continuous memory segments
* ARROW-6156 - [Java] Support compare semantics for ArrowBufPointer
* ARROW-6161 - [C++] Implements dataset::ParquetFile and associated Scan structures
* ARROW-6185 - [Java] Provide hash table based dictionary builder
* ARROW-6188 - [GLib] Add garrow\_array\_is\_in()
* ARROW-6196 - [Ruby] Add support for building Arrow::TimeNNArray by .new
* ARROW-6197 - [GLib] Add garrow\_decimal128\_rescale()
* ARROW-6203 - [GLib] Add garrow\_array\_sort\_to\_indices()
* ARROW-6204 - [GLib] Add garrow\_array\_is\_in\_chunked\_array()
* ARROW-6212 - [Java] Support vector rank operation
* ARROW-6229 - [C++] Add a DataSource implementation which scans a directory
* ARROW-6238 - [C++] Implement SimpleDataSource/SimpleDataFragment
* ARROW-6242 - [C++] Implements basic Dataset/Scanner/ScannerBuilder
* ARROW-6243 - [C++] Implement basic Filter expression classes
* ARROW-6244 - [C++] Implement Partition DataSource
* ARROW-6247 - [Java] Provide a common interface for float4 and float8 vectors
* ARROW-6250 - [Java] Implement ApproxEqualsVisitor comparing approx for floating point
* ARROW-6278 - [R] Read parquet files from raw vector
* ARROW-6288 - [Java] Implement TypeEqualsVisitor comparing vector type equals considering names and metadata
* ARROW-6306 - [Java] Support stable sort by stable comparators
* ARROW-6326 - [C++] Nullable fields when converting std::tuple to Table
* ARROW-6346 - [GLib] Add garrow\_array\_view()
* ARROW-6347 - [GLib] Add garrow\_array\_diff\_unified()
* ARROW-6397 - [C++][CI] Fix S3 minio failure
* ARROW-6419 - [Website] Blog post about Parquet dictionary performance work coming in 0.15.x release
* ARROW-6427 - [GLib] Add support for column names autogeneration CSV read option
* ARROW-6438 - [R] Add bindings for filesystem API
* ARROW-6480 - [Developer] Add command to generate and send e-mail report for a Crossbow run
* ARROW-6580 - [Java] Support comparison for unsigned integers
* ARROW-6614 - [C++][Dataset] Implement FileSystemDataSourceDiscovery
* ARROW-6675 - [JS] Add scanReverse function to dataFrame and filteredDataframe
* ARROW-6752 - [Go] implement Stringer for Null array
* ARROW-750 - [Format] Add LargeBinary and LargeString types

## Sub-task

* ARROW-4218 - [Rust] [Parquet] Implement ColumnReader
* ARROW-4365 - [Rust] [Parquet] Implement RecordReader
* ARROW-4507 - [Format] Create outline and introduction for new document.
* ARROW-4508 - [Format] Copy content from Layout.rst to new document.
* ARROW-4509 - [Format] Copy content from Metadata.rst to new document.
* ARROW-4510 - [Format] copy content from IPC.rst to new document.
* ARROW-4511 - [Format] remove individual documents in favor of new document once all content is moved
* ARROW-5846 - [Java] Create Avro adapter module and add dependencies
* ARROW-5861 - [Java] Initial implement to convert Avro record with primitive types
* ARROW-5988 - [Java] Avro adapter implement simple Record type
* ARROW-6035 - [Java] Avro adapter support convert nullable value
* ARROW-6069 - [Rust] [Parquet] Implement Converter to convert record reader to arrow primitive array.
* ARROW-6078 - [Java] Implement dictionary-encoded subfields for List type
* ARROW-6085 - [Rust] [DataFusion] Create traits for phsyical query plan
* ARROW-6086 - [Rust] [DataFusion] Implement parallel execution for parquet scan
* ARROW-6087 - [Rust] [DataFusion] Implement parallel execution for CSV scan
* ARROW-6088 - [Rust] [DataFusion] Implement parallel execution for projection
* ARROW-6089 - [Rust] [DataFusion] Implement parallel execution for selection
* ARROW-6090 - [Rust] [DataFusion] Implement parallel execution for hash aggregate
* ARROW-6097 - [Java] Avro adapter implement unions type
* ARROW-6101 - [Rust] [DataFusion] Create physical plan from logical plan
* ARROW-6199 - [Java] Avro adapter avoid potential resource leak.
* ARROW-6220 - [Java] Add API to avro adapter to limit number of rows returned at a time.
* ARROW-6265 - [Java] Avro adapter implement Array/Map/Fixed type
* ARROW-6287 - [Rust] [DataFusion] Refactor TableProvider to return thread-safe BatchIterator
* ARROW-6310 - [C++] Write 64-bit integers as strings in JSON integration test files
* ARROW-6314 - [C++] Implement changes to ensure flatbuffer alignment.
* ARROW-6315 - [Java] Make change to ensure flatbuffer reads are aligned
* ARROW-6316 - [Go]  Make change to ensure flatbuffer reads are aligned
* ARROW-6317 - [JS] Implement changes to ensure flatbuffer alignment
* ARROW-6318 - [Integration] Update integration test to use generated binaries to ensure backwards compatibility
* ARROW-6356 - [Java] Avro adapter implement Enum type and nested Record type
* ARROW-6401 - [Java] Implement dictionary-encoded subfields for Struct type
* ARROW-6460 - [Java] Add benchmark and large fake data UT for avro adapter
* ARROW-6474 - [Python] Provide mechanism for python to write out old format
* ARROW-6519 - [Java] Use IPC continuation token to mark EOS
* ARROW-6539 - [R] Provide mechanism to write out old format
* ARROW-6563 - [Rust] [DataFusion] Create "merge" execution plan
* ARROW-6599 - [Rust] [DataFusion] Implement SUM aggregate expression
* ARROW-6665 - [Rust] [DataFusion] Implement numeric literal expressions
* ARROW-6668 - [Rust] [DataFusion] Implement CAST expression
* ARROW-6669 - [Rust] [DataFusion] Implement physical expression for binary expressions

## Task

* ARROW-1875 - [Java] Write 64-bit ints as strings in integration test JSON files
* ARROW-2931 - [Crossbow] Windows builds are attempting to run linux and osx packaging tasks
* ARROW-5483 - [Java] add ValueVector constructors that take a Field object
* ARROW-5579 - [Java] shade flatbuffer dependency
* ARROW-5580 - [C++][Gandiva] Correct definitions of timestamp functions in Gandiva
* ARROW-5758 - [C++][Gandiva] Support casting decimals to varchar and vice versa
* ARROW-5841 - [Website] Add 0.14.0 release note
* ARROW-5867 - [C++][Gandiva] Add support for cast int to decimal
* ARROW-5872 - Support mod(double, double) method in Gandiva
* ARROW-5891 - [C++][Gandiva] Remove duplicates in function registries
* ARROW-5903 - [Java] Set methods in DecimalVector are slow
* ARROW-5934 - [Python] Bundle arrow's LICENSE with the wheels
* ARROW-5944 - [C++][Gandiva] Remove 'div' alias for 'divide'
* ARROW-5957 - [C++][Gandiva] Implement div function in Gandiva
* ARROW-5958 - [Python] Link zlib statically in the wheels
* ARROW-5975 - [C++][Gandiva] Add method to cast Date(in Milliseconds) to timestamp
* ARROW-6008 - [Release] Don't parallelize the bintray upload script
* ARROW-6009 - [Release][JS] Ignore NPM errors in the javascript release script
* ARROW-6023 - [C++][Gandiva] Add functions in Gandiva
* ARROW-6026 - [Doc] Add CONTRIBUTING.md
* ARROW-6034 - [C++][Gandiva] Add string functions in Gandiva
* ARROW-6094 - [Format][Flight] Add GetFlightSchema to Flight RPC
* ARROW-6134 - [C++][Gandiva] Add concat function in Gandiva
* ARROW-6137 - [C++][Gandiva] Change output format of castVARCHAR(timestamp) in Gandiva
* ARROW-6144 - [C++][Gandiva] Implement random function in Gandiva
* ARROW-6162 - [C++][Gandiva] Do not truncate string in castVARCHAR\_varchar when out\_len parameter is zero
* ARROW-6177 - [C++] Add Array::Validate()
* ARROW-6217 - [Website] Remove needless \_site/ directory
* ARROW-6383 - [Java] report outstanding child allocators on parent allocator close
* ARROW-6385 - [C++] Investigate xxh3
* ARROW-6422 - [Gandiva] Fix double-conversion linker issue
* ARROW-6490 - [Java] log error for leak in allocator close
* ARROW-6491 - [Java] fix master build failure caused by ErrorProne
* ARROW-6601 - [Java] Improve JDBC adapter performance & add benchmark
* ARROW-6616 - [Website] Release announcement blog post for 0.15
* ARROW-6725 - [CI] Disable 3rdparty fuzzit nightly builds

## Test

* ARROW-5525 - [C++][CI] Enable continuous fuzzing
* ARROW-5978 - [FlightRPC] [Java] Integration test client doesn't close buffers
* ARROW-6193 - [GLib] Add missing require in test
* ARROW-6218 - [Java] Add UINT type test in integration to avoid potential overflow

## Wish

* ARROW-3538 - [Python] ability to override the automated assignment of uuid for filenames when writing datasets
* ARROW-6142 - [R] Install instructions on linux could be clearer
* ARROW-6183 - [R] Document that you don't have to use tidyselect if you don't want
* ARROW-6292 - [C++] Add an option to build with mimalloc
* ARROW-6300 - [C++] Add io::OutputStream::Abort()
* ARROW-6525 - [C++] CloseFromDestructor() should perhaps not crash
* ARROW-6549 - [C++] Switch back to latest jemalloc 5.x
* ARROW-6613 - [C++] Remove dependency on boost::filesystem

# Apache Arrow 0.14.0 (29 June 2019)

## Bug

* ARROW-1837 - [Java] Unable to read unsigned integers outside signed range for bit width in integration tests
* ARROW-2119 - [C++][Java] Handle Arrow stream with zero record batch
* ARROW-2136 - [Python] Non-nullable schema fields not checked in conversions from pandas
* ARROW-2256 - [C++] Fuzzer builds fail out of the box on Ubuntu 16.04 using LLVM apt repos
* ARROW-2461 - [Python] Build wheels for manylinux2010 tag
* ARROW-3344 - [Python] test\_plasma.py fails (in test\_plasma\_list)
* ARROW-3399 - [Python] Cannot serialize numpy matrix object
* ARROW-3650 - [Python] Mixed column indexes are read back as strings
* ARROW-4021 - [Ruby] Error building red-arrow on msys2
* ARROW-4076 - [Python] schema validation and filters
* ARROW-4139 - [Python] Cast Parquet column statistics to unicode if UTF8 ConvertedType is set
* ARROW-4301 - [Java][Gandiva] Maven snapshot version update does not seem to update Gandiva submodule
* ARROW-4324 - [Python] Array dtype inference incorrect when created from list of mixed numpy scalars
* ARROW-4350 - [Python] dtype=object arrays cannot be converted to a list-of-list ListArray
* ARROW-4447 - [C++] Investigate dynamic linking for libthift
* ARROW-4516 - [Python] Error while creating a ParquetDataset on a path without \`\_common\_dataset\` but with an empty \`\_tempfile\`
* ARROW-4651 - [Format] Flight Location should be more flexible than a (host, port) pair
* ARROW-4675 - [Python] Error serializing bool ndarray in py2 and deserializing in py3
* ARROW-4694 - [CI] detect-changes.py is inconsistent
* ARROW-4723 - [Python] Skip \_files when reading a directory containing parquet files
* ARROW-4823 - [Python] read\_csv shouldn't close file handles it doesn't own
* ARROW-4845 - [R] Compiler warnings on Windows MingW64
* ARROW-4851 - [Java] BoundsChecking.java defaulting behavior for old drill parameter seems off
* ARROW-4885 - [Python] read\_csv() can't handle decimal128 columns
* ARROW-4886 - [Rust] Inconsistent behaviour with casting sliced primitive array to list array
* ARROW-4923 - Expose setters for Decimal vector that take long and double inputs
* ARROW-4934 - [Python] Address deprecation notice that will be a bug in Python 3.8
* ARROW-5019 - [C#] ArrowStreamWriter doesn't work on a non-seekable stream
* ARROW-5049 - [Python] org/apache/hadoop/fs/FileSystem class not found when pyarrow FileSystem used in spark
* ARROW-5051 - [GLib][Gandiva] Test failure in release verification script
* ARROW-5055 - [Ruby][MSYS2] libparquet needs to be installed in MSYS2 for ruby
* ARROW-5058 - [Release] 02-source.sh generates e-mail template with wrong links
* ARROW-5068 - [Gandiva][Packaging] Fix gandiva nightly builds after the CMake refactor
* ARROW-5090 - Parquet linking fails on MacOS due to @rpath in dylib
* ARROW-5092 - [C#] Source Link doesn't work with the C# release script
* ARROW-5095 - [Flight][C++] Flight DoGet doesn't expose server error message
* ARROW-5096 - [Packaging][deb] plasma-store-server packages are missing
* ARROW-5097 - [Packaging][CentOS6] arrow-lib has unresolvable dependencies
* ARROW-5098 - [Website] Update APT install document for 0.13.0
* ARROW-5100 - [JS] Writer swaps byte order if buffers share the same underlying ArrayBuffer
* ARROW-5117 - [Go] Panic when appending zero slices after initializing a builder
* ARROW-5119 - [Go] invalid Stringer implementation for array.Boolean
* ARROW-5129 - [Rust][Parquet] Column writer bug: check dictionary encoder when adding a new data page
* ARROW-5130 - [Python] Segfault when importing TensorFlow after Pyarrow
* ARROW-5132 - [Java] Errors on building gandiva\_jni.dll on Windows with Visual Studio 2017
* ARROW-5138 - [Python/C++] Row group retrieval doesn't restore index properly
* ARROW-5142 - [CI] Fix conda calls in AppVeyor scripts
* ARROW-5144 - [Python] ParquetDataset and ParquetPiece not serializable
* ARROW-5146 - [Dev] Merge script imposes directory name
* ARROW-5147 - [C++] get an error in building: Could NOT find DoubleConversion
* ARROW-5148 - [CI] [C++] LLVM-related compile errors
* ARROW-5149 - [Packaging][Wheel] Pin LLVM to version 7 in windows builds
* ARROW-5152 - [Python] CMake warnings when building
* ARROW-5159 - Unable to build benches in arrow crate.
* ARROW-5160 - [C++] ABORT\_NOT\_OK evalutes expression twice
* ARROW-5166 - [Python][Parquet] Statistics for uint64 columns may overflow
* ARROW-5167 - [C++] Upgrade string-view-light to latest
* ARROW-5169 - [Python] non-nullable fields are converted to nullable in {{Table.from\_pandas}}
* ARROW-5173 - [Go] handle multiple concatenated streams back-to-back
* ARROW-5174 - [Go] implement Stringer for DataTypes
* ARROW-5177 - [Python] ParquetReader.read\_column() doesn't check bounds
* ARROW-5183 - [CI] MinGW build failures on AppVeyor
* ARROW-5184 - [Rust] Broken links and other documentation warnings
* ARROW-5195 - [Python] read\_csv ignores null\_values on string types
* ARROW-5201 - [Python] Import ABCs from collections is deprecated in Python 3.7
* ARROW-5208 - [Python] Inconsistent resulting type during casting in pa.array() when mask is present
* ARROW-5214 - [C++] Offline dependency downloader misses some libraries
* ARROW-5217 - [Rust] [CI] DataFusion test failure
* ARROW-5232 - [Java] value vector size increases rapidly in case of clear/setSafe loop
* ARROW-5233 - [Go] migrate to new flatbuffers-v1.11.0
* ARROW-5237 - [Python] pandas\_version key in pandas metadata no longer populated
* ARROW-5240 - [C++][CI] cmake\_format 0.5.0 appears to fail the build
* ARROW-5242 - [C++] Arrow doesn't compile cleanly with Visual Studio 2017 Update 9 or later due to narrowing
* ARROW-5243 - [Java][Gandiva] Add test for decimal compare functions
* ARROW-5245 - [C++][CI] Unpin cmake\_format
* ARROW-5246 - [Go] use Go-1.12 in CI
* ARROW-5249 - [Java] Flight client doesn't handle auth correctly in some cases
* ARROW-5253 - [C++] external Snappy fails on Alpine
* ARROW-5254 - [Flight][Java] DoAction does not support result streams
* ARROW-5255 - [Java] Implement user-defined data types API
* ARROW-5260 - [Python][C++] Crash when deserializing from components in a fresh new process
* ARROW-5274 - [JavaScript] Wrong array type for countBy
* ARROW-5285 - [C++][Plasma] GpuProcessHandle is not released when GPU object deleted
* ARROW-5293 - [C++] Take kernel on DictionaryArray does not preserve ordered flag
* ARROW-5294 - [CI] setuptools\_scm failures
* ARROW-5296 - [Java] Sporadic Flight test failures
* ARROW-5301 - [Python] parquet documentation outdated on nthreads argument
* ARROW-5306 - [CI] [GLib] Disable GTK-Doc
* ARROW-5308 - [Go] remove deprecated Feather format
* ARROW-5314 - [Go] Incorrect Printing for String Arrays with Offsets
* ARROW-5325 - [Archery][Benchmark] Output properly formatted jsonlines from benchmark diff cli command
* ARROW-5330 - [Python] [CI] Run Python Flight tests on Travis-CI
* ARROW-5332 - [R] R package fails to build/install: error in dyn.load()
* ARROW-5348 - [CI] [Java] Gandiva checkstyle failure
* ARROW-5360 - [Rust] Builds are broken by rustyline on nightly 2019-05-16+
* ARROW-5362 - [C++] Compression round trip test can cause some sanitizers to to fail
* ARROW-5373 - [Java] Add missing details for Gandiva Java Build
* ARROW-5376 - [C++] Compile failure on gcc 5.4.0
* ARROW-5383 - [Go] update IPC flatbuf (new Duration type)
* ARROW-5387 - [Go] properly handle sub-slice of List
* ARROW-5388 - [Go] use arrow.TypeEqual in array.NewChunked
* ARROW-5390 - [CI] Job time limit exceeded on Travis
* ARROW-5398 - [Python] Flight tests broken by URI changes
* ARROW-5403 - [C++] Test failures not propagated in Windows shared builds
* ARROW-5411 - [C++][Python] Build error building on Mac OS Mojave
* ARROW-5412 - [Java] Integration test fails with UnsupportedOperationException
* ARROW-5419 - [C++] CSV strings\_can\_be\_null option doesn't respect all null\_values
* ARROW-5421 - [Packaging][Crossbow] Duplicated key in nightly test configuration
* ARROW-5430 - [Python] Can read but not write parquet partitioned on large ints
* ARROW-5435 - [Java] add test for IntervalYearVector#getAsStringBuilder
* ARROW-5437 - [Python] Missing pandas pytest marker from parquet tests
* ARROW-5446 - [C++] Use cmake header install directory instead of include
* ARROW-5448 - [CI] MinGW build failures on AppVeyor
* ARROW-5453 - [C++] Just-released cmake-format 0.5.2 breaks the build
* ARROW-5455 - [Rust] Build broken by 2019-05-30 Rust nightly
* ARROW-5456 - [GLib][Plasma] Installed plasma-glib may be used on building document
* ARROW-5457 - [GLib][Plasma] Environment variable name for test is wrong
* ARROW-5459 - [Go] implement Stringer for Float16 DataType
* ARROW-5462 - [Go] support writing zero-length List
* ARROW-5487 - [CI] [Python] Failure in docs build
* ARROW-5507 - [Plasma] [CUDA] Compile error
* ARROW-5514 - [C++] Printer for uint64 shows wrong values
* ARROW-5517 - [C++] Header collection CMake logic should only consider filename without directory included
* ARROW-5520 - [C++][Packaging] No NVidia CUDA toolkit on AArch64C
* ARROW-5521 - [Packaging] License check fails with Apache RAT 0.13
* ARROW-5528 - Concatenate() crashes when concatenating empty binary arrays.
* ARROW-5532 - [JS] Field Metadata Not Read
* ARROW-5551 - [Go] invalid FixedSizeArray representation
* ARROW-5553 - [Ruby] red-arrow gem does not compile on ruby:2.5 docker image
* ARROW-5576 - [C++] Flaky thrift\_ep tarball downloads
* ARROW-5577 - [C++] Link failure due to googletest shared library on Alpine Linux
* ARROW-5583 - [Java] When the isSet of a NullableValueHolder is 0, the buffer field should not be used
* ARROW-5584 - [Java] Add import for link reference in FieldReader javadoc
* ARROW-5589 - [C++][Fuzzing] arrow-ipc-fuzzing-test crash 2354085db0125113f04f7bd23f54b85cca104713
* ARROW-5592 - [Go] implement Duration array
* ARROW-5596 - [Python] Flight tests failing on Python 2.7
* ARROW-5601 - [gandiva] Error when projector with a string field
* ARROW-5603 - [Python] register pytest markers to avoid warnings
* ARROW-5605 - [C++][Fuzzing] arrow-ipc-fuzzing-test crash 74aec871d14bb6b07c72ea8f0e8c9f72cbe6b73c
* ARROW-5606 - [Python] pandas.RangeIndex.\_start/\_stop/\_step are deprecated
* ARROW-5608 - [C++][parquet] Invalid memory access when using parquet::arrow::ColumnReader
* ARROW-5615 - [C++] Compilation error due to C++11 string literals on gcc 5.4.0 Ubuntu 16.04
* ARROW-5616 - [Python] C++ build failure against Python 2.7 headers
* ARROW-5617 - [C++] thrift\_ep 0.12.0 fails to build when using ARROW\_BOOST\_VENDORED=ON
* ARROW-5619 - [C++] get\_apache\_mirror.py doesn't work with Python 3.5
* ARROW-5624 - [C++] -Duriparser\_SOURCE=BUNDLED is broken
* ARROW-5626 - [C++][Gandiva] Expression cache should consider precision and scale too
* ARROW-5629 - [C++] Fix Coverity issues
* ARROW-5631 - [C++] CMake 3.2 build is broken
* ARROW-5648 - [C++] Build fails on mingw without codecvt
* ARROW-5654 - [C++] ChunkedArray should validate the types of the arrays
* ARROW-5674 - [Python] Missing pandas pytest markers from test\_parquet.py
* ARROW-5675 - [Doc] Fix typo in documentation describing compile/debug workflow on macOS with Xcode IDE
* ARROW-5678 - [R][Lint] Fix hadolint docker linting error
* ARROW-5693 - [Go] skip IPC integration test for Decimal128
* ARROW-5697 - [GLib] c\_glib/Dockerfile is broken
* ARROW-5698 - [R] r/Dockerfile docker-compose build is broken
* ARROW-5709 - [C++] gandiva-date\_time\_test failure on Windows
* ARROW-5714 - [JS] Inconsistent behavior in Int64Builder with/without BigNum
* ARROW-5723 - [Gandiva][Crossbow] Builds failing
* ARROW-5728 - [Python] [CI] Travis-CI failures in test\_jvm.py
* ARROW-5730 - [Python][CI] Selectively skip test cases in the dask integration test
* ARROW-5732 - [C++] macOS builds failing idiosyncratically on master with warnings from pmmintrin.h
* ARROW-5735 - [C++] Appveyor builds failing persistently in thrift\_ep build
* ARROW-5737 - [C++][Gandiva] Gandiva not building in manylinux
* ARROW-5738 - [Crossbow][Conda] OSX package builds are failing with missing intrinsics
* ARROW-5739 - [CI] Fix docker python build
* ARROW-5750 - [Java] Java compilation failures on master
* ARROW-5754 - [C++]Missing override for ~GrpcStreamWriter?
* ARROW-5765 - [C++] TestDictionary.Validate test is crashed with release build
* ARROW-5770 - [C++] Fix -Wpessimizing-move in result.h
* ARROW-5771 - [Python] Docker python-nopandas job fails
* ARROW-5774 - [Java][Documentation] Document the need to checkout git submodules for flight
* ARROW-5781 - [Archery] Ensure benchmark clone accepts remotes in revision
* ARROW-5791 - [Python] pyarrow.csv.read\_csv hangs + eats all RAM
* ARROW-5816 - [Release] Parallel curl does not work reliably in verify-release-candidate-sh
* ARROW-61 - [Java] Method can return the value bigger than long MAX\_VALUE
* PARQUET-1402 - [C++] incorrect calculation column start offset for files created by parquet-mr 1.8.1
* PARQUET-1405 - [C++] 'Couldn't deserialize thrift' error when reading large binary column
* PARQUET-1565 - [C++] SEGV in FromParquetSchema with corrupt file from PARQUET-1481
* PARQUET-1571 - [C++] Can't read data from parquet file in C++ library
* PARQUET-1574 - [C++] parquet-encoding-test failed with msvc
* PARQUET-1581 - [C++] Fix undefined behavior in encoding.cc when num\_dictionary\_values is 0.

## Improvement

* ARROW-1496 - [JS] Upload coverage data to codecov.io
* ARROW-1957 - [Python] Write nanosecond timestamps using new NANO LogicalType Parquet unit
* ARROW-1983 - [Python] Add ability to write parquet \`\_metadata\` file
* ARROW-2057 - [Python] Configure size of data pages in pyarrow.parquet.write\_table
* ARROW-2217 - [C++] Add option to use dynamic linking for compression library dependencies
* ARROW-2298 - [Python] Add option to not consider NaN to be null when converting to an integer Arrow type
* ARROW-2707 - [C++] Implement Table::Slice methods using Column::Slice
* ARROW-2796 - [C++] Simplify symbols.map file, use when building libarrow\_python
* ARROW-2818 - [Python] Better error message when passing SparseDataFrame into Table.from\_pandas
* ARROW-2981 - [C++] Support scripts / documentation for running clang-tidy on codebase
* ARROW-3040 - [Go] add support for comparing Arrays
* ARROW-3041 - [Go] add support for TimeArray
* ARROW-3052 - [C++] Detect ORC system packages
* ARROW-3144 - [C++] Move "dictionary" member from DictionaryType to ArrayData to allow for changing dictionaries between Array chunks
* ARROW-3150 - [Python] Ship Flight-enabled Python wheels on Linux and Windows
* ARROW-3166 - [C++] Consolidate IO interfaces used in arrow/io and parquet-cpp
* ARROW-3200 - [C++] Add support for reading Flight streams with dictionaries
* ARROW-3290 - [C++] Toolchain support for secure gRPC
* ARROW-3294 - [C++] Test Flight RPC on Windows / Appveyor
* ARROW-3314 - [R] Set -rpath using pkg-config when building
* ARROW-3475 - [C++] Int64Builder.Finish(NumericArray<Int64Type>)
* ARROW-3572 - [Packaging] Correctly handle ssh origin urls for crossbow
* ARROW-3671 - [Go] implement Interval array
* ARROW-3676 - [Go] implement Decimal128 array
* ARROW-3679 - [Go] implement IPC protocol
* ARROW-3680 - [Go] implement Float16 array
* ARROW-3686 - [Python] Support for masked arrays in to/from numpy
* ARROW-3729 - [C++] Support for writing TIMESTAMP\_NANOS Parquet metadata
* ARROW-3758 - [R] Build R library on Windows, document build instructions for Windows developers
* ARROW-3759 - [R][CI] Build and test on Windows in Appveyor
* ARROW-3767 - [C++] Add cast for Null to any type
* ARROW-3794 - [R] Consider mapping INT8 to integer() not raw()
* ARROW-3804 - [R] Consider lowering required R runtime
* ARROW-3904 - [C++/Python] Validate scale and precision of decimal128 type
* ARROW-4013 - [Documentation][C++] Document how to build Apache Arrow on MSYS2
* ARROW-4020 - [Release] Remove source artifacts from dev dist system after release vote passes
* ARROW-4047 - [Python] Document use of int96 timestamps and options in Parquet docs
* ARROW-4159 - [C++] Check for -Wdocumentation issues
* ARROW-4194 - [Format] Metadata.rst does not specify timezone for Timestamp type
* ARROW-4337 - [C#] Array / RecordBatch Builder Fluent API
* ARROW-4343 - [C++] Add as complete as possible Ubuntu Trusty / 14.04 build to docker-compose setup
* ARROW-4356 - [CI] Add integration (docker) test for turbodbc
* ARROW-4452 - [Python] Serializing sparse torch tensors
* ARROW-4453 - [Python] Create Cython wrappers for SparseTensor
* ARROW-4467 -  [Rust] [DataFusion] Create a REPL & Dockerfile for DataFusion
* ARROW-4503 - [C#] ArrowStreamReader allocates and copies data excessively
* ARROW-4504 - [C++] Reduce the number of unit test executables
* ARROW-4505 - [C++] Nicer PrettyPrint for date32
* ARROW-4566 - [C++][Flight] Add option to run arrow-flight-benchmark against a perf server running on a different host
* ARROW-4596 - [Rust] [DataFusion] Implement COUNT aggregate function
* ARROW-4622 - [C++] [Python] MakeDense and MakeSparse in UnionArray should accept a vector of Field
* ARROW-4625 - [Flight] Wrap server busy-wait methods
* ARROW-4626 - [Flight] Add application metadata field to DoGet
* ARROW-4627 - [Flight] Add application metadata field to DoPut
* ARROW-4714 - [C++][Java] Providing JNI interface to Read ORC file via Arrow C++
* ARROW-4717 - [C#] Consider exposing ValueTask instead of Task
* ARROW-4787 - [C++] Include "null" values (perhaps with an option to toggle on/off) in hash kernel actions
* ARROW-4788 - [C++] Develop less verbose API for constructing StructArray
* ARROW-4800 - [C++] Create/port a StatusOr implementation to be able to return a status or a type
* ARROW-4824 - [Python] read\_csv should accept io.StringIO objects
* ARROW-4847 - [Python] Add pyarrow.table factory function that dispatches to various ctors based on type of input
* ARROW-4911 - [R] Support for building package for Windows
* ARROW-4912 - [C++, Python] Allow specifying column names to CSV reader
* ARROW-4945 - [Flight] Enable Flight integration tests in Travis
* ARROW-4968 - [Rust] StructArray builder and From<> methods should check that field types match schema
* ARROW-4990 - [C++] Kernel to compare array with array
* ARROW-4993 - [C++] Display summary at the end of CMake configuration
* ARROW-5000 - [Python] Fix deprecation warning from setup.py
* ARROW-5007 - [C++] Move DCHECK out of sse-utils
* ARROW-5020 - [C++][Gandiva] Split Gandiva-related conda packages for builds into separate .yml conda env file
* ARROW-5027 - [Python] Add JSON Reader
* ARROW-5038 - [Rust] [DataFusion] Implement AVG aggregate function
* ARROW-5039 - [Rust] [DataFusion] Fix bugs in CAST support
* ARROW-5045 - [Rust] Code coverage silently failing in CI
* ARROW-5053 - [Rust] [DataFusion] Use env var for location of arrow test data
* ARROW-5054 - [C++][Release] Test Flight in verify-release-candidate.sh
* ARROW-5061 - [Release] Improve 03-binary performance
* ARROW-5062 - [Java] Shade Java Guava dependency for Flight
* ARROW-5063 - [Java] FlightClient should not create a child allocator
* ARROW-5064 - [Release] Pass PKG\_CONFIG\_PATH to glib in the verification script
* ARROW-5066 - [Integration] Add flags to enable/disable implementations in integration/integration\_test.py
* ARROW-5076 - [Packaging] Improve post binary upload performance
* ARROW-5077 - [Rust] Release process should change Cargo.toml to use release versions
* ARROW-5078 - [Documentation] Sphinx is failed by RemovedInSphinx30Warning
* ARROW-5079 - [Release] Add a script to release C# package
* ARROW-5080 - [Release] Add a script to release Rust packages
* ARROW-5081 - [C++] Consistently use PATH\_SUFFIXES in CMake config
* ARROW-5083 - [Developer] In merge\_arrow\_pr.py script, allow user to set a released Fix Version
* ARROW-5088 - [C++] Do not set -Werror when using BUILD\_WARNING\_LEVEL=CHECKIN in release mode
* ARROW-5091 - [Flight] Rename FlightGetInfo message to FlightInfo
* ARROW-5093 - [Packaging] Add support for selective binary upload
* ARROW-5094 - [Packaging] Add APT/Yum verification scripts
* ARROW-5113 - [C++][Flight] Unit tests in C++ for DoPut
* ARROW-5116 - [Rust] move kernel related files under compute/kernels
* ARROW-5124 - [C++] Add support for Parquet in MinGW build
* ARROW-5136 - [Flight] Implement call options (timeouts)
* ARROW-5137 - [Flight] Implement authentication APIs
* ARROW-5157 - [Website] Add MATLAB to powered by Apache Arrow page
* ARROW-5162 - [Rust] [Parquet] Rename mod reader to arrow.
* ARROW-5163 - [Gandiva] Cast timestamp/date are incorrectly evaluating year 0097 to 1997
* ARROW-5165 - [Python][Documentation] Build docs don't suggest assigning $ARROW\_BUILD\_TYPE
* ARROW-5178 - [Python] Allow creating Table from Python dict
* ARROW-5179 - [Python] Return plain dicts, not OrderedDict, on Python 3.7+
* ARROW-5185 - [C++] Add support for Boost with CMake configuration file
* ARROW-5191 - [Rust] Expose CSV and JSON reader schemas
* ARROW-5204 - [C++] Improve BufferBuilder performance
* ARROW-5212 - [Go] Array BinaryBuilder in Go library has no access to resize the values buffer
* ARROW-5218 - [C++] Improve build when third-party library locations are specified
* ARROW-5219 - [C++] Build protobuf\_ep in parallel when using Ninja
* ARROW-5222 - [Python] Issues with installing pyarrow for development on MacOS
* ARROW-5225 - [Java] Improve performance of BaseValueVector#getValidityBufferSizeFromCount
* ARROW-5238 - [Python] Improve usability of pyarrow.dictionary function
* ARROW-5241 - [Python] Add option to disable writing statistics to parquet file
* ARROW-5252 - [C++] Change variant implementation
* ARROW-5256 - [Packaging][deb] Failed to build with LLVM 7.1.0
* ARROW-5257 - [Website] Update site to use "official" Apache Arrow logo, add clearly marked links to logo
* ARROW-5258 - [C++/Python] Expose file metadata of dataset pieces to caller
* ARROW-5261 - [C++] Finish implementation of scalar types for Duration and Interval
* ARROW-5262 - [Python] Fix typo
* ARROW-5264 - [Java] Allow enabling/disabling boundary checking by environmental variable
* ARROW-5269 - [C++] Whitelist benchmarks candidates for regression checks
* ARROW-5281 - [Rust] [Parquet] Move DataPageBuilder to test\_common
* ARROW-5284 - [Rust] Replace libc with std::alloc for memory allocation
* ARROW-5286 - [Python] support Structs in Table.from\_pandas given a known schema
* ARROW-5288 - [Documentation] Enrich the contribution guidelines
* ARROW-5289 - [C++] Move arrow/util/concatenate.h to arrow/array/
* ARROW-5291 - [Python] Add wrapper for "take" kernel on Array
* ARROW-5298 - [Rust] Add debug implementation for Buffer
* ARROW-5309 - [Python] Add clarifications to Python "append" methods that return new objects
* ARROW-5311 - [C++] Return more specific invalid Status in Take kernel
* ARROW-5317 - [Rust] [Parquet] impl IntoIterator for SerializedFileReader
* ARROW-5319 - [CI] Enable ccache with MinGW builds
* ARROW-5323 - [CI] Use compression with clcache
* ARROW-5328 - [R] Add shell scripts to do a full package rebuild and test locally
* ARROW-5334 - [C++] Add "Type" to names of arrow::Integer, arrow::FloatingPoint classes for consistency
* ARROW-5335 - [Python] Raise on variable dictionaries when converting to pandas
* ARROW-5339 - [C++] Add jemalloc to thirdparty dependency download script
* ARROW-5341 - [C++] Add instructions about fixing and testing for -Wdocumentation clang warnings locally
* ARROW-5349 - [Python/C++] Provide a way to specify the file path in parquet ColumnChunkMetaData
* ARROW-5361 - [R] Follow DictionaryType/DictionaryArray changes from ARROW-3144
* ARROW-5363 - [GLib] Fix coding styles
* ARROW-5364 - [C++] Use ASCII rather than UTF-8 in BuildUtils.cmake comment
* ARROW-5365 - [C++][CI] Add UBSan and ASAN into CI
* ARROW-5368 - [C++] Disable jemalloc by default with MinGW
* ARROW-5369 - [C++] Add support for glog on Windows
* ARROW-5370 - [C++] Detect system uriparser by default
* ARROW-5378 - [C++] Add local FileSystem implementation
* ARROW-5389 - [C++] Add an internal temporary directory API
* ARROW-5393 - [R] Add tests and example for read\_parquet()
* ARROW-5395 - [C++] Utilize stream EOS in File format
* ARROW-5407 - [C++] Integration test Travis CI entry builds many unnecessary targets
* ARROW-5413 - [C++] CSV reader doesn't remove BOM
* ARROW-5415 - [Release] Release script should update R version everywhere
* ARROW-5416 - [Website] Add Homebrew to project installation page
* ARROW-5418 - [CI][R] Run code coverage and report to codecov.io
* ARROW-5420 - [Java] Implement or remove getCurrentSizeInBytes in VariableWidthVector
* ARROW-5427 - [Python] RangeIndex serialization change implications
* ARROW-5428 - [C++] Add option to set "read extent" in arrow::io::BufferedInputStream
* ARROW-5429 - [Java] Provide alternative buffer allocation policy
* ARROW-5433 - [C++][Parquet] improve parquet-reader columns information
* ARROW-5436 - [Python] expose filters argument in parquet.read\_table
* ARROW-5438 - [JS] Utilize stream EOS in File format
* ARROW-5441 - [C++] Implement FindArrowFlight.cmake
* ARROW-5442 - [Website] Clarify what makes a release artifact "official"
* ARROW-5447 - [CI] [Ruby] CI is failed on AppVeyor
* ARROW-5452 - [R] Add documentation website (pkgdown)
* ARROW-5461 - [Java] Add micro-benchmarks for Float8Vector and allocators
* ARROW-5464 - [Archery] Bad --benchmark-filter default
* ARROW-5465 - [Crossbow] Support writing submitted job definition yaml to a file
* ARROW-5466 - [Java] Dockerize Java builds in Travis CI, run multiple JDKs in single entry
* ARROW-5470 - [CI] C++ local filesystem patch breaks Travis R job
* ARROW-5472 - [Development] Add warning to PR merge tool if no JIRA component is set
* ARROW-5474 - [C++] Document required Boost version
* ARROW-5477 - [C++] Check required RapidJSON version
* ARROW-5478 - [Packaging] Drop Ubuntu 14.04 support
* ARROW-5481 - [GLib] garrow\_seekable\_input\_stream\_peek() misses "error" parameter document
* ARROW-5488 - [R] Workaround when C++ lib not available
* ARROW-5492 - [R] Add "col\_select" argument to read\_\* functions to read subset of columns
* ARROW-5495 - [C++] Use HTTPS consistently for downloading dependencies
* ARROW-5496 - [R][CI] Fix relative paths in R codecov.io reporting
* ARROW-5498 - [C++] Build failure with Flatbuffers 1.11.0 and MinGW
* ARROW-5500 - [R] read\_csv\_arrow() signature should match readr::read\_csv()
* ARROW-5503 - [R] add read\_json()
* ARROW-5504 - [R] move use\_threads argument to global option
* ARROW-5509 - [R] write\_parquet()
* ARROW-5511 - [Packaging] Enable Flight in Conda packages
* ARROW-5513 - [Java] Refactor method name for getstartOffset to use camel case
* ARROW-5516 - [Python] Development page for pyarrow has a missing dependency in using pip
* ARROW-5518 - [Java] Set VectorSchemaRoot rowCount to 0 on allocateNew and clear
* ARROW-5524 - [C++] Turn off PARQUET\_BUILD\_ENCRYPTION in CMake if OpenSSL not found
* ARROW-5526 - [Developer] Add more prominent notice to GitHub issue template to direct bug reports to JIRA
* ARROW-5529 - [Flight] Allow serving with multiple TLS certificates
* ARROW-5531 - [Python] Support binary, utf8, and nested types in Array.from\_buffers
* ARROW-5533 - [Plasma] Plasma client should be thread-safe
* ARROW-5538 - [C++] Restrict minimum OpenSSL version to 1.0.2
* ARROW-5541 - [R] cast from negative int32 to uint32 and uint64 are now safe
* ARROW-5544 - [Archery] should not return non-zero in \`benchmark diff\` sub command on regression
* ARROW-5545 - [C++][Docs] Clarify expectation of UTC values for timestamps with time zones in C++ API docs
* ARROW-5547 - [C++][FlightRPC] arrow-flight.pc isn't provided
* ARROW-5552 - [Go] make Schema and Field implement Stringer
* ARROW-5554 - Add a python wrapper for arrow::Concatenate
* ARROW-5555 - [R] Add install\_arrow() function to assist the user in obtaining C++ runtime libraries
* ARROW-5556 - [Doc] Document JSON reader
* ARROW-5565 - [Python] Document how to use gdb when working on pyarrow
* ARROW-5567 - [C++]  Fix build error of memory-benchmark
* ARROW-5571 - [R] Rework handing of ARROW\_R\_WITH\_PARQUET
* ARROW-5574 - [R] documentation error for read\_arrow()
* ARROW-5582 - [Go] add support for comparing Records
* ARROW-5586 - [R] convert Array of LIST type to R lists
* ARROW-5587 - [Java] Add more maven style check for Java code
* ARROW-5590 - [R] Run "no libarrow" R build in the same CI entry if possible
* ARROW-5600 - [R] R package namespace cleanup
* ARROW-5604 - [Go] improve test coverage of type-traits
* ARROW-5609 - [C++] Set CMP0068 CMake policy to avoid macOS warnings
* ARROW-5612 - [Python][Documentation] Clarify date\_as\_object option behavior
* ARROW-5622 - [C++][Dataset] arrow-dataset.pc isn't provided
* ARROW-5625 - [R] convert Array of struct type to data frame columns
* ARROW-5632 - [Doc] Add some documentation describing compile/debug workflow on macOS with Xcode IDE
* ARROW-5633 - [Python] Enable bz2 in Linux wheels
* ARROW-5635 - [C++] Support "compacting" a table
* ARROW-5639 - [Java] Remove floating point computation from getOffsetBufferValueCapacity
* ARROW-5641 - [GLib] Remove enums files generated by GNU Autotools from Git targets
* ARROW-5643 - [Flight] Add ability to override hostname checking
* ARROW-5652 - [CI] Fix iwyu docker image
* ARROW-5656 - [Python] Enable Flight wheels on macOS
* ARROW-5659 - [C++] Add support for finding OpenSSL installed by Homebrew
* ARROW-5660 - [GLib][CI] Use the latest macOS image and all Homebrew based libraries
* ARROW-5662 - [C++] Add support for BOOST\_SOURCE=AUTO|BUNDLED|SYSTEM
* ARROW-5663 - [Packaging][RPM] Update CentOS packages for 0.14.0
* ARROW-5664 - [Crossbow] Execute nightly crossbow tests on CircleCI instead of Travis
* ARROW-5668 - [Python] Display "not null" in Schema.\_\_repr\_\_ for non-nullable fields
* ARROW-5669 - [Crossbow] manylinux1 wheel building failing
* ARROW-5670 - [Crossbow] get\_apache\_mirror.py fails with TLS error on macOS with Python 3.5
* ARROW-5671 - [crossbow] mac os python wheels failing
* ARROW-5683 - [R] Add snappy to Rtools Windows builds
* ARROW-5684 - [Packaging][deb] Add support for Ubuntu 19.04
* ARROW-5685 - [Packaging][deb] Add support for Apache Arrow Datasets
* ARROW-5687 - [C++] Remove remaining uses of ARROW\_BOOST\_VENDORED
* ARROW-5690 - [Packaging][Python] macOS wheels broken: libprotobuf.18.dylib missing
* ARROW-5694 - [Python] List of decimals are not supported when converting to pandas
* ARROW-5695 - [C#][Release] Run sourcelink test in verify-release-candidate.sh
* ARROW-5699 - [C++] Optimize parsing of Decimal128 in CSV
* ARROW-5702 - [C++] parquet::arrow::FileReader::GetSchema()
* ARROW-5705 - [Java] Optimize BaseValueVector#computeCombinedBufferSize logic
* ARROW-5706 - [Java] Remove type conversion in getValidityBufferValueCapacity
* ARROW-5707 - [Java] Improve the performance and code structure for ArrowRecordBatch
* ARROW-5710 - [C++] Allow compiling Gandiva with Ninja on Windows
* ARROW-5718 - [R] auto splice data frames in record\_batch() and table()
* ARROW-5721 - [Rust] Move array related code into a separate module
* ARROW-5724 - [R] [CI] AppVeyor build should use ccache
* ARROW-5725 - [Crossbow] Port conda recipes to azure pipelines
* ARROW-5727 - [Python] [CI] Install pytest-faulthandler before running tests
* ARROW-5748 - [Packaging][deb] Add support for Debian GNU/Linux buster
* ARROW-5749 - [Python] Add Python binding for Table::CombineChunks()
* ARROW-5751 - [Packaging][Python] Python macOS wheels have dynamic dependency on libcares
* ARROW-5752 - [Java] Improve the performance of ArrowBuf#setZero
* ARROW-5768 - [Release] There are needless newlines at the end of CHANGELOG.md
* ARROW-5773 - [R] Clean up documentation before release
* ARROW-5782 - [Release] Setup test data for Flight in dev/release/01-perform.sh
* ARROW-5783 - [Release][C#] Exclude dummy.git from RAT check
* ARROW-5785 - [Rust] Rust datafusion implementation should not depend on rustyline
* ARROW-5787 - [Release][Rust] Use local modules to verify RC
* ARROW-5793 - [Release] Avoid duplicate known host SSH error in dev/release/03-binary.sh
* ARROW-5794 - [Release] Skip uploading already uploaded binaries
* ARROW-5795 - [Release] Add missing waits on uploading binaries
* ARROW-5796 - [Release][APT] Update expected package list
* ARROW-5797 - [Release][APT] Update supported distributions
* ARROW-5820 - [Release] Remove undefined variable check from verify script
* ARROW-767 - [C++] Adopt FileSystem abstraction
* ARROW-835 - [Format] Add Timedelta type to describe time intervals
* PARQUET-1243 - [C++] Improve quality of error message for zero-length files, otherwise corrupted files
* PARQUET-1411 - [C++] Upgrade to use LogicalType annotations instead of ConvertedType
* PARQUET-1422 - [C++] Use Arrow IO interfaces natively rather than current parquet:: wrappers
* PARQUET-1523 - [C++] Vectorize comparator interface
* PARQUET-1569 - [C++] Consolidate testing header files
* PARQUET-1582 - [C++] Add ToString method ColumnDescriptor
* PARQUET-1583 - [C++] Remove parquet::Vector class
* PARQUET-1586 - [C++] Add --dump options to parquet-reader tool to dump def/rep levels

## New Feature

* ARROW-1012 - [C++] Create a configurable implementation of RecordBatchReader that reads from Apache Parquet files
* ARROW-1207 - [C++] Implement Map logical type
* ARROW-1261 - [Java] Add container type for Map logical type
* ARROW-1278 - Integration tests for Fixed Size List type
* ARROW-1279 - [Integration][Java] Integration tests for Map type
* ARROW-1280 - [C++] Implement Fixed Size List type
* ARROW-1349 - [Packaging] Provide APT and Yum repositories
* ARROW-1558 - [C++] Implement boolean selection kernels
* ARROW-1774 - [C++] Add "view" function to create zero-copy views for compatible types, if supported
* ARROW-2467 - [Rust] Generate code using Flatbuffers
* ARROW-2517 - [Java] Add list<decimal> writer
* ARROW-2835 - [C++] ReadAt/WriteAt are inconsistent with moving the files position
* ARROW-2969 - [R] Convert between StructArray and "nested" data.frame column containing data frame in each cell
* ARROW-3087 - [C++] Add kernels for comparison operations to scalars
* ARROW-3191 - [Java] Add support for ArrowBuf to point to arbitrary memory.
* ARROW-3419 - [C++] Run include-what-you-use checks as nightly build
* ARROW-3459 - [C++][Gandiva] Add support for variable length output vectors
* ARROW-3732 - [R] Add functions to write RecordBatch or Schema to Message value, then read back
* ARROW-3791 - [C++] Add type inference for boolean values in CSV files
* ARROW-3810 - [R] type= argument for Array and ChunkedArray
* ARROW-3811 - [R] struct arrays inference
* ARROW-3814 - [R] RecordBatch$from\_arrays()
* ARROW-3815 - [R] refine record batch factory
* ARROW-3848 - [R] allow nbytes to be missing in RandomAccessFile$Read()
* ARROW-3897 - [MATLAB] Add MATLAB support for writing numeric datatypes to a Feather file
* ARROW-4302 - [C++] Add OpenSSL to C++ build toolchain
* ARROW-4701 - [C++] Add JSON chunker benchmarks
* ARROW-4708 - [C++] Add multithreaded JSON reader
* ARROW-4741 - [Java] Add documentation to all classes and enable checkstyle for class javadocs
* ARROW-4805 - [Rust] Write temporal arrays to CSV
* ARROW-4806 - [Rust] Support casting temporal arrays in cast kernels
* ARROW-4827 - [C++] Implement benchmark comparison between two git revisions
* ARROW-5071 - [Benchmarking] Performs a benchmark run with archery
* ARROW-5115 - [JS] Implement the Vector Builders
* ARROW-5126 - [Rust] [Parquet] Convert parquet column desc to arrow data type
* ARROW-5150 - [Ruby] Add Arrow::Table#raw\_records
* ARROW-5155 - [GLib][Ruby] Add support for building union arrays from data type
* ARROW-5168 - [GLib] Add garrow\_array\_take()
* ARROW-5171 - [C++] Use LESS instead of LOWER in compare enum option.
* ARROW-5187 - [Rust] Ability to flatten StructArray into a RecordBatch
* ARROW-5188 - [Rust] Add temporal builders for StructArray
* ARROW-5189 - [Rust] [Parquet] Format individual fields within a parquet row
* ARROW-5203 - [GLib] Add support for Compare filter
* ARROW-5268 - [GLib] Add GArrowJSONReader
* ARROW-5290 - [Java] Provide a flag to enable/disable null-checking in vectors' get methods
* ARROW-5299 - [C++] ListArray comparison is incorrect
* ARROW-5329 - Add support for building MATLAB interface to Feather directly within MATLAB
* ARROW-5342 - [Format] Formalize extension type metadata in IPC protocol
* ARROW-5372 - [GLib] Add support for null/boolean values CSV read option
* ARROW-5384 - [Go] add FixedSizeList array
* ARROW-5396 - [JS] Ensure reader and writer support files and streams with no RecordBatches
* ARROW-5404 - [C++] nonstd::string\_view conflicts with std::string\_view in c++17
* ARROW-5432 - [Python] Add 'read\_at' method to pyarrow.NativeFile
* ARROW-5463 - [Rust] Implement AsRef for Buffer
* ARROW-5486 - [GLib] Add binding of gandiva::FunctionRegistry and related things
* ARROW-5512 - [C++] Draft initial public APIs for Datasets project
* ARROW-5534 - [GLib] Add garrow\_table\_concatenate()
* ARROW-5535 - [GLib] Add garrow\_table\_slice()
* ARROW-5537 - [JS] Support delta dictionaries in RecordBatchWriter and DictionaryBuilder
* ARROW-5581 - [Java] Provide interfaces and initial implementations for vector sorting
* ARROW-5597 - [Packaging][deb] Add Flight packages
* ARROW-5726 - [Java] Implement a common interface for int vectors
* ARROW-5755 - [Rust] [Parquet] Add derived clone for Type
* ARROW-5826 - [Website] Blog post for 0.14.0 release announcement
* ARROW-653 - [Python / C++] Add debugging function to print an array's buffer contents in hexadecimal
* ARROW-840 - [Python] Provide Python API for creating user-defined data types that can survive Arrow IPC
* ARROW-973 - [Website] Add FAQ page about project

## Sub-task

* ARROW-2102 - [C++] Implement take kernel functions - primitive value type
* ARROW-2103 - [C++] Implement take kernel functions - string/binary value type
* ARROW-2104 - [C++] Implement take kernel functions - nested array value type
* ARROW-2105 - [C++] Implement take kernel functions - properly handle special indices
* ARROW-4121 - [C++] Refactor memory allocation from InvertKernel
* ARROW-4971 - [Go] DataType equality
* ARROW-4972 - [Go] Array equality
* ARROW-4973 - [Go] Slice Array equality
* ARROW-4974 - [Go] Array approx equality
* ARROW-5108 - [Go] implement reading primitive arrays from Arrow file
* ARROW-5109 - [Go] implement reading binary/string arrays from Arrow file
* ARROW-5110 - [Go] implement reading struct arrays from Arrow file
* ARROW-5111 - [Go] implement reading list arrays from Arrow file
* ARROW-5112 - [Go] implement writing arrays to Arrow file
* ARROW-5127 - [Rust] [Parquet] Add page iterator
* ARROW-5172 - [Go] implement reading fixed-size binary arrays from Arrow file
* ARROW-5250 - [Java] remove javadoc suppression on methods.
* ARROW-5266 - [Go] implement read/write IPC for Float16
* ARROW-5392 - [C++][CI][MinGW] Disable static library build on AppVeyor
* ARROW-5467 - [Go] implement read/write IPC for Time32/Time64 arrays
* ARROW-5468 - [Go] implement read/write IPC for Timestamp arrays
* ARROW-5469 - [Go] implement read/write IPC for Date32/Date64 arrays
* ARROW-5591 - [Go] implement read/write IPC for Duration & Intervals
* ARROW-5621 - [Go] implement read/write IPC for Decimal128 arrays
* ARROW-5672 - [Java] Refactor redundant method modifier
* ARROW-5780 - [C++] Add benchmark for Decimal128 operations

## Task

* ARROW-2412 - [Integration] Add nested dictionary integration test
* ARROW-4086 - [Java] Add apis to debug alloc failures
* ARROW-4702 - [C++] Upgrade dependency versions
* ARROW-4719 - [C#] Implement ChunkedArray, Column and Table in C#
* ARROW-4904 - [C++] Move implementations in arrow/ipc/test-common.h into libarrow\_testing
* ARROW-4913 - [Java][Memory] Limit number of ledgers and arrowbufs
* ARROW-4956 - [C#] Allow ArrowBuffers to wrap external Memory in C#
* ARROW-4959 - [Gandiva][Crossbow] Builds broken
* ARROW-5056 - [Packaging] Adjust conda recipes to use ORC conda-forge package on unix systems
* ARROW-5164 - [Gandiva] [C++] Introduce 32bit hash functions
* ARROW-5226 - [Gandiva] support compare operators for decimal
* ARROW-5275 - [C++] Write generic filesystem tests
* ARROW-5313 - [Format] Comments on Field table are a bit confusing
* ARROW-5321 - [Gandiva][C++] add isnull and isnotnull for utf8 and binary types
* ARROW-5346 - [C++] Revert changes to qualify duration in vendored date code
* ARROW-5434 - [Java] Introduce wrappers for backward compatibility for ArrowBuf changes in ARROW-3191
* ARROW-5443 - [Gandiva][Crossbow] Turn parquet encryption off
* ARROW-5449 - [C++] Local filesystem implementation: investigate Windows UNC paths
* ARROW-5451 - [C++][Gandiva] Add round functions for decimals
* ARROW-5476 - [Java][Memory] Fix Netty ArrowBuf Slice
* ARROW-5485 - [Gandiva][Crossbow] OSx builds failing
* ARROW-5490 - [C++] Remove ARROW\_BOOST\_HEADER\_ONLY
* ARROW-5491 - [C++] Remove unecessary semicolons following MACRO definitions
* ARROW-5557 - [C++] Investigate performance of VisitBitsUnrolled on different platforms
* ARROW-5602 - [Java][Gandiva] Add test for decimal round functions
* ARROW-5637 - [Gandiva] [Java]Complete IN Expression
* ARROW-5650 - [Python] Update manylinux dependency versions
* ARROW-5661 - Support hash functions for decimal in Gandiva
* ARROW-5696 - [Gandiva] [C++] Introduce castVarcharVarchar
* ARROW-5701 - [C++][Gandiva] Build expressions only for the required selection vector types
* ARROW-5704 - [C++] Stop using ARROW\_TEMPLATE\_EXPORT for SparseTensorImpl class
* ARROW-5818 - [Java][Gandiva] support varlen output vectors
* PARQUET-1603 - [C++] rename parquet::LogicalType to parquet::ConvertedType

## Test

* ARROW-4523 - [JS] Add row proxy generation benchmark
* ARROW-4725 - [C++] Dictionary tests disabled under MinGW builds
* ARROW-5194 - [C++][Plasma] TEST(PlasmaSerialization, GetReply) is failing
* ARROW-5371 - [Release] Add tests for dev/release/00-prepare.sh
* ARROW-5397 - Test Flight TLS support
* ARROW-5479 - [Rust] [DataFusion] Use ARROW\_TEST\_DATA instead of relative path for testing
* ARROW-5493 - [Integration/Go] add Go support for IPC integration tests
* ARROW-5623 - [CI][GLib] Failed on macOS
* ARROW-5769 - [Java] org.apache.arrow.flight.TestTls is failed via dev/release/00-prepare.sh

## Wish

* ARROW-5102 - [C++] Reduce header dependencies
* ARROW-5145 - [C++] Release mode lacks convenience input validation
* ARROW-5190 - [R] Discussion: tibble dependency in R package
* ARROW-5401 - [CI] [C++] Print ccache statistics on Travis-CI

# Apache Arrow 0.13.0 (28 March 2019)

## Bug

* ARROW-2392 - [Python] pyarrow RecordBatchStreamWriter allows writing batches with different schemas
* ARROW-295 - Create DOAP File
* ARROW-3086 - [GLib] GISCAN fails due to conda-shipped openblas
* ARROW-3096 - [Python] Update Python source build instructions given Anaconda/conda-forge toolchain migration
* ARROW-3133 - [C++] Logical boolean kernels in kernels/boolean.cc cannot write into preallocated memory
* ARROW-3208 - [C++] Segmentation fault when casting dictionary to numeric with nullptr valid\_bitmap
* ARROW-3564 - [Python] writing version 2.0 parquet format with dictionary encoding enabled
* ARROW-3578 - [Release] Address spurious Apache RAT failures in source release script
* ARROW-3593 - [R] CI builds failing due to GitHub API rate limits
* ARROW-3606 - [Python] flake8 fails on Crossbow
* ARROW-3669 - [Python] Convert big-endian numbers or raise error in pyarrow.array
* ARROW-3843 - [Python] Writing Parquet file from empty table created with Table.from\_pandas(..., preserve\_index=False) fails
* ARROW-3923 - [Java] JDBC-to-Arrow Conversion: Unnecessary Calendar Requirement
* ARROW-4081 - [Go] Sum methods on Mac OS X panic when the array is empty
* ARROW-4104 - [Java] race in AllocationManager during release
* ARROW-4117 - [Python] "asv dev" command fails with latest revision
* ARROW-4181 - [Python] TestConvertStructTypes.test\_from\_numpy\_large failing
* ARROW-4192 - "./dev/run\_docker\_compose.sh" is out of date
* ARROW-4213 - [Flight] C++ and Java implementations are incompatible
* ARROW-4244 - Clarify language around padding/alignment
* ARROW-4250 - [C++][Gandiva] Use approximate comparisons for floating point numbers in gandiva-projector-test
* ARROW-4252 - [C++] Status error context strings missing lines of code
* ARROW-4253 - [GLib] Cannot use non-system Boost specified with $BOOST\_ROOT
* ARROW-4254 - [C++] Gandiva tests fail to compile with Boost in Ubuntu 14.04 apt
* ARROW-4255 - [C++] Schema::GetFieldIndex is not thread-safe
* ARROW-4261 - [C++] CMake paths for IPC, Flight, Thrift, and Plasma don't support using Arrow as a subproject
* ARROW-4264 - [C++] Document why DCHECKs are used in kernels
* ARROW-4267 - [Python/C++][Parquet] Segfault when reading rowgroups with duplicated columns
* ARROW-4274 - [Gandiva] static jni library broken after decimal changes
* ARROW-4275 - [C++] gandiva-decimal\_single\_test extremely slow
* ARROW-4280 - [C++][Documentation] It looks like flex and bison are required for parquet
* ARROW-4282 - [Rust] builder benchmark is broken
* ARROW-4284 - [C#] File / Stream serialization fails due to type mismatch / missing footer
* ARROW-4295 - [Plasma] Incorrect log message when evicting objects
* ARROW-4296 - [Plasma] Starting Plasma store with use\_one\_memory\_mapped\_file enabled crashes due to improper memory alignment
* ARROW-4312 - [C++] Lint doesn't work anymore ("[Errno 24] Too many open files")
* ARROW-4319 - plasma/store.h pulls ins flatbuffer dependency
* ARROW-4322 - [CI] docker nightlies fails after conda-forge compiler migration
* ARROW-4323 - [Packaging] Fix failing OSX clang conda forge builds
* ARROW-4326 - [C++] Development instructions in python/development.rst will not work for many Linux distros with new conda-forge toolchain
* ARROW-4327 - [Python] Add requirements-build.txt file to simplify setting up Python build environment
* ARROW-4328 - Make R build compatible with DARROW\_TENSORFLOW=ON
* ARROW-4329 - Python should include the parquet headers
* ARROW-4342 - [Gandiva][Java] spurious failures in projector cache test
* ARROW-4347 - [Python] Run Python Travis CI unit tests on Linux when Java codebase changed
* ARROW-4349 - [C++] Build all benchmarks on Windows without failing
* ARROW-4351 - [C++] Fail to build with static parquet
* ARROW-4355 - [C++] test-util functions are no longer part of libarrow
* ARROW-4360 - [C++] Query homebrew for Thrift
* ARROW-4364 - [C++] Fix -weverything -wextra compilation errors
* ARROW-4366 - [Docs] Change extension from format/README.md to format/README.rst
* ARROW-4367 - [C++] StringDictionaryBuilder segfaults on Finish with only null entries
* ARROW-4368 - Bintray repository signature verification fails
* ARROW-4370 - [Python] Table to pandas conversion fails for list of bool
* ARROW-4374 - [C++] DictionaryBuilder does not correctly report length and null\_count
* ARROW-4381 - [Docker] docker-compose build lint fails
* ARROW-4385 - [Python] default\_version of a release should not include SNAPSHOT
* ARROW-4389 - [R] Installing clang-tools in CI is failing on trusty
* ARROW-4395 - ts-node throws type error running \`bin/arrow2csv.js\`
* ARROW-4400 - [CI] install of clang tools failing
* ARROW-4403 - [Rust] CI fails due to formatting errors
* ARROW-4404 - [CI] AppVeyor toolchain build does not build anything
* ARROW-4407 - [C++] ExternalProject\_Add does not capture CC/CXX correctly
* ARROW-4410 - [C++] Fix InvertKernel edge cases
* ARROW-4413 - [Python] pyarrow.hdfs.connect() failing
* ARROW-4414 - [C++] Stop using cmake COMMAND\_EXPAND\_LISTS because it breaks package builds for older distros
* ARROW-4417 - [C++] Doc build broken
* ARROW-4420 - [INTEGRATION] Make spark integration test pass and test against spark's master branch
* ARROW-4421 - [Flight][C++] Handle large Flight data messages
* ARROW-4434 - [Python] Cannot create empty StructArray via pa.StructArray.from\_arrays
* ARROW-4440 - [C++] Fix flatbuffers build using msvc
* ARROW-4457 - [Python] Cannot create Decimal128 array using integers
* ARROW-4469 - [Python][C++] CI Failing for Python 2.7 and 3.6 with valgrind
* ARROW-4471 - [C++] Pass AR and RANLIB to all external projects
* ARROW-4474 - [Flight] FlightInfo should use signed integer types for payload size
* ARROW-4496 - [CI] CI failing for python Xcode 7.3
* ARROW-4498 - [Plasma] Plasma fails building with CUDA enabled
* ARROW-4500 - [C++] librt and pthread hacks can cause linking problems
* ARROW-4501 - [C++] Unique returns non-unique strings
* ARROW-4525 - [Rust] [Parquet] Convert ArrowError to ParquetError
* ARROW-4527 - [Packaging] Update linux packaging tasks to align with the LLVM 7 migration
* ARROW-4532 - [Java] varchar value buffer much larger than expected
* ARROW-4533 - [Python] Document how to run hypothesis tests
* ARROW-4535 - [C++] Fix MakeBuilder to preserve ListType's field name
* ARROW-4536 - Add data\_type argument in garrow\_list\_array\_new
* ARROW-4538 - [PYTHON] Remove index column from subschema in write\_to\_dataframe
* ARROW-4549 - [C++] Can't build benchmark code on CUDA enabled build
* ARROW-4550 - [JS] Fix AMD pattern
* ARROW-4559 - [Python] pyarrow can't read/write filenames with special characters
* ARROW-4563 - [Python] pa.decimal128 should validate inputs
* ARROW-4571 - [Format] Tensor.fbs file has multiple root\_type declarations
* ARROW-4573 - [Python] Add Flight unit tests
* ARROW-4576 - [Python] Benchmark failures
* ARROW-4577 - [C++] Interface link libraries declared on arrow\_shared target that are actually non-interface
* ARROW-4581 - [C++] gbenchmark\_ep is a dependency of unit tests when ARROW\_BUILD\_BENCHMARKS=ON
* ARROW-4582 - [C++/Python] Memory corruption on Pandas->Arrow conversion
* ARROW-4584 - [Python] Add built wheel to manylinux1 dockerignore.
* ARROW-4585 - [C++] Dependency of Flight C++ sources on generated protobuf is not respected
* ARROW-4587 - Flight C++ DoPut segfaults
* ARROW-4597 - [C++] Targets for system Google Mock shared library are missing
* ARROW-4601 - [Python] Master build is broken due to missing licence for .dockerignore
* ARROW-4608 - [C++] cmake script assumes that double-conversion installs static libs
* ARROW-4617 - [C++] Support double-conversion<3.1
* ARROW-4624 - [C++] Linker errors when building benchmarks
* ARROW-4629 - [Python] Pandas to arrow conversion slowed down by local imports
* ARROW-4639 - [CI] Crossbow build failing for Gandiva jars
* ARROW-4641 - [C++] Flight builds complain of -Wstrict-aliasing
* ARROW-4642 - [R] Change \`f\` to \`file\` in \`read\_parquet\_file()\`
* ARROW-4654 - [C++] Implicit Flight target dependencies cause compilation failure
* ARROW-4657 - [Release] gbenchmark should not be needed for verification
* ARROW-4658 - [C++] Shared gflags is also a run-time conda requirement
* ARROW-4659 - [CI] ubuntu/debian nightlies fail because of missing gandiva files
* ARROW-4660 - [C++] gflags fails to build due to CMake error
* ARROW-4664 - [C++] DCHECK macro conditions are evaluated in release builds
* ARROW-4669 - [Java] No Bounds checking on ArrowBuf.slice
* ARROW-4672 - [C++] clang-7 matrix entry is build using gcc
* ARROW-4680 - [CI] [Rust] Travis CI builds fail with latest Rust 1.34.0-nightly (2019-02-25)
* ARROW-4684 - [Python] CI failures in test\_cython.py
* ARROW-4687 - [Python] FlightServerBase.run should exit on Ctrl-C
* ARROW-4688 - [C++][Parquet] 16MB limit on (nested) column chunk prevents tuning row\_group\_size
* ARROW-4696 - Verify release script is over optimist with CUDA detection
* ARROW-4699 - [C++] json parser should not rely on null terminated buffers
* ARROW-4710 - [C++][R] New linting script skip files with "cpp" extension
* ARROW-4712 - [C++][CI] Clang7 Valgrind complains when not move shared\_ptr
* ARROW-4721 - [Rust] [DataFusion] Propagate schema in filter
* ARROW-4728 - [JS] Failing test Table#assign with a zero-length Null column round-trips through serialization
* ARROW-4737 - [C#] tests are not running in CI
* ARROW-4744 - [CI][C++] Mingw32 builds failing
* ARROW-4750 - [C++] RapidJSON triggers Wclass-memaccess on GCC 8+
* ARROW-4760 - [C++] protobuf 3.7 defines EXPECT\_OK that clashes with Arrow's macro
* ARROW-4766 - [C++] Casting empty boolean array causes segfault
* ARROW-4767 - [C#] ArrowStreamReader crashes while reading the end of a stream
* ARROW-4774 - [C++][Parquet] Call Table::Validate when writing a table
* ARROW-4775 - [Website] Site navbar cannot be expanded
* ARROW-4783 - [C++][CI] Mingw32 builds sometimes timeout
* ARROW-4796 - [Flight][Python] segfault in simple server implementation
* ARROW-4802 - [Python] Hadoop classpath discovery broken HADOOP\_HOME is a symlink
* ARROW-4807 - [Rust] Fix csv\_writer benchmark
* ARROW-4811 - [C++] An incorrect dependency leads "ninja" to re-evaluate steps unnecessarily on subsequent calls
* ARROW-4820 - [Python] hadoop class path derived not correct
* ARROW-4822 - [C++/Python] pyarrow.Table.equals segmentation fault on None
* ARROW-4828 - [Python] manylinux1 docker-compose context should be python/manylinux1
* ARROW-4850 - [CI] Integration test failures do not fail the Travis CI build
* ARROW-4853 - [Rust] Array slice doesn't work on ListArray and StructArray
* ARROW-4857 - [C++/Python/CI] docker-compose in manylinux1 crossbow jobs too old
* ARROW-4866 - [C++] zstd ExternalProject failing on Windows
* ARROW-4867 - [Python] Table.from\_pandas() column order not respected
* ARROW-4869 - [C++] Use of gmock fails in compute/kernels/util-internal-test.cc
* ARROW-4870 - [Ruby] gemspec has wrong msys2 dependency listed
* ARROW-4871 - [Flight][Java] Handle large Flight messages
* ARROW-4872 - [Python] Keep backward compatibility for ParquetDatasetPiece
* ARROW-4881 - [Python] bundle\_zlib CMake function still uses ARROW\_BUILD\_TOOLCHAIN
* ARROW-4900 - mingw-w64 < 5 does not have \_\_cpuidex
* ARROW-4903 - [C++] Building tests using only static libs not possible
* ARROW-4906 - [Format] Fix document to describe that SparseMatrixIndexCSR assumes indptr is sorted for each row
* ARROW-4918 - [C++] Add cmake-format to pre-commit
* ARROW-4928 - [Python] Hypothesis test failures
* ARROW-4931 - [C++] CMake fails on gRPC ExternalProject
* ARROW-4948 - [JS] Nightly test failing with "Cannot assign to read only property"
* ARROW-4950 - [C++] Thirdparty CMake error get\_target\_property() called with non-existent target LZ4::lz4
* ARROW-4952 - [C++] Equals / ApproxEquals behaviour undefined on FP NaNs
* ARROW-4954 - [Python] test failure with Flight enabled
* ARROW-4958 - [C++] Purely static linking broken
* ARROW-4961 - [C++][Python] Add GTest\_SOURCE=BUNDLED to relevant build docs that use conda-forge toolchain
* ARROW-4962 - [C++] Warning level to CHECKIN can't compile on modern GCC
* ARROW-4976 - [JS] RecordBatchReader should reset its Node/DOM streams
* ARROW-4984 - [Flight][C++] Flight server segfaults when port is in use
* ARROW-4986 - [CI] Travis fails to install llvm@7
* ARROW-4989 - [C++] Builds fails to find Ubuntu-packaged re2 library
* ARROW-4991 - [CI] Bump travis node version to 11.12
* ARROW-4997 - [C#] ArrowStreamReader doesn't consume whole stream and doesn't implement sync read
* ARROW-5009 - [C++] Cleanup using to std::\* in files
* ARROW-5010 - [Release] Fix release script with llvm-7
* ARROW-5012 - [C++] "testing" headers not installed
* ARROW-5023 - [Release] Default value syntax in shell is wrong
* ARROW-5024 - [Release] crossbow.py --arrow-version causes missing variable error
* ARROW-5025 - [Python][Packaging] wheel for Windows are broken
* ARROW-5026 - [Python][Packaging] conda package on non Windows is broken
* ARROW-5029 - [C++] Compilation warnings in release mode
* ARROW-5031 - [Dev] Release verification script does not run CUDA tests in Python
* ARROW-5042 - [Release] Wrong ARROW\_DEPENDENCY\_SOURCE in verification script
* ARROW-5043 - [Release][Ruby] red-arrow dependency can't be resolve in verification script
* ARROW-5044 - [Release][Rust] Format error in verification script
* ARROW-5046 - [Release][C++] Plasma test is fragile in verification script
* ARROW-5047 - [Release] Always set up parquet-testing in verification script
* ARROW-5048 - [Release][Rust] arrow-testing is missing in verification script
* ARROW-5050 - [C++] cares\_ep should build before grpc\_ep
* ARROW-5087 - [Debian] APT repository no longer contains libarrow-dev
* ARROW-5658 - [JAVA] Provide ability to resync VectorSchemaRoot if types change
* PARQUET-1482 - [C++] Unable to read data from parquet file generated with parquetjs
* PARQUET-1494 - [C++] Can't access parquet statistics on binary columns

## Improvement

* ARROW-1425 - [Python] Document semantic differences between Spark timestamps and Arrow timestamps
* ARROW-1639 - [Python] More efficient serialization for RangeIndex in serialize\_pandas
* ARROW-1807 - [JAVA] Reduce Heap Usage (Phase 3): consolidate buffers
* ARROW-1896 - [C++] Do not allocate memory for primitive outputs in CastKernel::Call implementation
* ARROW-2015 - [Java] Use Java Time and Date APIs instead of JodaTime
* ARROW-2022 - [Format] Add custom metadata field specific to a RecordBatch message
* ARROW-2112 - [C++] Enable cpplint to be run on Windows
* ARROW-2627 - [Python] Add option (or some equivalent) to toggle memory mapping functionality when using parquet.ParquetFile or other read entry points
* ARROW-3149 - [C++] Use gRPC (when it exists) from conda-forge for CI builds
* ARROW-3239 - [C++] Improve random data generation functions
* ARROW-3292 - [C++] Test Flight RPC in Travis CI
* ARROW-3297 - [Python] Python bindings for Flight C++ client
* ARROW-331 - [Python] Timeline for dropping Python 2.7 support
* ARROW-3361 - [R] Run cpp/build-support/cpplint.py on C++ source files
* ARROW-3364 - [Doc] Document docker compose setup
* ARROW-3367 - [INTEGRATION] Port Spark integration test to the docker-compose setup
* ARROW-3422 - [C++] Add "toolchain" target to ensure that all required toolchain libraries are built
* ARROW-3435 - [C++] Add option to use dynamic linking with re2
* ARROW-3532 - [Python] Schema, StructType, StructArray field retrieval by name should raise warning or exception for multiple matches
* ARROW-3550 - [C++] Use kUnknownNullCount in NumericArray constructor
* ARROW-3554 - [C++] Reverse traits for C++
* ARROW-3619 - [R] Expose global thread pool optins
* ARROW-3653 - [Python/C++] Support data copying between different GPU devices
* ARROW-3735 - [Python] Proper error handling in \_ensure\_type
* ARROW-3769 - [C++] Support reading non-dictionary encoded binary Parquet columns directly as DictionaryArray
* ARROW-3770 - [C++] Validate or add option to validate arrow::Table schema in parquet::arrow::FileWriter::WriteTable
* ARROW-3824 - [R] Document developer workflow for building project, running unit tests in r/README.md
* ARROW-3838 - [Rust] Implement CSV Writer
* ARROW-3846 - [Gandiva] Build on Windows
* ARROW-3882 - [Rust] PrimitiveArray<T> should support cast operations
* ARROW-3903 - [Python] Random array generator for Arrow conversion and Parquet testing
* ARROW-3926 - [Python] Add Gandiva bindings to Python wheels
* ARROW-3951 - [Go] implement a CSV writer
* ARROW-3954 - [Rust] Add Slice to Array and ArrayData
* ARROW-3965 - [Java] JDBC-to-Arrow Conversion: Configuration Object
* ARROW-3966 - [Java] JDBC-to-Arrow Conversion: JDBC Metadata in Schema Fields
* ARROW-3972 - [C++] Update to LLVM and Clang bits to 7.0
* ARROW-3985 - [C++] Pass -C option when compiling with ccache to avoid some warnings
* ARROW-4012 - [Documentation][C++] Document how to install Apache Arrow on MSYS2
* ARROW-4014 - [C++] Fix "LIBCMT" warnings on MSVC
* ARROW-4024 - [Python] Cython compilation error on cython==0.27.3
* ARROW-4031 - [C++] Refactor ArrayBuilder bitmap logic into TypedBufferBuilder<bool>
* ARROW-4056 - [C++] Upgrade to boost-cpp 1.69.0 again
* ARROW-4094 - [Python] Store RangeIndex in Parquet files as metadata rather than a physical data column
* ARROW-4110 - [C++] Do not generate distinct cast kernels when input and output type are the same
* ARROW-4123 - [C++] Improve linting workflow and documentation for Windows-based developers
* ARROW-4124 - [C++] Abstract aggregation kernel API
* ARROW-4142 - [Java] JDBC-to-Arrow: JDBC Arrays
* ARROW-4165 - [C++] Port cpp/apidoc/Windows.md and other files to Sphinx / rst
* ARROW-4180 - [Java] Reduce verbose logging of ArrowBuf creation events?
* ARROW-4196 - [Rust] Add explicit SIMD vectorization for arithmetic ops in "array\_ops"
* ARROW-4198 - [Gandiva] Add support to cast timestamp
* ARROW-4212 - [Python] [CUDA] Creating a CUDA buffer from Numba device array should be easier
* ARROW-4230 - [C++] Enable building flight against system gRPC
* ARROW-4234 - [C++] Add memory bandwidth benchmarks to arrow/util/machine-benchmark.cc
* ARROW-4235 - [GLib] Use "column\_builder" in GArrowRecordBatchBuilder
* ARROW-4236 - [JAVA] Distinct plasma client create exceptions
* ARROW-4245 - [Rust] Add Rustdoc header to each source file
* ARROW-4247 - [Packaging] Update verify script for 0.12.0
* ARROW-4251 - [C++] Add option to use vendored Boost in verify-release-candidate.sh
* ARROW-4263 - [Rust] Donate DataFusion
* ARROW-4268 - [C++] Add C primitive to Arrow:Type compile time in TypeTraits
* ARROW-4277 - [C++] Add gmock to toolchain
* ARROW-4285 - [Python] Use proper builder interface for serialization
* ARROW-4297 - [C++] Fix build for 32-bit MSYS2
* ARROW-4299 - [Ruby] Depend on the same version as Red Arrow
* ARROW-4305 - [Rust] Fix parquet version number in README
* ARROW-4307 - [C++] FIx doxygen warnings, include doxygen warning checks in CI linting
* ARROW-4310 - [Website] Update install document for 0.12.0
* ARROW-4315 - [Website] Home page of https://arrow.apache.org/ does not mention Go or Rust
* ARROW-4330 - [C++] Use FindThreads.cmake to handle -pthread compiler/link options
* ARROW-4332 - [Website] Instructions and scripts for publishing web site appear to be incorrect
* ARROW-4335 - [C++] Better document sparse tensor support
* ARROW-4336 - [C++] Default BUILD\_WARNING\_LEVEL to CHECKIN
* ARROW-4339 - [C++] rewrite cpp/README shorter, with a separate contribution guide
* ARROW-4340 - [C++] Update IWYU version in the \`lint\` dockerfile
* ARROW-4341 - [C++] Use TypedBufferBuilder<bool> in BooleanBuilder
* ARROW-4344 - [Java] Further cleanup maven output
* ARROW-4345 - [C++] Add Apache 2.0 license file to the Parquet-testing repository
* ARROW-4346 - [C++] Fix compiler warnings with gcc 8.2.0
* ARROW-4353 - [CI] Add jobs for 32-bit and 64-bit MinGW
* ARROW-4361 - [Website] Update commiters list
* ARROW-4362 - [Java] Test OpenJDK 11 in CI
* ARROW-4363 - [C++] Add CMake format checks
* ARROW-4372 - [C++] Embed precompiled bitcode in the gandiva library
* ARROW-4373 - [Packaging] Travis fails to deploy conda packages on OSX
* ARROW-4375 - [CI] Sphinx dependencies were removed from docs conda environment
* ARROW-4376 - [Rust] Implement from\_buf\_reader for csv::Reader
* ARROW-4377 - [Rust] Implement std::fmt::Debug for all PrimitiveArrays
* ARROW-4379 - Register pyarrow serializers for collections.Counter and collections.deque.
* ARROW-4383 - [C++] Use the CMake's standard find features
* ARROW-4388 - [Go] add DimNames() method to tensor Interface?
* ARROW-4393 - [Rust] coding style: apply 90 characters per line limit
* ARROW-4396 - Update Typedoc to support TypeScript 3.2
* ARROW-4399 - [C++] Remove usage of "extern template class" from NumericArray<T>
* ARROW-4401 - [Python] Alpine dockerfile fails to build because pandas requires numpy as build dependency
* ARROW-4406 - Ignore "\*\_$folder$" files on S3
* ARROW-4422 - [Plasma] Enforce memory limit in plasma, rather than relying on dlmalloc\_set\_footprint\_limit
* ARROW-4423 - [C++] Update version of vendored gtest to 1.8.1
* ARROW-4424 - [Python] Manylinux CI builds failing
* ARROW-4430 - [C++] add unit test for currently unused append method
* ARROW-4431 - [C++] Build gRPC as ExternalProject without allowing it to build its vendored dependencies
* ARROW-4436 - [Documentation] Clarify instructions for building documentation
* ARROW-4442 - [JS] Overly broad type annotation for Chunked typeId leading to type mismatches in generated typing
* ARROW-4444 - [Testing] Add DataFusion test files to arrow-testing repo
* ARROW-4445 - [C++][Gandiva] Run Gandiva-LLVM tests in Appveyor
* ARROW-4446 - [Python] Run Gandiva tests on Windows and Appveyor
* ARROW-4448 - [JAVA][Flight] Flaky Flight java test
* ARROW-4454 - [C++] fix unused parameter warnings
* ARROW-4455 - [Plasma] g++ 8 reports class-memaccess warnings
* ARROW-4459 - [Testing] Add git submodule for arrow-testing data files
* ARROW-4460 - [Website] Write blog post to announce DataFusion donation
* ARROW-4462 - [C++] Upgrade LZ4 v1.7.5 to v1.8.3 to compile with VS2017
* ARROW-4464 - [Rust] [DataFusion] Add support for LIMIT
* ARROW-4466 - [Rust] [DataFusion] Add support for Parquet data sources
* ARROW-4468 - [Rust] Implement BitAnd/BitOr for &Buffer (with SIMD)
* ARROW-4475 - [Python] Serializing objects that contain themselves
* ARROW-4476 - [Rust] [DataFusion] Post donation clean up tasks
* ARROW-4481 - [Website] Instructions for publishing web site are missing a step
* ARROW-4483 - [Website] Fix broken link (author) in DataFusion blog post
* ARROW-4485 - [CI] Determine maintenance approach to pinned conda-forge binutils package
* ARROW-4486 - [Python][CUDA] pyarrow.cuda.Context.foreign\_buffer should have a \`base=None\` argument
* ARROW-4488 - [Rust] From AsRef<[u8]> for Buffer does not ensure correct padding
* ARROW-4489 - [Rust] PrimitiveArray.value\_slice performs bounds checking when it should not
* ARROW-4490 - [Rust] Add explicit SIMD vectorization for boolean ops in "array\_ops"
* ARROW-4491 - [Python] Remove usage of std::to\_string and std::stoi
* ARROW-4499 - [Python][CI] Upgrade to latest flake8 3.7.5 in travis\_lint.sh
* ARROW-4502 - [C#] Add support for zero-copy reads
* ARROW-4513 - [Rust] Implement BitAnd/BitOr for &Bitmap
* ARROW-4528 - [C++] Update lint docker container to LLVM-7
* ARROW-4529 - [C++] Add test coverage for BitUtils::RoundDown
* ARROW-4531 - [C++] Handling of non-aligned slices in Sum kernel
* ARROW-4537 - [CI] Suppress shell warning on travis-ci
* ARROW-4547 - [Python][Documentation] Update python/development.rst with instructions for CUDA-enabled builds
* ARROW-4558 - [C++][Flight] Avoid undefined behavior with gRPC memory optimizations
* ARROW-4560 - [R] array() needs to take single input, not ...
* ARROW-4562 - [C++][Flight] Create outgoing composite grpc::ByteBuffer instead of allocating contiguous slice and copying IpcPayload into it
* ARROW-4565 - [R] Reading records with all non-null decimals SEGFAULTs
* ARROW-4568 - [C++] Add version macros to headers
* ARROW-4572 - [C++] Remove memory zeroing from PrimitiveAllocatingUnaryKernel
* ARROW-4583 - [Plasma] There are bugs reported by code scan tool
* ARROW-4586 - [Rust] Remove arrow/mod.rs as it is not needed
* ARROW-4590 - [Rust] Add explicit SIMD vectorization for comparison ops in "array\_ops"
* ARROW-4592 - [GLib] Stop configure immediately when GLib isn't available
* ARROW-4593 - [Ruby] Arrow::Array#[out\_of\_range] returns nil
* ARROW-4594 - [Ruby] Arrow::StructArray#[] returns Arrow::Struct instead of Arrow::Array
* ARROW-4595 - [Rust] [DataFusion] Implement DataFrame style API
* ARROW-4598 - [CI] Remove needless LLVM\_DIR for macOS
* ARROW-4602 - [Rust][ [DataFusion] Integrate query optimizer with ExecutionContext
* ARROW-4605 - [Rust] Move filter and limit code from DataFusion into compute module
* ARROW-4609 - [C++] Use google benchmark from toolchain
* ARROW-4610 - [Plasma] Avoid JNI from crashing
* ARROW-4611 - [C++] Rework CMake third-party logic
* ARROW-4612 - [Python] Use cython from PyPI for windows wheels build
* ARROW-4613 - [C++] Alpine build failing as libgtestd.so is not found
* ARROW-4614 - [C++/CI] Activate flight build in ci/docker\_build\_cpp.sh
* ARROW-4615 - [C++] Add checked\_pointer\_cast
* ARROW-4616 - [C++] Log message in BuildUtils as STATUS
* ARROW-4618 - [Docker] Makefile to build dependent docker images
* ARROW-4623 - [R] update Rcpp dependency
* ARROW-4628 - [Rust] [DataFusion] Implement type coercion query optimizer rule
* ARROW-4634 - [Rust] [Parquet] Reorganize test\_common mod to allow more test util codes.
* ARROW-4637 - [Python] Avoid importing Pandas unless necessary
* ARROW-4638 - [R] install instructions using brew
* ARROW-4640 - [Python] Add docker-compose configuration to build and test the project without pandas installed
* ARROW-4643 - [C++] Add compiler diagnostic color when using Ninja
* ARROW-4644 - [C++/Docker] Build Gandiva in the docker containers
* ARROW-4645 - [C++/Packaging] Ship Gandiva with OSX and Windows wheels
* ARROW-4646 - [C++/Packaging] Ship gandiva with the conda-forge packages
* ARROW-4655 - [Packaging] Parallelize binary upload
* ARROW-4667 - [C++] Suppress unused function warnings with MinGW
* ARROW-4670 - [Rust] compute::sum performance issue
* ARROW-4673 - [C++] Implement AssertDatumEquals
* ARROW-4676 - [C++] Add support for debug build with MinGW
* ARROW-4678 - [Rust] Minimize unstable feature usage
* ARROW-4679 - [Rust] [DataFusion] Implement in-memory DataSource
* ARROW-4681 - [Rust] [DataFusion] Implement parallel query execution using threads
* ARROW-4686 - Only accept 'y' or 'n' in merge\_arrow\_pr.py prompts
* ARROW-4689 - [Go] add support for WASM
* ARROW-4690 - [Python] Building TensorFlow compatible wheels for Arrow
* ARROW-4697 - [C++] Add URI parsing facility
* ARROW-4705 - [Rust] CSV reader should show line number and error message when failing to parse a line
* ARROW-4718 - Add ArrowStreamWriter/Reader ctors that leave open the underlying Stream
* ARROW-4727 - [Rust] Implement ability to check if two schemas are the same
* ARROW-4730 - [C++] Add docker-compose entry for testing Fedora build with system packages
* ARROW-4731 - [C++] Add docker-compose entry for testing Ubuntu Xenial build with system packages
* ARROW-4732 - [C++] Add docker-compose entry for testing Debian Testing build with system packages
* ARROW-4733 - [C++] Add CI entry that builds without the conda-forge toolchain but with system packages
* ARROW-4734 - [Go] Add option to write a header for CSV writer
* ARROW-4735 - [Go] Benchmark strconv.Format vs. fmt.Sprintf for CSV writer
* ARROW-4739 - [Rust] [DataFusion] It should be possible to share a logical plan between threads
* ARROW-4745 - [C++][Documentation] Document process for replicating static\_crt builds on windows
* ARROW-4749 - [Rust] RecordBatch::new() should return result instead of panicking
* ARROW-4754 - [CI][Java] Flaky TestAuth Flight test
* ARROW-4769 - [Rust] Improve array limit function where max records > len
* ARROW-4776 - [C++] DictionaryBuilder should support bootstrapping from an existing dict type
* ARROW-4777 - [C++/Python] manylinux1: Update lz4 to 1.8.3
* ARROW-4789 - [C++] Deprecate and and later remove arrow::io::ReadableFileInterface
* ARROW-4791 - Unused dependencies in arrow and datafusion
* ARROW-4794 - [Python] Make pandas an optional test dependency
* ARROW-4797 - [Plasma] Avoid store crash if not enough memory is available
* ARROW-4801 - [GLib] Suppress pkgconfig.generate() warnings
* ARROW-4817 - [Rust] [DataFusion] Small re-org of modules
* ARROW-4826 - [Go] export Flush method for CSV writer
* ARROW-4831 - [C++] CMAKE\_AR is not passed to ZSTD thirdparty dependency
* ARROW-4833 - [Release] Document how to update the brew formula in the release management guide
* ARROW-4834 - [R] Feature flag to disable parquet
* ARROW-4837 - [C++] Support c++filt on a custom path in the run-test.sh script
* ARROW-4839 - [C#] Add NuGet support
* ARROW-4846 - [Java] Update Jackson to 2.9.8
* ARROW-4849 - [C++] Add docker-compose entry for testing Ubuntu Bionic build with system packages
* ARROW-4854 - [Rust] Use Array Slice for limit kernel
* ARROW-4855 - [Packaging] Generate default package version based on cpp tags in crossbow.py
* ARROW-4858 - [Flight][Python] Enable custom FlightDataStream in Python
* ARROW-4865 - [Rust] Support casting lists and primitives to lists
* ARROW-4873 - [C++] Clarify documentation about how to use external ARROW\_PACKAGE\_PREFIX while also using CONDA dependency resolution
* ARROW-4878 - [C++] ARROW\_DEPENDENCY\_SOURCE=CONDA does not work properly with MSVC
* ARROW-4889 - [C++] Add STATUS messages for Protobuf in CMake
* ARROW-4891 - [C++] ZLIB include directories not added
* ARROW-4893 - [C++] conda packages should use $PREFIX inside of conda-build
* ARROW-4894 - [Rust] [DataFusion] Remove all uses of panic! from aggregate.rs
* ARROW-4896 - [Rust] [DataFusion] Remove all uses of panic! from tests
* ARROW-4897 - [Rust] [DataFusion] Improve Rustdoc
* ARROW-4898 - [C++] Old versions of FindProtobuf.cmake use ALL-CAPS for variables
* ARROW-4899 - [Rust] [DataFusion] Remove all uses of panic! from expression.rs
* ARROW-4905 - [C++][Plasma] Remove dlmalloc from client library
* ARROW-4908 - [Rust] [DataFusion] Add support for parquet date/time in int32/64 encoding
* ARROW-4910 - [Rust] [DataFusion] Remove all uses of unimplemented!
* ARROW-4922 - [Packaging] Use system libraris for .deb and .rpm
* ARROW-4926 - [Rust] [DataFusion] Update README for 0.13.0 release
* ARROW-4933 - [R] Autodetect Parquet support using pkg-config
* ARROW-4937 - [R] Clean pkg-config related logic
* ARROW-4939 - [Python] Add wrapper for "sum" kernel
* ARROW-4940 - [Rust] Enhance documentation for datafusion
* ARROW-4944 - [C++] Raise minimal required thrift-cpp to 0.11 in conda environment
* ARROW-4946 - [C++] Support detection of flatbuffers without FlatbuffersConfig.cmake
* ARROW-4947 - [Flight][C++/Python] Remove redundant schema parameter in DoGet
* ARROW-4964 - [Ruby] Add closed check if available on auto close
* ARROW-4969 - [C++] Set RPATH in correct order for test executables on OSX
* ARROW-4977 - [Ruby] Add support for building on Windows
* ARROW-4978 - [Ruby] Fix wrong internal variable name for table data
* ARROW-4979 - [GLib] Add missing lock to garrow::GIOInputStream
* ARROW-4980 - [GLib] Use GInputStream as the parent of GArrowInputStream
* ARROW-4983 - [Plasma] Unmap memory when the client is destroyed
* ARROW-4995 - [R] Make sure winbuilder tests pass for package
* ARROW-4996 - [Plasma] There are many log files in /tmp
* ARROW-5003 - [R] remove dependency on withr
* ARROW-5006 - [R] parquet.cpp does not include enough Rcpp
* ARROW-5011 - [Release] Add support in the source release script for custom hash
* ARROW-5013 - [Rust] [DataFusion] Refactor runtime expression support
* ARROW-5014 - [Java] Fix typos in Flight module
* ARROW-5018 - [Release] Include JavaScript implementation
* ARROW-5032 - [C++] Headers in vendored/datetime directory aren't installed
* ARROW-5075 - [Release] Add 0.13.0 release note
* ARROW-5084 - [Website] Blog post / release announcement for 0.13.0
* ARROW-572 - [C++] Apply visitor pattern in IPC metadata
* PARQUET-1508 - [C++] Enable reading from ByteArray and FixedLenByteArray decoders directly into arrow::BinaryBuilder or arrow::BinaryDictionaryBuilder
* PARQUET-1519 - [C++] Remove use of "extern template class" from parquet/column\_reader.h
* PARQUET-1521 - [C++] Do not use "extern template class" with parquet::ColumnWriter
* PARQUET-1525 - [C++] remove dependency on getopt in parquet tools

## New Feature

* ARROW-1572 - [C++] Implement "value counts" kernels for tabulating value frequencies
* ARROW-3107 - [C++] arrow::PrettyPrint for Column instances
* ARROW-3121 - [C++]  Mean kernel aggregate
* ARROW-3123 - [C++] Incremental Count, Count Not Null aggregator
* ARROW-3135 - [C++] Add helper functions for validity bitmap propagation in kernel context
* ARROW-3162 - [Python] Enable Flight servers to be implemented in pure Python
* ARROW-3289 - [C++] Implement DoPut command for Flight on client and server side
* ARROW-3311 - [R] Functions for deserializing IPC components from arrow::Buffer or from IO interface
* ARROW-3631 - [C#] Add Appveyor build for C#
* ARROW-3761 - [R] Bindings for CompressedInputStream, CompressedOutputStream
* ARROW-3816 - [R] nrow.RecordBatch method
* ARROW-4262 - [Website] Blog post to give preview into using R and Arrow with Apache Spark
* ARROW-4265 - [C++] Automatic conversion between Table and std::vector<std::tuple<..>>
* ARROW-4287 - [C++] Ensure minimal bison version on OSX for Thrift
* ARROW-4289 - [C++] Forward AR and RANLIB to thirdparty builds
* ARROW-4290 - [C++/Gandiva] Support detecting correct LLVM version in Homebrew
* ARROW-4291 - [Dev] Support selecting features in release scripts
* ARROW-4294 - [Plasma] Add support for evicting objects to external store
* ARROW-4298 - [Java] Building Flight fails with OpenJDK 11
* ARROW-4300 - [C++] Restore apache-arrow Homebrew recipe and define process for maintaining and updating for releases
* ARROW-4313 - Define general benchmark database schema
* ARROW-4318 - [C++] Add Tensor::CountNonZero
* ARROW-4352 - [C++] Add support for system Google Test
* ARROW-4386 - [Rust] Implement Date and Time Arrays
* ARROW-4397 - [C++] dim\_names in Tensor and SparseTensor
* ARROW-4449 - [Rust] Convert File to T: Read + Seek for schema inference
* ARROW-4472 - [Website][Python] Blog post about Python string memory use improvements in 0.12
* ARROW-4506 - [Ruby] Add Arrow::RecordBatch#raw\_records
* ARROW-4632 - [Ruby] Add BigDecimal#to\_arrow
* ARROW-4662 - [Python] Add type\_codes property in UnionType
* ARROW-4671 - [C++] MakeBuilder doesn't support Type::DICTIONARY
* ARROW-4692 - [Format][Documentation] Add more details about "sidecar" to flight proto
* ARROW-47 - [C++] Consider adding a scalar type object model
* ARROW-4707 - [C++] move BitsetStack to bit-util.h
* ARROW-4740 - [Java] Upgrade to JUnit 5
* ARROW-4782 - [C++] Prototype scalar and array expression types for developing deferred operator algebra
* ARROW-4835 - [GLib] Add boolean operations
* ARROW-4859 - [GLib] Add garrow\_numeric\_array\_mean()
* ARROW-4862 - [GLib] Add GArrowCastOptions::allow-invalid-utf8 property
* ARROW-4882 - [GLib] Add "Sum" functions
* ARROW-4887 - [GLib] Add garrow\_array\_count()
* ARROW-4901 - [Go] Run tests in Appveyor
* ARROW-4915 - [GLib] Add support for arrow::NullBuilder
* ARROW-4924 - [Ruby] Add Decimal128#to\_s(scale=nil)
* ARROW-4929 - [GLib] Add garrow\_array\_count\_values()
* ARROW-4955 - [GLib] Add garrow\_file\_is\_closed()
* ARROW-4981 - [Ruby] Add support for CSV data encoding conversion
* ARROW-5041 - [Release][C++] use bundled gtest and gmock in verify-release-candidate.bat
* ARROW-549 - [C++] Add function to concatenate like-typed arrays
* ARROW-585 - [C++] Define public API for user-defined data types
* ARROW-694 - [C++] Build JSON "scanner" for reading record batches from line-delimited JSON files

## Sub-task

* ARROW-3596 - [Packaging] Build gRPC in conda-forge
* ARROW-4061 - [Rust] [Parquet] Implement "spaced" version for non-dictionary encoding/decoding
* ARROW-4461 - [C++] Expose bit-util methods for binary boolean operations that don't allocate
* ARROW-4540 - [Rust] Add basic JSON reader
* ARROW-4543 - [C#] Update Flat Buffers code to latest version
* ARROW-4556 - [Rust] Preserve order of JSON inferred schema
* ARROW-4599 - [C++] Add support for system GFlags
* ARROW-4743 - [Java] Fix documentation in arrow memory module
* ARROW-4772 - Provide new ORC adapter interface that allow user to specify row number
* ARROW-4892 - [Rust] [DataFusion] Move SQL parser and planner into sql package
* ARROW-4895 - [Rust] [DataFusion] Move error.rs to top level package

## Task

* ARROW-2409 - [Rust] Test for build warnings, remove current warnings
* ARROW-3434 - [Packaging] Add Apache ORC C++ library to conda-forge
* ARROW-3511 - [Gandiva] support input selection vectors for both projector and filter
* ARROW-4071 - [Rust] Add rustfmt as a pre-commit hook
* ARROW-4072 - [Rust] Set default value for PARQUET\_TEST\_DATA
* ARROW-4204 - [Gandiva] implement decimal subtract
* ARROW-4205 - [Gandiva] Implement decimal multiply
* ARROW-4206 - [Gandiva] Implement decimal divide
* ARROW-4271 - [Rust] Move Parquet specific info to Parquet Readme
* ARROW-4273 - [Release] Fix verification script to use cf201901 conda-forge label
* ARROW-4281 - [CI] Use Ubuntu Xenial (16.04) VMs on Travis-CI
* ARROW-4303 - [Gandiva/Python] Build LLVM with RTTI in manylinux1 container
* ARROW-4321 - [CI] Setup conda-forge channel globally in docker containers
* ARROW-4334 - [CI] Setup conda-forge channel globally in travis builds
* ARROW-4358 - [Gandiva][Crossbow] Trusty build broken
* ARROW-4408 - [CPP/Doc] Remove outdated Parquet documentation
* ARROW-4425 - Add link to 'Contributing' page in the top-level Arrow README
* ARROW-4435 - [C#] Add .sln file and minor .csproj fix ups
* ARROW-4518 - [JS] add jsdelivr to package.json
* ARROW-4539 - [Java]List vector child value count not set correctly
* ARROW-4619 - [R]: Fix the autobrew script
* ARROW-4620 - [C#] Add unit tests for "Types" in arrow/csharp
* ARROW-4693 - [CI] Build boost library with multi precision
* ARROW-4751 - [C++] Add pkg-config to conda\_env\_cpp.yml
* ARROW-4756 - [CI] document the procedure to update docker image for manylinux1 builds
* ARROW-4758 - [Flight] Build fails on Mac due to missing Schema\_generated.h
* ARROW-4778 - [C++/Python] manylinux1: Update Thrift to 0.12.0
* ARROW-4786 - [C++/Python] Support better parallelisation in manylinux1 base build
* ARROW-4790 - [Python/Packaging] Update manylinux docker image in crossbow task
* ARROW-4808 - [Java][Vector] Convenience methods for setting decimal vector
* ARROW-4907 - [CI] Add docker container to inspect docker context
* ARROW-4909 - [CI] Use hadolint to lint Dockerfiles
* ARROW-4932 - [GLib] Use G\_DECLARE\_DERIVABLE\_TYPE macro
* ARROW-4951 - [C++] Turn off cpp benchmarks in cpp docker images
* ARROW-4994 - [website] Update Details for ptgoetz

## Test

* ARROW-4320 - [C++] Add tests for non-contiguous tensors
* ARROW-4704 - [CI][GLib] Plasma test is flaky
* ARROW-4724 - [C++] Python not being built nor test under MinGW builds
* ARROW-4768 - [C++][CI] arrow-test-array sometimes gets stuck in MinGW build
* ARROW-4793 - [Ruby] Suppress unused variable warning
* ARROW-4813 - [Ruby] Add tests for #== and #!=
* ARROW-4942 - [Ruby] Remove needless omits
* ARROW-4982 - [GLib][CI] Run tests on AppVeyor
* PARQUET-1532 - [C++] Can't build column reader test with MinGW

## Wish

* ARROW-3981 - [C++] Rename json.h

# Apache Arrow 0.12.0 (16 January 2019)

## Bug

* ARROW-1847 - [Doc] Document the difference between RecordBatch and Table in an FAQ fashion
* ARROW-2026 - [Python] Cast all timestamp resolutions to INT96 use\_deprecated\_int96\_timestamps=True
* ARROW-2038 - [Python] Follow-up bug fixes for s3fs Parquet support
* ARROW-2113 - [Python] Incomplete CLASSPATH with "hadoop" contained in it can fool the classpath setting HDFS logic
* ARROW-2591 - [Python] Segmentation fault when writing empty ListType column to Parquet
* ARROW-2592 - [Python] Error reading old Parquet file due to metadata backwards compatibility issue
* ARROW-2708 - [C++] Internal GetValues function in arrow::compute should check for nullptr
* ARROW-2970 - [Python] NumPyConverter::Visit for Binary/String/FixedSizeBinary can overflow
* ARROW-3048 - [Python] Import pyarrow fails if scikit-learn is installed from conda (boost-cpp / libboost issue)
* ARROW-3058 - [Python] Feather reads fail with unintuitive error when conversion from pandas yields ChunkedArray
* ARROW-3186 - [GLib] mesonbuild failures in Travis CI
* ARROW-3202 - [C++] Build does not succeed on Alpine Linux
* ARROW-3225 - [C++/Python] Pandas object conversion of ListType<DateType> and ListType<TimeType>
* ARROW-3324 - [Parquet] Free more internal resources when writing multiple row groups
* ARROW-3343 - [Java] Java tests fail non-deterministically with memory leak from Flight tests
* ARROW-3405 - [Python] Document CSV reader
* ARROW-3428 - [Python] from\_pandas gives incorrect results when converting floating point to bool
* ARROW-3436 - [C++] Boost version required by Gandiva is too new for Ubuntu 14.04
* ARROW-3437 - [Gandiva][C++] Configure static linking of libgcc, libstdc++ with LDFLAGS
* ARROW-3438 - [Packaging] Escaped bulletpoints in changelog
* ARROW-3445 - [GLib] Parquet GLib doesn't link Arrow GLib
* ARROW-3449 - [C++] Support CMake 3.2 for "out of the box" builds
* ARROW-3466 - [Python] Crash when importing tensorflow and pyarrow
* ARROW-3467 - Building against external double conversion is broken
* ARROW-3470 - [C++] Row-wise conversion tutorial has fallen out of date
* ARROW-3477 - [C++] Testsuite fails on 32 bit arch
* ARROW-3480 - [Website] Install document for Ubuntu is broken
* ARROW-3485 - [C++] Examples fail with Protobuf error
* ARROW-3494 - [C++] re2 conda-forge package not working in toolchain
* ARROW-3516 - [C++] Use unsigned type for difference of pointers in parallel\_memcpy
* ARROW-3517 - [C++] MinGW 32bit build causes g++ segv
* ARROW-3524 - [C++] Fix compiler warnings from ARROW-3409 on clang-6
* ARROW-3527 - [R] Unused variables in R-package C++ code
* ARROW-3528 - [R] Typo in R documentation
* ARROW-3535 - [Python] pip install tensorflow install too new numpy in manylinux1 build
* ARROW-3541 - [Rust] Update BufferBuilder to allow for new bit-packed BooleanArray
* ARROW-3544 - [Gandiva] Populate function registry in multiple compilation units to mitigate long compile times in release mode
* ARROW-3549 - [Rust] Replace i64 with usize for some bit utility functions
* ARROW-3573 - [Rust] with\_bitset does not set valid bits correctly
* ARROW-3580 - [Gandiva][C++] Build error with g++ 8.2.0
* ARROW-3586 - [Python] Segmentation fault when converting empty table to pandas with categoricals
* ARROW-3598 - [Plasma] plasma\_store\_server fails linking with GPU enabled
* ARROW-3613 - [Go] Resize does not correctly update the length
* ARROW-3614 - [R] Handle Type::TIMESTAMP from Arrow to R
* ARROW-3634 - [GLib] cuda.cpp compile error
* ARROW-3658 - [Rust] validation of offsets buffer is incorrect for \`List<T>\`
* ARROW-3670 - [C++] Use FindBacktrace to find execinfo.h support
* ARROW-3687 - [Rust] Anything measuring array slots should be \`usize\`
* ARROW-3698 - [C++] Segmentation fault when using a large table in Gandiva
* ARROW-3700 - [C++] CSV parser should allow ignoring empty lines
* ARROW-3703 - [Python] DataFrame.to\_parquet crashes if datetime column has time zones
* ARROW-3704 - [Gandiva] Can't build with g++ 8.2.0
* ARROW-3707 - [C++] test failure with zstd 1.3.7
* ARROW-3711 - [C++] Don't pass CXX\_FLAGS to C\_FLAGS
* ARROW-3712 - [CI] License check regression (RAT failure)
* ARROW-3715 - [C++] gflags\_ep fails to build with CMake 3.13
* ARROW-3716 - [R] Missing cases for ChunkedArray conversion
* ARROW-3728 - [Python] Merging Parquet Files - Pandas Meta in Schema Mismatch
* ARROW-3734 - [C++] Linking static zstd library fails on Arch x86-64
* ARROW-3740 - [C++] Calling ArrayBuilder::Resize with length smaller than current appended length results in invalid state
* ARROW-3742 - Fix pyarrow.types & gandiva cython bindings
* ARROW-3745 - [C++] CMake passes static libraries multiple times to linker
* ARROW-3754 - [Packaging] Zstd configure error on linux package builds
* ARROW-3756 - [CI/Docker/Java] Java tests are failing in docker-compose setup
* ARROW-3765 - [Gandiva] Segfault when the validity bitmap has not been allocated
* ARROW-3766 - [Python] pa.Table.from\_pandas doesn't use schema ordering
* ARROW-3768 - [Python] set classpath to hdfs not hadoop executable
* ARROW-3790 - [C++] Signed to unsigned integer cast yields incorrect results when type sizes are the same
* ARROW-3792 - [Python] Segmentation fault when writing empty RecordBatches to Parquet
* ARROW-3793 - [C++] TestScalarAppendUnsafe is not testing unsafe appends
* ARROW-3797 - [Rust] BinaryArray::value\_offset incorrect in offset case
* ARROW-3805 - [Gandiva] handle null validity bitmap in if-else expressions
* ARROW-3831 - [C++] arrow::util::Codec::Decompress() doesn't return decompressed data size
* ARROW-3835 - [C++] arrow::io::CompressedOutputStream::raw() impementation is missing
* ARROW-3837 - [C++] gflags link errors on Windows
* ARROW-3866 - [Python] Column metadata is not transferred to tables in pyarrow
* ARROW-3874 - [Gandiva] Cannot build: LLVM not detected correctly
* ARROW-3879 - [C++] cuda-test failure
* ARROW-3888 - [C++] Compilation warnings with gcc 7.3.0
* ARROW-3889 - [Python] creating schema with invalid paramaters causes segmanetation fault
* ARROW-3890 - [Python] Creating Array with explicit string type fails on Python 2.7
* ARROW-3894 - [Python] Error reading IPC file with no record batches
* ARROW-3898 - parquet-arrow example has compilation errors
* ARROW-3920 - Plasma reference counting not properly done in TensorFlow custom operator.
* ARROW-3931 - [C++] Make possible to build regardless of LANG
* ARROW-3936 - Add \_O\_NOINHERIT to the file open flags on Windows
* ARROW-3937 - [Rust] Rust nightly build is failing
* ARROW-3940 - [Python/Documentation] Add required packages to the development instruction
* ARROW-3941 - [R] RecordBatchStreamReader$schema
* ARROW-3942 - [R] Feather api fixes
* ARROW-3953 - Compat with pandas 0.24 rename of MultiIndex labels -> codes
* ARROW-3955 - [GLib] Add (transfer full) to free when no longer needed
* ARROW-3957 - [Python] Better error message when user connects to HDFS cluster with wrong port
* ARROW-3961 - [Python/Documentation] Fix wrong path in the pyarrow README
* ARROW-3969 - [Rust] CI build broken because rustfmt not available on nightly toolchain
* ARROW-3976 - [Ruby] Homebrew donation solicitation on CLI breaking CI builds
* ARROW-3977 - [Gandiva] gandiva cpp tests not running in CI
* ARROW-3979 - [Gandiva] fix all valgrind reported errors
* ARROW-3980 - [C++] Fix CRTP use in json-simple.cc
* ARROW-3989 - [Rust] CSV reader should handle case sensitivity for boolean values
* ARROW-3996 - [C++] Insufficient description on build
* ARROW-4008 - [C++] Integration test executable failure
* ARROW-4011 - [Gandiva] Refer irhelpers.bc in build directory
* ARROW-4019 - [C++] Fix coverity issues
* ARROW-4033 - [C++] thirdparty/download\_dependencies.sh uses tools or options not available in older Linuxes
* ARROW-4034 - [Ruby] Interface for FileOutputStream doesn't respect append=True
* ARROW-4041 - [CI] Python 2.7 run uses Python 3.6
* ARROW-4049 - [C++] Arrow never use glog even though glog is linked.
* ARROW-4052 - [C++] Linker errors with glog and gflags
* ARROW-4053 - [Python/Integration] HDFS Tests failing with I/O operation on closed file
* ARROW-4055 - [Python] Fails to convert pytz.utc with versions 2018.3 and earlier
* ARROW-4058 - [C++] arrow-io-hdfs-test fails when run against HDFS cluster from docker-compose
* ARROW-4065 - [C++] arrowTargets.cmake is broken
* ARROW-4066 - Instructions to create Sphinx documentation
* ARROW-4070 - [C++] ARROW\_BOOST\_VENDORED doesn't work properly with ninja build
* ARROW-4073 - [Python] Parquet test failures on AppVeyor
* ARROW-4074 - [Python] test\_get\_library\_dirs\_win32 fails if libraries installed someplace different from conda or wheel packages
* ARROW-4078 - [CI] Run Travis job where documentation is built when docs/ is changed
* ARROW-4088 - [Python] Table.from\_batches() fails when passed a schema with metadata
* ARROW-4089 - [Plasma] The tutorial is wrong regarding the parameter type of PlasmaClient.Create
* ARROW-4101 - [C++] Binary identity cast not implemented
* ARROW-4106 - [Python] Tests fail to run because hypothesis update broke its API
* ARROW-4109 - [Packaging] Missing glog dependency from arrow-cpp conda recipe
* ARROW-4113 - [R] Version number patch broke build
* ARROW-4114 - [C++][DOCUMENTATION] Add "python" to Linux build instructions
* ARROW-4115 - [Gandiva] valgrind complains that boolean output data buffer has uninited data
* ARROW-4118 - [Python] Error with "asv run"
* ARROW-4125 - [Python] ASV benchmarks fail to run if Plasma extension is not built (e.g. on Windows)
* ARROW-4126 - [Go] offset not used when accessing boolean array
* ARROW-4128 - [C++][DOCUMENTATION] Update style guide to reflect some more exceptions
* ARROW-4130 - [Go] offset not used when accessing binary array
* ARROW-4134 - [Packaging] Properly setup timezone in docker tests to prevent ORC adapter's abort
* ARROW-4135 - [Python] Can't reload a pandas dataframe containing a list of datetime.time
* ARROW-4138 - [Python] setuptools\_scm customization does not work for versions above 0.9.0 on Windows
* ARROW-4147 - [JAVA] Reduce heap usage for variable width vectors
* ARROW-4149 - [CI/C++] Parquet test misses ZSTD compression codec in CMake 3.2 nightly builds
* ARROW-4157 - [C++] -Wdocumentation failures with clang 6.0 on Ubuntu 18.04
* ARROW-4171 - [Rust] fix parquet crate release version
* ARROW-4173 - JIRA library name is wrong in error message of dev/merge\_arrow\_pr.py
* ARROW-4178 - [C++] Fix TSan and UBSan errors
* ARROW-4179 - [Python] Tests crashing on all platforms in CI
* ARROW-4185 - [Rust] Appveyor builds are broken
* ARROW-4186 - [C++] BitmapWriters clobber the first byte when length=0
* ARROW-4188 - [Rust] There should be a README in the top level rust directory
* ARROW-4197 - [C++] Emscripten compiler fails building Arrow
* ARROW-4200 - [C++] conda\_env\_\* files cannot be used to create a fresh conda environment on Windows
* ARROW-4209 - [Gandiva] returning IR structs causes issues with windows
* ARROW-4215 - [GLib] Fix typos in documentation
* ARROW-4227 - [GLib] Field in composite data type returns wrong data type
* ARROW-4237 - [Packaging] Fix CMAKE\_INSTALL\_LIBDIR in release verification script
* ARROW-4238 - [Packaging] Fix RC version conflict between crossbow and rake
* ARROW-4246 - [Plasma][Python] PlasmaClient.list doesn't work with CUDA enabled Plasma
* ARROW-4256 - [Release] Update Windows verification script for 0.12 release
* ARROW-4258 - [Python] Safe cast fails from numpy float64 array with nans to integer
* ARROW-4260 - [Python] test\_serialize\_deserialize\_pandas is failing in multiple build entries
* PARQUET-1426 - [C++] parquet-dump-schema has poor usability
* PARQUET-1458 - [C++] parquet::CompressionToString not recognizing brotli compression
* PARQUET-1469 - [C++] DefinitionLevelsToBitmap can overwrite prior decoded data
* PARQUET-1471 - [C++] Out of bounds access in statistics UpdateSpaced when writing optional list with null list slots

## Improvement

* ARROW-1423 - [C++] Create non-owned CudaContext from context handle provided by thirdparty user
* ARROW-1688 - [Java] Fail build on checkstyle warnings
* ARROW-1993 - [Python] Add function for determining implied Arrow schema from pandas.DataFrame
* ARROW-1994 - [Python] Test against Pandas master
* ARROW-2183 - [C++] Add helper CMake function for globbing the right header files
* ARROW-2211 - [C++] Use simpler hash functions for integers
* ARROW-2216 - [CI] CI descriptions and envars are misleading
* ARROW-2475 - [Format] Confusing array length description
* ARROW-2476 - [Python/Question] Maximum length of an Array created from ndarray
* ARROW-2483 - [Rust] use bit-packing for boolean vectors
* ARROW-2504 - [Website] Add ApacheCon NA link
* ARROW-2624 - [Python] Random schema and data generator for Arrow conversion and Parquet testing
* ARROW-2637 - [C++/Python] Build support and instructions for development on Alpine Linux
* ARROW-2670 - [C++/Python] Add Ubuntu 18.04 / gcc7 as a nightly build
* ARROW-2673 - [Python] Add documentation + docstring for ARROW-2661
* ARROW-2684 - [Python] Various documentation improvements
* ARROW-2759 - Export notification socket of Plasma
* ARROW-2803 - [C++] Put hashing function into src/arrow/util
* ARROW-2807 - [Python] Enable memory-mapping to be toggled in get\_reader when reading Parquet files
* ARROW-2808 - [Python] Add unit tests for ProxyMemoryPool, enable new default MemoryPool to be constructed
* ARROW-2919 - [C++] Improve error message when listing empty HDFS file
* ARROW-2968 - [R] Multi-threaded conversion from Arrow table to R data.frame
* ARROW-3038 - [Go] add support for StringArray
* ARROW-3063 - [Go] move list of supported/TODO features to confluence
* ARROW-3070 - [Release] Host binary artifacts for RCs and releases on ASF Bintray account instead of dist/mirror system
* ARROW-3131 - [Go] add test for Go-1.11
* ARROW-3161 - [Packaging] Ensure to run pyarrow unit tests in conda and wheel builds
* ARROW-3169 - [C++] Break array-test.cc and array.cc into multiple compilation units
* ARROW-3199 - [Plasma] Check for EAGAIN in recvmsg and sendmsg
* ARROW-3209 - [C++] Rename libarrow\_gpu to libarrow\_cuda
* ARROW-3230 - [Python] Missing comparisons on ChunkedArray, Table
* ARROW-3233 - [Python] Sphinx documentation for pyarrow.cuda GPU support
* ARROW-3278 - [Python] Retrieve StructType's and StructArray's field by name
* ARROW-3291 - [C++] Convenience API for constructing arrow::io::BufferReader from std::string
* ARROW-3312 - [R] Use same .clang-format file for both R binding C++ code and main C++ codebase
* ARROW-3315 - [R] Support for multi-threaded conversions from RecordBatch, Table to R data.frame
* ARROW-3318 - [C++] Convenience method for reading all batches from an IPC stream or file as arrow::Table
* ARROW-3331 - [C++] Add re2 to ThirdpartyToolchain
* ARROW-3353 - [Packaging] Build python 3.7 wheels
* ARROW-3358 - [Gandiva][C++] Replace usages of gandiva/status.h with arrow/status.h
* ARROW-3362 - [R] Guard against null buffers
* ARROW-3366 - [R] Dockerfile for docker-compose setup
* ARROW-3368 - [Integration/CI/Python] Add dask integration test to docker-compose setup
* ARROW-3402 - [Gandiva][C++] Utilize common bitmap operation implementations in precompiled IR routines
* ARROW-3409 - [C++] Add streaming compression interfaces
* ARROW-3421 - [C++] Add include-what-you-use setup to primary docker-compose.yml
* ARROW-3429 - [Packaging] Add a script to release binaries that use source archive at dist.apache.orgtable bit
* ARROW-3430 - [Packaging] Add workaround to verify 0.11.0
* ARROW-3431 - [GLib] Include Gemfile to archive
* ARROW-3432 - [Packaging] Variables aren't expanded Subversion commit message
* ARROW-3433 - [C++] Validate re2 with Windows toolchain, EP
* ARROW-3440 - [Gandiva][C++] Remove outdated cpp/src/gandiva/README.md, add build documentation to cpp/README.md
* ARROW-3441 - [Gandiva][C++] Produce fewer test executables
* ARROW-3442 - [C++] Use dynamic linking for unit tests, ensure coverage working properly with clang
* ARROW-3451 - [Python] Allocate CUDA memory from a CUcontext created by numba.cuda
* ARROW-3455 - [Gandiva][C++] Support pkg-config for Gandiva
* ARROW-3456 - [CI] Reuse docker images and optimize docker-compose containers
* ARROW-3460 - [Packaging] Add a script to rebase master on local release branch
* ARROW-3461 - [Packaging] Add a script to upload RC artifacts as the official release
* ARROW-3462 - [Packaging] Update CHANGELOG for 0.11.0
* ARROW-3463 - [Website] Update for 0.11.0
* ARROW-3465 - [Documentation] Fix gen\_apidocs' docker image
* ARROW-3473 - [Format] Update Layout.md document to clarify use of 64-bit array lengths
* ARROW-3474 - [GLib] Extend gparquet API with get\_schema and read\_column
* ARROW-3479 - [R] Support to write record\_batch as stream
* ARROW-3482 - [C++] Build with JEMALLOC by default
* ARROW-3488 - [Packaging] Separate crossbow task definition files for packaging and tests
* ARROW-3492 - [C++] Build jemalloc in parallel
* ARROW-3493 - [Java] Document BOUNDS\_CHECKING\_ENABLED
* ARROW-3506 - [Packaging] Nightly tests for docker-compose images
* ARROW-3508 - [C++] Build against double-conversion from conda-forge
* ARROW-3518 - [C++] Detect HOMEBREW\_PREFIX automatically
* ARROW-3521 - [GLib] Run Python using find\_program in meson.build
* ARROW-3530 - [Java/Python] Add conversion for pyarrow.Schema from org.apache…pojo.Schema
* ARROW-3533 - [Python/Documentation] Use sphinx\_rtd\_theme instead of Bootstrap
* ARROW-3539 - [CI/Packaging] Update scripts to build against vendored jemalloc
* ARROW-3542 - [C++] Use unsafe appends when building array from CSV
* ARROW-3545 - [C++/Python] Normalize child/field terminology with StructType
* ARROW-3547 - [R] Protect against Null crash when reading from RecordBatch
* ARROW-3548 - Speed up storing small objects in the object store.
* ARROW-3551 - Change MapD to OmniSci on Powered By page
* ARROW-3556 - [CI] Disable optimizations on Windows
* ARROW-3557 - [Python] Set language\_level in Cython sources
* ARROW-3558 - [Plasma] Remove fatal error when plasma client calls get on an unsealed object that it created.
* ARROW-3559 - Statically link libraries for plasma\_store\_server executable.
* ARROW-3562 - [R] Disallow creation of objects with null shared\_ptr<T>
* ARROW-3563 - [C++] Declare public link dependencies so arrow\_static, plasma\_static automatically pull in transitive dependencies
* ARROW-3566 - Clarify that the type of dictionary encoded field should be the encoded(index) type
* ARROW-3574 - Fix remaining bug with plasma static versus shared libraries.
* ARROW-3576 - [Python] Expose compressed file readers as NativeFile
* ARROW-3577 - [Go] add support for ChunkedArray
* ARROW-3581 - [Gandiva][C++] ARROW\_PROTOBUF\_USE\_SHARED isn't used
* ARROW-3582 - [CI] Gandiva C++ build is always triggered
* ARROW-3584 - [Go] add support for Table
* ARROW-3587 - [Python] Efficient serialization for Arrow Objects (array, table, tensor, etc)
* ARROW-3589 - [Gandiva] Make it possible to compile gandiva without JNI
* ARROW-3591 - [R] Support to collect decimal type
* ARROW-3600 - [Packaging] Support Ubuntu 18.10
* ARROW-3601 - [Rust] Release 0.11.0
* ARROW-3602 - [Gandiva] [Python] Add preliminary Cython bindings for Gandiva
* ARROW-3603 - [Gandiva][C++] Can't build with vendored Boost
* ARROW-3605 - Remove AE library from plasma header files.
* ARROW-3607 - [Java] delete() method via JNI for plasma
* ARROW-3611 - Give error more quickly when pyarrow serialization context is used incorrectly.
* ARROW-3612 - [Go] implement RecordBatch and RecordBatchReader
* ARROW-3615 - [R] Support for NaN
* ARROW-3618 - [Packaging/Documentation] Add \`-c conda-forge\` option to avoid PackagesNotFoundError
* ARROW-3620 - [Python] Document multithreading options in Sphinx and add to api.rst
* ARROW-3621 - [Go] implement TableBatchReader
* ARROW-3622 - [Go] implement Schema.Equal
* ARROW-3623 - [Go] implement Field.Equal
* ARROW-3624 - [Python/C++] Support for zero-sized device buffers
* ARROW-3626 - [Go] add a CSV TableReader
* ARROW-3629 - [Python] Add write\_to\_dataset to Python Sphinx API listing
* ARROW-3632 - [Packaging] Update deb names in dev/tasks/tasks.yml in dev/release/00-prepare.sh
* ARROW-3633 - [Packaging] Update deb names in dev/tasks/tasks.yml for 0.12.0
* ARROW-3636 - [C++/Python] Update arrow/python/pyarrow\_api.h
* ARROW-3638 - [C++][Python] Move reading from Feather as Table feature to C++ from Python
* ARROW-3639 - [Packaging] Run gandiva nightly packaging tasks
* ARROW-3640 - [Go] add support for Tensors
* ARROW-3641 - [C++/Python] remove public keyword from Cython api functions
* ARROW-3642 - [C++] Add arrowConfig.cmake generation
* ARROW-3645 - [Python] Document compression support in Sphinx
* ARROW-3646 - [Python] Add convenience factories to create IO streams
* ARROW-3647 - [R] Crash after unloading bit64 package
* ARROW-3648 - [Plasma] Add API to get metadata and data at the same time
* ARROW-3649 - [Rust] Refactor MutableBuffer's resize
* ARROW-3656 - [C++] Allow whitespace in numeric CSV fields
* ARROW-3657 - [R] Require bit64 package
* ARROW-3659 - [C++] Clang Travis build (matrix entry 2) might not actually be using clang
* ARROW-3661 - [Gandiva][GLib] Improve constant name
* ARROW-3666 - [C++] Improve CSV parser performance
* ARROW-3672 - [Go] implement Time32 array
* ARROW-3673 - [Go] implement Time64 array
* ARROW-3674 - [Go] implement Date32 array
* ARROW-3675 - [Go] implement Date64 array
* ARROW-3677 - [Go] implement FixedSizedBinary array
* ARROW-3681 - [Go] add benchmarks for CSV reader
* ARROW-3682 - [Go] unexport encoding/csv.Reader from CSV reader
* ARROW-3683 - [Go] add functional-option style to CSV reader
* ARROW-3684 - [Go] add chunk size option to CSV reader
* ARROW-3693 - [R] Invalid buffer for empty characters with null data
* ARROW-3694 - [Java] Avoid superfluous string creation when logging level is disabled
* ARROW-3695 - [Gandiva] Use add\_arrow\_lib()
* ARROW-3696 - [C++] Add feather::TableWriter::Write(table)
* ARROW-3697 - [Ruby] Add schema#[]
* ARROW-3708 - [Packaging] Nightly CentOS builds are failing
* ARROW-3718 - [Gandiva] Remove spurious gtest include
* ARROW-3719 - [GLib] Support read/write table to/from Feather
* ARROW-3720 - [GLib] Use "indices" instead of "indexes"
* ARROW-3721 - [Gandiva] [Python] Support all Gandiva literals
* ARROW-3722 - [C++] Allow specifying column types to CSV reader
* ARROW-3724 - [GLib] Update gitignore
* ARROW-3725 - [GLib] Add field readers to GArrowStructDataType
* ARROW-3727 - [Python] Document use of pyarrow.foreign\_buffer, cuda.foreign\_buffer in Sphinx
* ARROW-3733 - [GLib] Add to\_string() to GArrowTable and GArrowColumn
* ARROW-3736 - [CI/Docker] Ninja test in docker-compose run cpp hangs
* ARROW-3743 - [Ruby] Add support for saving/loading Feather
* ARROW-3744 - [Ruby] Use garrow\_table\_to\_string() in Arrow::Table#to\_s
* ARROW-3746 - [Gandiva] [Python] Make it possible to list all functions registered with Gandiva
* ARROW-3747 - [C++] Flip order of data members in arrow::Decimal128
* ARROW-3748 - [GLib] Add GArrowCSVReader
* ARROW-3749 - [GLib] Typos in documentation and test case name
* ARROW-3751 - [Python] Add more cython bindings for gandiva
* ARROW-3752 - [C++] Remove unused status::ArrowError
* ARROW-3753 - [Gandiva] Remove debug print
* ARROW-3773 - [C++] Remove duplicated AssertArraysEqual code in parquet/arrow/arrow-reader-writer-test.cc
* ARROW-3778 - [C++] Don't put implementations in test-util.h
* ARROW-3781 - [C++] Configure buffer size in arrow::io::BufferedOutputStream
* ARROW-3784 - [R] Array with type fails with x is not a vector
* ARROW-3785 - [C++] Use double-conversion conda package in CI toolchain
* ARROW-3787 - Implement From<ListArray> for BinaryArray
* ARROW-3788 - [Ruby] Add support for CSV parser writtin in C++
* ARROW-3795 - [R] Support for retrieving NAs from INT64 arrays
* ARROW-3796 - [Rust] Add Example for PrimitiveArrayBuilder
* ARROW-3800 - [C++] Vendor a string\_view backport
* ARROW-3803 - [C++/Python] Split C++ and Python unit test Travis CI jobs, run all C++ tests (including Gandiva) together
* ARROW-3819 - [Packaging] Update conda variant files to conform with feedstock after compiler migration
* ARROW-3821 - [Format/Documentation]: Fix typos and grammar issues in Flight.proto comments
* ARROW-3825 - [Python] The Python README.md does not show how to run the unit test suite
* ARROW-3834 - [Doc] Merge Python & C++ and move to top-level
* ARROW-3836 - [C++] Add PREFIX option to ADD\_ARROW\_BENCHMARK
* ARROW-3839 - [Rust] Add ability to infer schema in CSV reader
* ARROW-3841 - [C++] warning: catching polymorphic type by value
* ARROW-3845 - [Gandiva] [GLib] Add GGandivaNode
* ARROW-3847 - [GLib] Remove unnecessary “\”.
* ARROW-3849 - Leverage Armv8 crc32 extension instructions to accelerate the hash computation for Arm64.
* ARROW-3852 - [C++] used uninitialized warning
* ARROW-3853 - [C++] Implement string to timestamp cast
* ARROW-3854 - [GLib] Deprecate garrow\_gio\_{input,output}\_stream\_get\_raw()
* ARROW-3855 - [Rust] Schema/Field/Datatype should implement serde traits
* ARROW-3856 - [Ruby] Support compressed CSV save/load
* ARROW-3858 - [GLib] Use {class\_name}\_get\_instance\_private
* ARROW-3862 - [C++] Improve dependencies download script
* ARROW-3863 - [GLib] Use travis\_retry with brew bundle command
* ARROW-3865 - [Packaging] Add double-conversion dependency to conda forge recipes and the windows wheel build
* ARROW-3868 - [Rust] Build against nightly Rust in CI
* ARROW-3870 - [C++] Add Peek to InputStream API
* ARROW-3871 - [R] Replace usages of C++ GetValuesSafely with new methods on ArrayData
* ARROW-3878 - [Rust] Improve primitive types
* ARROW-3880 - [Rust] PrimitiveArray<T> should support simple math operations
* ARROW-3883 - [Rust] Update Rust README to reflect new functionality
* ARROW-3884 - [Python] Add LLVM6 to manylinux1 base image
* ARROW-3885 - [Rust] Update version to 0.12.0 and update release instructions on wiki
* ARROW-3886 - [C++] Additional test cases for ARROW-3831
* ARROW-3893 - [C++] Improve adaptive int builder performance
* ARROW-3895 - [Rust] CSV reader should return Result<Option<>> not Option<Result<>>
* ARROW-3899 - [Python] Table.to\_pandas converts Arrow date32[day] to pandas datetime64[ns]
* ARROW-3905 - [Ruby] Add StructDataType#[]
* ARROW-3906 - [C++] Break builder.cc into multiple compilation units
* ARROW-3908 - [Rust] Update rust dockerfile to use nightly toolchain
* ARROW-3910 - [Python] Set date\_as\_object to True in \*.to\_pandas as default after deduplicating logic implemented
* ARROW-3911 - [Python] Deduplicate datetime.date objects in Table.to\_pandas internals
* ARROW-3913 - [Gandiva] [GLib] Add GGandivaLiteralNode
* ARROW-3914 - [C++/Python/Packaging] Docker-compose setup for Alpine linux
* ARROW-3922 - [C++] improve the performance of bitmap operations
* ARROW-3925 - [Python] Include autoconf in Linux/macOS dependencies in conda environment
* ARROW-3928 - [Python] Add option to deduplicate PyBytes / PyString / PyUnicode objects in Table.to\_pandas conversion path
* ARROW-3929 - [Go] improve memory usage of CSV reader to improve runtime performances
* ARROW-3930 - [C++] Random test data generation is slow
* ARROW-3932 - [Python/Documentation] Include Benchmarks.md in Sphinx docs
* ARROW-3934 - [Gandiva] Don't compile precompiled tests if ARROW\_GANDIVA\_BUILD\_TESTS=off
* ARROW-3950 - [Plasma] Don't force loading the TensorFlow op on import
* ARROW-3952 - [Rust] Specify edition="2018" in Cargo.toml
* ARROW-3958 - [Plasma] Reduce number of IPCs
* ARROW-3960 - [Rust] remove extern crate for Rust 2018
* ARROW-3963 - [Packaging/Docker] Nightly test for building sphinx documentations
* ARROW-3964 - [Go] More readable example for csv.Reader
* ARROW-3967 - [Gandiva] [C++] Make gandiva/node.h public
* ARROW-3971 - [Python] Remove APIs deprecated in 0.11 and prior
* ARROW-3974 - [C++] Combine field\_builders\_ and children\_ members in array/builder.h
* ARROW-3982 - [C++] Allow "binary" input in simple JSON format
* ARROW-3984 - [C++] Exit with error if user hits zstd ExternalProject path
* ARROW-3986 - [C++] Write prose documentation
* ARROW-3988 - [C++] Do not build unit tests by default in build system
* ARROW-3994 - [C++] Remove ARROW\_GANDIVA\_BUILD\_TESTS option
* ARROW-3995 - [CI] Use understandable names in Travis Matrix
* ARROW-3997 - [C++] [Doc] Clarify dictionary encoding integer signedness (and width?)
* ARROW-4002 - [C++][Gandiva] Remove CMake version check
* ARROW-4004 - [GLib] Replace GPU with CUDA
* ARROW-4005 - [Plasma] [GLib] Add gplasma\_client\_disconnect()
* ARROW-4006 - Add CODE\_OF\_CONDUCT.md
* ARROW-4009 - [CI] Run Valgrind and C++ code coverage in different bulds
* ARROW-4010 - [C++] Enable Travis CI scripts to only build and install only certain targets
* ARROW-4015 - [Plasma] remove legacy interfaces for plasma manager
* ARROW-4017 - [C++] Check and update vendored libraries
* ARROW-4026 - [C++] Use separate modular $COMPONENT-test targets for unit tests
* ARROW-4029 - [C++] Define and document naming convention for internal / private header files not to be installed
* ARROW-4030 - [CI] Use travis\_terminate to halt builds when a step fails
* ARROW-4035 - [Ruby] Support msys2 mingw dependencies
* ARROW-4037 - [Packaging] Remove workaround to verify 0.11.0
* ARROW-4038 - [Rust] Add array\_ops methods for boolean AND, OR, NOT
* ARROW-4042 - [Rust] Inconsistent method naming between BinaryArray and PrimitiveArray
* ARROW-4048 - [GLib] Return ChunkedArray instead of Array in gparquet\_arrow\_file\_reader\_read\_column
* ARROW-4051 - [Gandiva] [GLib] Add support for null literal
* ARROW-4054 - [Python] Update gtest, flatbuffers and OpenSSL in manylinux1 base image
* ARROW-4069 - [Python] Add tests for casting from binary to utf8
* ARROW-4080 - [Rust] Improving lengthy build times in Appveyor
* ARROW-4082 - [C++] CMake tweaks: allow RelWithDebInfo, improve FindClangTools
* ARROW-4084 - [C++] Simplify Status and stringstream boilerplate
* ARROW-4085 - [GLib] Use "field" for struct data type
* ARROW-4087 - [C++] Make CSV nulls configurable
* ARROW-4093 - [C++] Deprecated method suggests wrong method
* ARROW-4098 - [Python] Deprecate pyarrow.open\_stream,open\_file in favor of pa.ipc.open\_stream/open\_file
* ARROW-4102 - [C++] FixedSizeBinary identity cast not implemented
* ARROW-4103 - [Documentation] Add README to docs/ root
* ARROW-4105 - Add rust-toolchain to enforce user to use nightly toolchain for building
* ARROW-4107 - [Python] Use ninja in pyarrow manylinux1 build
* ARROW-4116 - [Python] Clarify in development.rst that virtualenv cannot be used with miniconda/Anaconda
* ARROW-4122 - [C++] Initialize some uninitialized class members
* ARROW-4127 - [Documentation] Add Docker build instructions
* ARROW-4129 - [Python] Fix syntax problem in benchmark docs
* ARROW-4152 - [GLib] Remove an example to show Torch integration
* ARROW-4155 - [Rust] Implement array\_ops::sum() for PrimitiveArray<T>
* ARROW-4158 - [Dev] Allow maintainers to use a GitHub API token when merging pull requests
* ARROW-4160 - [Rust] Add README and executable files to parquet
* ARROW-4168 - [GLib] Use property to keep GArrowDataType passed in garrow\_field\_new()
* ARROW-4177 - [C++] Add ThreadPool and TaskGroup microbenchmarks
* ARROW-4191 - [C++] Use same CC and AR for jemalloc as for the main sources
* ARROW-4199 - [GLib] Add garrow\_seekable\_input\_stream\_peek()
* ARROW-4207 - [Gandiva] [GLib] Add support for IfNode
* ARROW-4211 - [GLib] Add GArrowFixedSizeBinaryDataType
* ARROW-4216 - [Python] Add CUDA API docs
* ARROW-4228 - [GLib] Add garrow\_list\_data\_type\_get\_field()
* ARROW-4229 - [Packaging] Set crossbow target explicitly to enable building arbitrary arrow repo
* ARROW-4233 - [Packaging] Create a Dockerfile to build source archive
* ARROW-4240 - [Packaging] Documents for Plasma GLib and Gandiva GLib are missing in source archive
* ARROW-4243 - [Python] Test failure with pandas 0.24.0rc1
* ARROW-4249 - [Plasma] Remove reference to logging.h from plasma/common.h
* ARROW-4257 - [Release] Update release verification script to check binaries on Bintray
* ARROW-4269 - [Python] AttributeError: module 'pandas.core' has no attribute 'arrays'
* ARROW-4276 - [Release] Remove needless Bintray authentication from binaries verify script
* ARROW-912 - [Python] Account for multiarch systems in development.rst
* PARQUET-1439 - [C++] Parquet build fails when PARQUET\_ARROW\_LINKAGE is static
* PARQUET-1449 - [C++] Can't build with ARROW\_BOOST\_VENDORED=ON
* PARQUET-1463 - [C++] Utilize revamped common hashing machinery for dictionary encoding
* PARQUET-1467 - [C++] Remove ChunkedAllocator code, now unused
* PARQUET-690 - [C++] Investigate / improve performance of Thrift utilities

## New Feature

* ARROW-1019 - [C++] Implement input stream and output stream with Gzip codec
* ARROW-1055 - [C++] GPU support library development
* ARROW-1492 - [C++] Type casting function kernel suite
* ARROW-1696 - [C++] Add codec benchmarks
* ARROW-2712 - [C#] Initial C# .NET library
* ARROW-3020 - [Python] Addition of option to allow empty Parquet row groups
* ARROW-3108 - [C++] arrow::PrettyPrint for Table instances
* ARROW-3126 - [Python] Make Buffered\* IO classes available to Python, incorporate into input\_stream, output\_stream factory functions
* ARROW-3184 - [C++] Add modular build targets, "all" target, and require explicit target when invoking make or ninja
* ARROW-3303 - [C++] Enable example arrays to be written with a simplified JSON representation
* ARROW-3306 - [R] Objects and support functions different kinds of arrow::Buffer
* ARROW-3307 - [R] Convert chunked arrow::Column to R vector
* ARROW-3310 - [R] Create wrapper classes for various Arrow IO interfaces
* ARROW-3340 - [R] support for dates and time classes
* ARROW-3355 - [R] Support for factors
* ARROW-3380 - [Python] Support reading CSV files and more from a gzipped file
* ARROW-3381 - [C++] Implement InputStream for bz2 files
* ARROW-3387 - [C++] Function to cast binary to string/utf8 with UTF8 validation
* ARROW-3398 - [Rust] Update existing Builder to use MutableBuffer internally
* ARROW-3407 - [C++] Add UTF8 conversion modes in CSV reader conversion options
* ARROW-3439 - [R] R language bindings for Feather format
* ARROW-3450 - [R] Wrap MemoryMappedFile class
* ARROW-3490 - [R] streaming arrow objects to output streams
* ARROW-3499 - [R] Expose arrow::ipc::Message type
* ARROW-3504 - [Plasma] Add support for Plasma Client to put/get raw bytes without pyarrow serialization.
* ARROW-3505 - [R] Read record batch and table
* ARROW-3515 - Introduce NumericTensor class
* ARROW-3529 - [Ruby] Import Red Parquet
* ARROW-3536 - [C++] Fast UTF8 validation functions
* ARROW-3537 - [Rust] Implement Tensor Type
* ARROW-3540 - [Rust] Incorporate BooleanArray into PrimitiveArray
* ARROW-3555 - [Plasma] Unify plasma client get function using metadata.
* ARROW-3567 - [Gandiva] [GLib] Add GLib bindings of Gandiva
* ARROW-3583 - [Python/Java] Create RecordBatch from VectorSchemaRoot
* ARROW-3592 - [Python] Get BinaryArray value as zero copy memory view
* ARROW-3608 - [R] Support for time32 and time64 array types
* ARROW-3610 - [C++] Add interface to turn stl\_allocator into arrow::MemoryPool
* ARROW-3630 - [Plasma] [GLib] Add GLib bindings of Plasma
* ARROW-3660 - [C++] Don't unnecessarily lock MemoryMappedFile for resizing in readonly files
* ARROW-3662 - [C++] Add a const overload to MemoryMappedFile::GetSize
* ARROW-3692 - [Gandiva] [Ruby] Add Ruby bindings of Gandiva
* ARROW-3723 - [Plasma] [Ruby] Add Ruby bindings of Plasma
* ARROW-3726 - [Rust] CSV Reader & Writer
* ARROW-3731 - [R] R API for reading and writing Parquet files
* ARROW-3738 - [C++] Add CSV conversion option to parse ISO8601-like timestamp strings
* ARROW-3741 - [R] Add support for arrow::compute::Cast to convert Arrow arrays from one type to another
* ARROW-3755 - [GLib] Support for CompressedInputStream, CompressedOutputStream
* ARROW-3760 - [R] Support Arrow CSV reader
* ARROW-3782 - [C++] Implement BufferedReader for C++
* ARROW-3798 - [GLib] Add support for column type CSV read options
* ARROW-3807 - [R] Missing Field API
* ARROW-3823 - [R] + buffer.complex
* ARROW-3830 - [GLib] Add GArrowCodec
* ARROW-3842 - [R] RecordBatchStreamWriter api
* ARROW-3864 - [GLib] Add support for allow-float-truncate cast option
* ARROW-3900 - [GLib] Add garrow\_mutable\_buffer\_set\_data()
* ARROW-3912 - [Plasma][GLib] Add support for creating and referring objects
* ARROW-3916 - [Python] Support caller-provided filesystem in \`ParquetWriter\` constructor
* ARROW-3924 - [Packaging][Plasma] Add support for Plasma deb/rpm packages
* ARROW-3938 - [Packaging] Stop to refer java/pom.xml to get version information
* ARROW-3945 - [Website] Blog post about Gandiva code donation
* ARROW-3946 - [GLib] Add support for union
* ARROW-3959 - [Rust] Time and Timestamp Support
* ARROW-4028 - [Rust] Merge parquet-rs codebase
* ARROW-4112 - [Packaging][Gandiva] Add support for deb packages
* ARROW-4132 - [GLib] Add more GArrowTable constructors
* ARROW-4141 - [Ruby] Add support for creating schema from raw Ruby objects
* ARROW-4153 - [GLib] Add builder\_append\_value() for consistency
* ARROW-4154 - [GLib] Add GArrowDecimal128DataType
* ARROW-4161 - [GLib] Add GPlasmaClientOptions
* ARROW-4162 - [Ruby] Add support for creating data types from description
* ARROW-4166 - [Ruby] Add support for saving to and loading from buffer
* ARROW-4174 - [Ruby] Add support for building composite array from raw Ruby objects
* ARROW-4175 - [GLib] Add support for decimal compare operators
* ARROW-4183 - [Ruby] Add Arrow::Struct as an element of Arrow::StructArray
* ARROW-4184 - [Ruby] Add Arrow::RecordBatch#to\_table
* ARROW-4214 - [Ruby] Add support for building RecordBatch from raw Ruby objects
* ARROW-4306 - [Release] Update website and add blog post announcing 0.12.0 release
* ARROW-45 - [Python] Add unnest/flatten function for List types
* ARROW-554 - [C++] Implement functions to conform unequal dictionaries amongst multiple Arrow arrays
* ARROW-854 - [Format] Support sparse tensor

## Sub-task

* ARROW-3272 - [Java] Document checkstyle deviations from Google style guide
* ARROW-3273 - [Java] checkstyle - fix javadoc style
* ARROW-3323 - [Java] checkstyle - fix naming
* ARROW-3347 - [Rust] Implement PrimitiveArrayBuilder
* ARROW-3568 - [Packaging] Run pyarrow unittests for windows wheels
* ARROW-3569 - [Packaging] Run pyarrow unittests when building conda package
* ARROW-3588 - [Java] checkstyle - fix license
* ARROW-3616 - [Java] checkstyle - fix remaining coding checks
* ARROW-3664 - [Rust] Add benchmark for PrimitiveArrayBuilder
* ARROW-3665 - [Rust] Implement StructArrayBuilder
* ARROW-3713 - [Rust] Implement BinaryArrayBuilder
* ARROW-3891 - [Java] Remove Long.bitCount with simple bitmap operations
* ARROW-3939 - [Rust] Remove macro definition for ListArrayBuilder
* ARROW-3948 - [CI][GLib] Set timeout to Homebrew
* ARROW-4060 - [Rust] Add Parquet/Arrow schema converter
* ARROW-4075 - [Rust] Reuse array builder after calling finish()
* ARROW-4172 - [Rust] more consistent naming in array builders

## Task

* ARROW-2337 - [Scripts] Windows release verification script should use boost DSOs instead of static linkage
* ARROW-2535 - [Python] Provide pre-commit hooks that check flake8
* ARROW-2560 - [Rust] The Rust README should include Rust-specific information on contributing
* ARROW-2653 - [C++] Refactor hash table support
* ARROW-2720 - [C++] Clean up cmake CXX\_STANDARD and PIC flag setting
* ARROW-3194 - [Java] Fix setValueCount in spitAndTransfer for variable width vectors
* ARROW-3383 - [Java] Run Gandiva tests in Travis CI
* ARROW-3384 - [Gandiva] Sync remaining commits from gandiva repo
* ARROW-3385 - [Java] [Gandiva] Deploy gandiva snapshot jars automatically
* ARROW-3427 - [C++] Add Windows support, Unix static libs for double-conversion package in conda-forge
* ARROW-3469 - [Gandiva] add travis entry for gandiva on OSX
* ARROW-3472 - [Gandiva] remove gandiva helpers library
* ARROW-3487 - [Gandiva] simplify NULL\_IF\_NULL functions that can return errors
* ARROW-3489 - [Gandiva] Support for in expressions
* ARROW-3501 - [Gandiva] Enable building with gcc 4.8.x on Ubuntu Trusty, similar distros
* ARROW-3519 - [Gandiva] Add support for functions that can return variable len output
* ARROW-3597 - [Gandiva] gandiva should integrate with ADD\_ARROW\_TEST for tests
* ARROW-3609 - [Gandiva] Move benchmark tests out of unit test
* ARROW-3701 - [Gandiva] Add support for decimal operations
* ARROW-3859 - [Java] Fix ComplexWriter backward incompatible change
* ARROW-3860 - [Gandiva] [C++] Add option to use -static-libstdc++ when building libgandiva\_jni.so
* ARROW-3867 - [Documentation] Uploading binary realase artifacts to Bintray
* ARROW-3970 - [Gandiva][C++] Remove unnecessary boost dependencies
* ARROW-3983 - [Gandiva][Crossbow] Use static boost while packaging
* ARROW-3993 - [JS] CI Jobs Failing
* ARROW-4039 - [Python] Update link to 'development.rst' page from Python README.md
* ARROW-4043 - [Packaging/Docker] Python tests on alpine miss pytest dependency
* ARROW-4044 - [Packaging/Python] Add hypothesis test dependency to pyarrow conda recipe
* ARROW-4045 - [Packaging/Python] Add hypothesis test dependency to wheel crossbow tests
* ARROW-4100 - [Gandiva][C++] Fix regex to ignore "." character
* ARROW-4148 - [CI/Python] Disable ORC on nightly Alpine builds
* ARROW-4151 - [Rust] Restructure project directories
* ARROW-4167 - [Gandiva] switch to arrow/util/variant
* ARROW-4210 - [Python] Mention boost-cpp directly in the conda meta.yaml for pyarrow
* ARROW-4239 - [Release] Updating .deb package names in the prepare script failed to run on OSX
* ARROW-4241 - [Packaging] Disable crossbow conda OSX clang builds
* ARROW-4266 - [Python][CI] Disable ORC tests in dask integration test
* ARROW-4270 - [Packaging][Conda] Update xcode version and remove toolchain builds

## Test

* ARROW-4137 - [Rust] Move parquet code into a separate crate

## Wish

* ARROW-3248 - [C++] Arrow tests should have label "arrow"
* ARROW-3260 - [CI] Make linting a separate job
* ARROW-3844 - [C++] Remove ARROW\_USE\_SSE and ARROW\_SSE3
* ARROW-3851 - [C++] "make check-format" is slow
* ARROW-4079 - [C++] Add machine benchmarks
* ARROW-4150 - [C++] Do not return buffers containing nullptr from internal allocations
* ARROW-4156 - [C++] xcodebuild failure for cmake generated project
* PARQUET-1271 - [C++] "parquet\_reader" should be "parquet-reader"

# Apache Arrow 0.11.0 (08 October 2018)

## Bug

* ARROW-1380 - [C++] Fix "still reachable" valgrind warnings when PLASMA\_VALGRIND=1
* ARROW-1661 - [Python] Python 3.7 support
* ARROW-1799 - [Plasma C++] Make unittest does not create plasma store executable
* ARROW-1996 - [Python] pyarrow.read\_serialized cannot read concatenated records
* ARROW-2027 - [C++] ipc::Message::SerializeTo does not pad the message body
* ARROW-2220 - Change default fix version in merge tool to be the next mainline release version
* ARROW-2310 - Source release scripts fail with Java8
* ARROW-2646 - [C++/Python] Pandas roundtrip for date objects
* ARROW-2776 - [C++] Do not pass -Wno-noexcept-type for compilers that do not support it
* ARROW-2782 - [Python] Ongoing Travis CI failures in Plasma unit tests
* ARROW-2814 - [Python] Unify PyObject\* sequence conversion paths for built-in sequences, NumPy arrays
* ARROW-2854 - [C++/Python] Casting float NaN to int should raise an error on safe cast
* ARROW-2925 - [JS] Documentation failing in docker container
* ARROW-2965 - [Python] Possible uint64 overflow issues in python\_to\_arrow.cc
* ARROW-2966 - [Python] Data type conversion error
* ARROW-2973 - [Python] pitrou/asv.git@customize\_commands does not work with the "new" way of activating conda
* ARROW-2974 - [Python] Replace usages of "source activate" with "conda activate" in CI scripts
* ARROW-2986 - [C++] /EHsc possibly needed for Visual Studio 2015 builds
* ARROW-2992 - [Python] Parquet benchmark failure
* ARROW-3006 - [GLib] .gir/.typelib for GPU aren't installed
* ARROW-3007 - [Packaging] libarrow-gpu10 deb for Ubuntu 18.04 has broken dependencies
* ARROW-3011 - [CI] Remove Slack notification
* ARROW-3012 - [Python] Installation crashes with setuptools\_scm error
* ARROW-3013 - [Website] Fix download links on website for tarballs, checksums
* ARROW-3015 - [Python] Fix documentation typo for pa.uint8
* ARROW-3047 - [C++] cmake downloads and builds ORC even though it's installed
* ARROW-3049 - [C++/Python] ORC reader fails on empty file
* ARROW-3053 - [Python] Pandas decimal conversion segfault
* ARROW-3056 - [Python] Indicate in NativeFile docstrings methods that are part of the RawIOBase API but not implemented
* ARROW-3061 - [Java] headroom does not take into account reservation
* ARROW-3065 - [Python] concat\_tables() failing from bad Pandas Metadata
* ARROW-3083 - [Python] Version in manylinux1 wheel builds is wrong
* ARROW-3093 - [C++] Linking errors with ORC enabled
* ARROW-3095 - [Python] test\_plasma.py fails
* ARROW-3098 - [Python] BufferReader doesn't adhere to the seek protocol
* ARROW-3100 - [CI] C/glib build broken on OS X
* ARROW-3125 - [Python] Update ASV instructions
* ARROW-3132 - Regenerate 0.10.0 changelog
* ARROW-3140 - [Plasma] Plasma fails building with GPU enabled
* ARROW-3141 - [Python] Tensorflow support in pyarrow wheels pins numpy>=1.14
* ARROW-3145 - [C++] Thrift compiler reruns in arrow/dbi/hiveserver2/thrift when using Ninja build
* ARROW-3173 - [Rust] dynamic\_types example does not run
* ARROW-3175 - [Java] Upgrade to official FlatBuffers release (Flatbuffers incompatibility)
* ARROW-3183 - [Python] get\_library\_dirs on Windows can give the wrong directory
* ARROW-3188 - [Python] Table.from\_arrays segfaults if lists and schema are passed
* ARROW-3190 - [C++] "WriteableFile" is misspelled, should be renamed "WritableFile" with deprecation for old name
* ARROW-3206 - [C++] Building with ARROW\_HIVESERVER2=ON with unit tests disabled causes error
* ARROW-3227 - [Python] NativeFile.write shouldn't accept unicode strings
* ARROW-3228 - [Python] Immutability of bytes is ignored
* ARROW-3231 - [Python] Sphinx's autodoc\_default\_flags is now deprecated
* ARROW-3237 - [CI] Update linux packaging filenames in rat exclusion list
* ARROW-3241 - [Plasma] test\_plasma\_list test failure on Ubuntu 14.04
* ARROW-3251 - [C++] Conversion warnings in cast.cc
* ARROW-3256 - [JS] File footer and message metadata is inconsistent
* ARROW-3279 - [C++] Allow linking Arrow tests dynamically on Windows
* ARROW-3299 - [C++] Appveyor builds failing
* ARROW-3322 - [CI] Rust job always runs on AppVeyor
* ARROW-3327 - [Python] manylinux container confusing
* ARROW-3338 - [Python] Crash when schema and columns do not match
* ARROW-3342 - Appveyor builds have stopped triggering on GitHub
* ARROW-3348 - Plasma store dies when an object that a dead client is waiting for gets created.
* ARROW-3354 - [Python] read\_record\_batch interfaces differ in pyarrow and pyarrow.cuda
* ARROW-3369 - [Packaging] Wheel builds are failing due to wheel 0.32 release
* ARROW-3370 - [Packaging] Centos 6 build is failing
* ARROW-3373 - Fix bug in which plasma store can die when client gets multiple objects and object becomes available.
* ARROW-3374 - [Python] Dictionary has out-of-bound index when creating DictionaryArray from Pandas with NaN
* ARROW-3393 - [C++] Fix compiler warning in util/task-group-cc on clang 6
* ARROW-3394 - [Java] Remove duplicate dependency entry in Flight
* ARROW-3403 - [Website] Source tarball link missing from install page
* ARROW-3420 - [C++] Fix outstanding include-what-you-use issues in src/arrow, src/parquet codebases

## Improvement

* ARROW-1521 - [C++] Add Reset method to BufferOutputStream to enable object reuse
* ARROW-1949 - [Python/C++] Add option to Array.from\_pandas and pyarrow.array to perform unsafe casts
* ARROW-1963 - [C++/Python] Create Array from sequence of numpy.datetime64
* ARROW-1968 - [Python] Unit testing setup for ORC files
* ARROW-2165 - enhance AllocatorListener to listen for child allocator addition and removal
* ARROW-2520 - [Rust] CI should also build against nightly Rust
* ARROW-2555 - [Python] Provide an option to convert on coerce\_timestamps instead of error
* ARROW-2583 - [Rust] Buffer should be typeless
* ARROW-2617 - [Rust] Schema should contain fields not columns
* ARROW-2687 - [JS] Example usage in README is outdated
* ARROW-2734 - [Python] Cython api example doesn't work by default on macOS
* ARROW-2799 - [Python] Add safe option to Table.from\_pandas to avoid unsafe casts
* ARROW-2813 - [C++] Strip uninformative lcov output from Travis CI logs
* ARROW-2817 - [C++] Enable libraries to be installed in msys2 on Windows
* ARROW-2840 - [C++] See if stream alignment logic can be simplified
* ARROW-2865 - [C++/Python] Reduce some duplicated code in python/builtin\_convert.cc
* ARROW-2889 - [C++] Add optional argument to ADD\_ARROW\_TEST CMake function to add unit test prefix
* ARROW-2900 - [Python] Improve performance of appending nested NumPy arrays in builtin\_convert.cc
* ARROW-2936 - [Python] Implement Table.cast for casting from one schema to another (if possible)
* ARROW-2952 - [C++] Dockerfile for running include-what-you-use checks
* ARROW-2964 - [Go] wire all currently implemented array types in array.MakeFromData
* ARROW-2971 - [Python] Give more descriptive names to python\_to\_arrow.cc/arrow\_to\_python.cc
* ARROW-2975 - [Plasma] TensorFlow op: Compilation only working if arrow found by pkg-config
* ARROW-2976 - [Python] Directory in pyarrow.get\_library\_dirs() on Travis doesn't contain libarrow.so
* ARROW-2983 - [Packaging] Verify source release and binary artifacts in different scripts
* ARROW-2989 - [C++] Remove deprecated APIs in 0.10.0 and below
* ARROW-2994 - [C++] Only include Python C header directories for Python-related compilation units
* ARROW-2996 - [C++] Fix typo in cpp/.clang-tidy
* ARROW-2998 - [C++] Add variants of AllocateBuffer, AllocateResizeableBuffer that return unique\_ptr<Buffer>
* ARROW-2999 - [Python] Do not run ASV benchmarks in every Travis CI build to improve runtimes
* ARROW-3000 - [Python] Do not build unit tests other than python-test in travis\_script\_python.sh
* ARROW-3005 - [Website] Update website and write blog post for 0.10.0 release announcement
* ARROW-3008 - [Packaging] Verify GPU related modules if available
* ARROW-3009 - [Python] pyarrow.orc uses APIs now prohibited in 0.10.0
* ARROW-3010 - [GLib] Update README to use Bundler
* ARROW-3017 - [C++] Don't throw exception in arrow/util/thread-pool.h
* ARROW-3018 - [Plasma] Improve random ObjectID generation
* ARROW-3019 - [Packaging] Use Bundler to verify Arrow GLib
* ARROW-3021 - [Go] support for List
* ARROW-3022 - [Go] support for Struct
* ARROW-3023 - [C++] Use gold linker in builds if it is available
* ARROW-3024 - [C++] Replace usages of std::mutex with atomics in memory\_pool.cc
* ARROW-3026 - [Plasma] Only run Plasma Python unit tests under valgrind once instead of twice in CI
* ARROW-3027 - [Ruby] Stop "git tag" by "rake release"
* ARROW-3028 - [Python] Trim unneeded work from documentation build in Travis CI
* ARROW-3029 - [Python] pkg\_resources is slow
* ARROW-3031 - [Go] Streamline release of Arrays and Builders
* ARROW-3034 - [Packaging] Source archive can't be extracted by bsdtar on MSYS2
* ARROW-3035 - [Rust] Examples in README.md do not run
* ARROW-3036 - [Go] add support for slicing Arrays
* ARROW-3037 - [Go] add support NullArray
* ARROW-3042 - [Go] add badge to GoDoc in the Go-Arrow README
* ARROW-3043 - [C++] pthread doesn't exist on MinGW
* ARROW-3044 - [Python] Remove all occurrences of cython's legacy property definition syntax
* ARROW-3046 - [GLib] Use rubyish method in test-orc-file-reader.rb
* ARROW-3062 - [Python] Extend fast libtensorflow\_framework.so compatibility workaround to Python 2.7
* ARROW-3064 - [C++] Add option to ADD\_ARROW\_TEST to indicate additional dependencies for particular unit test executables
* ARROW-3067 - [Packaging] Support dev/rc/release .deb/.rpm builds
* ARROW-3068 - [Packaging] Bump version to 0.11.0-SNAPSHOT
* ARROW-3069 - [Release] Stop using SHA1 checksums per ASF policy
* ARROW-3072 - [C++] Use ARROW\_RETURN\_NOT\_OK instead of RETURN\_NOT\_OK in header files
* ARROW-3076 - [Website] Add Google Analytics tags to C++, Python API docs
* ARROW-3088 - [Rust] Use internal \`Result<T>\` type instead of \`Result<T, ArrowError>\`
* ARROW-3105 - [Plasma] Improve flushing error message
* ARROW-3106 - [Website] Update committers and PMC roster on website
* ARROW-3111 - [Java] Enable changing default logging level when running tests
* ARROW-3114 - [Website] Add information about user@ mailing list to website / Community page
* ARROW-3116 - [Plasma] Add "ls" to object store
* ARROW-3117 - [GLib] Add garrow\_chunked\_array\_to\_string()
* ARROW-3127 - [C++] Add Tutorial about Sending Tensor from C++ to Python
* ARROW-3128 - [C++] Support system shared zlib
* ARROW-3129 - [Packaging] Stop to use deprecated BuildRoot and Group in .rpm
* ARROW-3130 - [Go] add initial support for Go modules
* ARROW-3136 - [C++] Clean up arrow:: public API
* ARROW-3142 - [C++] Fetch all libs from toolchain environment
* ARROW-3143 - [C++] CopyBitmap into existing memory
* ARROW-3147 - [C++] MSVC version isn't detected in code page 932
* ARROW-3148 - [C++]  MSVC shows C4819 warning on code page 932
* ARROW-3152 - [C++][Packaging] Use dynamic linking for zlib in conda recipes
* ARROW-3157 - [C++] Improve buffer creation for typed data
* ARROW-3158 - [C++] Handle float truncation during casting
* ARROW-3160 - [Python] Improve pathlib.Path support in parquet and filesystem modules
* ARROW-3163 - [Python] Cython dependency is missing in non wheel package
* ARROW-3167 - [CI] Limit clcache cache size
* ARROW-3170 - [C++] Implement "readahead spooler" class for background input buffering
* ARROW-3172 - [Rust] Update documentation for datatypes.rs
* ARROW-3174 - [Rust] run examples as part of CI
* ARROW-3177 - [Rust] Update expected error messages for tests that 'should panic'
* ARROW-3180 - [C++] Add docker-compose setup to simulate Travis CI run locally
* ARROW-3181 - [Packaging] Adjust conda package scripts to account for Parquet codebase migration
* ARROW-3195 - [C++] NumPy initialization error check is missing in test
* ARROW-3211 - [C++] gold linker doesn't work with MinGW-w64
* ARROW-3212 - [C++] Create deterministic IPC metadata
* ARROW-3213 - [C++] Use CMake to build vendored Snappy on Windows
* ARROW-3214 - [C++] Disable insecure warnings with MinGW build
* ARROW-3215 - [C++] Add support for finding libpython on MSYS2
* ARROW-3216 - [C++] libpython isn't linked to libarrow\_python in MinGW build
* ARROW-3217 - [C++] ARROW\_STATIC definition is missing in MinGW build
* ARROW-3218 - [C++] Utilities has needless pthread link in MinGW build
* ARROW-3219 - [C++] Use Win32 API in MinGW
* ARROW-3223 - [GLib] Use the same shared object versioning rule in C++
* ARROW-3229 - [Packaging]: Adjust wheel package scripts to account for Parquet codebase migration
* ARROW-3234 - [C++] Link order is wrong when ARROW\_ORC=on and ARROW\_PROTOBUF\_USE\_SHARED=ON
* ARROW-3235 - [Packaging] Update deb names
* ARROW-3236 - [C++] OutputStream bookkeeping logic when writing IPC file format is incorrect
* ARROW-3240 - [GLib] Add build instructions using Meson
* ARROW-3242 - [C++] Use coarser-grained dispatch to SIMD hash functions
* ARROW-3249 - [Python] Run flake8 on integration\_test.py and crossbow.py
* ARROW-3252 - [C++] Do not hard code the "v" part of versions in thirdparty toolchain
* ARROW-3257 - [C++] Stop to use IMPORTED\_LINK\_INTERFACE\_LIBRARIES
* ARROW-3258 - [GLib] CI is failued on macOS
* ARROW-3259 - [GLib] Rename "writeable" to "writable"
* ARROW-3261 - [Python] Add "field" method to select fields from StructArray
* ARROW-3262 - [Python] Implement \_\_getitem\_\_ with integers on pyarrow.Column
* ARROW-3267 - [Python] Create empty table from schema
* ARROW-3268 - [CI] Reduce conda times on AppVeyor
* ARROW-3269 - [Python] Fix warnings in unit test suite
* ARROW-3270 - [Release] Adjust release verification scripts to recent parquet migration
* ARROW-3274 - [Packaging] Missing glog dependency from conda-forge recipes
* ARROW-3276 - [Packaging] Add support Parquet related Linux packages
* ARROW-3281 - [Java] Make sure that WritableByteChannel in WriteChannel writes out complete bytes
* ARROW-3285 - [GLib] Add arrow\_cpp\_build\_type and arrow\_cpp\_build\_dir Meson options
* ARROW-3286 - [C++] ARROW\_EXPORT for RecordBatchBuilder is missing
* ARROW-3287 - [C++] "redeclared without dllimport attribute after being referenced with dll linkage" with MinGW
* ARROW-3288 - [GLib] Add new API index for 0.11.0
* ARROW-3300 - [Release] Update .deb package names in preparation
* ARROW-3301 - [Website] Update Jekyll and Bootstrap 4
* ARROW-3305 - [JS] Incorrect development documentation link in javascript readme
* ARROW-3309 - [JS] Missing links from DEVELOP.md
* ARROW-3313 - [R] Run clang-format, cpplint checks on R C++ code
* ARROW-3319 - [GLib] Expose AlignStream methods in InputStream, OutputStream classes
* ARROW-3320 - [C++] Improve float parsing performance
* ARROW-3321 - [C++] Improve integer parsing performance
* ARROW-3334 - [Python] Update conda packages to new numpy requirement
* ARROW-3335 - [Python] Add ccache to manylinux1 container
* ARROW-3349 - [C++] Use aligned API in MinGW
* ARROW-3356 - [Python] Document parameters of Table.to\_pandas method
* ARROW-3363 - [C++/Python] Add helper functions to detect scalar Python types
* ARROW-3375 - [Rust] Remove memory\_pool.rs
* ARROW-3376 - [C++] Add double-conversion to cpp/thirdparty/download\_dependencies.sh
* ARROW-3377 - [Gandiva][C++] Remove If statement from bit map set function
* ARROW-3392 - [Python] Support filters in disjunctive normal form in ParquetDataset
* ARROW-3395 - [C++/Python] Add docker container for linting
* ARROW-3397 - [C++] Use relative CMake path for modules
* ARROW-3400 - [Packaging] Add support Parquet GLib related Linux packages
* ARROW-3404 - [C++] Make CSV chunker faster
* ARROW-3411 - [Packaging] dev/release/01-perform.sh doesn't have executable bit
* ARROW-3412 - [Packaging] rat failure in dev/release/02-source.sh
* ARROW-3413 - [Packaging] dev/release/02-source.sh doesn't generate Parquet GLib document
* ARROW-3415 - [Packaging] dev/release/verify-release-cndidate.sh fails in "conda activate arrow-test"
* ARROW-3416 - [Packaging] dev/release/02-source.sh must use SHA512 instead of SHA1
* ARROW-3417 - [Packaging] dev/release/verify-release-cndidate.sh fails Parquet C++ test
* ARROW-3423 - [Packaging] Remove RC information from deb/rpm

## New Feature

* ARROW-1325 - [R] Bootstrap R bindings subproject
* ARROW-1424 - [Python] Initial bindings for libarrow\_gpu
* ARROW-1491 - [C++] Add casting implementations from strings to numbers or boolean
* ARROW-1563 - [C++] Implement logical unary and binary kernels for boolean arrays
* ARROW-1860 - [C++] Add data structure to "stage" a sequence of IPC messages from in-memory data
* ARROW-249 - [Flight] Define GRPC IDL / wire protocol for messaging with Arrow data
* ARROW-25 - [C++] Implement delimited file scanner / CSV reader
* ARROW-2750 - [MATLAB] Add MATLAB support for reading numeric types from Feather files
* ARROW-2979 - [GLib] Add operator functions in GArrowDecimal128
* ARROW-3050 - [C++] Adopt HiveServer2 client C++ codebase
* ARROW-3075 - [C++] Incorporate apache/parquet-cpp codebase into Arrow C++ codebase and build system
* ARROW-3090 - [Rust] Accompany error messages with assertions
* ARROW-3146 - [C++] Barebones Flight RPC server and client implementations
* ARROW-3182 - [C++] Merge Gandiva codebase
* ARROW-3187 - [Plasma] Make Plasma Log pluggable with glog
* ARROW-3196 - Enable merge\_arrow\_py.py script to merge Parquet patches and set fix versions
* ARROW-3197 - [C++] Add instructions to cpp/README.md about Parquet-only development and Arrow+Parquet
* ARROW-3250 - [C++] Create Buffer implementation that takes ownership for the memory from a std::string via std::move
* ARROW-3282 - [R] initial R functionality
* ARROW-3284 - [R] Adding R Error in Status
* ARROW-3339 - [R] Support for character vectors
* ARROW-3341 - [R] Support for logical vector
* ARROW-3360 - [GLib] Import Parquet bindings
* ARROW-3418 - [C++] Update Parquet snapshot version for release

## Sub-task

* ARROW-2948 - [Packaging] Generate changelog with crossbow
* ARROW-3115 - [Java] Style Checks - Fix import ordering
* ARROW-3171 - [Java] checkstyle - fix line length and indentation
* ARROW-3264 - [Java] checkstyle - fix whitespace
* ARROW-3357 - [Rust] Add a mutable buffer implementation

## Task

* ARROW-2338 - [Scripts] Windows release verification script should create a conda environment
* ARROW-2950 - [C++] Clean up util/bit-util.h
* ARROW-2958 - [C++] Flatbuffers EP fails to compile with GCC 8.1
* ARROW-2960 - [Packaging] Fix verify-release-candidate for binary packages and fix release cutting script for lib64 cmake issue
* ARROW-2991 - [CI] Cut down number of AppVeyor jobs
* ARROW-3001 - [Packaging] Don't modify PATH during rust release verification
* ARROW-3003 - [Doc] Enable Java doc in dev/gen\_apidocs/create\_documents.sh
* ARROW-3045 - [Python] Remove nullcheck from ipc Message and MessageReader
* ARROW-3057 - [INTEGRATION] Fix spark and hdfs dockerfiles
* ARROW-3059 - [C++] Streamline namespace array::test
* ARROW-3060 - [C++] Factor out parsing routines
* ARROW-3109 - [Python] Add Python 3.7 virtualenvs to manylinux1 container
* ARROW-3110 - [C++] Compilation warnings with gcc 7.3.0
* ARROW-3119 - [Packaging] Nightly packaging script fails
* ARROW-3153 - [Packaging] Fix broken nightly package builds introduced with recent cmake changes and orc tests
* ARROW-3350 - [Website] Fix powered by links
* ARROW-3352 - [Packaging] Fix recently failing wheel builds
* ARROW-3371 - [Python] Remove check\_metadata argument for Field.equals docstring
* ARROW-3382 - [C++] Run Gandiva tests in Travis CI

## Wish

* ARROW-3002 - [Python] Implement better DataType hash function
* ARROW-3094 - [Python] Allow lighter construction of pa.Schema / pa.StructType
* ARROW-3099 - [C++] Add benchmark for number parsing

# Apache Arrow 0.10.0 (02 August 2018)

## Bug

* ARROW-2059 - [Python] Possible performance regression in Feather read/write path
* ARROW-2101 - [Python] from\_pandas reads 'str' type as binary Arrow data with Python 2
* ARROW-2122 - [Python] Pyarrow fails to serialize dataframe with timestamp.
* ARROW-2182 - [Python] ASV benchmark setup does not account for C++ library changing
* ARROW-2193 - [Plasma] plasma\_store has runtime dependency on Boost shared libraries when ARROW\_BOOST\_USE\_SHARED=on
* ARROW-2195 - [Plasma] Segfault when retrieving RecordBatch from plasma store
* ARROW-2247 - [Python] Statically-linking boost\_regex in both libarrow and libparquet results in segfault
* ARROW-2273 - Cannot deserialize pandas SparseDataFrame
* ARROW-2300 - [Python] python/testing/test\_hdfs.sh no longer works
* ARROW-2305 - [Python] Cython 0.25.2 compilation failure
* ARROW-2314 - [Python] Union array slicing is defective
* ARROW-2326 - [Python] cannot import pip installed pyarrow on OS X (10.9)
* ARROW-2328 - Writing a slice with feather ignores the offset
* ARROW-2331 - [Python] Fix indexing implementations
* ARROW-2333 - [Python] boost bundling fails in setup.py
* ARROW-2342 - [Python] Aware timestamp type fails pickling
* ARROW-2346 - [Python] PYARROW\_CXXFLAGS doesn't accept multiple options
* ARROW-2349 - [Python] Boost shared library bundling is broken for MSVC
* ARROW-2351 - [C++] StringBuilder::append(vector<string>...) not implemented
* ARROW-2354 - [C++] PyDecimal\_Check() is much too slow
* ARROW-2355 - [Python] Unable to import pyarrow [0.9.0] OSX
* ARROW-2357 - Benchmark PandasObjectIsNull
* ARROW-2368 - DecimalVector#setBigEndian is not padding correctly for negative values
* ARROW-2369 - Large (>~20 GB) files written to Parquet via PyArrow are corrupted
* ARROW-2370 - [GLib] include path is wrong on Meson build
* ARROW-2371 - [GLib] gio-2.0 isn't required on GNU Autotools build
* ARROW-2372 - [Python] ArrowIOError: Invalid argument when reading Parquet file
* ARROW-2375 - [Rust] Buffer should release memory when dropped
* ARROW-2377 - [GLib] Travis-CI failures
* ARROW-2380 - [Python] Correct issues in numpy\_to\_arrow conversion routines
* ARROW-2382 - [Rust] List<T> was not using memory safely
* ARROW-2383 - [C++] Debian packages need to depend on libprotobuf
* ARROW-2387 - [Python] negative decimal values get spurious rescaling error
* ARROW-2391 - [Python] Segmentation fault from PyArrow when mapping Pandas datetime column to pyarrow.date64
* ARROW-2393 - [C++] arrow/status.h does not define ARROW\_CHECK needed for ARROW\_CHECK\_OK
* ARROW-2403 - [C++] arrow::CpuInfo::model\_name\_ destructed twice on exit
* ARROW-2405 - [C++] <functional> is missing in plasma/client.h
* ARROW-2418 - [Rust] List builder fails due to memory not being reserved correctly
* ARROW-2419 - [Site] Website generation depends on local timezone
* ARROW-2420 - [Rust] Memory is never released
* ARROW-2423 - [Python] PyArrow datatypes raise ValueError on equality checks against non-PyArrow objects
* ARROW-2424 - [Rust] Missing import causing broken build
* ARROW-2425 - [Rust] Array::from missing mapping for u8 type
* ARROW-2426 - [CI] glib build failure
* ARROW-2432 - [Python] from\_pandas fails when converting decimals if have None values
* ARROW-2437 - [C++] Change of arrow::ipc::ReadMessage signature breaks ABI compability
* ARROW-2441 - [Rust] Builder<T>::slice\_mut assertions are too strict
* ARROW-2443 - [Python] Conversion from pandas of empty categorical fails with ArrowInvalid
* ARROW-2450 - [Python] Saving to parquet fails for empty lists
* ARROW-2452 - [TEST] Spark integration test fails with permission error
* ARROW-2454 - [Python] Empty chunked array slice crashes
* ARROW-2455 - [C++] The bytes\_allocated\_ in CudaContextImpl isn't initialized
* ARROW-2457 - garrow\_array\_builder\_append\_values() won't work for large arrays
* ARROW-2459 - pyarrow: Segfault with pyarrow.deserialize\_pandas
* ARROW-2462 - [C++] Segfault when writing a parquet table containing a dictionary column from Record Batch Stream
* ARROW-2465 - [Plasma] plasma\_store fails to find libarrow\_gpu.so
* ARROW-2466 - [C++] misleading "append" flag to FileOutputStream
* ARROW-2468 - [Rust] Builder::slice\_mut should take mut self
* ARROW-2471 - [Rust] Assertion when pushing value to Builder/ListBuilder with zero capacity
* ARROW-2473 - [Rust] List assertion error with list of zero length
* ARROW-2474 - [Rust] Add windows support for memory pool abstraction
* ARROW-2489 - [Plasma] test\_plasma.py crashes
* ARROW-2491 - [Python] Array.from\_buffers does not work for ListArray
* ARROW-2492 - [Python] Prevent segfault on accidental call of pyarrow.Array
* ARROW-2500 - [Java] IPC Writers/readers are not always setting validity bits correctly
* ARROW-2502 - [Rust] Restore Windows Compatibility
* ARROW-2503 - [Python] Trailing space character in RowGroup statistics of pyarrow.parquet.ParquetFile
* ARROW-2509 - [CI] Intermittent npm failures
* ARROW-2511 - BaseVariableWidthVector.allocateNew is not throwing OOM when it can't allocate memory
* ARROW-2514 - [Python] Inferring / converting nested Numpy array is very slow
* ARROW-2515 - Errors with DictionaryArray inside of ListArray or other DictionaryArray
* ARROW-2518 - [Java] Restore Java unit tests and javadoc test to CI matrix
* ARROW-2530 - [GLib] Out-of-source build is failed
* ARROW-2534 - [C++] libarrow.so leaks zlib symbols
* ARROW-2545 - [Python] Arrow fails linking against statically-compiled Python
* ARROW-2554 - pa.array type inference bug when using NS-timestamp
* ARROW-2561 - [C++] Crash in cuda-test shutdown with coverage enabled
* ARROW-2564 - [C++] Rowwise Tutorial is out of date
* ARROW-2565 - [Plasma] new subscriber cannot receive notifications about existing objects
* ARROW-2570 - [Python] Add support for writing parquet files with LZ4 compression
* ARROW-2571 - [C++] Lz4Codec doesn't properly handle empty data
* ARROW-2575 - [Python] Exclude hidden files when reading Parquet dataset
* ARROW-2578 - [Plasma] Valgrind errors related to std::random\_device
* ARROW-2589 - [Python] test\_parquet.py regression with Pandas 0.23.0
* ARROW-2593 - [Python] TypeError: data type "mixed-integer" not understood
* ARROW-2594 - [Java] Vector reallocation does not properly clear reused buffers
* ARROW-2601 - [Python] MemoryPool bytes\_allocated causes seg
* ARROW-2603 - [Python] from pandas raises ArrowInvalid for date(time) subclasses
* ARROW-2615 - [Rust] Refactor introduced a bug around Arrays of String
* ARROW-2629 - [Plasma] Iterator invalidation for pending\_notifications\_
* ARROW-2630 - [Java] Typo in the document
* ARROW-2632 - [Java] ArrowStreamWriter accumulates ArrowBlock but does not use them
* ARROW-2640 - JS Writer should serialize schema metadata
* ARROW-2643 - [C++] Travis-CI build failure with cpp toolchain enabled
* ARROW-2644 - [Python] parquet binding fails building on AppVeyor
* ARROW-2655 - [C++] Failure with -Werror=conversion on gcc 7.3.0
* ARROW-2657 - Segfault when importing TensorFlow after Pyarrow
* ARROW-2668 - [C++] -Wnull-pointer-arithmetic warning with dlmalloc.c on clang 6.0, Ubuntu 14.04
* ARROW-2669 - [C++] EP\_CXX\_FLAGS not passed on when building gbenchmark
* ARROW-2675 - Arrow build error with clang-10 (Apple Clang / LLVM)
* ARROW-2683 - [Python] Resource Warning (Unclosed File) when using pyarrow.parquet.read\_table()
* ARROW-2690 - [C++] Plasma does not follow style conventions for variable and function names
* ARROW-2691 - [Rust] Travis fails due to formatting diff
* ARROW-2693 - [Python] pa.chunked\_array causes a segmentation fault on empty input
* ARROW-2694 - [Python] ArrayValue string conversion returns the representation instead of the converted python object string
* ARROW-2698 - [Python] Exception when passing a string to Table.column
* ARROW-2711 - [Python/C++] Pandas-Arrow doesn't roundtrip when column of lists has empty first element
* ARROW-2716 - [Python] Make manylinux1 base image independent of Python patch releases
* ARROW-2721 - [C++] Link error with Arrow C++ build with -DARROW\_ORC=ON on CentOS 7
* ARROW-2722 - [Python] ndarray to arrow conversion fails when downcasted from pandas to\_numeric
* ARROW-2723 - [C++] arrow-orc.pc is missing
* ARROW-2726 - [C++] The latest Boost version is wrong
* ARROW-2727 - [Java] Unable to build java/adapters module
* ARROW-2741 - [Python] pa.array from np.datetime[D] and type=pa.date64 produces invalid results
* ARROW-2744 - [Python] Writing to parquet crashes when writing a ListArray of empty lists
* ARROW-2745 - [C++] ORC ExternalProject needs to declare dependency on vendored protobuf
* ARROW-2747 - [CI] [Plasma] huge tables test failure on Travis
* ARROW-2754 - [Python] When installing pyarrow via pip, a debug build is created
* ARROW-2770 - [Packaging] Account for conda-forge compiler migration in conda recipes
* ARROW-2773 - [Python] Corrected parquet docs partition\_cols parameter name
* ARROW-2781 - [Python] Download boost using curl in manylinux1 image
* ARROW-2787 - [Python] Memory Issue passing table from python to c++ via cython
* ARROW-2795 - [Python] Run TensorFlow import workaround only on Linux
* ARROW-2806 - [Python] Inconsistent handling of np.nan
* ARROW-2810 - [Plasma] Plasma public headers leak flatbuffers.h
* ARROW-2812 - [Ruby] StructArray#[] raises NoMethodError
* ARROW-2820 - [Python] RecordBatch.from\_arrays does not validate array lengths are all equal
* ARROW-2823 - [C++] Search for flatbuffers in <root>/lib64
* ARROW-2841 - [Go] Fix recent Go build failures in Travis CI
* ARROW-2850 - [C++/Python] PARQUET\_RPATH\_ORIGIN=ON missing in manylinux1 build
* ARROW-2851 - [C++] Update RAT excludes for new install file names
* ARROW-2852 - [Rust] Mark Array as Sync and Send
* ARROW-2862 - [C++] Ensure thirdparty download directory has been created in thirdparty/download\_thirdparty.sh
* ARROW-2867 - [Python] Incorrect example for Cython usage
* ARROW-2871 - [Python] Array.to\_numpy is invalid for boolean arrays
* ARROW-2872 - [Python] Add pytest mark to opt into TensorFlow-related unit tests
* ARROW-2876 - [Packaging] Crossbow builds can hang if you cloned using SSH
* ARROW-2877 - [Packaging] crossbow submit results in duplicate Travis CI build
* ARROW-2878 - [Packaging] README.md does not mention setting GitHub API token in user's crossbow repo settings
* ARROW-2883 - [Plasma] Compilation warnings
* ARROW-2891 - Preserve schema in write\_to\_dataset
* ARROW-2894 - [Glib] Format tests broken due to recent refactor
* ARROW-2901 - [Java] Build is failing on Java9
* ARROW-2902 - [Python] HDFS Docker integration tests leave around files created by root
* ARROW-2911 - [Python] Parquet binary statistics that end in '\0' truncate last byte
* ARROW-2917 - [Python] Tensor requiring gradiant cannot be serialized with pyarrow.serialize
* ARROW-2920 - [Python] Segfault with pytorch 0.4
* ARROW-2926 - [Python] ParquetWriter segfaults in example where passed schema and table schema do not match
* ARROW-2930 - [C++] Trying to set target properties on not existing CMake target
* ARROW-2940 - [Python] Import error with pytorch 0.3
* ARROW-2945 - [Packaging] Update argument check for 02-source.sh
* ARROW-2955 - [Python] Typo in pyarrow's HDFS API result
* ARROW-2963 - [Python] Deadlock during fork-join and use\_threads=True
* ARROW-2978 - [Rust] Travis CI build is failing
* ARROW-2982 - The "--show-progress" option is only supported in wget 1.16 and higher
* ARROW-640 - [Python] Arrow scalar values should have a sensible \_\_hash\_\_ and comparison

## Improvement

* ARROW-1454 - [Python] More informative error message when attempting to write an unsupported Arrow type to Parquet format
* ARROW-1722 - [C++] Add linting script to look for C++/CLI issues
* ARROW-1731 - [Python] Provide for selecting a subset of columns to convert in RecordBatch/Table.from\_pandas
* ARROW-1744 - [Plasma] Provide TensorFlow operator to read tensors from plasma
* ARROW-1858 - [Python] Add documentation about parquet.write\_to\_dataset and related methods
* ARROW-1886 - [Python] Add function to "flatten" structs within tables
* ARROW-1928 - [C++] Add benchmarks comparing performance of internal::BitmapReader/Writer with naive approaches
* ARROW-1954 - [Python] Add metadata accessor to pyarrow.Field
* ARROW-2014 - [Python] Document read\_pandas method in pyarrow.parquet
* ARROW-2060 - [Python] Documentation for creating StructArray using from\_arrays or a sequence of dicts
* ARROW-2061 - [C++] Run ASAN builds in Travis CI
* ARROW-2074 - [Python] Allow type inference for struct arrays
* ARROW-2097 - [Python] Suppress valgrind stdout/stderr in Travis CI builds when there are no errors
* ARROW-2100 - [Python] Drop Python 3.4 support
* ARROW-2140 - [Python] Conversion from Numpy float16 array unimplemented
* ARROW-2141 - [Python] Conversion from Numpy object array to varsize binary unimplemented
* ARROW-2147 - [Python] Type inference doesn't work on lists of Numpy arrays
* ARROW-2222 - [C++] Add option to validate Flatbuffers messages
* ARROW-2224 - [C++] Get rid of boost regex usage
* ARROW-2241 - [Python] Simple script for running all current ASV benchmarks at a commit or tag
* ARROW-2264 - [Python] Efficiently serialize numpy arrays with dtype of unicode fixed length string
* ARROW-2276 - [Python] Tensor could implement the buffer protocol
* ARROW-2281 - [Python] Expose MakeArray to construct arrays from buffers
* ARROW-2285 - [Python] Can't convert Numpy string arrays
* ARROW-2287 - [Python] chunked array not iterable, not indexable
* ARROW-2301 - [Python] Add source distribution publishing instructions to package / release management documentation
* ARROW-2302 - [GLib] Run autotools and meson Linux builds in same Travis CI build entry
* ARROW-2308 - Serialized tensor data should be 64-byte aligned.
* ARROW-2315 - [C++/Python] Add method to flatten a struct array
* ARROW-2322 - Document requirements to run dev/release/01-perform.sh
* ARROW-2325 - [Python] Update setup.py to use Markdown project description
* ARROW-2332 - [Python] Provide API for reading multiple Feather files
* ARROW-2335 - [Go] Move Go README one directory higher
* ARROW-2340 - [Website] Add blog post about Go codebase donation
* ARROW-2341 - [Python] pa.union() mode argument unintuitive
* ARROW-2348 - [GLib] Remove Go example
* ARROW-2350 - Shrink size of spark\_integration Docker container
* ARROW-2376 - [Rust] Travis should run tests for Rust library
* ARROW-2378 - [Rust] Use rustfmt to format source code
* ARROW-2384 - Rust: Use Traits rather than defining methods directly
* ARROW-2388 - [C++] Arrow::StringBuilder::Append() uses null\_bytes not valid\_bytes
* ARROW-2395 - [Python] Correct flake8 errors outside of pyarrow/ directory
* ARROW-2396 - Unify Rust Errors
* ARROW-2397 - Document changes in Tensor encoding in IPC.md.
* ARROW-2400 - [C++] Status destructor is expensive
* ARROW-2402 - [C++] FixedSizeBinaryBuilder::Append lacks "const char\*" overload
* ARROW-2404 - Fix declaration of 'type\_id' hides class member warning in msvc build
* ARROW-2411 - [C++] Add method to append batches of null-terminated strings to StringBuilder
* ARROW-2413 - [Rust] Remove useless use of \`format!\`
* ARROW-2414 - [Documentation] Fix miscellaneous documentation typos
* ARROW-2415 - [Rust] Fix using references in pattern matching
* ARROW-2417 - [Rust] Review APIs for safety
* ARROW-2422 - [Python] Support more filter operators on Hive partitioned Parquet files
* ARROW-2427 - [C++] ReadAt implementations suboptimal
* ARROW-2430 - MVP for branch based packaging automation
* ARROW-2433 - [Rust] Add Builder.push\_slice(&[T])
* ARROW-2434 - [Rust] Add windows support
* ARROW-2435 - [Rust] Add memory pool abstraction.
* ARROW-2436 - [Rust] Add windows CI
* ARROW-2442 - [C++] Disambiguate Builder::Append overloads
* ARROW-2445 - [Rust] Add documentation and make some fields private
* ARROW-2448 - Segfault when plasma client goes out of scope before buffer.
* ARROW-2451 - Handle more dtypes efficiently in custom numpy array serializer.
* ARROW-2453 - [Python] Improve Table column access
* ARROW-2458 - [Plasma] PlasmaClient uses global variable
* ARROW-2463 - [C++] Update flatbuffers to 1.9.0
* ARROW-2469 - Make out arguments last in ReadMessage API.
* ARROW-2470 - [C++] FileGetSize() should not seek
* ARROW-2472 - [Rust] The Schema and Fields types should not have public attributes
* ARROW-2478 - [C++] Introduce a checked\_cast function that performs a dynamic\_cast in debug mode
* ARROW-2480 - [C++] Enable casting the value of a decimal to int32\_t or int64\_t
* ARROW-2481 - [Rust] Move calls to free() into memory.rs
* ARROW-2484 - [C++] Document ABI compliance checking
* ARROW-2485 - [C++] Output diff when run\_clang\_format.py reports a change
* ARROW-2486 - [C++/Python] Provide a Docker image that contains all dependencies for development
* ARROW-2488 - [C++] List Boost 1.67 as supported version
* ARROW-2506 - [Plasma] Build error on macOS
* ARROW-2507 - [Rust] Don't take a reference when not needed
* ARROW-2508 - [Python] pytest API changes make tests fail
* ARROW-2513 - [Python] DictionaryType should give access to index type and dictionary array
* ARROW-2516 - AppVeyor Build Matrix should be specific to the changes made in a PR
* ARROW-2521 - [Rust] Refactor Rust API to use traits and generics
* ARROW-2522 - [C++] Version shared library files
* ARROW-2525 - [GLib] Add garrow\_struct\_array\_flatten()
* ARROW-2526 - [GLib] Update .gitignore
* ARROW-2527 - [GLib] Enable GPU document
* ARROW-2529 - [C++] Update mention of clang-format to 5.0 in the docs
* ARROW-2531 - [C++] Update clang bits to 6.0
* ARROW-2533 - [CI] Fast finish failing AppVeyor builds
* ARROW-2536 - [Rust] ListBuilder uses wrong initial size for offset builder
* ARROW-2539 - [Plasma] Use unique\_ptr instead of raw pointer
* ARROW-2540 - [Plasma] add constructor/destructor to make sure dlfree is called automatically
* ARROW-2541 - [Plasma] Clean up macro usage
* ARROW-2544 - [CI] Run C++ tests with two jobs on Travis-CI
* ARROW-2547 - [Format] Fix off-by-one in List<List<byte>> example
* ARROW-2548 - [Format] Clarify \`List<Char>\` Array example
* ARROW-2549 - [GLib] Apply arrow::StatusCodes changes to GArrowError
* ARROW-2550 - [C++] Add missing status codes into arrow::StatusCode::CodeAsString()
* ARROW-2551 - [Plasma] Improve notification logic
* ARROW-2553 - [Python] Set MACOSX\_DEPLOYMENT\_TARGET in wheel build
* ARROW-2558 - [Plasma] avoid walk through all the objects when a client disconnects
* ARROW-2563 - [Rust] Poor caching in Travis-CI
* ARROW-2567 - [C++/Python] Unit is ignored on comparison of TimestampArrays
* ARROW-2568 - [Python] Expose thread pool size setting to Python, and deprecate "nthreads"
* ARROW-2569 - [C++] Improve thread pool size heuristic
* ARROW-2574 - [CI] Collect and publish Python coverage
* ARROW-2577 - [Plasma] Add ASV benchmarks
* ARROW-2580 - [GLib] Fix abs functions for Decimal128
* ARROW-2582 - [GLib] Add negate functions for Decimal128
* ARROW-2585 - [C++] Add Decimal128::FromBigEndian
* ARROW-2586 - [C++] Make child builders of ListBuilder and StructBuilder shared\_ptr's
* ARROW-2595 - [Plasma] operator[] creates entries in map
* ARROW-2596 - [GLib] Use the default value of GTK-Doc
* ARROW-2597 - [Plasma] remove UniqueIDHasher
* ARROW-2611 - [Python] Python 2 integer serialization
* ARROW-2612 - [Plasma] Fix deprecated PLASMA\_DEFAULT\_RELEASE\_DELAY
* ARROW-2626 - [Python] pandas ArrowInvalid message should include failing column name
* ARROW-2634 - [Go] Add LICENSE additions for Go subproject
* ARROW-2635 - [Ruby] LICENSE.txt isn't suitable
* ARROW-2636 - [Ruby] "Unofficial" package note is missing
* ARROW-2638 - [Python] Prevent calling extension class constructors directly
* ARROW-2639 - [Python] Remove unnecessary \_check\_nullptr methods
* ARROW-2641 - [C++] Investigate spurious memset() calls
* ARROW-2645 - [Java] ArrowStreamWriter accumulates DictionaryBatch ArrowBlocks
* ARROW-2649 - [C++] Add std::generate()-like function for faster bitmap writing
* ARROW-2656 - [Python] Improve ParquetManifest creation time
* ARROW-2662 - [Python] Add to\_pandas / to\_numpy to ChunkedArray
* ARROW-2663 - [Python] Make dictionary\_encode and unique accesible on Column / ChunkedArray
* ARROW-2664 - [Python] Implement \_\_getitem\_\_ / slicing on Buffer
* ARROW-2666 - [Python] numpy.asarray should trigger to\_pandas on Array/ChunkedArray
* ARROW-2672 - [Python] Build ORC extension in manylinux1 wheels
* ARROW-2674 - [Packaging] Start building nightlies
* ARROW-2676 - [Packaging] Deploy build artifacts to github releases
* ARROW-2677 - [Python] Expose Parquet ZSTD compression
* ARROW-2678 - [GLib] Add extra information to common build problems on macOS
* ARROW-2680 - [Python] Add documentation about type inference in Table.from\_pandas
* ARROW-2682 - [CI] Notify in Slack about broken builds
* ARROW-2689 - [Python] Remove references to timestamps\_to\_ms argument from documentation
* ARROW-2692 - [Python] Add test for writing dictionary encoded columns to chunked Parquet files
* ARROW-2695 - [Python] Prevent calling scalar contructors directly
* ARROW-2696 - [JAVA] enhance AllocationListener with an onFailedAllocation() call
* ARROW-2700 - [Python] Add simple examples to Array.cast docstring
* ARROW-2704 - [Java] IPC stream handling should be more friendly to low level processing
* ARROW-2713 - [Packaging] Fix linux package builds
* ARROW-2724 - [Packaging] Determine whether all the expected artifacts are uploaded
* ARROW-2725 - [JAVA] make Accountant.AllocationOutcome publicly visible
* ARROW-2731 - Allow usage of external ORC library
* ARROW-2732 - Update brew packages for macOS
* ARROW-2733 - [GLib] Cast garrow\_decimal128 to gint64
* ARROW-2738 - [GLib] Use Brewfile on installation process
* ARROW-2739 - [GLib] Use G\_DECLARE\_DERIVABLE\_TYPE for GArrowDecimalDataType and GArrowDecimal128ArrayBuilder
* ARROW-2740 - [Python] Add address property to Buffer
* ARROW-2742 - [Python] Allow Table.from\_batches to use Iterator of ArrowRecordBatches
* ARROW-2748 - [GLib] Add garrow\_decimal\_data\_type\_get\_scale() (and \_precision())
* ARROW-2749 - [GLib] Rename \*garrow\_decimal128\_array\_get\_value to \*garrow\_decimal128\_array\_format\_value
* ARROW-2752 - [GLib] Document garrow\_decimal\_data\_type\_new()
* ARROW-2755 - [Python] Allow using Ninja to build extension
* ARROW-2756 - [Python] Remove redundant imports and minor fixes in parquet tests
* ARROW-2758 - [Plasma] Use Scope enum in Plasma
* ARROW-2760 - [Python] Remove legacy property definition syntax from parquet module and test them
* ARROW-2761 - Support set filter operators on Hive partitioned Parquet files
* ARROW-2763 - [Python] Make parquet \_metadata file accessible from ParquetDataset
* ARROW-2780 - [Go] Run code coverage analysis
* ARROW-2794 - [Plasma] Add Delete method for multiple objects
* ARROW-2798 - [Plasma] Use hashing function that takes into account all UniqueID bytes
* ARROW-2802 - [Docs] Move release management guide to project wiki
* ARROW-2804 - [Website] Link to Developer wiki (Confluence) from front page
* ARROW-2805 - [Python] TensorFlow import workaround not working with tensorflow-gpu if CUDA is not installed
* ARROW-2809 - [C++] Decrease verbosity of lint checks in Travis CI
* ARROW-2811 - [Python] Test serialization for determinism
* ARROW-2815 - [CI] Suppress DEBUG logging when building Java library in C++ CI entries
* ARROW-2816 - [Python] Add \_\_iter\_\_ method to NativeFile
* ARROW-2821 - [C++] Only zero memory in BooleanBuilder in one place
* ARROW-2822 - [C++] Zero padding bytes in PoolBuffer::Resize
* ARROW-2827 - [C++] LZ4 and Zstd build may be failed in parallel build
* ARROW-2829 - [GLib] Add GArrowORCFileReader
* ARROW-2830 - [Packaging] Enable parallel build for deb package build again
* ARROW-2833 - [Python] Column.\_\_repr\_\_ will lock up Jupyter with large datasets
* ARROW-2834 - [GLib] Remove "enable\_" prefix from Meson options
* ARROW-2838 - [Python] Speed up null testing with Pandas semantics
* ARROW-2844 - [Packaging] Test OSX wheels after build
* ARROW-2847 - [Packaging] Fix artifact name matching for conda forge packages
* ARROW-2848 - [Packaging] lib\*.deb package name doesn't match so version
* ARROW-2849 - [Ruby] Arrow::Table#load supports ORC
* ARROW-2859 - [Python] Handle objects exporting the buffer protocol in open\_stream, open\_file, and RecordBatch\*Reader APIs
* ARROW-2861 - [Python] Add extra tips about using Parquet to store index-less pandas data
* ARROW-2864 - [Plasma] Add deletion cache to delete objects later
* ARROW-2869 - [Python] Add documentation for Array.to\_numpy
* ARROW-2886 - [Release] An unused variable exists
* ARROW-2890 - [Plasma] Make Python PlasmaClient.release private
* ARROW-2893 - [C++] Remove PoolBuffer class from public API and hide implementation details behind factory functions
* ARROW-2897 - Organize supported Ubuntu versions
* ARROW-2906 - [Website] Remove the link to slack channel
* ARROW-2907 - [GitHub] Improve "How to contribute patches"
* ARROW-2914 - [Integration] Add WindowPandasUDFTests to Spark Integration
* ARROW-2918 - [C++] Improve formatting of Struct pretty prints
* ARROW-2921 - [Release] Update .deb/.rpm changelos in preparation
* ARROW-2922 - [Release] Make python command name customizable
* ARROW-2923 - [Doc] Add instructions for running Spark integration tests
* ARROW-2937 - [Java] Follow-up changes to ARROW-2704
* ARROW-2943 - [C++] Implement BufferedOutputStream::Flush
* ARROW-2946 - [Packaging] Stop to use PWD in debian/rules
* ARROW-2947 - [Packaging] Remove Ubuntu Artful
* ARROW-2949 - [CI] repo.continuum.io can be flaky in builds
* ARROW-2951 - [CI] Changes in format/ should cause Appveyor builds to run
* ARROW-2953 - [Plasma] Store memory usage
* ARROW-2954 - [Plasma] Store object\_id only once in object table
* ARROW-2985 - [Ruby] Run unit tests in verify-release-candidate.sh
* ARROW-2988 - [Release] More automated release verification on Windows
* ARROW-2990 - [GLib] Fail to build with rpath-ed Arrow C++ on macOS
* ARROW-889 - [C++] Implement arrow::PrettyPrint for ChunkedArray
* ARROW-906 - [C++] Serialize Field metadata to IPC metadata

## New Feature

* ARROW-1018 - [C++] Add option to create FileOutputStream, ReadableFile from OS file descriptor
* ARROW-1163 - [Plasma][Java] Java client for Plasma
* ARROW-1388 - [Python] Add Table.drop method for removing columns
* ARROW-1715 - [Python] Implement pickling for Column, ChunkedArray, RecordBatch, Table
* ARROW-1780 - [Java] JDBC Adapter for Apache Arrow
* ARROW-1964 - [Python] Expose Builder classes
* ARROW-2207 - [GLib] Support decimal type
* ARROW-2267 - Rust bindings
* ARROW-2299 - [Go] Go language implementation
* ARROW-2319 - [C++] Add buffered output class implementing OutputStream interface
* ARROW-2330 - [C++] Optimize delta buffer creation with partially finishable array builders
* ARROW-2344 - [Go] Run Go unit tests in Travis CI
* ARROW-2361 - [Rust] Start native Rust Implementation
* ARROW-2381 - [Rust] Buffer<T> should have an Iterator
* ARROW-2385 - [Rust] Implement to\_json() for Field and DataType
* ARROW-2398 - [Rust] Provide a zero-copy builder for type-safe Buffer<T>
* ARROW-2401 - Support filters on Hive partitioned Parquet files
* ARROW-2407 - [GLib] Add garrow\_string\_array\_builder\_append\_values()
* ARROW-2408 - [Rust] It should be possible to get a &mut[T] from Builder<T>
* ARROW-2440 - [Rust] Implement ListBuilder<T>
* ARROW-2482 - [Rust] support nested types
* ARROW-2493 - [Python] Add support for pickling to buffers and arrays
* ARROW-2537 - [Ruby] Import
* ARROW-2576 - [GLib] Add abs functions for Decimal128.
* ARROW-2604 - [Java] Add method overload for VarCharVector.set(int,String)
* ARROW-2608 - [Java/Python] Add pyarrow.{Array,Field}.from\_jvm / jvm\_buffer
* ARROW-2613 - [Docs] Update the gen\_apidocs docker script
* ARROW-2661 - [Python/C++] Allow passing HDFS Config values via map/dict instead of needing an hdfs-site.xml file
* ARROW-2699 - [C++/Python] Add Table method that replaces a column with a new supplied column
* ARROW-2701 - [C++] Make MemoryMappedFile resizable
* ARROW-2729 - [GLib] Add decimal128 array builder
* ARROW-2751 - [GLib] Add garrow\_table\_replace\_column()
* ARROW-2753 - [GLib] Add garrow\_schema\_\*\_field()
* ARROW-2784 - [C++] MemoryMappedFile::WriteAt allow writing past the end
* ARROW-2790 - [C++] Buffers contain uninitialized memory
* ARROW-2824 - [GLib] Add garrow\_decimal128\_array\_get\_value()
* ARROW-2881 - [Website] Add Community tab to website
* ARROW-530 - C++/Python: Provide subpools for better memory allocation tracking
* ARROW-564 - [Python] Add methods to return vanilla NumPy arrays (plus boolean mask array if there are nulls)

## Sub-task

* ARROW-1868 - [Java] Change vector getMinorType to use MinorType instead of Types.MinorType
* ARROW-1913 - [Java] Fix Javadoc generation bugs with JDK8
* ARROW-2416 - [C++] Support system libprotobuf
* ARROW-2494 - Return status codes from PlasmaClient::Seal
* ARROW-2498 - [Java] Upgrade to JDK 1.8
* ARROW-2717 - [Packaging] Postfix conda artifacts with target arch
* ARROW-2718 - [Packaging] GPG sign downloaded artifacts

## Task

* ARROW-2055 - [Java] Upgrade to Java 8
* ARROW-2334 - [C++] Update boost to 1.66.0
* ARROW-2343 - [Java/Packaging] Run mvn clean in API doc builds
* ARROW-2345 - [Documentation] Fix bundle exec and set sphinx nosidebar to True
* ARROW-2353 - Test correctness of built wheel on AppVeyor
* ARROW-2464 - [Python] Use a python\_version marker instead of a condition
* ARROW-2477 - [Rust] Set up code coverage in CI
* ARROW-2543 - [Rust] CI should cache dependencies for faster builds
* ARROW-2562 - [C++] Upload coverage data to codecov.io
* ARROW-2566 - [CI] Add codecov.io badge to README
* ARROW-2614 - [CI] Remove 'group: deprecated' in Travis
* ARROW-2791 - [Packaging] Build Ubuntu 18.04 packages
* ARROW-2792 - [Packaging] Consider uploading tarballs to avoid naming conflicts
* ARROW-2836 - [Packaging] Expand build matrices to multiple tasks
* ARROW-2837 - [C++] ArrayBuilder::null\_bitmap returns PoolBuffer
* ARROW-2845 - [Packaging] Upload additional debian artifacts
* ARROW-2846 - [Packaging] Update nightly build in crossbow as well as the sample configuration
* ARROW-2855 - [C++] Blog post that outlines the benefits of using jemalloc
* ARROW-2868 - [Packaging] Fix centos-7 build
* ARROW-2875 - [Packaging] Don't attempt to download arrow archive in linux builds
* ARROW-2884 - [Packaging] Options to build packages from apache source archive
* ARROW-2898 - [Packaging] Setuptools\_scm just shipped a new version which fails to parse \`apache-arrow-<version>\` tag
* ARROW-2908 - [Rust] Update version to 0.10.0
* ARROW-2915 - [Packaging] Remove artifact form ubuntu-trusty build
* ARROW-2924 - [Java] mvn release fails when an older maven javadoc plugin is installed
* ARROW-2927 - [Packaging] AppVeyor wheel task is failing on initial checkout
* ARROW-2928 - [Packaging] AppVeyor crossbow conda builds are picking up boost 1.63.0 instead of the installed version
* ARROW-2929 - [C++] ARROW-2826 Breaks parquet-cpp 1.4.0 builds
* ARROW-2934 - [Packaging] Add checksums creation to sign subcommand
* ARROW-2935 - [Packaging] Add verify\_binary\_artifacts function to verify-release-candidate.sh
* ARROW-2944 - [Format] Arrow columnar format docs mentions VectorLayout that does not exist anymore
* ARROW-2962 - [Packaging] Bintray descriptor files are no longer needed
* ARROW-2977 - [Packaging] Release verification script should check rust too

## Test

* ARROW-2557 - [Rust] Add badge for code coverage in README
* ARROW-2895 - [Ruby] CI isn't ran when C++ is changed
* ARROW-2896 - [GLib] export are missing

## Wish

* ARROW-2286 - [Python] Allow subscripting pyarrow.lib.StructValue
* ARROW-2364 - [Plasma] PlasmaClient::Get() could take vector of object ids
* ARROW-2389 - [C++] Add StatusCode::OverflowError
* ARROW-2390 - [C++/Python] CheckPyError() could inspect exception type
* ARROW-2479 - [C++] Have a global thread pool
* ARROW-2499 - [C++] Add iterator facility for Python sequences
* ARROW-2505 - [C++] Disable MSVC warning C4800
* ARROW-2660 - [Python] Experiment with zero-copy pickling
* ARROW-2825 - [C++] Need AllocateBuffer / AllocateResizableBuffer variant with default memory pool
* ARROW-2826 - [C++] Clarification needed between ArrayBuilder::Init(), Resize() and Reserve()
* ARROW-902 - [C++] Build C++ project including thirdparty dependencies from local tarballs

# Apache Arrow 0.9.0 (16 March 2018)

## Bug

* ARROW-1345 - [Python] Conversion from nested NumPy arrays fails on integers other than int64, float32
* ARROW-1646 - [Python] pyarrow.array cannot handle NumPy scalar types
* ARROW-1856 - [Python] Auto-detect Parquet ABI version when using PARQUET\_HOME
* ARROW-1909 - [C++] Bug: Build fails on windows with "-DARROW\_BUILD\_BENCHMARKS=ON"
* ARROW-1912 - [Website] Add org affiliations to committers.html
* ARROW-1919 - Plasma hanging if object id is not 20 bytes
* ARROW-1924 - [Python] Bring back pickle=True option for serialization
* ARROW-1933 - [GLib] Build failure with --with-arrow-cpp-build-dir and GPU enabled Arrow C++
* ARROW-1940 - [Python] Extra metadata gets added after multiple conversions between pd.DataFrame and pa.Table
* ARROW-1941 - Table <–> DataFrame roundtrip failing
* ARROW-1943 - Handle setInitialCapacity() for deeply nested lists of lists
* ARROW-1944 - FindArrow has wrong ARROW\_STATIC\_LIB
* ARROW-1945 - [C++] Fix doxygen documentation of array.h
* ARROW-1946 - Add APIs to decimal vector for writing big endian data
* ARROW-1948 - [Java] ListVector does not handle ipc with all non-null values with none set
* ARROW-1950 - [Python] pandas\_type in pandas metadata incorrect for List types
* ARROW-1953 - [JS] JavaScript builds broken on master
* ARROW-1958 - [Python] Error in pandas conversion for datetimetz row index
* ARROW-1961 - [Python] Writing Parquet file with flavor='spark' loses pandas schema metadata
* ARROW-1966 - [C++] Support JAVA\_HOME paths in HDFS libjvm loading that include the jre directory
* ARROW-1971 - [Python] Add pandas serialization to the default
* ARROW-1972 - Deserialization of buffer objects (and pandas dataframes) segfaults on different processes.
* ARROW-1973 - [Python] Memory leak when converting Arrow tables with array columns to Pandas dataframes.
* ARROW-1976 - [Python] Handling unicode pandas columns on parquet.read\_table
* ARROW-1979 - [JS] JS builds handing in es2015:umd tests
* ARROW-1980 - [Python] Race condition in `write\_to\_dataset`
* ARROW-1982 - [Python] Return parquet statistics min/max as values instead of strings
* ARROW-1991 - [GLib] Docker-based documentation build is broken
* ARROW-1992 - [Python] to\_pandas crashes when using strings\_to\_categoricals on empty string cols on 0.8.0
* ARROW-1997 - [Python] to\_pandas with strings\_to\_categorical fails
* ARROW-1998 - [Python] Table.from\_pandas crashes when data frame is empty
* ARROW-1999 - [Python] from\_numpy\_dtype returns wrong types
* ARROW-2000 - Deduplicate file descriptors when plasma store replies to get request.
* ARROW-2002 - use pyarrow download file will raise queue.Full exceptions sometimes
* ARROW-2003 - [Python] Do not use deprecated kwarg in pandas.core.internals.make\_block
* ARROW-2005 - [Python] pyflakes warnings on Cython files not failing build
* ARROW-2008 - [Python] Type inference for int32 NumPy arrays (expecting list<int32>) returns int64 and then conversion fails
* ARROW-2010 - [C++] Compiler warnings with CHECKIN warning level in ORC adapter
* ARROW-2017 - Array initialization with  large (>2**31-1) uint64 values fails
* ARROW-2023 - [C++] Test opening IPC stream reader or file reader on an empty InputStream
* ARROW-2025 - [Python/C++] HDFS Client disconnect closes all open clients
* ARROW-2029 - [Python] Program crash on `HdfsFile.tell` if file is closed
* ARROW-2032 - [C++] ORC ep installs on each call to ninja build (even if no work to do)
* ARROW-2033 - pa.array() doesn't work with iterators
* ARROW-2039 - [Python] pyarrow.Buffer().to\_pybytes() segfaults
* ARROW-2040 - [Python] Deserialized Numpy array must keep ref to underlying tensor
* ARROW-2047 - [Python] test\_serialization.py uses a python executable in PATH rather than that used for a test run
* ARROW-2049 - ARROW-2049: [Python] Use python -m cython to run Cython, instead of CYTHON\_EXECUTABLE
* ARROW-2062 - [C++] Stalled builds in test\_serialization.py in Travis CI
* ARROW-2070 - [Python] chdir logic in setup.py buggy
* ARROW-2072 - [Python] decimal128.byte\_width crashes
* ARROW-2080 - [Python] Update documentation after ARROW-2024
* ARROW-2085 - HadoopFileSystem.isdir and .isfile should return False if the path doesn't exist
* ARROW-2106 - [Python] pyarrow.array can't take a pandas Series of python datetime objects.
* ARROW-2109 - [C++] Boost 1.66 compilation fails on Windows on linkage stage
* ARROW-2124 - [Python] ArrowInvalid raised if the first item of a nested list of numpy arrays is empty
* ARROW-2128 - [Python] Cannot serialize array of empty lists
* ARROW-2129 - [Python] Segmentation fault on conversion of empty array to Pandas
* ARROW-2131 - [Python] Serialization test fails on Windows when library has been built in place / not installed
* ARROW-2133 - [Python] Segmentation fault on conversion of empty nested arrays to Pandas
* ARROW-2135 - [Python] NaN values silently casted to int64 when passing explicit schema for conversion in Table.from\_pandas
* ARROW-2145 - [Python] Decimal conversion not working for NaN values
* ARROW-2150 - [Python] array equality defaults to identity
* ARROW-2151 - [Python] Error when converting from list of uint64 arrays
* ARROW-2153 - [C++/Python] Decimal conversion not working for exponential notation
* ARROW-2157 - [Python] Decimal arrays cannot be constructed from Python lists
* ARROW-2160 - [C++/Python] Fix decimal precision inference
* ARROW-2161 - [Python] Skip test\_cython\_api if ARROW\_HOME isn't defined
* ARROW-2162 - [Python/C++] Decimal Values with too-high precision are multiplied by 100
* ARROW-2167 - [C++] Building Orc extensions fails with the default BUILD\_WARNING\_LEVEL=Production
* ARROW-2170 - [Python] construct\_metadata fails on reading files where no index was preserved
* ARROW-2171 - [Python] OwnedRef is fragile
* ARROW-2172 - [Python] Incorrect conversion from Numpy array when stride % itemsize != 0
* ARROW-2173 - [Python] NumPyBuffer destructor should hold the GIL
* ARROW-2175 - [Python] arrow\_ep build is triggering during parquet-cpp build in Travis CI
* ARROW-2178 -  [JS] Fix JS html FileReader example
* ARROW-2179 - [C++] arrow/util/io-util.h missing from libarrow-dev
* ARROW-2192 - Commits to master should run all builds in CI matrix
* ARROW-2209 - [Python] Partition columns are not correctly loaded in schema of ParquetDataset
* ARROW-2210 - [C++] TestBuffer\_ResizeOOM has a memory leak with jemalloc
* ARROW-2212 - [C++/Python] Build Protobuf in base manylinux 1 docker image
* ARROW-2223 - [JS] installing umd release throws an error
* ARROW-2227 - [Python] Table.from\_pandas does not create chunked\_arrays.
* ARROW-2230 - [Python] JS version number is sometimes picked up
* ARROW-2232 - [Python] pyarrow.Tensor constructor segfaults
* ARROW-2234 - [JS] Read timestamp low bits as Uint32s
* ARROW-2240 - [Python] Array initialization with leading numpy nan fails with exception
* ARROW-2244 - [C++] Slicing NullArray should not cause the null count on the internal data to be unknown
* ARROW-2245 - [Python] Revert static linkage of parquet-cpp in manylinux1 wheel
* ARROW-2246 - [Python] Use namespaced boost in manylinux1 package
* ARROW-2251 - [GLib] Destroying GArrowBuffer while GArrowTensor that uses the buffer causes a crash
* ARROW-2254 - [Python] Local in-place dev versions picking up JS tags
* ARROW-2258 - [C++] Appveyor builds failing on master
* ARROW-2263 - [Python] test\_cython.py fails if pyarrow is not in import path (e.g. with inplace builds)
* ARROW-2265 - [Python] Serializing subclasses of np.ndarray returns a np.ndarray.
* ARROW-2268 - Remove MD5 checksums from release process
* ARROW-2269 - [Python] Cannot build bdist\_wheel for Python
* ARROW-2270 - [Python] ForeignBuffer doesn't tie Python object lifetime to C++ buffer lifetime
* ARROW-2272 - [Python] test\_plasma spams /tmp
* ARROW-2275 - [C++] Buffer::mutable\_data\_ member uninitialized
* ARROW-2280 - [Python] pyarrow.Array.buffers should also include the offsets
* ARROW-2284 - [Python] test\_plasma error on plasma\_store error
* ARROW-2288 - [Python] slicing logic defective
* ARROW-2297 - [JS] babel-jest is not listed as a dev dependency
* ARROW-2304 - [C++] MultipleClients test in io-hdfs-test fails on trunk
* ARROW-2306 - [Python] HDFS test failures
* ARROW-2307 - [Python] Unable to read arrow stream containing 0 record batches
* ARROW-2311 - [Python] Struct array slicing defective
* ARROW-2312 - [JS] verify-release-candidate-sh must be updated to include JS in integration tests
* ARROW-2313 - [GLib] Release builds must define NDEBUG
* ARROW-2316 - [C++] Revert Buffer::mutable\_data member to always inline
* ARROW-2318 - [C++] TestPlasmaStore.MultipleClientTest is flaky (hangs) in release builds
* ARROW-2320 - [C++] Vendored Boost build does not build regex library

## Improvement

* ARROW-1021 - [Python] Add documentation about using pyarrow from other Cython and C++ projects
* ARROW-1035 - [Python] Add ASV benchmarks for streaming columnar deserialization
* ARROW-1463 - [JAVA] Restructure ValueVector hierarchy to minimize compile-time generated code
* ARROW-1579 - [Java] Add dockerized test setup to validate Spark integration
* ARROW-1580 - [Python] Instructions for setting up nightly builds on Linux
* ARROW-1623 - [C++] Add convenience method to construct Buffer from a string that owns its memory
* ARROW-1632 - [Python] Permit categorical conversions in Table.to\_pandas on a per-column basis
* ARROW-1643 - [Python] Accept hdfs:// prefixes in parquet.read\_table and attempt to connect to HDFS
* ARROW-1706 - [Python] StructArray.from\_arrays should handle sequences that are coercible to arrays
* ARROW-1712 - [C++] Add method to BinaryBuilder to reserve space for value data
* ARROW-1835 - [C++] Create Arrow schema from std::tuple types
* ARROW-1861 - [Python] Fix up ASV setup, add developer instructions for writing new benchmarks and running benchmark suite locally
* ARROW-1872 - [Website] Populate hard-coded fields for current release from a YAML file
* ARROW-1927 - [Plasma] Implement delete function
* ARROW-1929 - [C++] Move various Arrow testing utility code from Parquet to Arrow codebase
* ARROW-1937 - [Python] Add documentation for different forms of constructing nested arrays from Python data structures
* ARROW-1942 - [C++] Hash table specializations for small integers
* ARROW-1947 - [Plasma] Change Client Create and Get to use Buffers
* ARROW-1951 - Add memcopy\_threads to serialization context
* ARROW-1962 - [Java] Add reset() to ValueVector interface
* ARROW-1969 - [C++] Do not build ORC adapter by default
* ARROW-1977 - [C++] Update windows dev docs
* ARROW-1978 - [Website] Add more visible link to "Powered By" page to front page, simplify Powered By
* ARROW-2004 - [C++] Add shrink\_to\_fit option in BufferBuilder::Resize
* ARROW-2007 - [Python] Sequence converter for float32 not implemented
* ARROW-2011 - Allow setting the pickler to use in pyarrow serialization.
* ARROW-2012 - [GLib] Support "make distclean"
* ARROW-2018 - [C++] Build instruction on macOS and Homebrew is incomplete
* ARROW-2019 - Control the memory allocated for inner vector in LIST
* ARROW-2024 - [Python] Remove global SerializationContext variables
* ARROW-2028 - [Python] extra\_cmake\_args needs to be passed through shlex.split
* ARROW-2031 - HadoopFileSystem isn't pickleable
* ARROW-2035 - [C++] Update vendored cpplint.py to a Py3-compatible one
* ARROW-2036 - NativeFile should support standard IOBase methods
* ARROW-2042 - [Plasma] Revert API change of plasma::Create to output a MutableBuffer
* ARROW-2043 - [C++] Change description from OS X to macOS
* ARROW-2046 - [Python] Add support for PEP519 - pathlib and similar objects
* ARROW-2048 - [Python/C++] Upate Thrift pin to 0.11
* ARROW-2050 - Support `setup.py pytest` to automatically fetch the test dependencies
* ARROW-2064 - [GLib] Add common build problems link to the install section
* ARROW-2065 - Fix bug in SerializationContext.clone().
* ARROW-2068 - [Python] Expose Array's buffers to Python users
* ARROW-2069 - [Python] Document that Plasma is not (yet) supported on Windows
* ARROW-2071 - [Python] Reduce runtime of builds in Travis CI
* ARROW-2073 - [Python] Create StructArray from sequence of tuples given a known data type
* ARROW-2076 - [Python] Display slowest test durations
* ARROW-2083 - Support skipping builds
* ARROW-2084 - [C++] Support newer Brotli static library names
* ARROW-2086 - [Python] Shrink size of arrow\_manylinux1\_x86\_64\_base docker image
* ARROW-2087 - [Python] Binaries of 3rdparty are not stripped in manylinux1 base image
* ARROW-2088 - [GLib] Add GArrowNumericArray
* ARROW-2089 - [GLib] Rename to GARROW\_TYPE\_BOOLEAN for consistency
* ARROW-2090 - [Python] Add context manager methods to ParquetWriter
* ARROW-2093 - [Python] Possibly do not test pytorch serialization in Travis CI
* ARROW-2094 - [Python] Use toolchain libraries and PROTOBUF\_HOME for protocol buffers
* ARROW-2095 - [C++] Suppress ORC EP build logging by default
* ARROW-2096 - [C++] Turn off Boost\_DEBUG to trim build output
* ARROW-2099 - [Python] Support DictionaryArray::FromArrays in Python bindings
* ARROW-2107 - [GLib] Follow arrow::gpu::CudaIpcMemHandle API change
* ARROW-2110 - [Python] Only require pytest-runner on test commands
* ARROW-2111 - [C++] Linting could be faster
* ARROW-2117 - [C++] Pin clang to version 5.0
* ARROW-2118 - [Python] Improve error message when calling parquet.read\_table on an empty file
* ARROW-2120 - Add possibility to use empty \_MSVC\_STATIC\_LIB\_SUFFIX for Thirdparties
* ARROW-2121 - [Python] Consider special casing object arrays in pandas serializers.
* ARROW-2132 - [Doc] Add links / mentions of Plasma store to main README
* ARROW-2137 - [Python] Don't print paths that are ignored when reading Parquet files
* ARROW-2138 - [C++] Have FatalLog abort instead of exiting
* ARROW-2142 - [Python] Conversion from Numpy struct array unimplemented
* ARROW-2143 - [Python] Provide a manylinux1 wheel for cp27m
* ARROW-2146 - [GLib] Implement Slice for ChunkedArray
* ARROW-2154 - [Python] \_\_eq\_\_ unimplemented on Buffer
* ARROW-2155 - [Python] pa.frombuffer(bytearray) returns immutable Buffer
* ARROW-2163 - Install apt dependencies separate from built-in Travis commands, retry on flakiness
* ARROW-2168 - [C++] Build toolchain builds with jemalloc
* ARROW-2169 - [C++] MSVC is complaining about uncaptured variables
* ARROW-2174 - [JS] Export format and schema enums
* ARROW-2177 - [C++] Remove support for specifying negative scale values in DecimalType
* ARROW-2180 - [C++] Remove APIs deprecated in 0.8.0 release
* ARROW-2181 - [Python] Add concat\_tables to API reference, add documentation on use
* ARROW-2184 - [C++] Add static constructor for FileOutputStream returning shared\_ptr to base OutputStream
* ARROW-2185 - Remove CI directives from squashed commit messages
* ARROW-2191 - [C++] Only use specific version of jemalloc
* ARROW-2198 - [Python] Docstring for parquet.read\_table is misleading or incorrect
* ARROW-2199 - [JAVA] Follow up fixes for ARROW-2019. Ensure density driven capacity is never less than 1 and propagate density throughout the vector tree
* ARROW-2203 - [C++] StderrStream class
* ARROW-2204 - [C++] Build fails with TLS error on parquet-cpp clone
* ARROW-2206 - [JS] Add Perspective as a community project
* ARROW-2218 - [Python] PythonFile should infer mode when not given
* ARROW-2231 - [CI] Use clcache on AppVeyor
* ARROW-2238 - [C++] Detect clcache in cmake configuration
* ARROW-2250 - plasma\_store process should cleanup on INT and TERM signals
* ARROW-2261 - [GLib] Can't share the same memory in GArrowBuffer safely
* ARROW-2279 - [Python] Better error message if lib cannot be found
* ARROW-2282 - [Python] Create StringArray from buffers
* ARROW-2283 - [C++] Support Arrow C++ installed in /usr detection by pkg-config
* ARROW-2289 - [GLib] Add  Numeric, Integer and FloatingPoint data types
* ARROW-2291 - [C++] README missing instructions for libboost-regex-dev
* ARROW-2292 - [Python] More consistent / intuitive name for pyarrow.frombuffer
* ARROW-2321 - [C++] Release verification script fails with if CMAKE\_INSTALL\_LIBDIR is not $ARROW\_HOME/lib
* ARROW-764 - [C++] Improve performance of CopyBitmap, add benchmarks

## New Feature

* ARROW-1394 - [Plasma] Add optional extension for allocating memory on GPUs
* ARROW-1705 - [Python] Create StructArray from sequence of dicts given a known data type
* ARROW-1757 - [C++] Add DictionaryArray::FromArrays alternate ctor that can check or sanitized "untrusted" indices
* ARROW-1832 - [JS] Implement JSON reader for integration tests
* ARROW-1920 - Add support for reading ORC files
* ARROW-1926 - [GLib] Add garrow\_timestamp\_data\_type\_get\_unit()
* ARROW-1930 - [C++] Implement Slice for ChunkedArray and Column
* ARROW-1931 - [C++] w4996 warning due to std::tr1 failing builds on Visual Studio 2017
* ARROW-1965 - [GLib] Add garrow\_array\_builder\_get\_value\_data\_type() and garrow\_array\_builder\_get\_value\_type()
* ARROW-1970 - [GLib] Add garrow\_chunked\_array\_get\_value\_data\_type() and garrow\_chunked\_array\_get\_value\_type()
* ARROW-2166 - [GLib] Implement Slice for Column
* ARROW-2176 - [C++] Extend DictionaryBuilder to support delta dictionaries
* ARROW-2190 - [GLib] Add add/remove field functions for RecordBatch.
* ARROW-2205 - [Python] Option for integer object nulls
* ARROW-2252 - [Python] Create buffer from address, size and base
* ARROW-2253 - [Python] Support \_\_eq\_\_ on scalar values
* ARROW-2262 - [Python] Support slicing on pyarrow.ChunkedArray
* ARROW-232 - C++/Parquet: Support writing chunked arrays as part of a table
* ARROW-633 - [Java] Add support for FixedSizeBinary type
* ARROW-634 - Add integration tests for FixedSizeBinary
* ARROW-969 - [C++/Python] Add add/remove field functions for RecordBatch

## Sub-task

* ARROW-1815 - [Java] Rename MapVector to StructVector

## Task

* ARROW-2052 - Unify OwnedRef and ScopedRef
* ARROW-2054 - Compilation warnings
* ARROW-2108 - [Python] Update instructions for ASV
* ARROW-2114 - [Python] Pull latest docker manylinux1 image
* ARROW-2123 - [JS] Upgrade to TS 2.7.1
* ARROW-2134 - [CI] Make Travis commit inspection more robust
* ARROW-2149 - [Python] reorganize test\_convert\_pandas.py
* ARROW-2156 - [CI] Isolate Sphinx dependencies
* ARROW-2197 - Document "undefined symbol" issue and workaround
* ARROW-2239 - [C++] Update build docs for Windows
* ARROW-2309 - [C++] Use std::make\_unsigned

## Test

* ARROW-1589 - [C++] Fuzzing for certain input formats

# Apache Arrow 0.8.0 (12 December 2017)

## Bug

* ARROW-1282 - Large memory reallocation by Arrow causes hang in jemalloc
* ARROW-1341 - [C++] Deprecate arrow::MakeTable in favor of new ctor from ARROW-1334
* ARROW-1347 - [JAVA] List null type should use consistent name for inner field
* ARROW-1398 - [Python] No support reading columns of type decimal(19,4)
* ARROW-1409 - [Format] Use for "page" attribute in Buffer in metadata
* ARROW-1540 - [C++] Fix valgrind warnings in cuda-test if possible
* ARROW-1541 - [C++] Race condition with arrow\_gpu
* ARROW-1543 - [C++] row\_wise\_conversion example doesn't correspond to ListBuilder constructor arguments
* ARROW-1555 - [Python] write\_to\_dataset on s3
* ARROW-1584 - [PYTHON] serialize\_pandas on empty dataframe
* ARROW-1585 - serialize\_pandas round trip fails on integer columns
* ARROW-1586 - [PYTHON] serialize\_pandas roundtrip loses columns name
* ARROW-1609 - Plasma: Build fails with Xcode 9.0
* ARROW-1615 - CXX flags for development more permissive than Travis CI builds
* ARROW-1617 - [Python] Do not use symlinks in python/cmake\_modules
* ARROW-1620 - Python: Download Boost in manylinux1 build from bintray
* ARROW-1624 - [C++] Follow up fixes / tweaks to compiler warnings for Plasma / LLVM 4.0, add to readme
* ARROW-1625 - [Serialization] Support OrderedDict properly
* ARROW-1629 - [C++] Fix problematic code paths identified by infer tool
* ARROW-1633 - [Python] numpy "unicode" arrays not understood
* ARROW-1640 - Resolve OpenSSL issues in Travis CI
* ARROW-1647 - [Plasma] Potential bug when reading/writing messages.
* ARROW-1653 - [Plasma] Use static cast to avoid compiler warning.
* ARROW-1656 - [C++] Endianness Macro is Incorrect on Windows And Mac
* ARROW-1657 - [C++] Multithreaded Read Test Failing on Arch Linux
* ARROW-1658 - [Python] Out of bounds dictionary indices causes segfault after converting to pandas
* ARROW-1663 - [Java] Follow up on ARROW-1347 and make schema backward compatible
* ARROW-1670 - [Python] Speed up deserialization code path
* ARROW-1672 - [Python] Failure to write Feather bytes column
* ARROW-1673 - [Python] NumPy boolean arrays get converted to uint8 arrays on NdarrayToTensor roundtrip
* ARROW-1676 - [C++] Correctly truncate oversized validity bitmaps when writing Feather format
* ARROW-1678 - [Python] Incorrect serialization of numpy.float16
* ARROW-1680 - [Python] Timestamp unit change not done in from\_pandas() conversion
* ARROW-1686 - Documentation generation script creates "apidocs" directory under site/java
* ARROW-1693 - [JS] Error reading dictionary-encoded integration test files
* ARROW-1695 - [Serialization] Fix reference counting of numpy arrays created in custom serialializer
* ARROW-1698 - [JS] File reader attempts to load the same dictionary batch more than once
* ARROW-1704 - [GLib] Go example in test suite is broken
* ARROW-1708 - [JS] Linter problem breaks master build
* ARROW-1709 - [C++] Decimal.ToString is incorrect for negative scale
* ARROW-1711 - [Python] flake8 checks still not failing builds
* ARROW-1714 - [Python] No named pd.Series name serialized as u'None'
* ARROW-1720 - [Python] Segmentation fault while trying to access an out-of-bound chunk
* ARROW-1723 - Windows: \_\_declspec(dllexport) specified when building arrow static library
* ARROW-1730 - [Python] Incorrect result from pyarrow.array when passing timestamp type
* ARROW-1732 - [Python] RecordBatch.from\_pandas fails on DataFrame with no columns when preserve\_index=False
* ARROW-1735 - [C++] Cast kernels cannot write into sliced output array
* ARROW-1738 - [Python] Wrong datetime conversion when pa.array with unit
* ARROW-1739 - [Python] Fix usages of assertRaises causing broken build
* ARROW-1742 - C++: clang-format is not detected correct on OSX anymore
* ARROW-1743 - [Python] Table to\_pandas fails when index contains categorical column
* ARROW-1745 - Compilation failure on Mac OS in plasma tests
* ARROW-1749 - [C++] Handle range of Decimal128 values that require 39 digits to be displayed
* ARROW-1751 - [Python] Pandas 0.21.0 introduces a breaking API change for MultiIndex construction
* ARROW-1754 - [Python] Fix buggy Parquet roundtrip when an index name is the same as a column name
* ARROW-1756 - [Python] Observed int32 overflow in Feather write/read path
* ARROW-1762 - [C++] unittest failure for language environment
* ARROW-1764 - [Python] Add -c conda-forge for Windows dev installation instructions
* ARROW-1766 - [GLib] Fix failing builds on OSX
* ARROW-1768 - [Python] Fix suppressed exception in ParquetWriter.\_\_del\_\_
* ARROW-1770 - [GLib] Fix GLib compiler warning
* ARROW-1771 - [C++] ARROW-1749 Breaks Public API test in parquet-cpp
* ARROW-1776 - [C++[ arrow::gpu::CudaContext::bytes\_allocated() isn't defined
* ARROW-1778 - [Python] Link parquet-cpp statically, privately in manylinux1 wheels
* ARROW-1781 - [CI] OSX Builds on Travis-CI time out often
* ARROW-1788 - Plasma store crashes when trying to abort objects for disconnected client
* ARROW-1791 - Integration tests generate date[DAY] values outside of reasonable range
* ARROW-1793 - [Integration] fix a typo for README.md
* ARROW-1800 - [C++] Fix and simplify random\_decimals
* ARROW-1805 - [Python] ignore non-parquet files when exploring dataset
* ARROW-1811 - [C++/Python] Rename all Decimal based APIs to Decimal128
* ARROW-1812 - Plasma store modifies hash table while iterating during client disconnect
* ARROW-1829 - [Plasma] Clean up eviction policy bookkeeping
* ARROW-1830 - [Python] Error when loading all the files in a dictionary
* ARROW-1836 - [C++] Fix C4996 warning from arrow/util/variant.h on MSVC builds
* ARROW-1840 - [Website] The installation command failed on Windows10 anaconda environment.
* ARROW-1845 - [Python] Expose Decimal128Type
* ARROW-1852 - [Plasma] Make retrieving manager file descriptor const
* ARROW-1853 - [Plasma] Fix off-by-one error in retry processing
* ARROW-1863 - [Python] PyObjectStringify could render bytes-like output for more types of objects
* ARROW-1865 - [C++] Adding a column to an empty Table fails
* ARROW-1869 - Fix typo in LowCostIdentityHashMap
* ARROW-1871 - [Python/C++] Appending Python Decimals with different scales requires rescaling
* ARROW-1873 - [Python] Segmentation fault when loading total 2GB of parquet files
* ARROW-1877 - Incorrect comparison in JsonStringArrayList.equals
* ARROW-1879 - [Python] Dask integration tests are not skipped if dask is not installed
* ARROW-1881 - [Python] setuptools\_scm picks up JS version tags
* ARROW-1882 - [C++] Reintroduce DictionaryBuilder
* ARROW-1883 - [Python] BUG: Table.to\_pandas metadata checking fails if columns are not present
* ARROW-1889 - [Python] --exclude is not available in older git versions
* ARROW-1890 - [Python] Masking for date32 arrays not working
* ARROW-1891 - [Python] NaT date32 values are only converted to nulls if from\_pandas is used
* ARROW-1892 - [Python] Unknown list item type: binary
* ARROW-1893 - [Python] test\_primitive\_serialization fails on Python 2.7.3
* ARROW-1895 - [Python] Add field\_name to pandas index metadata
* ARROW-1897 - [Python] Incorrect numpy\_type for pandas metadata of Categoricals
* ARROW-1904 - [C++] Deprecate PrimitiveArray::raw\_values
* ARROW-1906 - [Python] Creating a pyarrow.Array with timestamp of different unit is not casted
* ARROW-1908 - [Python] Construction of arrow table from pandas DataFrame with duplicate column names crashes
* ARROW-1910 - CPP README Brewfile link incorrect
* ARROW-1914 - [C++] make -j may fail to build with -DARROW\_GPU=on
* ARROW-1915 - [Python] Parquet tests should be optional
* ARROW-1916 - [Java] Do not exclude java/dev/checkstyle from source releases
* ARROW-1917 - [GLib] Must set GI\_TYPELIB\_PATH in verify-release-candidate.sh
* ARROW-226 - [C++] libhdfs: feedback to help determining cause of failure in opening file path
* ARROW-641 - [C++] Do not build/run io-hdfs-test if ARROW\_HDFS=off

## Improvement

* ARROW-1087 - [Python] add get\_include to expose directory containing header files
* ARROW-1134 - [C++] Allow C++/CLI projects to build with Arrow
* ARROW-1178 - [Python] Create alternative to Table.from\_pandas that yields a list of RecordBatch objects with a given chunk size
* ARROW-1226 - [C++] Improve / correct doxygen function documentation in arrow::ipc
* ARROW-1371 - [Website] Add "Powered By" page to the website
* ARROW-1455 - [Python] Add Dockerfile for validating Dask integration outside of usual CI
* ARROW-1488 - [C++] Implement ArrayBuilder::Finish in terms of internal::ArrayData
* ARROW-1498 - [GitHub] Add CONTRIBUTING.md and ISSUE\_TEMPLATE.md
* ARROW-1503 - [Python] Add serialization callbacks for pandas objects in pyarrow.serialize
* ARROW-1522 - [C++] Support pyarrow.Buffer as built-in type in pyarrow.serialize
* ARROW-1523 - [C++] Add helper data struct with methods for reading a validity bitmap possibly having a non-zero offset
* ARROW-1524 - [C++] More graceful solution for handling non-zero offsets on inputs and outputs in compute library
* ARROW-1525 - [C++] Change functions in arrow/compare.h to not return Status
* ARROW-1526 - [Python] Unit tests to exercise code path in PARQUET-1100
* ARROW-1535 - [Python] Enable sdist source tarballs to build assuming that Arrow C++ libraries are available on the host system
* ARROW-1538 - [C++] Support Ubuntu 14.04 in .deb packaging automation
* ARROW-1539 - [C++] Remove functions deprecated as of 0.7.0 and prior releases
* ARROW-1556 - [C++] Incorporate AssertArraysEqual function from PARQUET-1100 patch
* ARROW-1588 - [C++/Format] Harden Decimal Format
* ARROW-1593 - [PYTHON] serialize\_pandas should pass through the preserve\_index keyword
* ARROW-1594 - [Python] Enable multi-threaded conversions in Table.from\_pandas
* ARROW-1600 - [C++] Zero-copy Buffer constructor from std::string
* ARROW-1602 - [C++] Add IsValid/IsNotNull method to arrow::Array
* ARROW-1603 - [C++] Add BinaryArray method to get a value as a std::string
* ARROW-1604 - [Python] Support common type aliases in cast(...) and various type= arguments
* ARROW-1605 - [Python] pyarrow.array should be able to yield smaller integer types without an explicit cast
* ARROW-1607 - [C++] Implement DictionaryBuilder for Decimals
* ARROW-1613 - [Java] ArrowReader should not close the input ReadChannel
* ARROW-1616 - [Python] Add "write" method to RecordBatchStreamWriter that dispatches to write\_table/write\_back as appropriate
* ARROW-1626 - Add make targets to run the inter-procedural static analysis tool called "infer".
* ARROW-1627 - [JAVA] Reduce heap usage(Phase 2) - memory footprint in AllocationManager.BufferLedger
* ARROW-1630 - [Serialization] Support Python datetime objects
* ARROW-1635 - Add release management guide for PMCs
* ARROW-1641 - [C++] Do not include <mutex> in public headers
* ARROW-1651 - [JS] Lazy row accessor in Table
* ARROW-1652 - [JS] Separate Vector into BatchVector and CompositeVector
* ARROW-1654 - [Python] pa.DataType cannot be pickled
* ARROW-1662 - Move OSX Dependency management into brew bundle Brewfiles
* ARROW-1665 - [Serialization] Support more custom datatypes in the default serialization context
* ARROW-1666 - [GLib] Enable gtk-doc on Travis CI Mac environment
* ARROW-1671 - [C++] Change arrow::MakeArray to not return Status
* ARROW-1675 - [Python] Use RecordBatch.from\_pandas in FeatherWriter.write
* ARROW-1677 - [Blog] Add blog post on Ray and Arrow Python serialization
* ARROW-1679 - [GLib] Add garrow\_record\_batch\_reader\_read\_next()
* ARROW-1683 - [Python] Restore "TimestampType" to pyarrow namespace
* ARROW-1684 - [Python] Simplify user API for reading nested Parquet columns
* ARROW-1689 - [Python] Categorical Indices Should Be Zero-Copy
* ARROW-1691 - [Java] Conform Java Decimal type implementation to format decisions in ARROW-1588
* ARROW-1701 - [Serialization] Support zero copy PyTorch Tensor serialization
* ARROW-1702 - Update jemalloc in manylinux1 build
* ARROW-1703 - [C++] Vendor exact version of jemalloc we depend on
* ARROW-1707 - Update dev README after movement to GitBox
* ARROW-1716 - [Format/JSON] Use string integer value for Decimals in JSON
* ARROW-1721 - [Python] Support null mask in places where it isn't supported in numpy\_to\_arrow.cc
* ARROW-1724 - [Packaging] Support Ubuntu 17.10
* ARROW-1725 - [Packaging] Upload .deb for Ubuntu 17.10
* ARROW-1726 - [GLib] Add setup description to verify C GLib build
* ARROW-1727 - [Format] Expand Arrow streaming format to permit new dictionaries and deltas / additions to existing dictionaries
* ARROW-1728 - [C++] Run clang-format checks in Travis CI
* ARROW-1737 - [GLib] Use G\_DECLARE\_DERIVABLE\_TYPE
* ARROW-1746 - [Python] Add build dependencies for Arch Linux
* ARROW-1747 - [C++] Don't export symbols of statically linked libraries
* ARROW-1750 - [C++] Remove the need for arrow/util/random.h
* ARROW-1753 - [Python] Provide for matching subclasses with register\_type in serialization context
* ARROW-1755 - [C++] Add build options for MSVC to use static runtime libraries
* ARROW-1758 - [Python] Remove pickle=True option for object serialization
* ARROW-1763 - [Python] DataType should be hashable
* ARROW-1765 - [Doc] Use dependencies from conda in C++ docker build
* ARROW-1785 - [Format/C++/Java] Remove VectorLayout metadata from Flatbuffers metadata
* ARROW-1787 - [Python] Support reading parquet files into DataFrames in a backward compatible way
* ARROW-1794 - [C++/Python] Rename DecimalArray to Decimal128Array
* ARROW-1801 - [Docs] Update install instructions to use red-data-tools repos
* ARROW-1808 - [C++] Make RecordBatch interface virtual to permit record batches that lazy-materialize columns
* ARROW-1809 - [GLib] Use .xml instead of .sgml for GTK-Doc main file
* ARROW-1810 - [Plasma] Remove test shell scripts
* ARROW-1817 - Configure JsonFileReader to read NaN for floats
* ARROW-1826 - [JAVA] Avoid branching at cell level (copyFrom)
* ARROW-1828 - [C++] Implement hash kernel specialization for BooleanType
* ARROW-1834 - [Doc] Build documentation in separate build folders
* ARROW-1838 - [C++] Use compute::Datum uniformly for input argument to kernels
* ARROW-1841 - [JS] Update text-encoding-utf-8 and tslib for node ESModules support
* ARROW-1849 - [GLib] Add input checks to GArrowRecordBatch
* ARROW-1850 - [C++] Use const void* in Writable::Write instead of const uint8\_t*
* ARROW-1854 - [Python] Improve performance of serializing object dtype ndarrays
* ARROW-1855 - [GLib] Add workaround for build failure on macOS
* ARROW-1864 - [Java] Upgrade Netty to 4.1.x
* ARROW-1884 - [C++] Make JsonReader/JsonWriter classes internal APIs
* ARROW-1901 - [Python] Support recursive mkdir for DaskFilesystem
* ARROW-1902 - [Python] Remove mkdir race condition from write\_to\_dataset
* ARROW-1905 - [Python] Add more functions for checking exact types in pyarrow.types
* ARROW-1911 - Add Graphistry to Arrow JS proof points
* ARROW-905 - [Docs] Add Dockerfile for reproducible documentation generation
* ARROW-942 - Support integration testing on Python 2.7
* ARROW-950 - [Site] Add Google Analytics tag

## New Feature

* ARROW-1032 - [JS] Support custom\_metadata
* ARROW-1047 - [Java] Add generalized stream writer and reader interfaces that are decoupled from IO / message framing
* ARROW-1114 - [C++] Create Record Batch Builder class as a reusable and efficient way to transpose row-by-row data to columns
* ARROW-1250 - [Python] Define API for user type checking of array types
* ARROW-1482 - [C++] Implement casts between date32 and date64
* ARROW-1483 - [C++] Implement casts between time32 and time64
* ARROW-1484 - [C++] Implement (safe and unsafe) casts between timestamps and times of different units
* ARROW-1486 - [C++] Decide if arrow::RecordBatch needs to be copyable
* ARROW-1487 - [C++] Implement casts from List<A> to List<B>, where a cast function is defined from any A to B
* ARROW-1559 - [C++] Kernel implementations for "unique" (compute distinct elements of array)
* ARROW-1573 - [C++] Implement stateful kernel function that uses DictionaryBuilder to compute dictionary indices
* ARROW-1575 - [Python] Add pyarrow.column factory function
* ARROW-1577 - [JS] Package release script for NPM modules
* ARROW-1631 -  [C++] Add GRPC to ThirdpartyToolchain.cmake
* ARROW-1637 - [C++] IPC round-trip for null type
* ARROW-1648 - C++: Add cast from Dictionary[NullType] to NullType
* ARROW-1649 - C++: Print number of nulls in PrettyPrint for NullArray
* ARROW-1667 - [GLib] Support Meson
* ARROW-1685 - [GLib] Add GArrowTableReader
* ARROW-1690 - [GLib] Add garrow\_array\_is\_valid()
* ARROW-1697 - [GitHub] Add ISSUE\_TEMPLATE.md
* ARROW-1718 - [Python] Implement casts from timestamp to date32/date64 and support in Array.from\_pandas
* ARROW-1734 - C++/Python: Add cast function on Column-level
* ARROW-1736 - [GLib] Add GArrowCastOptions:allow-time-truncate
* ARROW-1748 - [GLib] Add GArrowRecordBatchBuilder
* ARROW-1752 - [Packaging] Add GPU packages for Debian and Ubuntu
* ARROW-1767 - [C++] Support file reads and writes over 2GB on Windows
* ARROW-1772 - [C++] Add public-api-test module in style of parquet-cpp
* ARROW-1773 - [C++] Add casts from date/time types to compatible signed integers
* ARROW-1775 - Ability to abort created but unsealed Plasma objects
* ARROW-1777 - [C++] Add static ctor ArrayData::Make for nicer syntax in places
* ARROW-1782 - [Python] Expose compressors as pyarrow.compress, pyarrow.decompress
* ARROW-1783 - [Python] Convert SerializedPyObject to/from sequence of component buffers with minimal memory allocation / copying
* ARROW-1784 - [Python] Read and write pandas.DataFrame in pyarrow.serialize by decomposing the BlockManager rather than coercing to Arrow format
* ARROW-1802 - [GLib] Add Arrow GPU support
* ARROW-1806 - [GLib] Add garrow\_record\_batch\_writer\_write\_table()
* ARROW-1844 - [C++] Basic benchmark suite for hash kernels
* ARROW-1857 - [Python] Add switch for boost linkage with static parquet in wheels
* ARROW-1859 - [GLib] Add GArrowDictionaryDataType
* ARROW-1862 - [GLib] Add GArrowDictionaryArray
* ARROW-1874 - [GLib] Add garrow\_array\_unique()
* ARROW-1878 - [GLib] Add garrow\_array\_dictionary\_encode()
* ARROW-480 - [Python] Add accessors for Parquet column statistics
* ARROW-504 - [Python] Add adapter to write pandas.DataFrame in user-selected chunk size to streaming format
* ARROW-507 - [C++/Python] Construct List container from offsets and values subarrays
* ARROW-541 - [JS] Implement JavaScript-compatible implementation
* ARROW-571 - [Python] Add APIs to build Parquet files incrementally from Arrow tables
* ARROW-587 - Add JIRA fix version to merge tool
* ARROW-609 - [C++] Function for casting from days since UNIX epoch to int64 date
* ARROW-838 - [Python] Efficient construction of arrays from non-pandas 1D NumPy arrays
* ARROW-972 - [Python] Add test cases and basic APIs for UnionArray

## Sub-task

* ARROW-1471 - [JAVA] Document requirements and non/requirements for ValueVector updates
* ARROW-1472 - [JAVA] Design updated ValueVector Object Hierarchy
* ARROW-1473 - [JAVA] Create Prototype Code Hierarchy (Implementation Phase 1)
* ARROW-1474 - [JAVA] ValueVector hierarchy (Implementation Phase 2)
* ARROW-1476 - [JAVA] Implement final ValueVector updates
* ARROW-1710 - [Java] Remove non-nullable vectors in new vector class hierarchy
* ARROW-1717 - [Java] Remove public static helper method in vector classes for JSONReader/Writer
* ARROW-1719 - [Java] Remove accessor/mutator
* ARROW-1779 - [Java] Integration test breaks without zeroing out validity vectors
* ARROW-1819 - [Java] Remove legacy vector classes
* ARROW-1867 - [Java] Add BitVector APIs from old vector class
* ARROW-1885 - [Java] Restore previous MapVector class names

## Task

* ARROW-1369 - Support boolean types in the javascript arrow reader library
* ARROW-1818 - Examine Java Dependencies
* ARROW-1827 - [Java] Add checkstyle config file and header file

## Test

* ARROW-1549 - [JS] Integrate auto-generated Arrow test files
* ARROW-1821 - Add integration test case to explicitly check for optional validity buffer
* ARROW-1839 - [C++/Python] Add Decimal Parquet Read/Write Tests

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
