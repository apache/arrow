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

