# Options used to build arrow:

## Compile and link options:
### Compiler flags to append when compiling Arrow
set(ARROW_CXXFLAGS "-O3 -mcpu=neoverse-n1 -mno-outline-atomics -mllvm=-polly -mllvm=-polly-vectorizer=stripmine -pipe -fcolor-diagnostics -Wno-error -fmacro-backtrace-limit=0 -D__CUDACC_VER_MAJOR__=12 -D__CUDACC_VER_MINOR__=6 -D__CUDACC_VER_BUILD__=85 -I/usr/local/cuda/include -I/usr/local/include -fcolor-diagnostics -Wno-error -v -v")
### Build static libraries
set(ARROW_BUILD_STATIC "OFF")
### Build shared libraries
set(ARROW_BUILD_SHARED "ON")
### Arbitrary string that identifies the kind of package
### (for informational purposes)
set(ARROW_PACKAGE_KIND "")
### The Arrow git commit id (if any)
set(ARROW_GIT_ID "69050ec1438bb019bcfa1386ffd98e5ba5349331")
### The Arrow git commit description (if any)
set(ARROW_GIT_DESCRIPTION "")
### Whether to create position-independent target
set(ARROW_POSITION_INDEPENDENT_CODE "ON")
### Use ccache when compiling (if available)
set(ARROW_USE_CCACHE "OFF")
### Use sccache when compiling (if available),
### takes precedence over ccache if a storage backend is configured
set(ARROW_USE_SCCACHE "OFF")
### Use ld.gold for linking on Linux (if available)
set(ARROW_USE_LD_GOLD "OFF")
### Use the LLVM lld for linking (if available)
set(ARROW_USE_LLD "ON")
### Use mold for linking on Linux (if available)
set(ARROW_USE_MOLD "OFF")
### Compile-time SIMD optimization level
set(ARROW_SIMD_LEVEL "NEON")
### Max runtime SIMD optimization level
set(ARROW_RUNTIME_SIMD_LEVEL "MAX")
### Build with Altivec if compiler has support
set(ARROW_ALTIVEC "ON")
### Build Arrow libraries with RATH set to $ORIGIN
set(ARROW_RPATH_ORIGIN "ON")
### Build Arrow libraries with install_name set to @rpath
set(ARROW_INSTALL_NAME_RPATH "ON")
### Pass -ggdb flag to debug builds
set(ARROW_GGDB_DEBUG "OFF")
### Whether the system libc is musl or not
set(ARROW_WITH_MUSL "OFF")
### Enable threading in Arrow core
set(ARROW_ENABLE_THREADING "ON")

## Test and benchmark options:
### Build the Arrow examples
set(ARROW_BUILD_EXAMPLES "OFF")
### Build the Arrow googletest unit tests
set(ARROW_BUILD_TESTS "OFF")
### Enable timing-sensitive tests
set(ARROW_ENABLE_TIMING_TESTS "OFF")
### Build the Arrow integration test executables
set(ARROW_BUILD_INTEGRATION "OFF")
### Build the Arrow micro benchmarks
set(ARROW_BUILD_BENCHMARKS "OFF")
### Build the Arrow micro reference benchmarks
set(ARROW_BUILD_BENCHMARKS_REFERENCE "OFF")
### Build benchmarks that do a longer exploration of performance
set(ARROW_BUILD_DETAILED_BENCHMARKS "OFF")
### Linkage of Arrow libraries with unit tests executables.
set(ARROW_TEST_LINKAGE "shared")
### Build Arrow Fuzzing executables
set(ARROW_FUZZING "OFF")
### Enable unit tests which use large memory
set(ARROW_LARGE_MEMORY_TESTS "OFF")

## Coverage options:
### Build with C++ code coverage enabled
set(ARROW_GENERATE_COVERAGE "OFF")

## Checks options:
### Run the test suite using valgrind --tool=memcheck
set(ARROW_TEST_MEMCHECK "OFF")
### Enable Address Sanitizer checks
set(ARROW_USE_ASAN "OFF")
### Enable Thread Sanitizer checks
set(ARROW_USE_TSAN "OFF")
### Enable Undefined Behavior sanitizer checks
set(ARROW_USE_UBSAN "OFF")

## Project component options:
### Build the Arrow Acero Engine Module
set(ARROW_ACERO "ON")
### Build Arrow with Azure support (requires the Azure SDK for C++)
set(ARROW_AZURE "OFF")
### Build Arrow commandline utilities
set(ARROW_BUILD_UTILITIES "ON")
### Build all Arrow Compute kernels
set(ARROW_COMPUTE "ON")
### Build the Arrow CSV Parser Module
set(ARROW_CSV "ON")
### Build the Arrow CUDA extensions (requires CUDA toolkit)
set(ARROW_CUDA "ON")
### Build the Arrow Dataset Modules
set(ARROW_DATASET "ON")
### Build the Arrow Filesystem Layer
set(ARROW_FILESYSTEM "ON")
### Build the Arrow Flight RPC System (requires GRPC, Protocol Buffers)
set(ARROW_FLIGHT "ON")
### Build the Arrow Flight SQL extension
set(ARROW_FLIGHT_SQL "ON")
### Build the Arrow Flight SQL ODBC extension
set(ARROW_FLIGHT_SQL_ODBC "OFF")
### Build the Gandiva libraries
set(ARROW_GANDIVA "OFF")
### Build Arrow with GCS support (requires the Google Cloud Platform 
set(ARROW_GCS "OFF")
### Build the Arrow HDFS bridge
set(ARROW_HDFS "OFF")
### Build the Arrow IPC extensions
set(ARROW_IPC "ON")
### Build the Arrow jemalloc-based allocator
set(ARROW_JEMALLOC "OFF")
### Build Arrow with JSON support (requires RapidJSON)
set(ARROW_JSON "ON")
### Build the Arrow mimalloc-based allocator
set(ARROW_MIMALLOC "OFF")
### Build the Parquet libraries
set(ARROW_PARQUET "ON")
### Build the Arrow ORC adapter
set(ARROW_ORC "OFF")
### Build some components needed by PyArrow.
### (This is a deprecated option. Use CMake presets instead.)
set(ARROW_PYTHON "OFF")
### Build Arrow with S3 support (requires the AWS SDK for C++)
set(ARROW_S3 "ON")
### Build the Arrow S3 filesystem as a dynamic module
set(ARROW_S3_MODULE "OFF")
### Build the Skyhook libraries
set(ARROW_SKYHOOK "OFF")
### Build the Arrow Substrait Consumer Module
set(ARROW_SUBSTRAIT "ON")
### Build Arrow with TensorFlow support enabled
set(ARROW_TENSORFLOW "OFF")
### Build the Arrow testing libraries
set(ARROW_TESTING "OFF")

## Thirdparty toolchain options:
### Method to use for acquiring arrow's build dependencies
set(ARROW_DEPENDENCY_SOURCE "SYSTEM")
### Show output from ExternalProjects rather than just logging to files
set(ARROW_VERBOSE_THIRDPARTY_BUILD "ON")
### Link to shared libraries
set(ARROW_DEPENDENCY_USE_SHARED "ON")
### Rely on Boost shared libraries where relevant
set(ARROW_BOOST_USE_SHARED "ON")
### Rely on Brotli shared libraries where relevant
set(ARROW_BROTLI_USE_SHARED "ON")
### Rely on Bz2 shared libraries where relevant
set(ARROW_BZ2_USE_SHARED "ON")
### Rely on GFlags shared libraries where relevant
set(ARROW_GFLAGS_USE_SHARED "ON")
### Rely on gRPC shared libraries where relevant
set(ARROW_GRPC_USE_SHARED "ON")
### Rely on jemalloc shared libraries where relevant
set(ARROW_JEMALLOC_USE_SHARED "ON")
### Rely on LLVM shared libraries where relevant
set(ARROW_LLVM_USE_SHARED "OFF")
### Rely on lz4 shared libraries where relevant
set(ARROW_LZ4_USE_SHARED "ON")
### Rely on OpenSSL shared libraries where relevant
set(ARROW_OPENSSL_USE_SHARED "ON")
### Rely on Protocol Buffers shared libraries where relevant
set(ARROW_PROTOBUF_USE_SHARED "ON")
### Rely on snappy shared libraries where relevant
set(ARROW_SNAPPY_USE_SHARED "ON")
### Rely on thrift shared libraries where relevant
set(ARROW_THRIFT_USE_SHARED "ON")
### Rely on utf8proc shared libraries where relevant
set(ARROW_UTF8PROC_USE_SHARED "ON")
### Rely on zstd shared libraries where relevant
set(ARROW_ZSTD_USE_SHARED "ON")
### Build libraries with glog support for pluggable logging
set(ARROW_USE_GLOG "ON")
### Build with backtrace support
set(ARROW_WITH_BACKTRACE "OFF")
### Build libraries with OpenTelemetry support for distributed tracing
set(ARROW_WITH_OPENTELEMETRY "OFF")
### Build with Brotli compression
set(ARROW_WITH_BROTLI "ON")
### Build with BZ2 compression
set(ARROW_WITH_BZ2 "ON")
### Build with lz4 compression
set(ARROW_WITH_LZ4 "ON")
### Build with Snappy compression
set(ARROW_WITH_SNAPPY "ON")
### Build with zlib compression
set(ARROW_WITH_ZLIB "ON")
### Build with zstd compression
set(ARROW_WITH_ZSTD "ON")
### Build with support for Unicode properties using the utf8proc library
### (only used if ARROW_COMPUTE is ON or ARROW_GANDIVA is ON)
set(ARROW_WITH_UTF8PROC "ON")
### Build with support for regular expressions using the re2 library
### (only used if ARROW_COMPUTE or ARROW_GANDIVA is ON)
set(ARROW_WITH_RE2 "ON")

## Parquet options:
### Build the Parquet executable CLI tools. Requires static libraries to be built.
set(PARQUET_BUILD_EXECUTABLES "OFF")
### Build the Parquet examples. Requires static libraries to be built.
set(PARQUET_BUILD_EXAMPLES "OFF")
### Build support for encryption. Fail if OpenSSL is not found
set(PARQUET_REQUIRE_ENCRYPTION "ON")

## Gandiva options:
### Include -static-libstdc++ -static-libgcc when linking with
### Gandiva static libraries
set(ARROW_GANDIVA_STATIC_LIBSTDCPP "OFF")
### Compiler flags to append when pre-compiling Gandiva operations
set(ARROW_GANDIVA_PC_CXX_FLAGS "-O3 -mcpu=neoverse-n1 -mno-outline-atomics -mllvm=-polly -mllvm=-polly-vectorizer=stripmine -pipe -fcolor-diagnostics -Wno-error -fmacro-backtrace-limit=0 -D__CUDACC_VER_MAJOR__=12 -D__CUDACC_VER_MINOR__=6 -D__CUDACC_VER_BUILD__=85 -I/usr/local/cuda/include -I/usr/local/include -fcolor-diagnostics -Wno-error -v -v")

## Cross compiling options:
### grpc_cpp_plugin path to be used
set(ARROW_GRPC_CPP_PLUGIN "")

## Advanced developer options:
### Compile with extra error context (line numbers, code)
set(ARROW_EXTRA_ERROR_CONTEXT "ON")
### If enabled install ONLY targets that have already been built. Please be
### advised that if this is enabled 'install' will fail silently on components
### that have not been built
set(ARROW_OPTIONAL_INSTALL "ON")
### Use a custom install directory for GDB plugin.
### In general, you don't need to specify this because the default
### (CMAKE_INSTALL_FULL_BINDIR on Windows, CMAKE_INSTALL_FULL_LIBDIR otherwise)
### is reasonable.
set(ARROW_GDB_INSTALL_DIR "/usr/local/lib64")