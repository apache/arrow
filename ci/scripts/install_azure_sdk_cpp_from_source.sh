pushd /tmp && git clone https://github.com/Azure/azure-sdk-for-cpp.git && cd azure-sdk-for-cpp

cd .. && rm -rf build && mkdir build && cd build

# Rust dependency setup
pacman -S mingw-w64-ucrt-x86_64-rust

RUSTC_PATH=$(which rustc | sed 's/\\/\//g')

# Install extra dependencies needed to build azure-sdk-for-cpp from source
vcpkg install azure-macro-utils-c:x64-mingw-dynamic \
  nlohmann-json:x64-mingw-dynamic \
  opentelemetry-cpp:x64-mingw-dynamic \
  wil:x64-mingw-dynamic \
  --triplet x64-mingw-dynamic \
  --host-triplet x64-mingw-dynamic

# TODO: tidy this up to run in CI context, paths are probably wrong as this was for local debugging.
cmake .. -G "Ninja" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_MAKE_PROGRAM=$(cygpath -m $(which ninja)) \
  -DRust_COMPILER=$(cygpath -m $(which rustc)) \
  -DCMAKE_PREFIX_PATH=$(cygpath -m "../../vcpkg/installed/x64-mingw-dynamic") \
  -Dopentelemetry-cpp_DIR=$(cygpath -m "../../vcpkg/installed/x64-mingw-dynamic/share/opentelemetry-cpp/opentelemetry-cpp-config.cmake") \
  -DVCPKG_TARGET_TRIPLET=x64-mingw-dynamic \
  -DVCPKG_HOST_TRIPLET=x64-mingw-dynamic \
  -DVCPKG_MANIFEST_INSTALL=ON \
  -DVCPKG_MANIFEST_FEATURES="core" \
  -DAZURE_SDK_DISABLE_AUTO_VCPKG=ON \
  -DBUILD_TRANSPORT_CURL=ON \
  -DBUILD_TRANSPORT_WINHTTP=OFF \
  -DSTORAGE_XML_BACKEND=Libxml2 \
  -DBUILD_TESTING=OFF \
  -DRUN_UNITTESTS=OFF \
  -DVCPKG_MANIFEST_MODE=OFF \
  -DWARNINGS_AS_ERRORS=OFF \
  -DCMAKE_CXX_FLAGS="-include cstring -include cstdint -include winsock2.h -DWS_XML_STRING_NULL={0,NULL} -DSIO_IDEAL_SEND_BACKLOG_QUERY=0x48000005 -DNOMINMAX=1 -D_WIN32_WINNT=0x0A00"

ninja azure-core
# Azure Identity package seems to be incompatible with MinGW.
# Everything else works but azure-identity has many errors.
# ninja azure-identity
ninja azure-storage-blobs
ninja azure-storage-common
ninja azure-storage-files-datalake

# Go back to the directory that called this script
popd
