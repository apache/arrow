# Maintainer: Jeroen Ooms
_realname=arrow
pkgbase=mingw-w64-${_realname}
pkgname="${MINGW_PACKAGE_PREFIX}-${_realname}"
pkgver=0.13.0.9000
pkgrel=8000
pkgdesc="Apache Arrow is a cross-language development platform for in-memory data (mingw-w64)"
arch=("any")
url="https://arrow.apache.org/"
license=("Apache-2.0")
depends=("${MINGW_PACKAGE_PREFIX}-boost"
         "${MINGW_PACKAGE_PREFIX}-zlib"
         "${MINGW_PACKAGE_PREFIX}-double-conversion"
         "${MINGW_PACKAGE_PREFIX}-thrift"
         "${MINGW_PACKAGE_PREFIX}-zlib")
makedepends=("${MINGW_PACKAGE_PREFIX}-cmake"
             "${MINGW_PACKAGE_PREFIX}-gcc")
options=("staticlibs" "strip" "!buildflags")
source=("${_realname}"::"git+https://github.com/apache/arrow")
sha256sums=("SKIP")

cmake_build_type=release

#source_dir=${_realname}-${pkgver}
#source_dir=apache-${_realname}-${pkgver}
source_dir=arrow
cpp_build_dir=build-${CARCH}-cpp
c_glib_build_dir=build-${CARCH}-c-glib

pkgver() {
  cd "$source_dir"
  grep Version r/DESCRIPTION | cut -d " " -f 2
}

prepare() {
  pushd ${source_dir}
  #patch -p1 -N -i ${srcdir}/3923.patch
  popd
}

build() {
  ARROW_CPP_DIR="$(pwd)/${source_dir}/cpp"
  [[ -d ${cpp_build_dir} ]] && rm -rf ${cpp_build_dir}
  mkdir -p ${cpp_build_dir}
  pushd ${cpp_build_dir}

  # Workaround to fix static libparquet
  export CXXFLAGS="-DARROW_STATIC"

  export CC="/C/Rtools${MINGW_PREFIX/mingw/mingw_}/bin/gcc"
  export CXX="/C/Rtools${MINGW_PREFIX/mingw/mingw_}/bin/g++"
  export PATH="/C/Rtools${MINGW_PREFIX/mingw/mingw_}/bin:$PATH"
  export CPPFLAGS="-I${MINGW_PREFIX}/include"
  export LIBS="-L${MINGW_PREFIX}/libs"

  MSYS2_ARG_CONV_EXCL="-DCMAKE_INSTALL_PREFIX=" \
    ${MINGW_PREFIX}/bin/cmake.exe \
    ${ARROW_CPP_DIR} \
    -G "MSYS Makefiles" \
    -DCMAKE_INSTALL_PREFIX=${MINGW_PREFIX} \
    -DCMAKE_BUILD_TYPE=${cmake_build_type} \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_PARQUET=ON \
    -DARROW_PLASMA=OFF \
    -DARROW_HDFS=OFF \
    -DARROW_PYTHON=OFF \
    -DARROW_BOOST_USE_SHARED=OFF \
    -DARROW_WITH_SNAPPY=OFF \
    -DARROW_WITH_ZSTD=OFF \
    -DARROW_WITH_LZ4=OFF \
    -DARROW_JEMALLOC=OFF \
    -DARROW_BUILD_SHARED=OFF\
    -DARROW_BOOST_VENDORED=OFF \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_BROTLI=OFF \
    -DARROW_USE_GLOG=OFF \
    -DARROW_BUILD_UTILITIES=ON \
    -DARROW_TEST_LINKAGE="static" \
    -Ddouble-conversion_ROOT="${MINGW_PREFIX}" \
    -DThrift_ROOT="${MINGW_PREFIX}"

  sed -i 's/-fPIC/ /g' flatbuffers_ep-prefix/src/flatbuffers_ep-stamp/flatbuffers_ep-configure-RELEASE.cmake

  make
  popd
}

check() {
  # TODO
  # make -C ${cpp_build_dir} test

  # PATH=$(pwd)/${c_glib_build_dir}/arrow-glib:$(pwd)/${cpp_build_dir}/${cmake_build_type}:$PATH \
  #   ninja -C ${c_glib_build_dir} test

  :
}

package() {
  make -C ${cpp_build_dir} DESTDIR="${pkgdir}" install

  local PREFIX_DEPS=$(cygpath -am ${MINGW_PREFIX})
  pushd "${pkgdir}${MINGW_PREFIX}/lib/pkgconfig"
  for pc in *.pc; do
    sed -s "s|${PREFIX_DEPS}|${MINGW_PREFIX}|g" -i $pc
  done
  popd
}
