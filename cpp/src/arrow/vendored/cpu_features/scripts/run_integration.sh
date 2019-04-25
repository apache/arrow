#!/usr/bin/env bash

readonly SCRIPT_FOLDER=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
readonly PROJECT_FOLDER="${SCRIPT_FOLDER}/.."
readonly ARCHIVE_FOLDER=~/cpu_features_archives
readonly QEMU_INSTALL=${ARCHIVE_FOLDER}/qemu
readonly DEFAULT_CMAKE_ARGS=" -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON"

function extract() {
  case $1 in
    *.tar.bz2)   tar xjf "$1"    ;;
    *.tar.xz)    tar xJf "$1"    ;;
    *.tar.gz)    tar xzf "$1"    ;;
    *)
      echo "don't know how to extract '$1'..."
      exit 1
  esac
}

function unpackifnotexists() {
  mkdir -p "${ARCHIVE_FOLDER}"
  cd "${ARCHIVE_FOLDER}" || exit
  local URL=$1
  local RELATIVE_FOLDER=$2
  local DESTINATION="${ARCHIVE_FOLDER}/${RELATIVE_FOLDER}"
  if [[  ! -d "${DESTINATION}" ]] ; then
    local ARCHIVE_NAME=$(echo ${URL} | sed 's/.*\///')
    test -f "${ARCHIVE_NAME}" || wget -q "${URL}"
    extract "${ARCHIVE_NAME}"
    rm -f "${ARCHIVE_NAME}"
  fi
}

function installqemuifneeded() {
  local VERSION=${QEMU_VERSION:=2.11.1}
  local ARCHES=${QEMU_ARCHES:=arm aarch64 i386 x86_64 mips mipsel mips64 mips64el}
  local TARGETS=${QEMU_TARGETS:=$(echo "$ARCHES" | sed 's#$# #;s#\([^ ]*\) #\1-linux-user #g')}

  if echo "${VERSION} ${TARGETS}" | cmp --silent ${QEMU_INSTALL}/.build -; then
    echo "qemu ${VERSION} up to date!"
    return 0
  fi

  echo "VERSION: ${VERSION}"
  echo "TARGETS: ${TARGETS}"

  rm -rf ${QEMU_INSTALL}

  # Checking for a tarball before downloading makes testing easier :-)
  local QEMU_URL="http://wiki.qemu-project.org/download/qemu-${VERSION}.tar.xz"
  local QEMU_FOLDER="qemu-${VERSION}"
  unpackifnotexists ${QEMU_URL} ${QEMU_FOLDER}
  cd ${QEMU_FOLDER} || exit

  ./configure \
    --prefix="${QEMU_INSTALL}" \
    --target-list="${TARGETS}" \
    --disable-docs \
    --disable-sdl \
    --disable-gtk \
    --disable-gnutls \
    --disable-gcrypt \
    --disable-nettle \
    --disable-curses \
    --static

  make -j4
  make install

  echo "$VERSION $TARGETS" > ${QEMU_INSTALL}/.build
}

function assert_defined(){
  local VALUE=${1}
  : "${VALUE?"${1} needs to be defined"}"
}

function integrate() {
  cd "${PROJECT_FOLDER}"
  case "${OS}" in
   "Windows_NT") CMAKE_BUILD_ARGS="--config Debug --target ALL_BUILD"
                 CMAKE_TEST_FILES="${BUILD_DIR}/test/Debug/*_test.exe"
                 DEMO=${BUILD_DIR}/Debug/list_cpu_features.exe
                 ;;
   *)            CMAKE_BUILD_ARGS="--target all"
                 CMAKE_TEST_FILES="${BUILD_DIR}/test/*_test"
                 DEMO=${BUILD_DIR}/list_cpu_features
                 ;;
  esac

  # Generating CMake configuration
  cmake -H. -B"${BUILD_DIR}" ${DEFAULT_CMAKE_ARGS} "${CMAKE_ADDITIONAL_ARGS[@]}" -G"${CMAKE_GENERATOR:-Unix Makefiles}"

  # Building
  cmake --build "${BUILD_DIR}" ${CMAKE_BUILD_ARGS}

  # Running tests if needed
  if [[ "${QEMU_ARCH}" == "DISABLED" ]]; then
    return
  fi
  RUN_CMD=""
  if [[ -n "${QEMU_ARCH}" ]]; then
    installqemuifneeded
    RUN_CMD="${QEMU_INSTALL}/bin/qemu-${QEMU_ARCH} ${QEMU_ARGS[@]}"
  fi
  for test_binary in ${CMAKE_TEST_FILES}; do
    ${RUN_CMD} ${test_binary}
  done
  ${RUN_CMD} ${DEMO}
}

function expand_linaro_config() {
  assert_defined TARGET
  local LINARO_ROOT_URL=https://releases.linaro.org/components/toolchain/binaries/7.2-2017.11

  local GCC_URL=${LINARO_ROOT_URL}/${TARGET}/gcc-linaro-7.2.1-2017.11-x86_64_${TARGET}.tar.xz
  local GCC_RELATIVE_FOLDER="gcc-linaro-7.2.1-2017.11-x86_64_${TARGET}"
  unpackifnotexists "${GCC_URL}" "${GCC_RELATIVE_FOLDER}"

  local SYSROOT_URL=${LINARO_ROOT_URL}/${TARGET}/sysroot-glibc-linaro-2.25-2017.11-${TARGET}.tar.xz
  local SYSROOT_RELATIVE_FOLDER=sysroot-glibc-linaro-2.25-2017.11-${TARGET}
  unpackifnotexists "${SYSROOT_URL}" "${SYSROOT_RELATIVE_FOLDER}"

  local SYSROOT_FOLDER=${ARCHIVE_FOLDER}/${SYSROOT_RELATIVE_FOLDER}
  local GCC_FOLDER=${ARCHIVE_FOLDER}/${GCC_RELATIVE_FOLDER}

  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_SYSTEM_NAME=Linux)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_SYSTEM_PROCESSOR=${TARGET})

  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_SYSROOT=${SYSROOT_FOLDER})
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_C_COMPILER=${GCC_FOLDER}/bin/${TARGET}-gcc)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_CXX_COMPILER=${GCC_FOLDER}/bin/${TARGET}-g++)

  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_FIND_ROOT_PATH_MODE_PROGRAM=NEVER)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY)

  QEMU_ARGS+=(-L ${SYSROOT_FOLDER})
  QEMU_ARGS+=(-E LD_LIBRARY_PATH=/lib)
}

function expand_codescape_config() {
  assert_defined TARGET
  local DATE=2017.10-08
  local CODESCAPE_URL=https://codescape.mips.com/components/toolchain/${DATE}/Codescape.GNU.Tools.Package.${DATE}.for.MIPS.MTI.Linux.CentOS-5.x86_64.tar.gz
  local GCC_URL=${CODESCAPE_URL}
  local GCC_RELATIVE_FOLDER="mips-mti-linux-gnu/${DATE}"
  unpackifnotexists "${GCC_URL}" "${GCC_RELATIVE_FOLDER}"

  local GCC_FOLDER=${ARCHIVE_FOLDER}/${GCC_RELATIVE_FOLDER}
  local MIPS_FLAGS=""
  local LIBC_FOLDER_SUFFIX=""
  local FLAVOUR=""
  case "${TARGET}" in
    "mips32")    MIPS_FLAGS="-EB -mabi=32"; FLAVOUR="mips-r2-hard"; LIBC_FOLDER_SUFFIX="lib" ;;
    "mips32el")  MIPS_FLAGS="-EL -mabi=32"; FLAVOUR="mipsel-r2-hard"; LIBC_FOLDER_SUFFIX="lib" ;;
    "mips64")    MIPS_FLAGS="-EB -mabi=64"; FLAVOUR="mips-r2-hard"; LIBC_FOLDER_SUFFIX="lib64" ;;
    "mips64el")  MIPS_FLAGS="-EL -mabi=64"; FLAVOUR="mipsel-r2-hard"; LIBC_FOLDER_SUFFIX="lib64" ;;
    *)           echo 'unknown mips platform'; exit 1;;
  esac

  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_FIND_ROOT_PATH=${GCC_FOLDER})
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_SYSTEM_NAME=Linux)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_SYSTEM_PROCESSOR=${TARGET})
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_C_COMPILER=mips-mti-linux-gnu-gcc)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_CXX_COMPILER=mips-mti-linux-gnu-g++)
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_C_COMPILER_ARG1="${MIPS_FLAGS}")
  CMAKE_ADDITIONAL_ARGS+=(-DCMAKE_CXX_COMPILER_ARG1="${MIPS_FLAGS}")

  local SYSROOT_FOLDER=${GCC_FOLDER}/sysroot/${FLAVOUR}

  # Keeping only the sysroot of interest to save on travis cache.
  if [[ "${CONTINUOUS_INTEGRATION}" = "true" ]]; then
    for folder in ${GCC_FOLDER}/sysroot/*; do
      if [[ "${folder}" != "${SYSROOT_FOLDER}" ]]; then
        rm -rf ${folder}
      fi
    done
  fi

  local LIBC_FOLDER=${GCC_FOLDER}/mips-mti-linux-gnu/lib/${FLAVOUR}/${LIBC_FOLDER_SUFFIX}
  QEMU_ARGS+=(-L ${SYSROOT_FOLDER})
  QEMU_ARGS+=(-E LD_PRELOAD=${LIBC_FOLDER}/libstdc++.so.6:${LIBC_FOLDER}/libgcc_s.so.1)
}

function expand_environment_and_integrate() {
  assert_defined PROJECT_FOLDER
  assert_defined TARGET

  BUILD_DIR="${PROJECT_FOLDER}/cmake_build/${TARGET}"
  mkdir -p "${BUILD_DIR}"

  declare -a CONFIG_NAMES=()
  declare -a QEMU_ARGS=()
  declare -a CMAKE_ADDITIONAL_ARGS=()

  case ${TOOLCHAIN} in
    LINARO)    expand_linaro_config     ;;
    CODESCAPE) expand_codescape_config  ;;
    NATIVE)    QEMU_ARCH=""             ;;
    *)         echo "Unknown toolchain '${TOOLCHAIN}'..."; exit 1;;
  esac
  integrate
}

if [ "${CONTINUOUS_INTEGRATION}" = "true" ]; then
  QEMU_ARCHES=${QEMU_ARCH}
  expand_environment_and_integrate
fi
