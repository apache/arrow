#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# libarrow downloads the source of its thirdparty dependencies at build time
# when they aren't found on the system. This script downloads those source
# tarballs ahead of time so that they can be shipped in the R package, which
# lets building without network access (e.g. on CRAN) still include features
# like Parquet. nixlibs.R uses them when the dependencies are not found on the
# system, and turns off the features whose dependencies aren't vendored.
#
# To keep the package small, each tarball is trimmed down to what the libarrow
# build uses (no tests, docs, other language bindings, ...). Trimmed tarballs
# no longer match the checksums pinned in versions.txt, so this script records
# the upstream and trimmed checksums of each tarball in DESTDIR/checksums.txt.
# nixlibs.R uses the tarballs whose upstream checksum matches the versions.txt
# of the libarrow source it builds (tools/cpp, or ../cpp in a git checkout) and
# replaces it with the trimmed one for the duration of the build.
#
# checksums.txt also records a checksum of how each tarball was trimmed (see
# trim_recipe_sha256()). A tarball that DESTDIR already has is only downloaded
# and trimmed again if its upstream version or the way it's trimmed changed.
#
# Trimming is done by, in order of preference:
#   1. tools/trim/<name>.sh IN_TARBALL OUT_TARBALL, if it exists, where <name>
#      is the lowercase dependency name (e.g. ARROW_BOOST_URL -> boost). Use
#      this for dependencies that need more than a few `rm`s.
#   2. Otherwise, trim_generic() plus the dependency-specific removals in
#      trim_dependency() below.
#
# Usage: tools/vendor_dependencies.sh [DESTDIR]
# Must be run after tools/cpp has been synced (see `make sync-cpp`). Needs curl
# and cmake (to handle .tar.bz2 without a bzip2 binary).
#
# Environment variables:
#   ARROW_VENDOR_DOWNLOAD_DIR: where to cache the upstream (untrimmed)
#     tarballs between runs (default: tools/vendored-downloads, which is
#     ignored by git and R CMD build).

set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DESTDIR="${1:-${SOURCE_DIR}/vendored}"
DOWNLOAD_DIR="${ARROW_VENDOR_DOWNLOAD_DIR:-${SOURCE_DIR}/vendored-downloads}"
VERSIONS_TXT="${SOURCE_DIR}/cpp/thirdparty/versions.txt"

# If you change this list, you also need to edit
# `vendored_dependencies` (and `vendored_features`) in nixlibs.R
VENDORED_DEPENDENCIES=(
  # Required even for a minimal build
  ARROW_URIPARSER_URL
  ARROW_XSIMD_URL
  # Parquet (ARROW_PARQUET turns on Thrift and simdjson; Thrift needs zlib and
  # Boost headers)
  ARROW_BOOST_URL
  ARROW_SIMDJSON_URL
  ARROW_THRIFT_URL
  ARROW_ZLIB_URL
  # Compression codecs (not Brotli or Zstandard, to keep the package small)
  ARROW_BZIP2_URL
  ARROW_LZ4_URL
  ARROW_SNAPPY_URL
  # Compute (RE2, utf8proc), JSON (RapidJSON, simdjson) and the allocator
  ARROW_MIMALLOC_URL
  ARROW_RAPIDJSON_URL
  ARROW_RE2_URL
  ARROW_UTF8PROC_URL
)

if [ ! -f "${VERSIONS_TXT}" ]; then
  echo "${VERSIONS_TXT} not found. Run this from \`make sync-cpp\`." >&2
  exit 1
fi

# Load `DEPENDENCIES` and the *_BUILD_SHA256_CHECKSUM variables.
# shellcheck source=/dev/null
source "${VERSIONS_TXT}"

# Don't add macOS metadata (._* files, extended attributes) to the tarballs
export COPYFILE_DISABLE=1

# Checksum of the file $1, or of stdin without arguments
sha256() {
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "${1:--}" | cut -d " " -f 1
  else
    shasum -a 256 "${1:--}" | cut -d " " -f 1
  fi
}

# rm -rf the given paths, relative to the current directory
rmd() {
  rm -rf "$@"
}

# Remove everything in the current directory except the given names
keep_only() {
  local entry name keep
  for entry in * .[!.]*; do
    [ -e "${entry}" ] || continue
    keep=false
    for name in "$@"; do
      if [ "${entry}" = "${name}" ]; then
        keep=true
      fi
    done
    if [ "${keep}" = "false" ]; then
      rm -rf "${entry}"
    fi
  done
}

# Remove directories that no bundled build of our dependencies compiles.
# Arguments are paths (relative to the source root) to keep anyway.
trim_generic() {
  local dir keep k
  find . -depth -type d \( -name test -o -name tests -o -name testdata \
    -o -name doc -o -name docs -o -name example -o -name examples \
    -o -name benchmark -o -name benchmarks -o -name bench -o -name fuzz \) \
    -print | while read -r dir; do
    keep=false
    for k in "$@"; do
      if [ "${dir}" = "./${k}" ]; then
        keep=true
      fi
    done
    if [ "${keep}" = "false" ]; then
      rm -rf "${dir}"
    fi
  done
}

# Trim an extracted source tree (the current directory) for the dependency
# named $1 (e.g. "thrift")
trim_dependency() {
  case "$1" in
    bzip2)
      rm -f manual.* sample* ./*.html
      trim_generic
      ;;
    lz4)
      # Only lib/ is built (LZ4_BUILD_CLI=OFF), with build/cmake. LZ4's
      # LICENSE says that the files outside of lib/ are GPL-2.0-or-later
      # unless they say otherwise, so only keep what the build needs plus
      # the license and README (which build/cmake references).
      keep_only lib build LICENSE README.md
      trim_generic
      (cd build && keep_only cmake)
      ;;
    mimalloc)
      rmd ide
      trim_generic
      ;;
    rapidjson)
      rmd bin thirdparty
      trim_generic
      ;;
    re2)
      rmd benchlog python app
      trim_generic
      ;;
    simdjson)
      # Built from src/ and include/. singleheader/ is a 20 MB amalgamation.
      rmd singleheader images jsonexamples tools scripts
      trim_generic
      ;;
    thrift)
      # Only the C++ library is built (BUILD_COMPILER=OFF, WITH_*=OFF)
      rmd tutorial test compiler contrib doc debian
      (cd lib && keep_only cpp CMakeLists.txt)
      trim_generic
      ;;
    zlib)
      rmd contrib
      # zlib's CMakeLists.txt builds test/example.c unless ZLIB_BUILD_EXAMPLES=OFF
      trim_generic test
      ;;
    *)
      trim_generic
      ;;
  esac
}

# Extract $1, trim it for dependency $3 and write the result to $2
trim_tarball() {
  local in="$1" out="$2" name="$3"
  local work src
  if [ -f "${SOURCE_DIR}/trim/${name}.sh" ]; then
    bash "${SOURCE_DIR}/trim/${name}.sh" "${in}" "${out}"
    return
  fi
  # Under TRIM_DIR, so the EXIT trap removes it if anything below fails
  work="$(mktemp -d "${TRIM_DIR}/work.XXXXXX")"
  # Use CMake's tar (libarchive) for .tar.bz2: tar -j needs a bzip2 binary,
  # which e.g. the R CI images don't have.
  case "${in}" in
    *.tar.bz2) (cd "${work}" && cmake -E tar xf "${in}") ;;
    *) tar -xf "${in}" -C "${work}" ;;
  esac
  # Every tarball we vendor has a single top-level directory
  src="$(cd "${work}" && ls)"
  if [ "$(echo "${src}" | wc -l)" -ne 1 ] || [ ! -d "${work}/${src}" ]; then
    echo "Expected a single top-level directory in ${in}" >&2
    return 1
  fi
  (cd "${work}/${src}" && trim_dependency "${name}")
  case "${out}" in
    *.tar.bz2) (cd "${work}" && cmake -E tar cjf "${out}" "${src}") ;;
    *.tar.gz) tar -czf "${out}" -C "${work}" "${src}" ;;
    *)
      echo "Don't know how to compress ${out}" >&2
      return 1
      ;;
  esac
  # Free the space now rather than at exit: the extracted trees can be large
  rm -rf "${work}"
}

# Print a checksum of everything that determines how dependency $1 is
# trimmed, so that a tarball is trimmed again when any of it changes: this
# script, tools/trim/<name>.sh if there is one, and for those, the libarrow
# CMake (e.g. tools/trim/boost.sh reads the Boost libraries to keep from
# ThirdpartyToolchain.cmake) and BOOST_INCLUDE_LIBRARIES (which overrides it).
trim_recipe_sha256() {
  local name="$1"
  {
    cat "${SOURCE_DIR}/vendor_dependencies.sh"
    if [ -f "${SOURCE_DIR}/trim/${name}.sh" ]; then
      cat "${SOURCE_DIR}/trim/${name}.sh"
      cat "${SOURCE_DIR}/cpp/cmake_modules/ThirdpartyToolchain.cmake"
      echo "BOOST_INCLUDE_LIBRARIES=${BOOST_INCLUDE_LIBRARIES:-}"
    fi
  } | sha256
}

mkdir -p "${DESTDIR}" "${DOWNLOAD_DIR}"
# trim_tarball() changes directories, so it needs absolute paths
DOWNLOAD_DIR="$(cd "${DOWNLOAD_DIR}" && pwd)"
TRIM_DIR="$(mktemp -d)"
trap 'rm -rf "${TRIM_DIR}"' EXIT
# Newly trimmed tarballs are staged here and only moved into DESTDIR, together
# with the new checksums.txt, once all of them are done. So if this script
# fails, DESTDIR still has the previous tarballs with a matching checksums.txt.
STAGING_DIR="${TRIM_DIR}/staging"
mkdir "${STAGING_DIR}"
MANIFEST="${DESTDIR}/checksums.txt"
keep=(checksums.txt)
sums="filename upstream_sha256 vendored_sha256 trim_sha256"$'\n'
for vendored_url_var in "${VENDORED_DEPENDENCIES[@]}"; do
  url=""
  for dep_packed in "${DEPENDENCIES[@]}"; do
    # Unpack each entry of the form "$home_var $tar_out $dep_url"
    IFS=" " read -r dep_url_var dep_tar_name dep_url <<< "${dep_packed}"
    if [ "${dep_url_var}" = "${vendored_url_var}" ]; then
      tar_name="${dep_tar_name}"
      url="${dep_url}"
      break
    fi
  done
  if [ -z "${url}" ]; then
    echo "${vendored_url_var} not found in versions.txt" >&2
    exit 1
  fi
  # e.g. ARROW_XSIMD_URL -> ARROW_XSIMD_BUILD_SHA256_CHECKSUM and xsimd
  checksum_var="${vendored_url_var%_URL}_BUILD_SHA256_CHECKSUM"
  checksum="${!checksum_var}"
  name="$(echo "${vendored_url_var}" | sed -e 's/^ARROW_//' -e 's/_URL$//' | tr '[:upper:]' '[:lower:]')"
  keep+=("${tar_name}")

  trim_checksum="$(trim_recipe_sha256 "${name}")"

  # Skip the download and the trimming if DESTDIR already has this tarball,
  # trimmed the same way from the same upstream tarball
  out="${DESTDIR}/${tar_name}"
  if [ -f "${out}" ] && [ -f "${MANIFEST}" ]; then
    recorded="$(awk -v f="${tar_name}" '$1 == f { print $2, $3, $4 }' "${MANIFEST}")"
    if [ "${recorded}" = "${checksum} $(sha256 "${out}") ${trim_checksum}" ]; then
      echo "${tar_name} is up to date"
      sums="${sums}${tar_name} ${recorded}"$'\n'
      continue
    fi
  fi

  upstream="${DOWNLOAD_DIR}/${tar_name}"
  if [ ! -f "${upstream}" ] || [ "$(sha256 "${upstream}")" != "${checksum}" ]; then
    echo "Downloading ${url}"
    curl --fail --silent --show-error --location --retry 3 --output "${upstream}" "${url}"
    if [ "$(sha256 "${upstream}")" != "${checksum}" ]; then
      echo "Checksum mismatch for ${upstream}" >&2
      rm -f "${upstream}"
      exit 1
    fi
  fi

  # Keep the file name, so the extension says how to compress it
  echo "Trimming ${tar_name}"
  trim_tarball "${upstream}" "${STAGING_DIR}/${tar_name}" "${name}"
  sums="${sums}${tar_name} ${checksum} $(sha256 "${STAGING_DIR}/${tar_name}") ${trim_checksum}"$'\n'
done

# Replace the tarballs and checksums.txt together. Remove checksums.txt first:
# without it nixlibs.R uses none of the tarballs, so being interrupted part way
# through can't leave tarballs that don't match their recorded checksums.
rm -f "${MANIFEST}"
for file in "${STAGING_DIR}"/*; do
  [ -e "${file}" ] || continue
  mv "${file}" "${DESTDIR}/"
done
# See read_vendored_checksums() in nixlibs.R
printf "%s" "${sums}" > "${TRIM_DIR}/checksums.txt"
mv "${TRIM_DIR}/checksums.txt" "${MANIFEST}"

# Remove tarballs for other versions (or dependencies we no longer vendor)
for file in "${DESTDIR}"/*; do
  [ -e "${file}" ] || continue
  stale=true
  for name in "${keep[@]}"; do
    if [ "$(basename "${file}")" = "${name}" ]; then
      stale=false
    fi
  done
  if [ "${stale}" = "true" ]; then
    rm -f "${file}"
  fi
done
