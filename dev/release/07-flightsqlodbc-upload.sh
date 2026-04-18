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
#
# FlightSQL ODBC Release Signing Script
#
# This script handles the signing of FlightSQL ODBC Windows binaries and MSI
# installer. It requires jsign to be configured with ASF code signing
# credentials. Keep reading below:
#
# Required environment variables:
#
#   ESIGNER_STOREPASS - The ssl.com credentials in "username|password" format
#   ESIGNER_KEYPASS   - The ssl.com eSigner secret code (not the PIN)
#
# Set these in .env.
#
# How to get ESIGNER_KEYPASS:
#
# 1. Log into ssl.com
# 2. In your Dashboard, under "invitations", click the link under the order. Or
#    go to Orders, find the order, expand the order, and click "certificate
#    details"
# 3. Enter your PIN to get your OTP. This is ESIGNER_KEYPASS.
#
# If you don't have access, see https://infra.apache.org/code-signing-use.html.

set -e
set -u
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit 1
fi

if [ -z "${ESIGNER_STOREPASS:-}" ]; then
  echo "ERROR: ESIGNER_STOREPASS is not set" >&2
  exit 1
fi
if [ -z "${ESIGNER_KEYPASS:-}" ]; then
  echo "ERROR: ESIGNER_KEYPASS is not set" >&2
  exit 1
fi

. "${SOURCE_DIR}/utils-env.sh"

version=$1
rc=$2

version_with_rc="${version}-rc${rc}"
tag="apache-arrow-${version_with_rc}"

dll_unsigned="arrow_flight_sql_odbc_unsigned.dll"
dll_signed="arrow_flight_sql_odbc.dll"

: "${GITHUB_REPOSITORY:=apache/arrow}"

: "${PHASE_DEFAULT=1}"
: "${PHASE_SIGN_DLL=${PHASE_DEFAULT}}"
: "${PHASE_BUILD_MSI=${PHASE_DEFAULT}}"
: "${PHASE_SIGN_MSI=${PHASE_DEFAULT}}"

if [ "${PHASE_SIGN_DLL}" -eq 0 ] && [ "${PHASE_BUILD_MSI}" -eq 0 ] && [ "${PHASE_SIGN_MSI}" -eq 0 ]; then
  echo "No phases specified. Exiting."
  exit 1
fi

# Utility function to use jsign to check if a file is signed or not
is_signed() {
  local file="$1"
  local exit_code
  jsign extract --format PEM "${file}" > /dev/null 2>&1
  exit_code=$?
  # jsign writes a PEM file even though it also prints to stdout. Clean up after
  # it. Use -f since so it still runs on unsigned files without error.
  rm -f "${file}.sig.pem"

  return ${exit_code}
}

# Use dev/release/tmp for temporary files
tmp_dir="${SOURCE_DIR}/tmp"
if [ -e "${tmp_dir}" ]; then
  echo "ERROR: temp dir already exists: ${tmp_dir}. Remove it manually and run again." >&2
  exit 1
fi

if [ "${PHASE_SIGN_DLL}" -gt 0 ]; then
  echo "[1/9] Downloading ${dll_unsigned} from release..."
  gh release download "${tag}" \
    --repo "${GITHUB_REPOSITORY}" \
    --pattern "${dll_unsigned}" \
    --dir "${tmp_dir}"
  if is_signed "${tmp_dir}/${dll_unsigned}"; then
    echo "ERROR: ${dll_unsigned} is already signed" >&2
    exit 1
  fi

  echo "[2/9] Signing ${dll_signed}..."
  echo "NOTE: Running jsign. You may be prompted for your OTP PIN..."
  jsign --storetype ESIGNER \
    --alias d97c5110-c66a-4c0c-ac0c-1cd6af812ee6 \
    --storepass "${ESIGNER_STOREPASS}" \
    --keypass "${ESIGNER_KEYPASS}" \
    --tsaurl="http://ts.ssl.com" \
    --tsmode RFC3161 \
    --alg SHA256 \
    "${tmp_dir}/${dll_unsigned}"
  if ! is_signed "${tmp_dir}/${dll_signed}"; then
    echo "ERROR: ${dll_signed} is not signed" >&2
    exit 1
  fi

  echo "[3/9] Uploading signed DLL to GitHub Release..."
  gh release upload "${tag}" \
    --repo "${GITHUB_REPOSITORY}" \
    --clobber \
    "${tmp_dir}/${dll_signed}"

  echo "[4/9] Removing unsigned DLL from GitHub Release..."
  gh release delete-asset "${tag}" \
    --repo "${GITHUB_REPOSITORY}" \
    --yes \
    "${dll_unsigned}"
fi

if [ "${PHASE_BUILD_MSI}" -gt 0 ]; then
  echo "[5/9] Triggering odbc_release_step in cpp_extra.yml workflow..."
  run_url=$(gh workflow run cpp_extra.yml \
    --repo "${GITHUB_REPOSITORY}" \
    --ref "${tag}" \
    --field odbc_release_step=true 2>&1 | grep -oE 'https://[^ ]+')
  run_id=${run_url##*/} # Extract the run ID from the URL (the part after the last slash)
  if [ -z "${run_id}" ]; then
    echo "ERROR: failed to get run ID from workflow trigger" >&2
    exit 1
  fi
  echo "Triggered run: ${run_url}"

  echo "[6/9] Waiting for workflow to complete. This can take a very long time..."
  gh run watch "${run_id}" --repo "${GITHUB_REPOSITORY}" --exit-status --interval 60
  echo "Run id ${run_id} completed."
fi

if [ "${PHASE_SIGN_MSI}" -gt 0 ]; then
  echo "[7/9] Downloading unsigned MSI..."
  gh release download "${tag}" \
    --repo "${GITHUB_REPOSITORY}" \
    --pattern "Apache-Arrow-Flight-SQL-ODBC-${version}-win64.msi" \
    --dir "${tmp_dir}"
  msi="${tmp_dir}/Apache-Arrow-Flight-SQL-ODBC-${version}-win64.msi"
  if is_signed "${msi}"; then
    echo "ERROR: MSI is already signed" >&2
    exit 1
  fi

  echo "[8/9] Signing MSI..."
  echo "NOTE: Running jsign. You may be prompted for your OTP PIN..."
  jsign --storetype ESIGNER \
    --alias d97c5110-c66a-4c0c-ac0c-1cd6af812ee6 \
    --storepass "${ESIGNER_STOREPASS}" \
    --keypass "${ESIGNER_KEYPASS}" \
    --tsaurl="http://ts.ssl.com" \
    --tsmode RFC3161 \
    --alg SHA256 \
    "${msi}"
  if ! is_signed "${msi}"; then
    echo "ERROR: MSI is not signed" >&2
    exit 1
  fi

  echo "[9/9] Uploading signed MSI to GitHub Release..."
  gh release upload "${tag}" \
    --repo "${GITHUB_REPOSITORY}" \
    --clobber \
    "${msi}"
fi
