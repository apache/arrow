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

Param([Parameter(Mandatory=$true)][int32]$n_bits)

echo "" > env.ps1

If (${n_bits} -eq 32) {
    $carch = "i686"
} Else {
    $carch = "x86_64"
}
echo "`$Env:MINGW_CHOST = `"${carch}-w64-mingw32`"" `
  >> ../env.ps1
echo "`$Env:MINGW_PACKAGE_PREFIX = `"mingw-w64-${carch}`"" `
  >> ../env.ps1
echo "`$Env:MINGW_PREFIX = `"/mingw${n_bits}`"" `
  >> ../env.ps1
echo "`$Env:MSYSTEM_CARCH = `"${carch}`"" `
  >> ../env.ps1
echo "`$Env:MSYSTEM_CHOST = `"${carch}-w64-mingw32`"" `
  >> ../env.ps1
echo "`$Env:MSYSTEM_PREFIX = `"/mingw${n_bits}`"" `
  >> ../env.ps1
echo "`$Env:MSYSTEM = `"MINGW${n_bits}`"" `
  >> ../env.ps1

$MSYS_ROOT_WINDOWS = "C:\msys64"
echo "`$Env:MSYS_ROOT_WINDOWS = `"${MSYS_ROOT_WINDOWS}`"" `
  >> ../env.ps1

$MINGW_PREFIX_WINDOWS = "${MSYS_ROOT_WINDOWS}\mingw${n_bits}"
echo "`$Env:MINGW_PREFIX_WINDOWS = `"${MINGW_PREFIX_WINDOWS}`"" `
  >> ../env.ps1

echo "`$Env:PATH = `"${MSYS_ROOT_WINDOWS}\usr\bin;`${Env:PATH}`"" `
  >> ../env.ps1
echo "`$Env:PATH = `"${MINGW_PREFIX_WINDOWS}\bin;`${Env:PATH}`"" `
  >> ../env.ps1
