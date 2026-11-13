// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// BCrypt algorithm pseudo-handles (available since Windows 10 1703). The
// mingw-w64 headers shipped with Rtools45 (mingw-w64 11.0) don't define them,
// but the bundled google-cloud-cpp uses them in google/cloud/internal/win32/.
// This file is force-included into every translation unit via CXXFLAGS by
// ci/scripts/r_windows_build.sh, so it deliberately includes nothing itself:
// BCRYPT_ALG_HANDLE is resolved at the point of use. Values match mingw-w64
// master include/bcrypt.h.
#ifndef BCRYPT_MD5_ALG_HANDLE
#define BCRYPT_MD5_ALG_HANDLE ((BCRYPT_ALG_HANDLE)0x00000021)
#endif
#ifndef BCRYPT_SHA256_ALG_HANDLE
#define BCRYPT_SHA256_ALG_HANDLE ((BCRYPT_ALG_HANDLE)0x00000041)
#endif
#ifndef BCRYPT_HMAC_SHA256_ALG_HANDLE
#define BCRYPT_HMAC_SHA256_ALG_HANDLE ((BCRYPT_ALG_HANDLE)0x000000b1)
#endif
#ifndef BCRYPT_RSA_ALG_HANDLE
#define BCRYPT_RSA_ALG_HANDLE ((BCRYPT_ALG_HANDLE)0x000000e1)
#endif
