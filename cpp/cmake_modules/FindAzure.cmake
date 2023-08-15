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

if(Azure_FOUND)
  return()
endif()

set(FIND_PACKAGE_ARGUMENTS)
list(APPEND FIND_PACKAGE_ARGUMENTS CONFIG)
if(Azure_FIND_REQUIRED)
  list(APPEND FIND_PACKAGE_ARGUMENTS REQUIRED)
endif()

find_package(azure-core-cpp ${FIND_PACKAGE_ARGUMENTS})
find_package(azure-identity-cpp ${FIND_PACKAGE_ARGUMENTS})
find_package(azure-storage-blobs-cpp ${FIND_PACKAGE_ARGUMENTS})
find_package(azure-storage-common-cpp ${FIND_PACKAGE_ARGUMENTS})
find_package(azure-storage-files-datalake-cpp ${FIND_PACKAGE_ARGUMENTS})

if(azure-core-cpp_FOUND AND azure-identity-cpp_FOUND AND azure-storage-blobs-cpp_FOUND
  AND azure-storage-common-cpp_FOUND AND azure-storage-files-datalake-cpp_FOUND)
  list(APPEND
    AZURE_SDK_LINK_LIBRARIES
    Azure::azure-core
    Azure::azure-identity
    Azure::azure-storage-blobs
    Azure::azure-storage-common
    Azure::azure-storage-files-datalake
  )
  set(Azure_FOUND TRUE)
else()
  set(Azure_FOUND FALSE)
endif()
