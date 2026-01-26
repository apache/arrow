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

# Headers: public api
arrow_install_all_headers("parquet/encryption")

if(ARROW_TESTING)
  # Add library for DBPATestAgent
  add_library(DBPATestAgent SHARED external/dbpa_test_agent.cc)

  # DBPATestAgent configuration
  target_link_libraries(DBPATestAgent PUBLIC arrow_shared magic_enum_header_only tcb_span
                                             dbps_interface)

  set_target_properties(DBPATestAgent PROPERTIES OUTPUT_NAME "DBPATestAgent")

  # Add test for LoadableEncryptorUtils
  # (depends on DBPATestAgent)
  add_parquet_test(loadable-encryptor-utils-test
                   SOURCES
                   external/loadable_encryptor_utils_test.cc
                   external/test_utils.cc
                   LABELS
                   "parquet-tests"
                   "encryption-tests")

  # Add test for DBPALibraryWrapper
  add_parquet_test(dbpa-library-wrapper-test
                   SOURCES
                   external/dbpa_library_wrapper_test.cc
                   LABELS
                   "parquet-tests"
                   "encryption-tests")

  # Add a test for dbpa_enum_utils
  add_parquet_test(dbpa-utils-test
                   SOURCES
                   external/dbpa_enum_utils_test.cc
                   LABELS
                   "parquet-tests"
                   "encryption-tests")

  # Add a test for dbpa_executor_test
  add_parquet_test(dbpa-executor-test
                   SOURCES
                   external/dbpa_executor_test.cc
                   LABELS
                   "parquet-tests"
                   "encryption-tests")
endif()
