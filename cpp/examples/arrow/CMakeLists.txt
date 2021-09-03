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

ADD_ARROW_EXAMPLE(row_wise_conversion_example)

if (ARROW_COMPUTE)
  ADD_ARROW_EXAMPLE(compute_register_example)
endif()

if (ARROW_PARQUET AND ARROW_DATASET)
  if (ARROW_BUILD_SHARED)
    set(DATASET_EXAMPLES_LINK_LIBS arrow_dataset_shared)
  else()
    set(DATASET_EXAMPLES_LINK_LIBS arrow_dataset_static)
  endif()

  ADD_ARROW_EXAMPLE(dataset_parquet_scan_example
    EXTRA_LINK_LIBS
    ${DATASET_EXAMPLES_LINK_LIBS})
  add_dependencies(dataset_parquet_scan_example parquet)

  ADD_ARROW_EXAMPLE(dataset_documentation_example
    EXTRA_LINK_LIBS
    ${DATASET_EXAMPLES_LINK_LIBS})
  add_dependencies(dataset_documentation_example parquet)
endif()
