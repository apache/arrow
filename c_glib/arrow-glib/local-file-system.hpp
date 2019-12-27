/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow-glib/local-file-system.h>

GArrowLocalFileSystemOptions *
garrow_local_file_system_options_new_raw(
    std::shared_ptr<arrow::fs::LocalFileSystemOptions> *arrow_local_file_system_options);

std::shared_ptr<arrow::fs::LocalFileSystemOptions>
garrow_local_file_system_options_get_raw(GArrowLocalFileSystemOptions *local_file_system_options);
