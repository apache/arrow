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

#include <arrow/filesystem/api.h>

#include <arrow-glib/file-system.h>

GARROW_EXTERN
GArrowFileInfo *
garrow_file_info_new_raw(const arrow::fs::FileInfo &arrow_file_info);

GARROW_EXTERN
arrow::fs::FileInfo *
garrow_file_info_get_raw(GArrowFileInfo *file_info);

GARROW_EXTERN
GArrowFileSystem *
garrow_file_system_new_raw(std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system);

GARROW_EXTERN
std::shared_ptr<arrow::fs::FileSystem>
garrow_file_system_get_raw(GArrowFileSystem *file_system);

GARROW_EXTERN
GArrowSubTreeFileSystem *
garrow_sub_tree_file_system_new_raw(
  std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system,
  GArrowFileSystem *base_file_system);

GARROW_EXTERN
GArrowSlowFileSystem *
garrow_slow_file_system_new_raw(std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system,
                                GArrowFileSystem *base_file_system);

#ifdef ARROW_S3
GARROW_EXTERN
arrow::fs::S3GlobalOptions *
garrow_s3_global_options_get_raw(GArrowS3GlobalOptions *options);
#endif
