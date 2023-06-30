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

#include <parquet/metadata.h>

#include <parquet-glib/metadata.h>

GParquetColumnChunkMetadata *
gparquet_column_chunk_metadata_new_raw(
  parquet::ColumnChunkMetaData *parquet_metadata,
  GParquetRowGroupMetadata *owner);
parquet::ColumnChunkMetaData *
gparquet_column_chunk_metadata_get_raw(GParquetColumnChunkMetadata *metadata);

GParquetRowGroupMetadata *
gparquet_row_group_metadata_new_raw(
  parquet::RowGroupMetaData *parquet_metadata,
  GParquetFileMetadata *owner);
parquet::RowGroupMetaData *
gparquet_row_group_metadata_get_raw(GParquetRowGroupMetadata *metadata);

GParquetFileMetadata *
gparquet_file_metadata_new_raw(
  std::shared_ptr<parquet::FileMetaData> *parquet_metadata);
std::shared_ptr<parquet::FileMetaData>
gparquet_file_metadata_get_raw(GParquetFileMetadata *metadata);
