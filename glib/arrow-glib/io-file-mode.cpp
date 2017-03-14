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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/io-file-mode.hpp>

/**
 * SECTION: io-file-mode
 * @title: GArrowIOFileMode
 * @short_description: File mode mapping between Arrow and arrow-glib
 *
 * #GArrowIOFileMode provides file modes corresponding to
 * `arrow::io::FileMode::type` values.
 */

GArrowIOFileMode
garrow_io_file_mode_from_raw(arrow::io::FileMode::type mode)
{
  switch (mode) {
  case arrow::io::FileMode::type::READ:
    return GARROW_IO_FILE_MODE_READ;
  case arrow::io::FileMode::type::WRITE:
    return GARROW_IO_FILE_MODE_WRITE;
  case arrow::io::FileMode::type::READWRITE:
    return GARROW_IO_FILE_MODE_READWRITE;
  default:
    return GARROW_IO_FILE_MODE_READ;
  }
}

arrow::io::FileMode::type
garrow_io_file_mode_to_raw(GArrowIOFileMode mode)
{
  switch (mode) {
  case GARROW_IO_FILE_MODE_READ:
    return arrow::io::FileMode::type::READ;
  case GARROW_IO_FILE_MODE_WRITE:
    return arrow::io::FileMode::type::WRITE;
  case GARROW_IO_FILE_MODE_READWRITE:
    return arrow::io::FileMode::type::READWRITE;
  default:
    return arrow::io::FileMode::type::READ;
  }
}
