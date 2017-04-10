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

#include <arrow-glib/arrow-glib.h>
#include <arrow-glib/array.hpp>
#include <arrow-glib/array-builder.hpp>
#include <arrow-glib/boolean-array.hpp>
#include <arrow-glib/boolean-data-type.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/column.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/double-array.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/float-array.hpp>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table.hpp>
#include <arrow-glib/type.hpp>

#include <arrow-glib/io-file.hpp>
#include <arrow-glib/io-file-mode.hpp>
#include <arrow-glib/io-file-output-stream.hpp>
#include <arrow-glib/io-input-stream.hpp>
#include <arrow-glib/io-memory-mapped-file.hpp>
#include <arrow-glib/io-output-stream.hpp>
#include <arrow-glib/io-random-access-file.hpp>
#include <arrow-glib/io-readable.hpp>
#include <arrow-glib/io-writeable.hpp>

#include <arrow-glib/ipc-file-reader.hpp>
#include <arrow-glib/ipc-file-writer.hpp>
#include <arrow-glib/ipc-metadata-version.hpp>
#include <arrow-glib/ipc-stream-reader.hpp>
#include <arrow-glib/ipc-stream-writer.hpp>
