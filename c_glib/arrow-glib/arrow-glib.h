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

#include <arrow-glib/array.h>
#include <arrow-glib/array-builder.h>
#include <arrow-glib/binary-array.h>
#include <arrow-glib/binary-array-builder.h>
#include <arrow-glib/binary-data-type.h>
#include <arrow-glib/boolean-array.h>
#include <arrow-glib/boolean-array-builder.h>
#include <arrow-glib/boolean-data-type.h>
#include <arrow-glib/chunked-array.h>
#include <arrow-glib/column.h>
#include <arrow-glib/data-type.h>
#include <arrow-glib/double-array.h>
#include <arrow-glib/double-array-builder.h>
#include <arrow-glib/double-data-type.h>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.h>
#include <arrow-glib/field.h>
#include <arrow-glib/float-array.h>
#include <arrow-glib/float-array-builder.h>
#include <arrow-glib/float-data-type.h>
#include <arrow-glib/int8-array.h>
#include <arrow-glib/int8-array-builder.h>
#include <arrow-glib/int8-data-type.h>
#include <arrow-glib/int8-tensor.h>
#include <arrow-glib/int16-array.h>
#include <arrow-glib/int16-array-builder.h>
#include <arrow-glib/int16-data-type.h>
#include <arrow-glib/int32-array.h>
#include <arrow-glib/int32-array-builder.h>
#include <arrow-glib/int32-data-type.h>
#include <arrow-glib/int64-array.h>
#include <arrow-glib/int64-array-builder.h>
#include <arrow-glib/int64-data-type.h>
#include <arrow-glib/list-array.h>
#include <arrow-glib/list-array-builder.h>
#include <arrow-glib/list-data-type.h>
#include <arrow-glib/null-array.h>
#include <arrow-glib/null-data-type.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>
#include <arrow-glib/string-array.h>
#include <arrow-glib/string-array-builder.h>
#include <arrow-glib/string-data-type.h>
#include <arrow-glib/struct-array.h>
#include <arrow-glib/struct-array-builder.h>
#include <arrow-glib/struct-data-type.h>
#include <arrow-glib/table.h>
#include <arrow-glib/tensor.h>
#include <arrow-glib/type.h>
#include <arrow-glib/uint8-array.h>
#include <arrow-glib/uint8-array-builder.h>
#include <arrow-glib/uint8-data-type.h>
#include <arrow-glib/uint8-tensor.h>
#include <arrow-glib/uint16-array.h>
#include <arrow-glib/uint16-array-builder.h>
#include <arrow-glib/uint16-data-type.h>
#include <arrow-glib/uint32-array.h>
#include <arrow-glib/uint32-array-builder.h>
#include <arrow-glib/uint32-data-type.h>
#include <arrow-glib/uint64-array.h>
#include <arrow-glib/uint64-array-builder.h>
#include <arrow-glib/uint64-data-type.h>

#include <arrow-glib/file.h>
#include <arrow-glib/file-mode.h>
#include <arrow-glib/file-output-stream.h>
#include <arrow-glib/input-stream.h>
#include <arrow-glib/memory-mapped-file.h>
#include <arrow-glib/output-stream.h>
#include <arrow-glib/random-access-file.h>
#include <arrow-glib/readable.h>
#include <arrow-glib/writeable.h>
#include <arrow-glib/writeable-file.h>

#include <arrow-glib/file-reader.h>
#include <arrow-glib/file-writer.h>
#include <arrow-glib/metadata-version.h>
#include <arrow-glib/stream-reader.h>
#include <arrow-glib/stream-writer.h>
