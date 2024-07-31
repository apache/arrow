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

#include <glib-object.h>

#include <arrow-glib/version.h>

#include <arrow-glib/array.h>
#include <arrow-glib/array-builder.h>
#include <arrow-glib/chunked-array.h>
#include <arrow-glib/codec.h>
#include <arrow-glib/compute.h>
#include <arrow-glib/data-type.h>
#include <arrow-glib/datum.h>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.h>
#include <arrow-glib/expression.h>
#include <arrow-glib/field.h>
#include <arrow-glib/interval.h>
#include <arrow-glib/memory-pool.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/scalar.h>
#include <arrow-glib/schema.h>
#include <arrow-glib/table.h>
#include <arrow-glib/table-builder.h>
#include <arrow-glib/tensor.h>
#include <arrow-glib/type.h>

#include <arrow-glib/file.h>
#include <arrow-glib/file-mode.h>
#include <arrow-glib/input-stream.h>
#include <arrow-glib/output-stream.h>
#include <arrow-glib/readable.h>
#include <arrow-glib/writable.h>
#include <arrow-glib/writable-file.h>

#include <arrow-glib/ipc-options.h>
#include <arrow-glib/metadata-version.h>
#include <arrow-glib/reader.h>
#include <arrow-glib/writer.h>

#include <arrow-glib/file-system.h>
#include <arrow-glib/local-file-system.h>
