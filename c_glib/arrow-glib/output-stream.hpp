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

#include <arrow/io/interfaces.h>

#include <arrow-glib/output-stream.h>

/**
 * GArrowOutputStreamInterface:
 *
 * It wraps `arrow::io::OutputStream`.
 */
struct _GArrowOutputStreamInterface
{
  GTypeInterface parent_iface;

  std::shared_ptr<arrow::io::OutputStream> (*get_raw)(GArrowOutputStream *file);
};

std::shared_ptr<arrow::io::OutputStream> garrow_output_stream_get_raw(GArrowOutputStream *output_stream);
