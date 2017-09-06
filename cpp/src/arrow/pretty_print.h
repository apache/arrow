// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_PRETTY_PRINT_H
#define ARROW_PRETTY_PRINT_H

#include <ostream>
#include <string>

#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Status;

struct PrettyPrintOptions {
  int indent;
};

/// \brief Print human-readable representation of RecordBatch
ARROW_EXPORT
Status PrettyPrint(const RecordBatch& batch, int indent, std::ostream* sink);

/// \brief Print human-readable representation of Array
ARROW_EXPORT
Status PrettyPrint(const Array& arr, int indent, std::ostream* sink);

ARROW_EXPORT
Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::ostream* sink);

ARROW_EXPORT
Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::string* result);

ARROW_EXPORT
Status DebugPrint(const Array& arr, int indent);

}  // namespace arrow

#endif  // ARROW_PRETTY_PRINT_H
