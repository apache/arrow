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

#include "arrow/compute/cast.h"

#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/compute/cast.h"
#include "arrow/compute/cast_internal.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/logging.h"
#include "arrow/util/reflection_internal.h"

namespace arrow {

using internal::ToTypeName;

namespace compute {
namespace internal {

const FunctionDoc dictionary_decode_doc{
    "decode a dictionary array to normal array",
    ("decode a dictionary array to normal array, which is implemented by cast"),
    {""},
    "null",false};
class DictionaryDecodeMetaFunction : public MetaFunction {
 public:
  DictionaryDecodeMetaFunction()
      : MetaFunction("dictionary_decode", Arity::Unary(), dictionary_decode_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    return null;
  }
};

}  // namespace internal
}  // namespace compute

}  // namespace arrow