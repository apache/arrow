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

#ifndef GANDIVA_IN_HOLDER_H
#define GANDIVA_IN_HOLDER_H

#include <string>
#include <unordered_set>

#include "gandiva/arrow.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

#ifdef GDV_HELPERS
namespace helpers {
#endif

/// Function Holder for IN Expressions
class InHolder {
 public:
  explicit InHolder(const VariantSet& values) : values(values) {}

  bool HasValue(int32_t value) const {
    return values.find(Variant(value)) != values.end();
  }

  bool HasValue(int64_t value) const {
    return values.find(Variant(value)) != values.end();
  }

  bool HasValue(std::string value) const {
    return values.find(Variant(value)) != values.end();
  }

 private:
  VariantSet values;
};

#ifdef GDV_HELPERS
}  // namespace helpers
#endif

}  // namespace gandiva

#endif  // GANDIVA_IN_HOLDER_H
