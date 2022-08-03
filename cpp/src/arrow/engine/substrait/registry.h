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

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/type_fwd.h"

#include "substrait/algebra.pb.h"  // IWYU pragma: export

namespace arrow {

namespace engine {

class ARROW_ENGINE_EXPORT SubstraitConversionRegistry {
 public:
  virtual ~SubstraitConversionRegistry() = default;
  using SubstraitConverter = std::function<Result<std::unique_ptr<substrait::Rel>>(
      const std::shared_ptr<Schema>&, const compute::Declaration&, ExtensionSet*,
      const ConversionOptions&)>;

  virtual Result<SubstraitConverter> GetConverter(const std::string& factory_name) = 0;

  virtual Status RegisterConverter(std::string factory_name,
                                   SubstraitConverter converter) = 0;
};

ARROW_ENGINE_EXPORT SubstraitConversionRegistry* default_substrait_conversion_registry();

}  // namespace engine
}  // namespace arrow
