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

// This API is EXPERIMENTAL.

#pragma once

namespace arrow {
namespace engine {

/// How strictly to adhere to the input structure when converting between Substrait and
/// Acero representations of a plan. This allows the user to trade conversion accuracy
/// for performance and lenience.
enum class ConversionStrictness {
  /// Prevent information loss by rejecting incoming plans that use features or contain
  /// metadata that cannot be exactly represented in the output format in a way that
  /// will round-trip. Relations/nodes must map one-to-one.
  PEDANTIC,

  /// When an incoming plan uses a feature that cannot be exactly represented in the
  /// output format, attempt to emulate that feature as opposed to immediately
  /// rejecting the plan. For example, a Substrait SortRel with a complex sort key
  /// expression may be emulated using a project-order-project triple. Relations/nodes
  /// will thus map one-to-many.
  PRESERVE_STRUCTURE,

  /// Attempt to prevent performance-related regressions caused by differences in how
  /// operations are represented in the input and output format, by allowing for
  /// optimizations that cross structural boundaries. For example, the converter may
  /// collapse chains of project nodes into one.
  BEST_EFFORT,
};

/// Controls how to convert between Substrait and Acero representations of a plan.
struct ConversionOptions {
  /// Controls how strictly the converter is to adhere to the structure of the input.
  ConversionStrictness strictness = ConversionStrictness::PEDANTIC;
};

}  // namespace engine
}  // namespace arrow
