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

#include "arrow/engine/substrait/function_internal.h"

#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/compute/api_scalar.h"


namespace arrow{
namespace engine{

SubstraitToArrow substrait_add_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(1);
  auto value_2 = call.args(2);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));
  auto options = call.args(0);
  if (options.has_enum_()) {
    auto overflow_handling = options.enum_();
    if(overflow_handling.has_specified()){
    std::string overflow_type = overflow_handling.specified();
    if(overflow_type == "SILENT"){
      return arrow::compute::call("add", {expression_1,expression_2}, compute::ArithmeticOptions());
    } else if (overflow_type == "SATURATE") {
      return Status::Invalid("Arrow does not support a saturating add");
    } else {
      return arrow::compute::call("add_checked", {expression_1,expression_2}, compute::ArithmeticOptions(true));
    }
  } else {
    return arrow::compute::call("add", {expression_1,expression_2}, compute::ArithmeticOptions());
  }
  } else {
      return Status::Invalid("Substrait Function Options should be an enum");
  }
};

const ArrowToSubstrait arrow_add_to_substrait = [] (const arrow::compute::Expression::Call& call, ExtensionSet* ext_set_) -> Result<substrait::Expression::ScalarFunction*> {
  substrait::Expression::ScalarFunction substrait_call;
  
  ARROW_ASSIGN_OR_RAISE(auto function_reference, ext_set_->EncodeFunction("add"));
  substrait_call.set_function_reference(function_reference);

  substrait::Expression::Enum options;
  std::string overflow_handling = "ERROR";
  options.set_specified(overflow_handling);
  substrait_call.add_args()->set_allocated_enum_(&options);
  
  auto expression_1 = call.arguments[0];
  auto expression_2 = call.arguments[1];
  
  ARROW_ASSIGN_OR_RAISE(auto value_1, ToProto(expression_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto value_2, ToProto(expression_2, ext_set_));

  substrait_call.add_args()->CopyFrom(*value_1);
  substrait_call.add_args()->CopyFrom(*value_2);
  return &substrait_call;
};
// ArrowToSubstrait arrow_unchecked_add_to_substrait = [] (const arrow::compute::Expression::Call& call, std::vector<substrait::Expression> args) {
//   auto overflow_behavior = substrait::Expression::Enum;
//   overflow_behavior.set_specified("SILENT");
//   auto substrait_call = substrait::FunctionSignature_Scalar;
//   substrait_call.add_name("add");

//   substrait_call.add_args(std::move(args));
//   substrait_call.add_args({overflow_behavior});
//   return substrait_call;
// };


// Boolean Functions mapping
SubstraitToArrow substrait_not_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  return arrow::compute::call("invert", {expression_1});
};

SubstraitToArrow substrait_or_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  return arrow::compute::call("or_kleene", {expression_1});
};

SubstraitToArrow substrait_and_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  return arrow::compute::call("and_kleene", {expression_1});
};

SubstraitToArrow substrait_xor_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  auto value_2 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));
  return arrow::compute::call("xor", {expression_1, expression_2});
};

// ArrowToSubstrait arrow_invert_to_substrait = [] (const arrow::compute::Expression::Call& call, std::vector<substrait::Expression> args) {
//   auto substrait_call = substrait::FunctionSignature_Scalar;
//   substrait_call.add_name("not");
//   substrait_call.add_args(std::move(args));
//   return substrait_call;
// };

// ArrowToSubstrait arrow_or_kleene_to_substrait = [] (const arrow::compute::Expression::Call& call, std::vector<substrait::Expression> args) {
//   auto substrait_call = substrait::FunctionSignature_Scalar;
//   substrait_call.add_name("or");
//   substrait_call.add_args(std::move(args));
//   return substrait_call;
// };


// ArrowToSubstrait arrow_and_kleene_to_substrait = [] (const arrow::compute::Expression::Call& call, std::vector<substrait::Expression> args) {
//   auto substrait_call = substrait::FunctionSignature_Scalar;
//   substrait_call.add_name("and");
//   substrait_call.add_args(std::move(args));
//   return substrait_call;
// };

// ArrowToSubstrait arrow_xor_to_substrait = [] (const arrow::compute::Expression::Call& call, std::vector<substrait::Expression> args) {
//   auto substrait_call = substrait::FunctionSignature_Scalar;
//   substrait_call.add_name("xor");
//   substrait_call.add_args(std::move(args));
//   return substrait_call;
// };

// Comparison Functions mapping
SubstraitToArrow substrait_lt_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  auto value_2 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));
  return arrow::compute::call("less", {expression_1, expression_2});
};

SubstraitToArrow substrait_gt_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  auto value_2 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));
  return arrow::compute::call("greater", {expression_1, expression_2});
};

SubstraitToArrow substrait_lte_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  auto value_2 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));  
  return arrow::compute::call("less_equal", {expression_1, expression_2});
};

SubstraitToArrow substrait_not_equal_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  auto value_2 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));  
  return arrow::compute::call("not_equal", {expression_1, expression_2});
};

SubstraitToArrow substrait_equal_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  auto value_2 = call.args(1);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  ARROW_ASSIGN_OR_RAISE(auto expression_2, FromProto(value_2, ext_set_));    
  return arrow::compute::call("equal", {expression_1, expression_2});
};

SubstraitToArrow substrait_is_null_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  return arrow::compute::call("is_null", {expression_1});
};

SubstraitToArrow substrait_is_not_null_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
  auto value_1 = call.args(0);
  ExtensionSet ext_set_;
  ARROW_ASSIGN_OR_RAISE(auto expression_1, FromProto(value_1, ext_set_));
  return arrow::compute::call("is_valid", {expression_1});
};

// SubstraitToArrow substrait_is_not_distinct_from_to_arrow = [] (const substrait::Expression::ScalarFunction& call) -> Result<arrow::compute::Expression>  {
//   auto null_check = arrow::compute::call("is_null", call.args(2));
//   if(null_check){
//     return arrow::compute::call("not_equal", {null_check,null_check});
//   }
//   return arrow::compute::all("not_equal", call.args(2));
// };
}
}
