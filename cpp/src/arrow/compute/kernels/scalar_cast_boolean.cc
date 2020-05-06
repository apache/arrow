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

// Cast types to boolean

namespace arrow {
namespace compute {

struct IsNonZero {
  template <typename InType, typename OutType>
  static OutType Call(FunctionContext*, InType val) {
    return val != 0;
  }
};

struct ParseBooleanString {
  template <typename OutType = bool>
  static OutType Call(FunctionContext* ctx, util::string_view val) {
    internal::StringConverter<BooleanType> converter;
  }
};

void RegisterBooleanCasts(FunctionRegistry* registry) {
  ScalarDispatcher dispatcher("cast_boolean", /*num_args=*/1);
  auto out_type = boolean();
  for (const auto& in_type : kNumberTypes) {
    auto func = codegen::MakePrimitiveUnary<BooleanType, IsNonZero>(in_type));
    dispatcher.Add(ScalarKernel({in_type}, out_type, func));
  }
}

// String to Boolean
template <typename I>
struct CastFunctor<BooleanType, I, enable_if_t<is_string_like_type<I>::value>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    typename TypeTraits<I>::ArrayType input_array(input.Copy());
    internal::FirstTimeBitmapWriter writer(output->buffers[1]->mutable_data(),
                                           output->offset, input.length);

    for (int64_t i = 0; i < input.length; ++i) {
      if (input_array.IsNull(i)) {
        writer.Next();
        continue;
      }

      bool value;
      auto str = input_array.GetView(i);
      if (!converter(str.data(), str.length(), &value)) {
        ctx->SetStatus(Status::Invalid("Failed to cast String '",
                                       input_array.GetString(i), "' into ",
                                       output->type->ToString()));
        return;
      }

      if (value) {
        writer.Set();
      } else {
        writer.Clear();
      }
      writer.Next();
    }
    writer.Finish();
  }
};

}  // namespace compute
}  // namespace arrow
