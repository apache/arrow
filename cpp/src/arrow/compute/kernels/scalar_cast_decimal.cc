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

// Implementation of casting to decimal types (including between
// decimal types)

namespace arrow {
namespace compute {

// Decimal to Decimal

template <>
struct CastFunctor<Decimal128Type, Decimal128Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type_inst = checked_cast<const Decimal128Type&>(*input.type);
    const auto& out_type_inst = checked_cast<const Decimal128Type&>(*output->type);
    auto in_scale = in_type_inst.scale();
    auto out_scale = out_type_inst.scale();

    auto out_data = output->GetMutableValues<uint8_t>(1);

    const auto write_zero = [](uint8_t* out_data) { memset(out_data, 0, 16); };

    if (options.allow_decimal_truncate) {
      if (in_scale < out_scale) {
        // Unsafe upscale
        auto convert_value = [&](util::optional<util::string_view> v) {
          if (v.has_value()) {
            auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
            dec_value.IncreaseScaleBy(out_scale - in_scale).ToBytes(out_data);
          } else {
            write_zero(out_data);
          }
          out_data += 16;
        };
        VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
      } else {
        // Unsafe downscale
        auto convert_value = [&](util::optional<util::string_view> v) {
          if (v.has_value()) {
            auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
            dec_value.ReduceScaleBy(in_scale - out_scale, false).ToBytes(out_data);
          } else {
            write_zero(out_data);
          }
          out_data += 16;
        };
        VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
      }
    } else {
      // Safe rescale
      auto convert_value = [&](util::optional<util::string_view> v) {
        if (v.has_value()) {
          auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
          auto result = dec_value.Rescale(in_scale, out_scale);
          if (ARROW_PREDICT_FALSE(!result.ok())) {
            ctx->SetStatus(result.status());
            write_zero(out_data);
          } else {
            (*std::move(result)).ToBytes(out_data);
          }
        } else {
          write_zero(out_data);
        }
        out_data += 16;
      };
      VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
    }
  }
};

}  // namespace compute
}  // namespace arrow
