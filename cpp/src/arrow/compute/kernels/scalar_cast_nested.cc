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

// Implementation of casting to (or between) list types

namespace arrow {
namespace compute {

template <typename TypeClass>
class ListCastKernel : public CastKernelBase {
 public:
  ListCastKernel(std::unique_ptr<UnaryKernel> child_caster,
                 std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)), child_caster_(std::move(child_caster)) {}

  Status Call(KernelContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());

    const ArrayData& in_data = *input.array();
    DCHECK_EQ(TypeClass::type_id, in_data.type->id());
    ArrayData* result;

    if (in_data.offset != 0) {
      return Status::NotImplemented(
          "Casting sliced lists (non-zero offset) not yet implemented");
    }

    if (out->kind() == Datum::NONE) {
      out->value = ArrayData::Make(out_type_, in_data.length);
    }

    result = out->array().get();

    // Copy buffers from parent
    result->buffers = in_data.buffers;

    Datum casted_child;
    RETURN_NOT_OK(InvokeWithAllocation(ctx, child_caster_.get(), in_data.child_data[0],
                                       &casted_child));
    DCHECK_EQ(Datum::ARRAY, casted_child.kind());
    result->child_data.push_back(casted_child.array());
    return Status::OK();
  }

 private:
  std::unique_ptr<UnaryKernel> child_caster_;
};

}  // namespace compute
}  // namespace arrow
