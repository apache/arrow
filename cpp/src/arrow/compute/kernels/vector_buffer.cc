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

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/registry.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

namespace compute {
namespace internal {

namespace {

struct GetByteRangesArray {
  const std::shared_ptr<ArrayData>& input;
  int64_t offset;
  int64_t length;
  UInt64Builder* range_starts;
  UInt64Builder* range_offsets;
  UInt64Builder* range_lengths;

  Status VisitBitmap(const std::shared_ptr<Buffer>& buffer) {
    if (buffer) {
      uint64_t data_start = reinterpret_cast<uint64_t>(buffer->data());
      RETURN_NOT_OK(range_starts->Append(data_start));
      RETURN_NOT_OK(range_offsets->Append(BitUtil::RoundDown(offset, 8) / 8));
      RETURN_NOT_OK(range_lengths->Append(BitUtil::CoveringBytes(offset, length)));
    }
    return Status::OK();
  }

  Status VisitFixedWidthArray(const Buffer& buffer, const FixedWidthType& type) {
    uint64_t data_start = reinterpret_cast<uint64_t>(buffer.data());
    uint64_t offset_bits = offset * type.bit_width();
    uint64_t offset_bytes = BitUtil::RoundDown(offset_bits, 8) / 8;
    uint64_t end_byte =
        BitUtil::RoundUp(offset_bits + (length * type.bit_width()), 8) / 8;
    uint64_t length_bytes = (end_byte - offset_bytes);
    RETURN_NOT_OK(range_starts->Append(data_start));
    RETURN_NOT_OK(range_offsets->Append(offset_bytes));
    return range_lengths->Append(length_bytes);
  }

  Status Visit(const FixedWidthType& type) {
    static_assert(sizeof(uint8_t*) <= sizeof(uint64_t),
                  "Undefined behavior if pointer larger than uint64_t");
    RETURN_NOT_OK(VisitBitmap(input->buffers[0]));
    RETURN_NOT_OK(VisitFixedWidthArray(*input->buffers[1], type));
    if (input->dictionary) {
      // This is slightly imprecise because we always assume the entire dictionary is
      // referenced.  If this array has an offset it may only be referencing a portion of
      // the dictionary
      GetByteRangesArray dict_visitor{input->dictionary,
                                      input->dictionary->offset,
                                      input->dictionary->length,
                                      range_starts,
                                      range_offsets,
                                      range_lengths};
      return VisitTypeInline(*input->dictionary->type, &dict_visitor);
    }
    return Status::OK();
  }

  Status Visit(const NullType& type) { return Status::OK(); }

  template <typename BaseBinaryType>
  Status VisitBaseBinary(const BaseBinaryType& type) {
    using offset_type = typename BaseBinaryType::offset_type;
    RETURN_NOT_OK(VisitBitmap(input->buffers[0]));

    const Buffer& offsets_buffer = *input->buffers[1];
    RETURN_NOT_OK(
        range_starts->Append(reinterpret_cast<uint64_t>(offsets_buffer.data())));
    RETURN_NOT_OK(range_offsets->Append(sizeof(offset_type) * offset));
    RETURN_NOT_OK(range_lengths->Append(sizeof(offset_type) * length));

    const offset_type* offsets = input->GetValues<offset_type>(1, offset);
    const Buffer& values = *input->buffers[2];
    offset_type start = offsets[0];
    offset_type end = offsets[length];
    RETURN_NOT_OK(range_starts->Append(reinterpret_cast<uint64_t>(values.data())));
    RETURN_NOT_OK(range_offsets->Append(static_cast<uint64_t>(start)));
    return range_lengths->Append(static_cast<uint64_t>(end - start));
  }

  Status Visit(const BinaryType& type) { return VisitBaseBinary(type); }

  Status Visit(const LargeBinaryType& type) { return VisitBaseBinary(type); }

  template <typename BaseListType>
  Status VisitBaseList(const BaseListType& type) {
    using offset_type = typename BaseListType::offset_type;
    RETURN_NOT_OK(VisitBitmap(input->buffers[0]));

    const Buffer& offsets_buffer = *input->buffers[1];
    RETURN_NOT_OK(
        range_starts->Append(reinterpret_cast<uint64_t>(offsets_buffer.data())));
    RETURN_NOT_OK(range_offsets->Append(sizeof(offset_type) * offset));
    RETURN_NOT_OK(range_lengths->Append(sizeof(offset_type) * length));

    const offset_type* offsets = input->GetValues<offset_type>(1, offset);
    int64_t start = static_cast<int64_t>(offsets[0]);
    int64_t end = static_cast<int64_t>(offsets[length]);
    GetByteRangesArray child{input->child_data[0], start,         end - start,
                             range_starts,         range_offsets, range_lengths};
    return VisitTypeInline(*type.value_type(), &child);
  }

  Status Visit(const ListType& type) { return VisitBaseList(type); }

  Status Visit(const LargeListType& type) { return VisitBaseList(type); }

  Status Visit(const FixedSizeListType& type) {
    RETURN_NOT_OK(VisitBitmap(input->buffers[0]));
    GetByteRangesArray child{input->child_data[0],
                             offset * type.list_size(),
                             length * type.list_size(),
                             range_starts,
                             range_offsets,
                             range_lengths};
    return VisitTypeInline(*type.value_type(), &child);
  }

  Status Visit(const StructType& type) {
    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{input->child_data[i],
                               offset + input->child_data[i]->offset,
                               length,
                               range_starts,
                               range_offsets,
                               range_lengths};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
    }
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    // Skip validity map for DenseUnionType
    // Types buffer is always int8
    RETURN_NOT_OK(VisitFixedWidthArray(
        *input->buffers[1], *std::dynamic_pointer_cast<FixedWidthType>(int8())));
    // Offsets buffer is always int32
    RETURN_NOT_OK(VisitFixedWidthArray(
        *input->buffers[2], *std::dynamic_pointer_cast<FixedWidthType>(int32())));

    // We have to loop through the types buffer to figure out the correct
    // offset / length being referenced in the child arrays
    std::array<std::size_t, UnionType::kMaxTypeCode> type_code_index_lookup;
    for (std::size_t i = 0; i < type.type_codes().size(); i++) {
      type_code_index_lookup[static_cast<std::size_t>(type.type_codes()[i])] = i;
    }
    std::vector<int64_t> lengths_per_type(type.type_codes().size());
    std::vector<int64_t> offsets_per_type(type.type_codes().size());
    const int8_t* type_codes = input->GetValues<int8_t>(1, 0);
    for (const int8_t* it = type_codes; it != type_codes + offset; it++) {
      offsets_per_type[type_code_index_lookup[static_cast<std::size_t>(*it)]]++;
    }
    for (const int8_t* it = type_codes + offset; it != type_codes + offset + length;
         it++) {
      lengths_per_type[type_code_index_lookup[static_cast<std::size_t>(*it)]]++;
    }

    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{
          input->child_data[i], offsets_per_type[i] + input->child_data[i]->offset,
          lengths_per_type[i],  range_starts,
          range_offsets,        range_lengths};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
    }

    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) {
    // Skip validity map for SparseUnionType
    // Types buffer is always int8
    RETURN_NOT_OK(VisitFixedWidthArray(
        *input->buffers[1], *std::dynamic_pointer_cast<FixedWidthType>(int8())));

    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{input->child_data[i],
                               offset + input->child_data[i]->offset,
                               length,
                               range_starts,
                               range_offsets,
                               range_lengths};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
    }

    return Status::OK();
  }

  Status Visit(const ExtensionType& extension_type) {
    GetByteRangesArray storage{input,        offset,        length,
                               range_starts, range_offsets, range_lengths};
    return VisitTypeInline(*extension_type.storage_type(), &storage);
  }

  Status Visit(const DataType& type) {
    return Status::TypeError("Extracting byte ranges not supported for type ",
                             type.ToString());
  }

  static std::shared_ptr<DataType> RangesType() {
    return struct_(
        {field("start", uint64()), field("offset", uint64()), field("length", uint64())});
  }

  Result<std::shared_ptr<Array>> MakeRanges() {
    std::shared_ptr<Array> range_starts_arr, range_offsets_arr, range_lengths_arr;
    RETURN_NOT_OK(range_starts->Finish(&range_starts_arr));
    RETURN_NOT_OK(range_offsets->Finish(&range_offsets_arr));
    RETURN_NOT_OK(range_lengths->Finish(&range_lengths_arr));
    return StructArray::Make(
        {range_starts_arr, range_offsets_arr, range_lengths_arr},
        {field("start", uint64()), field("offset", uint64()), field("length", uint64())});
  }

  static Result<std::shared_ptr<Array>> Exec(const std::shared_ptr<ArrayData>& input) {
    UInt64Builder range_starts, range_offsets, range_lengths;
    GetByteRangesArray self{input,         input->offset,  input->length,
                            &range_starts, &range_offsets, &range_lengths};
    RETURN_NOT_OK(VisitTypeInline(*input->type, &self));
    return self.MakeRanges();
  }
};

const FunctionDoc byte_ranges_doc(
    "Returns an array of byte ranges referenced by the input",
    (R"(The output is a StructArray {"start": int64, "offset": int64, "length":)"
     " int64} where each item represents a range of memory addressed by buffers in"
     " the input.  The ranges should have no overlap even if buffers are shared in the"
     " input.  If any of the arrays are sliced zero-copy views of the data this method"
     " will return the sliced ranges that are referenced.  There is a slight exception"
     " in the case of dictionary arrays.  If a dictionary array is sliced we will still"
     " assume the entire dictionary is referenced by the sliced offsets array.  This"
     " will lead to overestimation if not all dictionary values are referenced.  If"
     " the first argument is a scalar this will return an empty array since there are no"
     " buffers referenced by the scalar."),
    {"input"});

class ByteRangesMetaFunction : public MetaFunction {
 public:
  ByteRangesMetaFunction()
      : MetaFunction("byte_ranges", Arity::Unary(), &byte_ranges_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    DCHECK_GT(args.size(), 0);
    switch (args[0].kind()) {
      case Datum::SCALAR: {
        std::unique_ptr<ArrayBuilder> builder;
        RETURN_NOT_OK(
            MakeBuilder(ctx->memory_pool(), GetByteRangesArray::RangesType(), &builder));
        RETURN_NOT_OK(builder->Resize(0));
        return builder->Finish();
      }
      case Datum::ARRAY: {
        const std::shared_ptr<ArrayData>& array = args[0].array();
        DCHECK(array);
        return GetByteRangesArray::Exec(array);
      }
      case Datum::CHUNKED_ARRAY: {
        const std::shared_ptr<ChunkedArray>& chunked_array = args[0].chunked_array();
        DCHECK(chunked_array);
        ArrayVector chunks;
        for (const auto& chunk : chunked_array->chunks()) {
          ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> chunk_ranges,
                                GetByteRangesArray::Exec(chunk->data()));
          chunks.push_back(chunk_ranges);
        }
        return Concatenate(chunks, ctx->memory_pool());
      }
      case Datum::RECORD_BATCH: {
        const std::shared_ptr<RecordBatch>& record_batch = args[0].record_batch();
        ArrayVector columns;
        for (const auto& column : record_batch->columns()) {
          ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> column_ranges,
                                GetByteRangesArray::Exec(column->data()));
          columns.push_back(column_ranges);
        }
        return Concatenate(columns, ctx->memory_pool());
      }
      case Datum::TABLE: {
        const std::shared_ptr<Table>& table = args[0].table();
        ArrayVector chunks;
        for (const auto& column : table->columns()) {
          for (const auto& chunk : column->chunks()) {
            ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> chunk_ranges,
                                  GetByteRangesArray::Exec(chunk->data()));
            chunks.push_back(chunk_ranges);
          }
        }
        return Concatenate(chunks, ctx->memory_pool());
      }
      default:
        return Status::NotImplemented("Datum kind ", args[0].kind(),
                                      " not supported for byte_ranges");
    }
  }
};

}  // namespace

void RegisterVectorBuffer(FunctionRegistry* registry) {
  // ByteRanges kernel
  DCHECK_OK(registry->AddFunction(std::make_shared<ByteRangesMetaFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
