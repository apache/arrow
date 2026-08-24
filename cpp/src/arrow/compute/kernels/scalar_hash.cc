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

#include <algorithm>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/util.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/key_hash_internal.h"
#include "arrow/compute/light_array_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/compute/util.h"
#include "arrow/result.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

// ------------------------------
// Kernel implementations
// It is expected that HashArrowType is either UInt32Type or UInt64Type (default)

// Free function (not dependent on ArrowType/Hasher) to avoid codegen per instantiation.
// Only called with a plain column; HashArray routes everything else (see
// NeedsRecursiveHash) elsewhere first.
Result<KeyColumnArray> ToColumnArray(const ArraySpan& array) {
  KeyColumnMetadata metadata;
  const uint8_t* validity_buffer = nullptr;
  const uint8_t* fixed_length_buffer = nullptr;
  const uint8_t* var_length_buffer = nullptr;

  if (array.GetBuffer(0) != nullptr) {
    validity_buffer = array.GetBuffer(0)->data();
  }
  if (array.GetBuffer(1) != nullptr) {
    fixed_length_buffer = array.GetBuffer(1)->data();
  }

  auto type = array.type;
  auto type_id = type->id();
  if (type_id == Type::NA) {
    metadata = KeyColumnMetadata(true, 0, true);
  } else if (type_id == Type::BOOL) {
    metadata = KeyColumnMetadata(true, 0);
  } else if (is_fixed_width(type_id)) {
    metadata = KeyColumnMetadata(true, type->bit_width() / 8);
  } else if (is_binary_like(type_id)) {
    metadata = KeyColumnMetadata(false, sizeof(uint32_t));
    if (array.GetBuffer(2) != nullptr) {
      var_length_buffer = array.GetBuffer(2)->data();
    }
  } else if (is_large_binary_like(type_id)) {
    metadata = KeyColumnMetadata(false, sizeof(uint64_t));
    if (array.GetBuffer(2) != nullptr) {
      var_length_buffer = array.GetBuffer(2)->data();
    }
  } else {
    return Status::TypeError("Unsupported column data type ", type->name(),
                             " used with hash32/hash64 compute kernel");
  }

  return KeyColumnArray(metadata, array.length, validity_buffer, fixed_length_buffer,
                        var_length_buffer);
}

// Whether HashArray must handle `type_id` itself rather than passing it to
// ToColumnArray/HashMultiColumn. Broader than is_nested(): EXTENSION and DICTIONARY
// aren't nested, but ToColumnArray has no case for either (and hashing a dictionary's
// raw indices would be wrong anyway).
bool NeedsRecursiveHash(Type::type type_id) {
  return type_id == Type::EXTENSION || type_id == Type::DICTIONARY || is_nested(type_id);
}

// Writes `array`'s own validity into `out_validity` (a fresh 0-offset bitmap), rebasing
// off array.offset. Only called for types whose validity really is a plain bitmap; union
// and run-end-encoded, which ArraySpan::IsValid computes specially, never reach here.
void WriteOwnValidity(const ArraySpan& array, uint8_t* out_validity) {
  if (array.GetBuffer(0) == nullptr) {
    // No bitmap: every row shares one answer, all valid except for NullType, which is
    // implicitly all-null.
    bit_util::SetBitsTo(out_validity, 0, array.length,
                        /*bits_are_set=*/array.type->id() != Type::NA);
    return;
  }
  ::arrow::internal::CopyBitmap(array.GetBuffer(0)->data(), array.offset, array.length,
                                out_validity, /*dest_offset=*/0);
}

// Folds one row's child hashes into a single hash. Seeded with CombineHashes(0, 0) rather
// than 0 just so an empty list doesn't hash to a bare 0 -- a hash-quality nicety, not a
// requirement, since a list row's validity is independent of its hash value.
template <typename c_type, typename Hasher>
c_type CombineRange(const c_type* value_hashes, int64_t start, int64_t end) {
  c_type combined = Hasher::CombineHashes(0, 0);
  for (int64_t j = start; j < end; j++) {
    combined = Hasher::CombineHashes(combined, value_hashes[j]);
  }
  return combined;
}

template <typename ArrowType, typename Hasher>
struct FastHashScalar {
  using c_type = typename ArrowType::c_type;

  // Hashes the [offset, offset + length) slice of `child` into hash values plus real
  // validity, always based at offset 0 whatever `child`'s own offset (callers read the
  // buffers row-0-based). Only a null row's validity bit is meaningful, not its hash
  // value; callers folding these into a parent hash must handle that (see
  // HashListArray).
  static Result<std::shared_ptr<ArrayData>> HashChild(const ArraySpan& child,
                                                      int64_t offset, int64_t length,
                                                      LightContext* hash_ctx,
                                                      ExecContext* exec_ctx) {
    auto sliced = child;
    sliced.SetSlice(offset, length);
    auto arrow_type = TypeTraits<ArrowType>::type_singleton();
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(sliced.length * sizeof(c_type),
                                                      exec_ctx->memory_pool()));
    ARROW_ASSIGN_OR_RAISE(auto validity,
                          AllocateBitmap(sliced.length, exec_ctx->memory_pool()));
    ARROW_RETURN_NOT_OK(HashArray(sliced, hash_ctx, exec_ctx,
                                  buffer->mutable_data_as<c_type>(),
                                  validity->mutable_data()));
    return ArrayData::Make(arrow_type, sliced.length,
                           {std::move(validity), std::move(buffer)}, kUnknownNullCount);
  }

  static Status HashStructArray(const ArraySpan& array, LightContext* hash_ctx,
                                ExecContext* exec_ctx, c_type* out,
                                uint8_t* out_validity) {
    // Row validity is the struct's own ANDed with every field's (in place, as
    // swiss_join.cc does for multi-column nulls): an independently-null field makes the
    // row invalid too (GH-17211), just like the struct row being null.
    WriteOwnValidity(array, out_validity);

    if (array.child_data.empty()) {
      // struct<>: HashMultiColumn needs >=1 column, so give every row one fixed hash;
      // validity is already fully set above.
      c_type empty_struct_hash = Hasher::CombineHashes(0, 0);
      for (int64_t i = 0; i < array.length; i++) {
        out[i] = empty_struct_hash;
      }
      return Status::OK();
    }

    std::vector<std::shared_ptr<ArrayData>> child_hashes(array.child_data.size());
    std::vector<KeyColumnArray> columns(array.child_data.size());
    for (size_t i = 0; i < array.child_data.size(); i++) {
      // By reference: ArraySpan owns a child_data vector, so copying one heap-allocates.
      const ArraySpan& child = array.child_data[i];
      // `child` may have its own offset independent of the struct's (see
      // StructArray::GetFlattenedField): struct row r reads child row
      // (child.offset + array.offset + r).
      if (NeedsRecursiveHash(child.type->id())) {
        // StructArray::Slice() doesn't reslice child_data, so `child` may be larger
        // than this slice of `array` references -- hash only the referenced range.
        ARROW_ASSIGN_OR_RAISE(child_hashes[i],
                              HashChild(child, child.offset + array.offset, array.length,
                                        hash_ctx, exec_ctx));
        ::arrow::internal::BitmapAnd(out_validity, 0, child_hashes[i]->buffers[0]->data(),
                                     0, array.length, 0, out_validity);
        ARROW_ASSIGN_OR_RAISE(auto column, ToColumnArray(*child_hashes[i]));
        // child_hashes[i] already covers exactly [0, array.length): no further slice.
        columns[i] = column.Slice(0, array.length);
      } else {
        if (child.GetBuffer(0) != nullptr) {
          ::arrow::internal::BitmapAnd(out_validity, 0, child.GetBuffer(0)->data(),
                                       child.offset + array.offset, array.length, 0,
                                       out_validity);
        } else if (child.type->id() == Type::NA) {
          // No bitmap, but NullType is implicitly all-null: invalidates every row.
          bit_util::SetBitsTo(out_validity, 0, array.length, false);
        }
        ARROW_ASSIGN_OR_RAISE(auto column, ToColumnArray(child));
        columns[i] = column.Slice(child.offset + array.offset, array.length);
      }
    }
    Hasher::HashMultiColumn(columns, hash_ctx, out);
    return Status::OK();
  }

  // Handles FIXED_SIZE_LIST, LARGE_LIST, LIST, and MAP. `offsets` is null for
  // FIXED_SIZE_LIST, which uses `list_size` as a constant stride instead.
  template <typename OffsetT>
  static Status HashListArray(const ArraySpan& array, int64_t list_size,
                              const OffsetT* offsets, LightContext* hash_ctx,
                              ExecContext* exec_ctx, c_type* out, uint8_t* out_validity) {
    // The range of `values` this array actually references, as logical indices relative
    // to values.offset. Needed because ArraySpan::SetSlice() doesn't reslice child_data,
    // so `values` can be far larger than what this (possibly sliced) array covers.
    // offsets[] are already such logical indices; FIXED_SIZE_LIST derives them from its
    // constant stride instead.
    int64_t rel_start = 0, rel_end = 0;
    if (array.length > 0) {
      if (offsets != nullptr) {
        rel_start = offsets[0];
        rel_end = offsets[array.length];
      } else {
        rel_start = array.offset * list_size;
        rel_end = (array.offset + array.length) * list_size;
      }
    }

    // By reference: ArraySpan owns a child_data vector, so copying one heap-allocates.
    const ArraySpan& values = array.child_data[0];
    // Element k of the result is original values row (values.offset + rel_start + k).
    ARROW_ASSIGN_OR_RAISE(auto value_hashes,
                          HashChild(values, values.offset + rel_start,
                                    rel_end - rel_start, hash_ctx, exec_ctx));
    // Zero the null elements' hashes: CombineRange folds values blind, and a null slot's
    // bytes are undefined per the columnar spec, so otherwise a null element contributes
    // whatever garbage it sat on and list<struct<f0:int32>> rows [{f0: 7}] and [null]
    // (whose f0 slot also holds 7) hash alike. Filling only the gaps between runs of
    // valid rows leaves the common all-valid case free. HashStructArray needs no
    // equivalent: HashMultiColumn gets its fields' validity and already fixes each null
    // row's contribution.
    c_type* value_hash_data = value_hashes->buffers[1]->mutable_data_as<c_type>();
    int64_t valid_end = 0;
    ::arrow::internal::VisitSetBitRunsVoid(
        value_hashes->buffers[0]->data(), /*offset=*/0, value_hashes->length,
        [&](int64_t position, int64_t run_length) {
          std::fill(value_hash_data + valid_end, value_hash_data + position, c_type{0});
          valid_end = position + run_length;
        });
    std::fill(value_hash_data + valid_end, value_hash_data + value_hashes->length,
              c_type{0});

    if (offsets != nullptr) {
      // offsets[] index the values child; value_hash_data starts at rel_start.
      for (int64_t i = 0; i < array.length; i++) {
        out[i] = CombineRange<c_type, Hasher>(value_hash_data, offsets[i] - rel_start,
                                              offsets[i + 1] - rel_start);
      }
    } else {
      // rel_start is array.offset * list_size, so row i starts at i * list_size.
      for (int64_t i = 0; i < array.length; i++) {
        int64_t start = i * list_size;
        out[i] = CombineRange<c_type, Hasher>(value_hash_data, start, start + list_size);
      }
    }
    // A list/map row's validity is its own only -- what's inside it (even a null
    // element, or a null row's non-empty offset range) never changes that. value_hashes'
    // validity buffer is deliberately not consulted here.
    WriteOwnValidity(array, out_validity);
    return Status::OK();
  }

  // Routes to the per-shape hashing routine for `array`'s type, writing both hash
  // values (`out`) and real per-row validity (`out_validity`, a fresh 0-offset bitmap,
  // same convention `out` has via ArraySpan::GetValues).
  static Status HashArray(const ArraySpan& array, LightContext* hash_ctx,
                          ExecContext* exec_ctx, c_type* out, uint8_t* out_validity) {
    auto type_id = array.type->id();
    if (type_id == Type::FIXED_SIZE_BINARY && array.type->byte_width() == 0) {
      // Zero-width values carry no data, so every row holds the same empty byte string
      // and must hash identically. ToColumnArray can only describe this as a fixed-width
      // column of length 0, exactly how a bit-packed boolean is encoded too, so
      // HashMultiColumn would call HashBit and take each row's hash from a bit that
      // doesn't exist -- uninitialized garbage, differing per row and per slice.
      std::fill(out, out + array.length, Hasher::CombineHashes(0, 0));
      WriteOwnValidity(array, out_validity);
      return Status::OK();
    } else if (!NeedsRecursiveHash(type_id)) {
      ARROW_ASSIGN_OR_RAISE(auto column, ToColumnArray(array));
      std::vector<KeyColumnArray> columns{column.Slice(array.offset, array.length)};
      Hasher::HashMultiColumn(columns, hash_ctx, out);
      // A plain column's own validity is the whole story, and HashMultiColumn has
      // already folded it into the hash values via ToColumnArray's buffer.
      WriteOwnValidity(array, out_validity);
      return Status::OK();
    } else if (type_id == Type::EXTENSION) {
      auto extension_type = checked_cast<const ExtensionType*>(array.type);
      auto storage_array = array;
      storage_array.type = extension_type->storage_type().get();
      return HashArray(storage_array, hash_ctx, exec_ctx, out, out_validity);
    } else if (type_id == Type::DICTIONARY) {
      // Hash the logical values, not the indices -- otherwise two dictionaries
      // encoding the same values differently would hash differently, and a valid
      // index pointing at a null dictionary entry would be missed. Cast's decode
      // (Take under the hood) already produces a correct validity buffer for both, so
      // recursing into the decoded array handles validity for free. Reuse the
      // caller's ExecContext rather than a synthesized default one, same as other
      // kernels' dictionary-decode path (see EnsureDictionaryDecoded).
      auto dict_type = checked_cast<const DictionaryType*>(array.type);
      ARROW_ASSIGN_OR_RAISE(auto decoded,
                            Cast(*MakeArray(array.ToArrayData()), dict_type->value_type(),
                                 CastOptions::Safe(dict_type->value_type()), exec_ctx));
      return HashArray(*decoded->data(), hash_ctx, exec_ctx, out, out_validity);
    } else if (type_id == Type::STRUCT) {
      return HashStructArray(array, hash_ctx, exec_ctx, out, out_validity);
    } else if (type_id == Type::FIXED_SIZE_LIST) {
      auto list_size = checked_cast<const FixedSizeListType*>(array.type)->list_size();
      return HashListArray<int32_t>(array, list_size, /*offsets=*/nullptr, hash_ctx,
                                    exec_ctx, out, out_validity);
    } else if (type_id == Type::LARGE_LIST) {
      return HashListArray<int64_t>(array, /*list_size=*/0, array.GetValues<int64_t>(1),
                                    hash_ctx, exec_ctx, out, out_validity);
    } else if (is_list_like(type_id)) {
      // LIST and MAP both use 32-bit offsets.
      return HashListArray<int32_t>(array, /*list_size=*/0, array.GetValues<int32_t>(1),
                                    hash_ctx, exec_ctx, out, out_validity);
    } else {
      // NeedsRecursiveHash claims this type needs recursive handling, but no branch
      // above knows how (e.g. a union or run-end-encoded type that somehow slipped
      // past HashableMatcher's rejection) -- fail loudly and locally rather than
      // silently falling through to a mismatched case.
      return Status::NotImplemented("Unsupported column data type ", array.type->name(),
                                    " used with hash32/hash64 compute kernel");
    }
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& input_arg, ExecResult* out) {
    ARROW_DCHECK_EQ(input_arg.num_values(), 1);
    ARROW_DCHECK(input_arg[0].is_array());
    ArraySpan hash_input = input_arg[0].array;

    auto exec_ctx = default_exec_context();
    if (ctx && ctx->exec_context()) {
      exec_ctx = ctx->exec_context();
    }

    // Initialize stack-based memory allocator used by Hashing32 and Hashing64
    util::TempVectorStack stack_memallocator;
    ARROW_RETURN_NOT_OK(stack_memallocator.Init(exec_ctx->memory_pool(),
                                                Hasher::kHashBatchTempStackUsage));

    // Prepare context used by Hashing32 and Hashing64
    LightContext hash_ctx;
    hash_ctx.hardware_flags = exec_ctx->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;

    // Call the hashing function, overloaded based on OutputCType
    ArraySpan* result_span = out->array_span_mutable();
    c_type* result_ptr = result_span->GetValues<c_type>(1);

    // HashArray writes validity into a fresh, 0-offset bitmap (matching `result_ptr`'s
    // own 0-based convention). The kernel's real output buffer may start at a nonzero
    // bit offset (e.g. contiguous chunked preallocation), so translate in one final
    // pass below instead of threading an offset through the whole recursive engine.
    ARROW_ASSIGN_OR_RAISE(auto validity,
                          AllocateBitmap(hash_input.length, exec_ctx->memory_pool()));
    ARROW_RETURN_NOT_OK(
        HashArray(hash_input, &hash_ctx, exec_ctx, result_ptr, validity->mutable_data()));

    const uint8_t* validity_data = validity->data();
    int64_t out_null_count = 0;
    int64_t row = 0;
    ::arrow::internal::GenerateBitsUnrolled(
        result_span->buffers[0].data, result_span->offset, hash_input.length, [&] {
          bool is_valid = bit_util::GetBit(validity_data, row++);
          out_null_count += !is_valid;
          return is_valid;
        });
    result_span->null_count = out_null_count;

    return Status::OK();
  }
};

class HashableMatcher : public TypeMatcher {
 public:
  HashableMatcher() {}

  bool Matches(const DataType& type) const override {
    // Unwrap extension/dictionary types (recursively, either nesting order) so an
    // unsupported storage/value type is rejected here, not with a raw TypeError deep
    // inside HashArray/ToColumnArray/Cast.
    const DataType* physical_type = &type;
    while (true) {
      if (physical_type->id() == Type::EXTENSION) {
        physical_type =
            checked_cast<const ExtensionType&>(*physical_type).storage_type().get();
      } else if (physical_type->id() == Type::DICTIONARY) {
        physical_type =
            checked_cast<const DictionaryType&>(*physical_type).value_type().get();
      } else {
        break;
      }
    }
    return !(is_union(*physical_type) || is_binary_view_like(*physical_type) ||
             is_list_view(*physical_type) ||
             physical_type->id() == Type::RUN_END_ENCODED);
  }

  bool Equals(const TypeMatcher& other) const override {
    if (this == &other) {
      return true;
    }
    auto casted = dynamic_cast<const HashableMatcher*>(&other);
    return casted != nullptr;
  }

  std::string ToString() const override { return "hashable"; }
};

const FunctionDoc hash32_doc{
    "Construct a hash for every element of the input argument",
    ("This function is not suitable for cryptographic purposes.\n"
     "Hash results are 32-bit. A null input row produces a null in the output."),
    {"hash_input"}};

const FunctionDoc hash64_doc{
    "Construct a hash for every element of the input argument",
    ("This function is not suitable for cryptographic purposes.\n"
     "Hash results are 64-bit. A null input row produces a null in the output."),
    {"hash_input"}};

}  // namespace

void RegisterScalarHash(FunctionRegistry* registry) {
  // Create hash32 and hash64 function instances
  auto hash32 = std::make_shared<ScalarFunction>("hash32", Arity::Unary(), hash32_doc);
  auto hash64 = std::make_shared<ScalarFunction>("hash64", Arity::Unary(), hash64_doc);

  // Add 32-bit and 64-bit kernels to hash32 and hash64 functions
  auto type_matcher = std::make_shared<HashableMatcher>();
  ScalarKernel kernel32({InputType(type_matcher)}, OutputType(uint32()),
                        FastHashScalar<UInt32Type, Hashing32>::Exec);
  ScalarKernel kernel64({InputType(type_matcher)}, OutputType(uint64()),
                        FastHashScalar<UInt64Type, Hashing64>::Exec);
  kernel32.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel64.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  ARROW_DCHECK_OK(hash32->AddKernel(std::move(kernel32)));
  ARROW_DCHECK_OK(hash64->AddKernel(std::move(kernel64)));

  // Register hash32 and hash64 functions
  ARROW_DCHECK_OK(registry->AddFunction(std::move(hash32)));
  ARROW_DCHECK_OK(registry->AddFunction(std::move(hash64)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
