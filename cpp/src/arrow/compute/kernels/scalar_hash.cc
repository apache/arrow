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
// Only ever called with a plain column: HashArray routes anything else (nested,
// DICTIONARY, EXTENSION -- see NeedsRecursiveHash) elsewhere first.
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

// Whether `type_id` needs HashArray's own recursive handling instead of going straight
// to ToColumnArray/HashMultiColumn. Broader than is_nested(): EXTENSION and DICTIONARY
// aren't nested types, but ToColumnArray has no case for either (and hashing a
// dictionary's raw indices would be wrong regardless).
bool NeedsRecursiveHash(Type::type type_id) {
  return type_id == Type::EXTENSION || type_id == Type::DICTIONARY || is_nested(type_id);
}

// Writes `array`'s own per-row validity into `out_validity` (a fresh 0-offset bitmap),
// rebasing off `array.offset`. Only ever called for types whose validity really is a
// plain bitmap -- union and run-end-encoded, the types ArraySpan::IsValid computes
// specially, never reach here (HashArray rejects them; see NeedsRecursiveHash).
void WriteOwnValidity(const ArraySpan& array, uint8_t* out_validity) {
  if (array.GetBuffer(0) == nullptr) {
    // No bitmap, so every row shares one answer: all valid, or (for the NA type, whose
    // null_count SetSlice keeps equal to length) all null.
    bit_util::SetBitsTo(out_validity, 0, array.length,
                        /*bits_are_set=*/array.null_count != array.length);
    return;
  }
  ::arrow::internal::CopyBitmap(array.GetBuffer(0)->data(), array.offset, array.length,
                                out_validity, /*dest_offset=*/0);
}

// Overwrites the hash values of invalid rows with a fixed constant.
//
// A null row's own output validity already says "null", so its hash value is irrelevant
// to *this* array's result -- but not to a parent's. When this array is a struct field or
// a list's values, the parent folds these values into its own combined hash (see
// CombineRange, HashMultiColumn), and per the columnar spec a null slot's underlying
// bytes are undefined: they may hold real-looking leftover data. Without canonicalizing,
// a null element would contribute whatever garbage it happens to sit on, so e.g.
// list<struct<f0:int32>> rows [{f0: 7}] and [null] (whose f0 slot also holds 7) would
// hash identically. Zero is just a canonical constant here -- unlike the previous design,
// nothing reads nullness back out of the hash value.
//
// Implemented by scanning runs of valid rows and filling the gaps between them, so the
// common all-valid case costs one run and zero writes.
template <typename c_type>
void CanonicalizeInvalidHashes(int64_t length, const uint8_t* validity, c_type* out) {
  int64_t valid_end = 0;
  ::arrow::internal::VisitSetBitRunsVoid(
      validity, /*offset=*/0, length, [&](int64_t position, int64_t run_length) {
        std::fill(out + valid_end, out + position, c_type{0});
        valid_end = position + run_length;
      });
  std::fill(out + valid_end, out + length, c_type{0});
}

// Folds one row's child hashes into a single hash. Seeded with CombineHashes(0, 0)
// rather than 0 just so an empty list doesn't hash to a bare 0 (hash-quality nicety,
// not a correctness requirement -- a list/map row's validity is independent of its
// hash value; see HashListArray). Free function since it only depends on c_type/Hasher.
template <typename c_type, typename Hasher>
c_type CombineRange(const c_type* value_hashes, int64_t start, int64_t end) {
  c_type combined = Hasher::CombineHashes(0, 0);
  for (int64_t j = start; j < end; j++) {
    combined = Hasher::CombineHashes(combined, value_hashes[j]);
  }
  return combined;
}

// Combines rows for LIST/LARGE_LIST/MAP, whose offsets buffers differ only in width.
// `bias` is rel_start: offsets[i] is a logical index into the values child, while
// value_hash_data starts at rel_start, so offsets[i] - bias locates row i's first
// element.
template <typename c_type, typename Hasher, typename OffsetT>
void CombineOffsetRows(int64_t length, const OffsetT* offsets, int64_t bias,
                       const c_type* value_hash_data, c_type* out) {
  for (int64_t i = 0; i < length; i++) {
    out[i] = CombineRange<c_type, Hasher>(value_hash_data, offsets[i] - bias,
                                          offsets[i + 1] - bias);
  }
}

template <typename ArrowType, typename Hasher>
struct FastHashScalar {
  using c_type = typename ArrowType::c_type;

  // Hashes the [offset, offset + length) slice of `child`, returning both its hash
  // values and real per-row validity, always starting at offset 0 regardless of
  // `child`'s or `offset`'s own offset (callers read its buffers as row-0-based).
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
    // Callers fold these values into a parent's combined hash, so a null row's value
    // must be deterministic rather than whatever undefined bytes it sat on.
    CanonicalizeInvalidHashes(sliced.length, validity->data(),
                              buffer->mutable_data_as<c_type>());
    return ArrayData::Make(arrow_type, sliced.length,
                           {std::move(validity), std::move(buffer)}, kUnknownNullCount);
  }

  static Status HashStructArray(const ArraySpan& array, LightContext* hash_ctx,
                                ExecContext* exec_ctx, c_type* out,
                                uint8_t* out_validity) {
    // Row validity starts as the struct's own; each field's own is ANDed in below
    // in-place (same idiom as e.g. swiss_join.cc's multi-column null intersection): a
    // field that's independently null still makes the row invalid (GH-17211), same as
    // the struct row being null itself.
    WriteOwnValidity(array, out_validity);

    if (array.child_data.empty()) {
      // No fields (e.g. struct<>): HashMultiColumn requires >=1 column, so every row
      // just gets the same defined hash value; validity is already fully set above.
      c_type empty_struct_hash = Hasher::CombineHashes(0, 0);
      for (int64_t i = 0; i < array.length; i++) {
        out[i] = empty_struct_hash;
      }
      return Status::OK();
    }

    std::vector<std::shared_ptr<ArrayData>> child_hashes(array.child_data.size());
    std::vector<KeyColumnArray> columns(array.child_data.size());
    KeyColumnArray column;
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
        ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(*child_hashes[i]));
        // child_hashes[i] already covers exactly [0, array.length): no further slice.
        columns[i] = column.Slice(0, array.length);
      } else {
        if (child.GetBuffer(0) != nullptr) {
          ::arrow::internal::BitmapAnd(out_validity, 0, child.GetBuffer(0)->data(),
                                       child.offset + array.offset, array.length, 0,
                                       out_validity);
        }
        ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(child));
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
    ARROW_ASSIGN_OR_RAISE(auto value_hashes,
                          HashChild(values, values.offset + rel_start,
                                    rel_end - rel_start, hash_ctx, exec_ctx));
    const c_type* value_hash_data = value_hashes->buffers[1]->data_as<c_type>();
    // value_hash_data[k] corresponds to original row (values.offset + rel_start + k).

    if (offsets != nullptr) {
      CombineOffsetRows<c_type, Hasher>(array.length, offsets, rel_start, value_hash_data,
                                        out);
    } else {
      // rel_start is array.offset * list_size (see above), so row i's elements start at
      // value_hash_data[i * list_size].
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
    if (!NeedsRecursiveHash(type_id)) {
      KeyColumnArray column;
      ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(array));
      std::vector<KeyColumnArray> columns{column.Slice(array.offset, array.length)};
      Hasher::HashMultiColumn(columns, hash_ctx, out);
      // This array's own validity is the whole story for a plain column: HashMultiColumn
      // already folded it into the hash values via ToColumnArray's real validity buffer.
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
