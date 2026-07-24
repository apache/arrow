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
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/key_hash_internal.h"
#include "arrow/compute/light_array_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/compute/util.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {

// Define symbols visible within `arrow::compute::internal` in this file;
// these symbols are not visible outside of this file.
namespace {

// ------------------------------
// Kernel implementations
// It is expected that HashArrowType is either UInt32Type or UInt64Type (default)

// Not dependent on the ArrowType/Hasher template arguments below, so defined
// as a free function to avoid unnecessary code generation per instantiation.
// Never called with a nested (list-like or struct) array: HashArray handles those
// itself, either via HashChild (which reduces them to a plain UInt32/UInt64 array
// before it ever reaches here) or the is_list_like branch directly.
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

// Zeroes out[i] wherever array is null.
template <typename c_type>
void ZeroNulls(const ArraySpan& array, c_type* out) {
  if (array.GetBuffer(0) == nullptr) {
    return;
  }
  for (int64_t i = 0; i < array.length; i++) {
    if (array.IsNull(i)) {
      out[i] = 0;
    }
  }
}

// Since the kernel declares OUTPUT_NOT_NULL, a null row's hash is a literal 0 (see
// ZeroNulls) rather than a separate validity bit -- so a valid row whose combined hash
// happens to be 0 would otherwise be indistinguishable from a null row. Remap it to a
// fixed nonzero sentinel instead, scoped to this kernel's output rather than changing
// the shared hashing engine used by hash-join and group-by too. Must run after
// ZeroNulls, and after any children (e.g. struct fields, list values) already had their
// own such remap applied, so this only catches collisions introduced by this array's
// own combine step.
template <typename c_type, typename Hasher>
void RemapValidZeroHashes(const ArraySpan& array, c_type* out) {
  for (int64_t i = 0; i < array.length; i++) {
    if (out[i] == 0 && array.IsValid(i)) {
      out[i] = Hasher::CombineHashes(0, 0);
    }
  }
}

// Folds one row's child hashes into a single hash. Seeded with CombineHashes(0, 0),
// not 0, so an empty list doesn't collide with a null list (zeroed separately below).
// Free function since it only depends on c_type/Hasher, not ArrowType.
template <typename c_type, typename Hasher>
c_type CombineRange(const c_type* value_hashes, int64_t start, int64_t end) {
  c_type combined = Hasher::CombineHashes(0, 0);
  for (int64_t j = start; j < end; j++) {
    combined = Hasher::CombineHashes(combined, value_hashes[j]);
  }
  return combined;
}

// Combines rows for LIST/LARGE_LIST/MAP, whose offsets buffers differ only in width.
// `bias` is values.offset + rel_start, so offsets[i] - bias locates row i.
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

  // Hashes the [offset, offset + length) slice of `child`.
  static Result<std::shared_ptr<ArrayData>> HashChild(const ArraySpan& child,
                                                      int64_t offset, int64_t length,
                                                      LightContext* hash_ctx,
                                                      MemoryPool* memory_pool) {
    auto sliced = child;
    sliced.SetSlice(offset, length);
    auto arrow_type = TypeTraits<ArrowType>::type_singleton();
    auto buffer_size = sliced.length * sizeof(c_type);
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(buffer_size, memory_pool));
    ARROW_RETURN_NOT_OK(
        HashArray(sliced, hash_ctx, memory_pool, buffer->mutable_data_as<c_type>()));
    // No validity buffer needed: HashArray already zeroed out[i] for every
    // genuinely-null row of `sliced` above (via ZeroNulls or the valid-0 remap, both
    // of which correctly use sliced's own offset), so null-ness is already fully
    // encoded in the hash values themselves. A reused validity buffer would need
    // rebasing anyway, since it's unshifted (reading row i needs bit `sliced.offset +
    // i`) while this returned ArrayData has offset 0 and callers read its buffers
    // directly as row-0-based (see ToColumnArray).
    return ArrayData::Make(arrow_type, sliced.length, {nullptr, std::move(buffer)},
                           /*null_count=*/0);
  }

  static Status HashStructArray(const ArraySpan& array, LightContext* hash_ctx,
                                MemoryPool* memory_pool, c_type* out) {
    if (array.child_data.empty()) {
      // No fields to combine (e.g. struct<>): HashMultiColumn requires at least one
      // column (it reads cols[0] unconditionally), so give every row the same defined
      // hash here instead, then let ZeroNulls below zero out the actually-null rows.
      c_type empty_struct_hash = Hasher::CombineHashes(0, 0);
      for (int64_t i = 0; i < array.length; i++) {
        out[i] = empty_struct_hash;
      }
      ZeroNulls(array, out);
      return Status::OK();
    }
    std::vector<std::shared_ptr<ArrayData>> child_hashes(array.child_data.size());
    std::vector<KeyColumnArray> columns(array.child_data.size());
    // A field independently null within an otherwise-valid struct row must still hash
    // to 0 (GH-17211: see NestedNullFieldWithinValidStructHashesToZero), same as a
    // struct row that's null itself -- so the collision remap below must not touch
    // those rows, only rows where a 0 combined hash is a genuine coincidence.
    std::vector<bool> any_child_null(array.length, false);
    KeyColumnArray column;
    for (size_t i = 0; i < array.child_data.size(); i++) {
      auto child = array.child_data[i];
      // `child` may have its own offset independent of the struct's (a struct field
      // can itself be a slice; see StructArray::GetFlattenedField), so both compose:
      // struct row r reads child row (child.offset + array.offset + r).
      if (is_nested(child.type->id())) {
        // StructArray::Slice() doesn't reslice child_data, so `child` may be far
        // larger than what this slice of `array` references. Hash only the
        // referenced range instead of all of `child` (same idea as the list/map
        // paths above).
        ARROW_ASSIGN_OR_RAISE(child_hashes[i],
                              HashChild(child, child.offset + array.offset, array.length,
                                        hash_ctx, memory_pool));
        // child_hashes[i] is guaranteed (by this same remap, applied recursively) to
        // be 0 for row r iff that field's row is null OR contains a null somewhere
        // within it -- a direct child.IsNull(r) check isn't enough here, since a
        // nested field can propagate a *deeper* null (e.g. struct<f0: struct<g0:
        // null>>) while looking valid at this level.
        const c_type* child_hash_data = child_hashes[i]->buffers[1]->data_as<c_type>();
        for (int64_t r = 0; r < array.length; r++) {
          if (child_hash_data[r] == 0) {
            any_child_null[r] = true;
          }
        }
        ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(*child_hashes[i]));
        // child_hashes[i] already covers exactly [0, array.length): no further slice.
        columns[i] = column.Slice(0, array.length);
      } else {
        for (int64_t r = 0; r < array.length; r++) {
          if (child.IsNull(array.offset + r)) {
            any_child_null[r] = true;
          }
        }
        ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(child));
        columns[i] = column.Slice(child.offset + array.offset, array.length);
      }
    }
    Hasher::HashMultiColumn(columns, hash_ctx, out);
    // A null struct row's children may still look valid, so force 0 explicitly.
    ZeroNulls(array, out);
    for (int64_t i = 0; i < array.length; i++) {
      if (out[i] == 0 && array.IsValid(i) && !any_child_null[i]) {
        out[i] = Hasher::CombineHashes(0, 0);
      }
    }
    return Status::OK();
  }

  // Handles FIXED_SIZE_LIST, LARGE_LIST, LIST, and MAP. `offsets` is null for
  // FIXED_SIZE_LIST, which uses `list_size` as a constant stride instead. HashArray
  // computes rel_start/rel_end (the range of `values` actually referenced, since
  // ArrayData::Slice() doesn't slice child_data) since it already branches on type_id.
  template <typename OffsetT>
  static Status HashListArray(const ArraySpan& array, int64_t list_size,
                              const OffsetT* offsets, int64_t rel_start, int64_t rel_end,
                              LightContext* hash_ctx, MemoryPool* memory_pool,
                              c_type* out) {
    auto values = array.child_data[0];
    ARROW_ASSIGN_OR_RAISE(auto value_hashes,
                          HashChild(values, values.offset + rel_start,
                                    rel_end - rel_start, hash_ctx, memory_pool));
    const c_type* value_hash_data = value_hashes->buffers[1]->data_as<c_type>();
    // value_hash_data[k] corresponds to original row (values.offset + rel_start + k).

    // Fold every row, then zero nulls separately below; benchmarking showed this beats
    // branchless masking (masking costs every row; branch prediction handles nulls free).
    if (offsets != nullptr) {
      CombineOffsetRows<c_type, Hasher>(array.length, offsets, rel_start, value_hash_data,
                                        out);
    } else {
      for (int64_t i = 0; i < array.length; i++) {
        int64_t start = (array.offset + i) * list_size - rel_start;
        out[i] = CombineRange<c_type, Hasher>(value_hash_data, start, start + list_size);
      }
    }
    // Required: a null row's offset range isn't guaranteed empty, and even an empty
    // one would collide with CombineRange's seed for a real empty list.
    ZeroNulls(array, out);
    RemapValidZeroHashes<c_type, Hasher>(array, out);
    return Status::OK();
  }

  // Routes to the per-shape hashing routine for `array`'s type.
  static Status HashArray(const ArraySpan& array, LightContext* hash_ctx,
                          MemoryPool* memory_pool, c_type* out) {
    auto type_id = array.type->id();
    if (type_id == Type::EXTENSION) {
      auto extension_type = checked_cast<const ExtensionType*>(array.type);
      auto storage_array = array;
      storage_array.type = extension_type->storage_type().get();
      return HashArray(storage_array, hash_ctx, memory_pool, out);
    } else if (type_id == Type::STRUCT) {
      return HashStructArray(array, hash_ctx, memory_pool, out);
    } else if (type_id == Type::FIXED_SIZE_LIST) {
      auto list_size = checked_cast<const FixedSizeListType*>(array.type)->list_size();
      // rel_start/rel_end are logical indices into `values` (i.e. relative to
      // values.offset, not the physical buffer), matching what HashListArray expects.
      int64_t rel_start = 0, rel_end = 0;
      if (array.length > 0) {
        rel_start = array.offset * list_size;
        rel_end = (array.offset + array.length) * list_size;
      }
      return HashListArray<int32_t>(array, list_size, nullptr, rel_start, rel_end,
                                    hash_ctx, memory_pool, out);
    } else if (type_id == Type::LARGE_LIST) {
      auto offsets = array.GetValues<int64_t>(1);
      // offsets[] are already logical indices into `values` (relative to
      // values.offset), so rel_start/rel_end need no further adjustment here.
      int64_t rel_start = 0, rel_end = 0;
      if (array.length > 0) {
        rel_start = offsets[0];
        rel_end = offsets[array.length];
      }
      return HashListArray<int64_t>(array, 0, offsets, rel_start, rel_end, hash_ctx,
                                    memory_pool, out);
    } else if (is_list_like(type_id)) {
      // LIST and MAP both use 32-bit offsets, which are already logical indices into
      // `values` (relative to values.offset).
      auto offsets = array.GetValues<int32_t>(1);
      int64_t rel_start = 0, rel_end = 0;
      if (array.length > 0) {
        rel_start = offsets[0];
        rel_end = offsets[array.length];
      }
      return HashListArray<int32_t>(array, 0, offsets, rel_start, rel_end, hash_ctx,
                                    memory_pool, out);
    } else {
      KeyColumnArray column;
      ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(array));
      std::vector<KeyColumnArray> columns{column.Slice(array.offset, array.length)};
      Hasher::HashMultiColumn(columns, hash_ctx, out);
      // HashIntImp (used for fixed-width types up to 8 bytes) doesn't special-case an
      // all-zero-bits key (e.g. integer 0, float 0.0), so it can legitimately produce
      // the same 0 that HashMultiColumn uses as the null sentinel for actually-null
      // rows.
      RemapValidZeroHashes<c_type, Hasher>(array, out);
      return Status::OK();
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
    ARROW_RETURN_NOT_OK(
        HashArray(hash_input, &hash_ctx, exec_ctx->memory_pool(), result_ptr));

    return Status::OK();
  }
};

class HashableMatcher : public TypeMatcher {
 public:
  HashableMatcher() {}

  bool Matches(const DataType& type) const override {
    // Unwrap extension types (recursively, in case of nesting) so a union/view/REE
    // storage type is rejected here too, rather than passing dispatch and failing
    // later with a raw TypeError from HashArray/ToColumnArray.
    const DataType* physical_type = &type;
    while (physical_type->id() == Type::EXTENSION) {
      physical_type =
          checked_cast<const ExtensionType&>(*physical_type).storage_type().get();
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
     "Hash results are 32-bit and emitted for each row, including NULLs."),
    {"hash_input"}};

const FunctionDoc hash64_doc{
    "Construct a hash for every element of the input argument",
    ("This function is not suitable for cryptographic purposes.\n"
     "Hash results are 64-bit and emitted for each row, including NULLs."),
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
  kernel32.null_handling = NullHandling::OUTPUT_NOT_NULL;
  kernel64.null_handling = NullHandling::OUTPUT_NOT_NULL;
  ARROW_DCHECK_OK(hash32->AddKernel(std::move(kernel32)));
  ARROW_DCHECK_OK(hash64->AddKernel(std::move(kernel64)));

  // Register hash32 and hash64 functions
  ARROW_DCHECK_OK(registry->AddFunction(std::move(hash32)));
  ARROW_DCHECK_OK(registry->AddFunction(std::move(hash64)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
