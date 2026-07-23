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

template <typename ArrowType, typename Hasher>
struct FastHashScalar {
  using c_type = typename ArrowType::c_type;

  // Folds the hashes of one list-like row's children into a single hash. Seeded
  // with CombineHashes(0, 0) rather than 0 so an empty list doesn't collide with
  // a null list (zeroed separately below).
  static c_type CombineRange(const c_type* value_hashes, int64_t start, int64_t end) {
    c_type combined = Hasher::CombineHashes(0, 0);
    for (int64_t j = start; j < end; j++) {
      combined = Hasher::CombineHashes(combined, value_hashes[j]);
    }
    return combined;
  }

  static Result<std::shared_ptr<ArrayData>> HashChild(const ArraySpan& child,
                                                      LightContext* hash_ctx,
                                                      MemoryPool* memory_pool) {
    auto arrow_type = TypeTraits<ArrowType>::type_singleton();
    auto buffer_size = child.length * sizeof(c_type);
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(buffer_size, memory_pool));
    ARROW_RETURN_NOT_OK(
        HashArray(child, hash_ctx, memory_pool, buffer->mutable_data_as<c_type>()));
    return ArrayData::Make(arrow_type, child.length,
                           {child.GetBuffer(0), std::move(buffer)}, child.null_count);
  }

  static Status HashArray(const ArraySpan& array, LightContext* hash_ctx,
                          MemoryPool* memory_pool, c_type* out) {
    // KeyColumnArray objects are being passed to the hashing utility
    KeyColumnArray column;
    std::vector<KeyColumnArray> columns(1);

    auto type_id = array.type->id();
    if (type_id == Type::EXTENSION) {
      auto extension_type = checked_cast<const ExtensionType*>(array.type);
      auto storage_array = array;
      storage_array.type = extension_type->storage_type().get();
      return HashArray(storage_array, hash_ctx, memory_pool, out);
    }

    if (type_id == Type::STRUCT) {
      std::vector<std::shared_ptr<ArrayData>> child_hashes(array.child_data.size());
      columns.resize(array.child_data.size());
      for (size_t i = 0; i < array.child_data.size(); i++) {
        auto child = array.child_data[i];
        if (is_nested(child.type->id())) {
          ARROW_ASSIGN_OR_RAISE(child_hashes[i], HashChild(child, hash_ctx, memory_pool));
          ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(*child_hashes[i]));
        } else {
          ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(child));
        }
        columns[i] = column.Slice(array.offset, array.length);
      }
      Hasher::HashMultiColumn(columns, hash_ctx, out);
      // A struct's own null bit is independent of its children's; a null struct row
      // commonly still has valid-looking child data, so it must be forced to 0
      // explicitly rather than relying on the per-field columns above.
      ZeroNulls(array, out);
    } else if (is_list_like(type_id)) {
      // Each child element is already hashed by HashChild; fold each row's slice of
      // child hashes down to one value directly, rather than routing through
      // Hasher::HashMultiColumn.
      auto values = array.child_data[0];
      ARROW_ASSIGN_OR_RAISE(auto value_hashes, HashChild(values, hash_ctx, memory_pool));
      const c_type* value_hash_data = value_hashes->buffers[1]->data_as<c_type>();

      // Fold every row unconditionally, then zero nulls in a separate pass below;
      // benchmarking showed this beats branchlessly masking each row inline (branch
      // prediction handles the null pattern for free; masking pays on every row).
      if (type_id == Type::FIXED_SIZE_LIST) {
        auto list_size = checked_cast<const FixedSizeListType*>(array.type)->list_size();
        for (int64_t i = 0; i < array.length; i++) {
          int64_t start = (array.offset + i) * list_size - values.offset;
          out[i] = CombineRange(value_hash_data, start, start + list_size);
        }
      } else if (type_id == Type::LARGE_LIST) {
        const int64_t* offsets = array.GetValues<int64_t>(1);
        for (int64_t i = 0; i < array.length; i++) {
          out[i] = CombineRange(value_hash_data, offsets[i] - values.offset,
                                offsets[i + 1] - values.offset);
        }
      } else {
        // LIST and MAP both use 32-bit offsets.
        const int32_t* offsets = array.GetValues<int32_t>(1);
        for (int64_t i = 0; i < array.length; i++) {
          out[i] = CombineRange(value_hash_data, offsets[i] - values.offset,
                                offsets[i + 1] - values.offset);
        }
      }
      // Not optional: a null row's offset range isn't guaranteed empty (it may span
      // undefined memory per the columnar format spec), and even an empty range would
      // otherwise collide with CombineRange's seed for a real empty list.
      ZeroNulls(array, out);
    } else {
      ARROW_ASSIGN_OR_RAISE(column, ToColumnArray(array));
      columns[0] = column.Slice(array.offset, array.length);
      Hasher::HashMultiColumn(columns, hash_ctx, out);
    }
    return Status::OK();
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
    return !(is_union(type) || is_binary_view_like(type) || is_list_view(type) ||
             type.id() == Type::RUN_END_ENCODED);
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
