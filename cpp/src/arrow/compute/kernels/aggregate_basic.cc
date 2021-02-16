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

#include <map>
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_basic_internal.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace compute {

namespace {

void AggregateConsume(KernelContext* ctx, const ExecBatch& batch) {
  checked_cast<ScalarAggregator*>(ctx->state())->Consume(ctx, batch);
}

void AggregateMerge(KernelContext* ctx, KernelState&& src, KernelState* dst) {
  checked_cast<ScalarAggregator*>(dst)->MergeFrom(ctx, std::move(src));
}

void AggregateFinalize(KernelContext* ctx, Datum* out) {
  checked_cast<ScalarAggregator*>(ctx->state())->Finalize(ctx, out);
}

}  // namespace

void AddAggKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                  ScalarAggregateFunction* func, SimdLevel::type simd_level,
                  bool nomerge) {
  ScalarAggregateKernel kernel(std::move(sig), init, AggregateConsume, AggregateMerge,
                               AggregateFinalize, nomerge);
  // Set the simd level
  kernel.simd_level = simd_level;
  DCHECK_OK(func->AddKernel(kernel));
}

namespace aggregate {

// ----------------------------------------------------------------------
// Count implementation

struct CountImpl : public ScalarAggregator {
  explicit CountImpl(CountOptions options) : options(std::move(options)) {}

  void Consume(KernelContext*, const ExecBatch& batch) override {
    const ArrayData& input = *batch[0].array();
    const int64_t nulls = input.GetNullCount();
    this->nulls += nulls;
    this->non_nulls += input.length - nulls;
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const CountImpl&>(src);
    this->non_nulls += other_state.non_nulls;
    this->nulls += other_state.nulls;
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    const auto& state = checked_cast<const CountImpl&>(*ctx->state());
    switch (state.options.count_mode) {
      case CountOptions::COUNT_NON_NULL:
        *out = Datum(state.non_nulls);
        break;
      case CountOptions::COUNT_NULL:
        *out = Datum(state.nulls);
        break;
      default:
        ctx->SetStatus(Status::Invalid("Unknown CountOptions encountered"));
        break;
    }
  }

  CountOptions options;
  int64_t non_nulls = 0;
  int64_t nulls = 0;
};

struct GroupedAggregator {
  // GroupedAggregator subclasses are expected to be constructible from
  // const FunctionOptions*. Will probably need an Init method as well
  virtual ~GroupedAggregator() = default;

  virtual void Consume(KernelContext*, const ExecBatch& batch,
                       const uint32_t* group_ids) = 0;

  virtual void Finalize(KernelContext* ctx, Datum* out) = 0;

  static Result<std::unique_ptr<GroupedAggregator>> Make(std::string function,
                                                         const FunctionOptions* options);
};

struct GroupedCountImpl : public GroupedAggregator {
  explicit GroupedCountImpl(const FunctionOptions* options)
      : options(checked_cast<const CountOptions&>(*options)) {}

  void Consume(KernelContext* ctx, const ExecBatch& batch,
               const uint32_t* group_ids) override {
    if (batch.length == 0) return;

    // maybe a batch of group_ids should include the min/max group id
    auto max_group = *std::max_element(group_ids, group_ids + batch.length);
    if (max_group >= counts.size()) {
      counts.resize(max_group + 1, 0);
    }

    if (options.count_mode == CountOptions::COUNT_NON_NULL) {
      auto input = batch[0].make_array();

      for (int64_t i = 0; i < input->length(); ++i) {
        if (input->IsNull(i)) continue;
        counts[group_ids[i]]++;
      }
    } else {
      for (int64_t i = 0; i < batch.length; ++i) {
        counts[group_ids[i]]++;
      }
    }
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    KERNEL_ASSIGN_OR_RAISE(auto counts_buf, ctx,
                           ctx->Allocate(sizeof(int64_t) * counts.size()));
    std::copy(counts.begin(), counts.end(),
              reinterpret_cast<int64_t*>(counts_buf->mutable_data()));
    *out = std::make_shared<Int64Array>(counts.size(), std::move(counts_buf));
  }

  CountOptions options;
  std::vector<int64_t> counts;
};

struct GroupedSumImpl : public GroupedAggregator {
  explicit GroupedSumImpl(const FunctionOptions*) {}

  void Consume(KernelContext* ctx, const ExecBatch& batch,
               const uint32_t* group_ids) override {
    if (batch.length == 0) return;

    // maybe a batch of group_ids should include the min/max group id
    auto max_group = *std::max_element(group_ids, group_ids + batch.length);
    if (max_group >= sums.size()) {
      sums.resize(max_group + 1, 0.0);
    }

    DCHECK_EQ(batch[0].type()->id(), Type::DOUBLE);
    auto input = batch[0].array_as<DoubleArray>();

    for (int64_t i = 0; i < input->length(); ++i) {
      if (input->IsNull(i)) continue;
      sums[group_ids[i]] += input->Value(i);
    }
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    KERNEL_ASSIGN_OR_RAISE(auto sums_buf, ctx,
                           ctx->Allocate(sizeof(double) * sums.size()));
    std::copy(sums.begin(), sums.end(),
              reinterpret_cast<double*>(sums_buf->mutable_data()));
    *out = std::make_shared<DoubleArray>(sums.size(), std::move(sums_buf));
  }

  std::vector<double> sums;
};

Result<std::unique_ptr<GroupedAggregator>> GroupedAggregator::Make(
    std::string function, const FunctionOptions* options) {
  if (function == "count") {
    return ::arrow::internal::make_unique<GroupedCountImpl>(options);
  }
  if (function == "sum") {
    return ::arrow::internal::make_unique<GroupedSumImpl>(options);
  }
  return Status::NotImplemented("Grouped aggregate ", function);
}

std::unique_ptr<KernelState> CountInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<CountImpl>(
      static_cast<const CountOptions&>(*args.options));
}

// ----------------------------------------------------------------------
// Sum implementation

template <typename ArrowType>
struct SumImplDefault : public SumImpl<ArrowType, SimdLevel::NONE> {};

template <typename ArrowType>
struct MeanImplDefault : public MeanImpl<ArrowType, SimdLevel::NONE> {};

std::unique_ptr<KernelState> SumInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<SumImplDefault> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

std::unique_ptr<KernelState> MeanInit(KernelContext* ctx, const KernelInitArgs& args) {
  SumLikeInit<MeanImplDefault> visitor(ctx, *args.inputs[0].type);
  return visitor.Create();
}

// ----------------------------------------------------------------------
// MinMax implementation

std::unique_ptr<KernelState> MinMaxInit(KernelContext* ctx, const KernelInitArgs& args) {
  MinMaxInitState<SimdLevel::NONE> visitor(
      ctx, *args.inputs[0].type, args.kernel->signature->out_type().type(),
      static_cast<const MinMaxOptions&>(*args.options));
  return visitor.Create();
}

// ----------------------------------------------------------------------
// Any implementation

struct BooleanAnyImpl : public ScalarAggregator {
  void Consume(KernelContext*, const ExecBatch& batch) override {
    // short-circuit if seen a True already
    if (this->any == true) {
      return;
    }

    const auto& data = *batch[0].array();
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[0], data.offset, data.buffers[1], data.offset, data.length);
    int64_t position = 0;
    while (position < data.length) {
      const auto block = counter.NextAndBlock();
      if (block.popcount > 0) {
        this->any = true;
        break;
      }
      position += block.length;
    }
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const BooleanAnyImpl&>(src);
    this->any |= other.any;
  }

  void Finalize(KernelContext*, Datum* out) override {
    out->value = std::make_shared<BooleanScalar>(this->any);
  }
  bool any = false;
};

std::unique_ptr<KernelState> AnyInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<BooleanAnyImpl>();
}

// ----------------------------------------------------------------------
// All implementation

struct BooleanAllImpl : public ScalarAggregator {
  void Consume(KernelContext*, const ExecBatch& batch) override {
    // short-circuit if seen a false already
    if (this->all == false) {
      return;
    }

    const auto& data = *batch[0].array();
    arrow::internal::OptionalBinaryBitBlockCounter counter(
        data.buffers[1], data.offset, data.buffers[0], data.offset, data.length);
    int64_t position = 0;
    while (position < data.length) {
      const auto block = counter.NextOrNotBlock();
      if (!block.AllSet()) {
        this->all = false;
        break;
      }
      position += block.length;
    }
  }

  void MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const BooleanAllImpl&>(src);
    this->all &= other.all;
  }

  void Finalize(KernelContext*, Datum* out) override {
    out->value = std::make_shared<BooleanScalar>(this->all);
  }
  bool all = true;
};

std::unique_ptr<KernelState> AllInit(KernelContext*, const KernelInitArgs& args) {
  return ::arrow::internal::make_unique<BooleanAllImpl>();
}

struct GroupByImpl : public ScalarAggregator {
  void Consume(KernelContext* ctx, const ExecBatch& batch) override {
    ArrayDataVector aggregands, keys;

    size_t i;
    for (i = 0; i < aggregators.size(); ++i) {
      aggregands.push_back(batch[i].array());
    }
    while (i < static_cast<size_t>(batch.num_values())) {
      keys.push_back(batch[i++].array());
    }

    auto key64 = batch[aggregators.size()].array_as<Int64Array>();
    if (key64->null_count() != 0) {
      ctx->SetStatus(Status::NotImplemented("nulls in key column"));
      return;
    }

    const int64_t* key64_raw = key64->raw_values();

    std::vector<uint32_t> group_ids(batch.length);
    for (int64_t i = 0; i < batch.length; ++i) {
      uint64_t key = key64_raw[i];
      auto iter = map_.find(key);
      if (iter == map_.end()) {
        group_ids[i] = static_cast<uint32_t>(keys_.size());
        keys_.push_back(key);
        map_.insert(std::make_pair(key, group_ids[i]));
      } else {
        group_ids[i] = iter->second;
      }
    }

    for (size_t i = 0; i < aggregators.size(); ++i) {
      ExecBatch aggregand_batch{{aggregands[i]}, batch.length};
      aggregators[i]->Consume(ctx, aggregand_batch, group_ids.data());
      if (ctx->HasError()) return;
    }
  }

  void MergeFrom(KernelContext* ctx, KernelState&& src) override {
    // TODO(michalursa) merge two hash tables
  }

  void Finalize(KernelContext* ctx, Datum* out) override {
    FieldVector out_fields(aggregators.size() + 1);
    ArrayDataVector out_columns(aggregators.size() + 1);
    for (size_t i = 0; i < aggregators.size(); ++i) {
      Datum aggregand;
      aggregators[i]->Finalize(ctx, &aggregand);
      if (ctx->HasError()) return;
      out_columns[i] = aggregand.array();
      out_fields[i] = field(options.aggregates[i].name, aggregand.type());
    }

    int64_t length = keys_.size();
    KERNEL_ASSIGN_OR_RAISE(auto key_buf, ctx, ctx->Allocate(sizeof(int64_t) * length));
    std::copy(keys_.begin(), keys_.end(),
              reinterpret_cast<int64_t*>(key_buf->mutable_data()));
    auto key = std::make_shared<Int64Array>(length, std::move(key_buf));

    out_columns.back() = key->data();
    out_fields.back() = field(options.key_names[0], key->type());

    *out = ArrayData::Make(struct_(std::move(out_fields)), key->length(),
                           {/*null_bitmap=*/nullptr}, std::move(out_columns));
  }

  std::map<uint64_t, uint32_t> map_;
  std::vector<uint64_t> keys_;

  GroupByOptions options;
  std::vector<std::unique_ptr<GroupedAggregator>> aggregators;
};

std::unique_ptr<KernelState> GroupByInit(KernelContext* ctx, const KernelInitArgs& args) {
  // TODO(michalursa) do construction of group by implementation
  auto impl = ::arrow::internal::make_unique<GroupByImpl>();
  impl->options = *checked_cast<const GroupByOptions*>(args.options);
  const auto& aggregates = impl->options.aggregates;

  if (aggregates.size() > args.inputs.size()) {
    ctx->SetStatus(Status::Invalid("more aggegates than inputs!"));
    return nullptr;
  }

  impl->aggregators.resize(aggregates.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ctx->SetStatus(GroupedAggregator::Make(aggregates[i].function, aggregates[i].options)
                       .Value(&impl->aggregators[i]));
    if (ctx->HasError()) return nullptr;
  }

  size_t n_keys = args.inputs.size() - aggregates.size();
  if (n_keys != 1) {
    ctx->SetStatus(Status::NotImplemented("more than one key"));
    return nullptr;
  }

  if (args.inputs.back().type->id() != Type::INT64) {
    ctx->SetStatus(
        Status::NotImplemented("key of type", args.inputs.back().type->ToString()));
    return nullptr;
  }

  return impl;
}

void AddBasicAggKernels(KernelInit init,
                        const std::vector<std::shared_ptr<DataType>>& types,
                        std::shared_ptr<DataType> out_ty, ScalarAggregateFunction* func,
                        SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[InT] -> scalar[OutT]
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

void AddMinMaxKernels(KernelInit init,
                      const std::vector<std::shared_ptr<DataType>>& types,
                      ScalarAggregateFunction* func, SimdLevel::type simd_level) {
  for (const auto& ty : types) {
    // array[T] -> scalar[struct<min: T, max: T>]
    auto out_ty = struct_({field("min", ty), field("max", ty)});
    auto sig = KernelSignature::Make({InputType::Array(ty)}, ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, simd_level);
  }
}

}  // namespace aggregate

namespace internal {
namespace {

const FunctionDoc count_doc{"Count the number of null / non-null values",
                            ("By default, non-null values are counted.\n"
                             "This can be changed through CountOptions."),
                            {"array"},
                            "CountOptions"};

const FunctionDoc sum_doc{
    "Sum values of a numeric array", ("Null values are ignored."), {"array"}};

const FunctionDoc mean_doc{"Compute the mean of a numeric array",
                           ("Null values are ignored. The result is always computed\n"
                            "as a double, regardless of the input types"),
                           {"array"}};

const FunctionDoc min_max_doc{"Compute the minimum and maximum values of a numeric array",
                              ("Null values are ignored by default.\n"
                               "This can be changed through MinMaxOptions."),
                              {"array"},
                              "MinMaxOptions"};

const FunctionDoc any_doc{
    "Test whether any element in a boolean array evaluates to true.",
    ("Null values are ignored."),
    {"array"}};

const FunctionDoc all_doc{
    "Test whether all elements in a boolean array evaluate to true.",
    ("Null values are ignored."),
    {"array"}};

// TODO(michalursa) add FunctionDoc for group_by
const FunctionDoc group_by_doc{"", (""), {}};

}  // namespace

void RegisterScalarAggregateBasic(FunctionRegistry* registry) {
  static auto default_count_options = CountOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "count", Arity::Unary(), &count_doc, &default_count_options);

  // Takes any array input, outputs int64 scalar
  InputType any_array(ValueDescr::ARRAY);
  AddAggKernel(KernelSignature::Make({any_array}, ValueDescr::Scalar(int64())),
               aggregate::CountInit, func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("sum", Arity::Unary(), &sum_doc);
  aggregate::AddBasicAggKernels(aggregate::SumInit, {boolean()}, int64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, SignedIntTypes(), int64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, UnsignedIntTypes(), uint64(),
                                func.get());
  aggregate::AddBasicAggKernels(aggregate::SumInit, FloatingPointTypes(), float64(),
                                func.get());
  // Add the SIMD variants for sum
#if defined(ARROW_HAVE_RUNTIME_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX512)
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddSumAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddSumAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  func = std::make_shared<ScalarAggregateFunction>("mean", Arity::Unary(), &mean_doc);
  aggregate::AddBasicAggKernels(aggregate::MeanInit, {boolean()}, float64(), func.get());
  aggregate::AddBasicAggKernels(aggregate::MeanInit, NumericTypes(), float64(),
                                func.get());
  // Add the SIMD variants for mean
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddMeanAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddMeanAvx512AggKernels(func.get());
  }
#endif
  DCHECK_OK(registry->AddFunction(std::move(func)));

  static auto default_minmax_options = MinMaxOptions::Defaults();
  func = std::make_shared<ScalarAggregateFunction>("min_max", Arity::Unary(),
                                                   &min_max_doc, &default_minmax_options);
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, {boolean()}, func.get());
  aggregate::AddMinMaxKernels(aggregate::MinMaxInit, NumericTypes(), func.get());
  // Add the SIMD variants for min max
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    aggregate::AddMinMaxAvx2AggKernels(func.get());
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    aggregate::AddMinMaxAvx512AggKernels(func.get());
  }
#endif

  DCHECK_OK(registry->AddFunction(std::move(func)));

  // any
  func = std::make_shared<ScalarAggregateFunction>("any", Arity::Unary(), &any_doc);
  aggregate::AddBasicAggKernels(aggregate::AnyInit, {boolean()}, boolean(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // all
  func = std::make_shared<ScalarAggregateFunction>("all", Arity::Unary(), &all_doc);
  aggregate::AddBasicAggKernels(aggregate::AllInit, {boolean()}, boolean(), func.get());
  DCHECK_OK(registry->AddFunction(std::move(func)));

  // group_by
  func = std::make_shared<ScalarAggregateFunction>("group_by", Arity::VarArgs(),
                                                   &group_by_doc);
  // aggregate::AddBasicAggKernels(aggregate::GroupByInit, {null()}, null(), func.get());
  {
    InputType any_array(ValueDescr::ARRAY);
    auto sig = KernelSignature::Make({any_array}, ValueDescr::Array(int64()), true);
    AddAggKernel(std::move(sig), aggregate::GroupByInit, func.get(), SimdLevel::NONE,
                 true);
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
  // TODO(michalursa) add Kernels to the function named "group_by"
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
