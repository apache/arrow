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

#include <optional>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/aggregate_var_std_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/int128_internal.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

Result<std::shared_ptr<Buffer>> NullBitmapAnd(MemoryPool* pool, const ArraySpan& x,
                                              const ArraySpan& y, int64_t* offset) {
  DCHECK(x.length == y.length);
  auto& x_null = x.buffers[0];
  auto& y_null = y.buffers[0];
  if (x_null.data == NULLPTR) {
    *offset = y.offset;
    return y_null.owner ? *y_null.owner
                        : std::make_shared<Buffer>(y_null.data, y_null.size);
  }
  if (y_null.data == NULLPTR) {
    *offset = x.offset;
    return x_null.owner ? *x_null.owner
                        : std::make_shared<Buffer>(x_null.data, x_null.size);
  }

  *offset = 0;
  return ::arrow::internal::BitmapAnd(pool, x_null.data, x.offset, y_null.data, y.offset,
                                      x.length, 0);
}

// Caller should hold a strong reference to the buffer while the returned
// BufferSpan is alive.
BufferSpan BufferSpanFromBuffer(const std::shared_ptr<Buffer>& buffer) {
  BufferSpan span;
  span.data = const_cast<uint8_t*>(buffer->data());
  span.size = buffer->size();
  span.owner = &buffer;
  return span;
}

[[maybe_unused]] Result<BufferSpan> NullBitmapAnd(MemoryPool* pool, const ArraySpan& x,
                                                  const ArraySpan& y, int64_t* offset,
                                                  std::shared_ptr<Buffer>* scratch) {
  ARROW_ASSIGN_OR_RAISE(*scratch, NullBitmapAnd(pool, x, y, offset));
  return BufferSpanFromBuffer(*scratch);
}

// Intermediate result for covariance calculation between two random variables
struct CovarianceResult {
  // Are all pairs in the input run are valid (both sides valid)
  bool all_valid = true;
  // Number of valid pairs in the input run
  int64_t count = 0;

  // The value for all the means below is undefined when count == 0 and
  // shouldn't be considered when merging results of finalizing the calculation.

  // Covariance of two random variables is shift-invariant:
  //
  //     Covar(X, Y) = Covar(X - k_x, Y - k_y)
  //
  // To make the calculation of m2 more numerically stable we can pick k_x and
  // k_y to be x_mean and y_mean. This idea is leveraged by the
  // TwoPassCovarianceCalculator.
  //
  // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Two-pass

  // Mean of the left-hand-side values in the valid pairs
  double x_mean;
  // Mean of the right-hand-side values in the valid pairs
  double y_mean;
  // m2 is SUM(X * Y) shifted by the means: m2 = SUM((X - x_mean) * (Y - y_mean))
  double m2;

  void MergeFrom(const CovarianceResult& other) {
    this->all_valid = this->all_valid && other.all_valid;
    if (other.count == 0) {
      return;
    }
    if (this->count == 0) {
      this->count = other.count;
      this->x_mean = other.x_mean;
      this->y_mean = other.y_mean;
      this->m2 = other.m2;
      return;
    }

    const int64_t new_count = this->count + other.count;
    const double new_x_mean =
        (this->count * this->x_mean + other.count * other.x_mean) / new_count;
    const double new_y_mean =
        (this->count * this->y_mean + other.count * other.y_mean) / new_count;
    const double new_m2 =
        this->m2 + other.m2 +
        this->count * ((this->x_mean - new_x_mean) * (this->y_mean - new_y_mean)) +
        other.count * ((other.x_mean - new_x_mean) * (other.y_mean - new_y_mean));

    this->count = new_count;
    this->x_mean = new_x_mean;
    this->y_mean = new_y_mean;
    this->m2 = new_m2;
  }

  std::optional<double> Finalize(const VarianceOptions& options) const {
    if (count <= options.ddof || count < options.min_count ||
        (!all_valid && !options.skip_nulls)) {
      return std::nullopt;
    }
    return m2 / (count - options.ddof);
  }
};

// PRE-CONDITION to all member functions of this class and its sub-classes:
//
//   null_count is less than size of the input
//
// This prevents any division by zero and makes all type-specialized
// calculations much cleaner.
//
// The two subclasses -- TextbookCovarianceCalculator and
// TwoPassCovarianceCalculator -- are used for the Array and Array case. The
// appropriate calculator can be chosen by the
// CovariantCalculatorForArrayAndArray<> template.
struct CovarianceCalculator {
  const int decimal_scale;

  explicit CovarianceCalculator(int decimal_scale) : decimal_scale(decimal_scale) {}

  template <typename T>
  double ToDouble(T value) const {
    return static_cast<double>(value);
  }
  double ToDouble(const Decimal128& value) const { return value.ToDouble(decimal_scale); }
  double ToDouble(const Decimal256& value) const { return value.ToDouble(decimal_scale); }

  template <typename ArrowType>
  double CalculateMean(const BufferSpan& null_bitmap, int64_t null_bitmap_offset,
                       int64_t null_count, const ArraySpan& array) const {
    using CType = typename TypeTraits<ArrowType>::CType;
    using SumType = typename internal::GetSumType<ArrowType>::SumType;

    DCHECK_LT(null_count, array.length);
    const int64_t valid_count = array.length - null_count;
    const auto* values = array.GetValues<CType>(1);
    const auto sum = internal::SumArray<CType, SumType, SimdLevel::NONE>(
        null_bitmap, null_bitmap_offset, null_count, values, array.length);
    return ToDouble(sum) / valid_count;
  }

  // Scalar and Scalar
  template <typename ArrowType1, typename ArrowType2>
  CovarianceResult Calculate(const Scalar& x, const Scalar& y,
                             int64_t valid_count) const {
    DCHECK_GT(valid_count, 0);

    CovarianceResult res;
    res.all_valid = true;
    res.count = valid_count;
    res.x_mean = ToDouble(UnboxScalar<ArrowType1>::Unbox(x));
    res.y_mean = ToDouble(UnboxScalar<ArrowType2>::Unbox(y));
    res.m2 = 0;
    return res;
  }

  // Array and Scalar
  template <typename ArrowType1, typename ArrowType2>
  CovarianceResult Calculate(const BufferSpan& null_bitmap, int64_t null_bitmap_offset,
                             int64_t null_count, const ArraySpan& x,
                             const Scalar& y) const {
    DCHECK_LT(null_count, x.length);

    CovarianceResult res;
    res.all_valid = null_count == 0;
    res.count = x.length - null_count;
    res.x_mean =
        CalculateMean<ArrowType1>(null_bitmap, null_bitmap_offset, null_count, x);
    res.y_mean = ToDouble(UnboxScalar<ArrowType2>::Unbox(y));
    res.m2 = 0;
    return res;
  }
};

// Textbook algorithm to be used when both arrays are of an small integer
// type (<= 32 bits).
struct TextbookCovarianceCalculator : public CovarianceCalculator {
  explicit TextbookCovarianceCalculator(int decimal_scale)
      : CovarianceCalculator(decimal_scale) {}

  // Array and Array
  template <typename ArrowType1, typename ArrowType2>
  CovarianceResult Calculate(const BufferSpan& null_bitmap, int64_t null_bitmap_offset,
                             int64_t null_count, const ArraySpan& x,
                             const ArraySpan& y) const {
    // FIXME(felipecrv): implement TextbookCovarianceCalculator for int*-int* operands
    DCHECK(false);
    return {};
  }
};

// Two-pass algorithm to be used when the TextbookCovarianceCalculator is not
// appropriate. See CovariantCalculatorForArrayAndArray for the detailed
// selection logic.
struct TwoPassCovarianceCalculator : public CovarianceCalculator {
  explicit TwoPassCovarianceCalculator(int decimal_scale)
      : CovarianceCalculator(decimal_scale) {}

  // Array and Array
  template <typename ArrowType1, typename ArrowType2>
  CovarianceResult Calculate(const BufferSpan& null_bitmap, int64_t null_bitmap_offset,
                             int64_t null_count, const ArraySpan& x,
                             const ArraySpan& y) const {
    DCHECK_LT(null_count, x.length);
    const int64_t valid_count = x.length - null_count;

    using CType1 = typename TypeTraits<ArrowType1>::CType;
    using CType2 = typename TypeTraits<ArrowType2>::CType;

    // First pass: calculate means
    const double x_mean =
        CalculateMean<ArrowType1>(null_bitmap, null_bitmap_offset, null_count, x);
    const double y_mean =
        CalculateMean<ArrowType2>(null_bitmap, null_bitmap_offset, null_count, y);

    // Second pass: calculate m2
    const auto* x_values = x.GetValues<CType1>(1);
    const auto* y_values = y.GetValues<CType2>(1);
    const double m2 = internal::SumTwoArrays<CType1, CType2, double, SimdLevel::NONE>(
        null_bitmap, null_bitmap_offset, null_count, x_values, y_values, x.length,
        [this, x_mean, y_mean](CType1 value1, CType2 value2) {
          const double v1 = ToDouble(value1);
          const double v2 = ToDouble(value2);
          return (v1 - x_mean) * (v2 - y_mean);
        });

    CovarianceResult res;
    res.all_valid = null_count == 0;
    res.count = valid_count;
    res.x_mean = x_mean;
    res.y_mean = y_mean;
    res.m2 = m2;
    return res;
  }
};

template <typename ArrowType>
struct IsNaivelySummableType {
  using CType = typename TypeTraits<ArrowType>::CType;

  static constexpr bool value =
      !is_floating_type<ArrowType>::value && (sizeof(CType) <= 4);
};

template <typename ArrowType1, typename ArrowType2>
using CovariantCalculatorForArrayAndArray = typename std::conditional<
    IsNaivelySummableType<ArrowType1>::value && IsNaivelySummableType<ArrowType2>::value,
    TextbookCovarianceCalculator, TwoPassCovarianceCalculator>::type;

struct BaseCovarianceState {
  int32_t decimal_scale;
  VarianceOptions options;

  // Scratch buffer for null bitmap calculation
  std::shared_ptr<Buffer> scratch;

  BaseCovarianceState(int32_t decimal_scale, VarianceOptions options)
      : decimal_scale(decimal_scale), options(options) {}

  ~BaseCovarianceState() = default;
};

template <typename ArrowType1, typename ArrowType2>
struct CovarianceState : public BaseCovarianceState {
  using ThisType = CovarianceState<ArrowType1, ArrowType2>;

  CovarianceState(int32_t decimal_scale, VarianceOptions options)
      : BaseCovarianceState(decimal_scale, options) {}

  // Array and Array (implementation)
  CovarianceResult ConsumeImpl(const BufferSpan& null_bitmap, int64_t null_bitmap_offset,
                               int64_t null_count, const ArraySpan& x,
                               const ArraySpan& y) {
    const bool all_valid = null_count == 0;
    const int64_t valid_count = x.length - null_count;
    if (valid_count == 0 || (!all_valid && !options.skip_nulls)) {
      CovarianceResult res;
      res.all_valid = all_valid;
      res.count = 0;
      return res;
    }
    using Calculator = CovariantCalculatorForArrayAndArray<ArrowType1, ArrowType2>;
    const Calculator calculator(decimal_scale);
    return calculator.template Calculate<ArrowType1, ArrowType2>(
        null_bitmap, null_bitmap_offset, null_count, x, y);
  }

  // Array and Array
  Result<CovarianceResult> Consume(MemoryPool* pool, const ArraySpan& x,
                                   const ArraySpan& y) {
    // Compute a combined null bitmap of x and y -- so we can consider only the
    // pairs that are valid on both sides.
    int64_t null_bitmap_offset = 0;
    auto null_bitmap_result = NullBitmapAnd(pool, x, y, &null_bitmap_offset, &scratch);
    if (!null_bitmap_result.ok()) {
      return null_bitmap_result.status();
    }
    const auto null_bitmap = std::move(null_bitmap_result).ValueOrDie();

    const int64_t null_count =
        null_bitmap.data ? x.length - ::arrow::internal::CountSetBits(
                                          null_bitmap.data, null_bitmap_offset, x.length)
                         : 0;
    return ConsumeImpl(null_bitmap, null_bitmap_offset, null_count, x, y);
  }

  // Scalar and Scalar
  CovarianceResult Consume(const Scalar& x, const Scalar& y, int64_t count) {
    CovarianceResult res;
    if (count == 0) {
      res.all_valid = true;
      res.count = 0;
      return res;
    }
    if (!x.is_valid || !y.is_valid) {
      res.all_valid = false;
      res.count = 0;
      return res;
    }
    const CovarianceCalculator calculator(decimal_scale);
    res = calculator.Calculate<ArrowType1, ArrowType2>(x, y, count);
    return res;
  }

  // Array and Scalar
  CovarianceResult Consume(const ArraySpan& x, const Scalar& y) {
    CovarianceResult res;
    if (x.length == 0) {
      res.all_valid = true;
      res.count = 0;
      return res;
    }
    if (!y.is_valid) {
      res.all_valid = false;
      res.count = 0;
      return res;
    }

    const BufferSpan null_bitmap = x.buffers[0];
    const int64_t null_count = x.GetNullCount();
    const int64_t null_bitmap_offset = x.offset;

    const bool all_valid = null_count == 0;
    const int64_t valid_count = x.length - null_count;
    if (valid_count == 0 || (!all_valid && !options.skip_nulls)) {
      res.all_valid = all_valid;
      res.count = 0;
      return res;
    }

    const CovarianceCalculator calculator(decimal_scale);
    res = calculator.Calculate<ArrowType1, ArrowType2>(null_bitmap, null_bitmap_offset,
                                                       null_count, x, y);
    return res;
  }
};

template <typename ArrowType1, typename ArrowType2>
struct CovarianceImpl : public ScalarAggregator {
  using ThisType = CovarianceImpl<ArrowType1, ArrowType2>;

  int decimal_scale;
  std::shared_ptr<DataType> out_type;
  VarianceOptions options;

  std::optional<CovarianceState<ArrowType1, ArrowType2>> state;
  std::optional<CovarianceState<ArrowType2, ArrowType1>> state_commuted;
  CovarianceResult result;

  CovarianceImpl(int32_t decimal_scale, std::shared_ptr<DataType> out_type,
                 const VarianceOptions& options)
      : decimal_scale(decimal_scale), out_type(std::move(out_type)), options(options) {}

  Status Consume(KernelContext* ctx, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      this->state.emplace(decimal_scale, options);
      if (batch[1].is_array()) {
        const auto res =
            this->state->Consume(ctx->memory_pool(), batch[0].array, batch[1].array);
        if (!res.ok()) {
          return res.status();
        }
        this->result = res.ValueOrDie();
      } else {
        this->result = this->state->Consume(batch[0].array, *batch[1].scalar);
      }
    } else {
      this->state_commuted.emplace(decimal_scale, options);
      if (batch[1].is_array()) {
        this->result = this->state_commuted->Consume(batch[1].array, *batch[0].scalar);
      } else {
        this->result = this->state_commuted->Consume(*batch[1].scalar, *batch[0].scalar,
                                                     batch.length);
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->result.MergeFrom(other.result);
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    const auto maybe_covar = this->result.Finalize(options);
    out->value = maybe_covar.has_value() ? std::make_shared<DoubleScalar>(*maybe_covar)
                                         : std::make_shared<DoubleScalar>();
    return Status::OK();
  }
};

struct CovarianceInitState {
  std::unique_ptr<KernelState> state;
  KernelContext* ctx;
  const DataType& in_type1;
  const DataType& in_type2;
  const std::shared_ptr<DataType>& out_type;
  const VarianceOptions& options;

  CovarianceInitState(KernelContext* ctx, const DataType& in_type1,
                      const DataType& in_type2, const std::shared_ptr<DataType>& out_type,
                      const VarianceOptions& options)
      : ctx(ctx),
        in_type1(in_type1),
        in_type2(in_type2),
        out_type(out_type),
        options(options) {}

  template <typename T>
  struct is_accepted_numeric_type {
    static const bool value =
        is_number_type<T>::value && !std::is_base_of<HalfFloatType, T>::value;
  };

  void Visit(const DataType& type1, const DataType& type2) {
    // no-op
  }

  template <typename Type1, typename Type2>
  enable_if_t<is_accepted_numeric_type<Type1>::value &&
              is_accepted_numeric_type<Type2>::value>
  Visit(const Type1&, const Type2&) {
    state.reset(new CovarianceImpl<Type1, Type2>(/*decimal_scale=*/0, out_type, options));
  }

  template <typename Type1, typename Type2>
  enable_if_t<is_decimal_type<Type1>::value && is_accepted_numeric_type<Type2>::value>
  Visit(const Type1& type1, const Type2&) {
    state.reset(new CovarianceImpl<Type1, Type2>(/*decimal_scale=*/type1.scale(),
                                                 out_type, options));
  }

  template <typename Type1, typename Type2>
  enable_if_t<is_accepted_numeric_type<Type1>::value && is_decimal_type<Type2>::value>
  Visit(const Type1&, const Type2& type2) {
    state.reset(new CovarianceImpl<Type1, Type2>(type2.scale(), out_type, options));
  }

  template <typename Type1, typename Type2>
  enable_if_t<is_decimal_type<Type1>::value && is_decimal_type<Type2>::value> Visit(
      const Type1& type1, const Type2& type2) {
    state.reset(new CovarianceImpl<Type1, Type2>(type1.scale() + type2.scale(), out_type,
                                                 options));
  }

#define OUTER_ACTION(TYPE_CLASS1)                                                     \
  case TYPE_CLASS1##Type::type_id: {                                                  \
    const auto& concrete_in_type1 = checked_cast<const TYPE_CLASS1##Type&>(in_type1); \
    VisitType1(concrete_in_type1, in_type2);                                          \
    break;                                                                            \
  }

#define INNER_ACTION(TYPE_CLASS2)                                                     \
  case TYPE_CLASS2##Type::type_id: {                                                  \
    const auto& concrete_in_type2 = checked_cast<const TYPE_CLASS2##Type&>(in_type2); \
    Visit(concrete_in_type1, concrete_in_type2);                                      \
    break;                                                                            \
  }

  template <typename ConcreteType1>
  void VisitType1(const ConcreteType1& concrete_in_type1, const DataType& in_type2) {
    switch (in_type2.id()) {
      ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(INNER_ACTION);
      default:
        break;
    }
  }

  Result<std::unique_ptr<KernelState>> Create() {
    switch (in_type1.id()) {
      ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(OUTER_ACTION);
      default:
        break;
    }

    if (this->state) {
      return std::move(this->state);
    }
    return Status::NotImplemented("Types not implemented");
  }

#undef INNER_ACTION
#undef OUTER_ACTION
};

Result<std::unique_ptr<KernelState>> CovarianceInit(KernelContext* ctx,
                                                    const KernelInitArgs& args) {
  CovarianceInitState visitor(ctx, *args.inputs[0].type, *args.inputs[1].type,
                              args.kernel->signature->out_type().type(),
                              static_cast<const VarianceOptions&>(*args.options));
  return visitor.Create();
}

void AddCovarianceAggKernels(KernelInit init,
                             const std::vector<std::shared_ptr<DataType>>& types,
                             ScalarAggregateFunction* func) {
  for (const auto& type1 : types) {
    for (const auto& type2 : types) {
      auto sig = KernelSignature::Make({InputType(type1->id()), InputType(type2->id())},
                                       float64());
      AddAggKernel(std::move(sig), init, func);
    }
  }
}

const FunctionDoc covariance_doc{
    "Calculate the covariance between two numeric arrays",
    ("The number of degrees of freedom can be controlled using VarianceOptions.\n"
     "By default (`ddof` = 0), the population covariance is calculated.\n"
     "Nulls are ignored.  If there are not enough non-null value pairs in the input\n"
     "to satisfy `ddof`, null is returned."),
    {"array1", "array2"},
    "VarianceOptions"};

std::shared_ptr<ScalarAggregateFunction> AddCovarianceAggKernels() {
  static auto default_covar_options = VarianceOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>(
      "covariance", Arity::Binary(), covariance_doc, &default_covar_options);
  AddCovarianceAggKernels(CovarianceInit, NumericTypes(), func.get());
  AddCovarianceAggKernels(CovarianceInit, {decimal128(1, 1), decimal256(1, 1)},
                          func.get());
  return func;
}

};  // namespace

void RegisterScalarAggregateCovariance(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(internal::AddCovarianceAggKernels()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
