#include "arrow/array/builder_base.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/result.h"
#include "arrow/util/hashing.h"
#include "arrow/visitor.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

class Intersector : public TypeVisitor {
 public:
  Intersector(ExecContext* ctx, const Array& arr1, const Array& arr2, Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        arr1_(arr1),
        arr2_(arr2),
        physical_type_(GetPhysicalType(arr1.type())),
        output_(output) {}

  Status Run() { return physical_type_->Accept(this); }

// TODO(wayd): we should just use VISIT_SORTABLE_PHYSICAL_TYPES
// but BooleanType returned const qualifiers that were
// getting discarded when constructing the buffer for the output
#define VISIT_INTERSECTABLE_PHYSICAL_TYPES(VISIT) \
  VISIT(Int8Type)                            \
  VISIT(Int16Type)                           \
  VISIT(Int32Type)                           \
  VISIT(Int64Type)                           \
  VISIT(UInt8Type)                           \
  VISIT(UInt16Type)                          \
  VISIT(UInt32Type)                          \
  VISIT(UInt64Type)                          \
  VISIT(FloatType)                           \
  VISIT(DoubleType)                          \
  VISIT(BinaryType)                          \
  VISIT(LargeBinaryType)                     \
  VISIT(FixedSizeBinaryType)                 \
  VISIT(Decimal128Type)                      \
  VISIT(Decimal256Type)

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return IntersectInternal<TYPE>(); }

  VISIT_INTERSECTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

  template <typename InType>
  Status IntersectInternal() {
    using ArrayType = typename TypeTraits<InType>::ArrayType;
    using PhysicalType = typename GetViewType<InType>::PhysicalType;

    ARROW_ASSIGN_OR_RAISE(auto uniqs1, CallFunction("unique", {arr1_}));
    ARROW_ASSIGN_OR_RAISE(auto uniqs2, CallFunction("unique", {arr2_}));

    const ArrayVector arrays{uniqs1.make_array(), uniqs2.make_array()};
    const ChunkedArray chunked_arr(arrays);
    ARROW_ASSIGN_OR_RAISE(
        auto sort_indices,
        ::arrow::compute::SortIndices(chunked_arr, SortOrder::Ascending, ctx_));
    ARROW_ASSIGN_OR_RAISE(auto sorted,
                          ::arrow::compute::Take(Datum(chunked_arr), sort_indices,
                                                 TakeOptions::Defaults(), ctx_));

    std::vector<PhysicalType> keep{};
    const stl::ChunkedArrayIterator<ArrayType> iterator(*sorted.chunked_array());
    for (auto i = 0; i < chunked_arr.length() - 1; i++) {
      if (iterator[i] == iterator[i + 1]) {
        keep.push_back(*iterator[i]);
      }
    }

    auto out_type = arr1_.type();
    auto length = keep.size();
    std::vector<std::shared_ptr<Buffer>> buffers(2);

    buffers[1] = arrow::Buffer::Copy(arrow::Buffer::Wrap(keep), arrow::default_cpu_memory_manager()).ValueOrDie();
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    *output_ = Datum(out);

    return arrow::Status::OK();
  }

  ExecContext* ctx_;
  const Array& arr1_;
  const Array& arr2_;
  const std::shared_ptr<DataType> physical_type_;
  Datum* output_;
};

const FunctionDoc intersect_doc("Find the intersection of two arrays",
                                "This function computes the intersection of two arrays.",
                                {"array1", "array2"});

class IntersectMetaFunction : public MetaFunction {
 public:
  IntersectMetaFunction() : MetaFunction("intersect", Arity::Binary(), intersect_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const {
    // FunctionOptions are currently discarded
    // TODO(wayd): ensure args[0] and args[1] are the same
    switch (args[0].kind()) {
      case Datum::ARRAY: {
        return Intersect(*args[0].make_array(), *args[1].make_array(), ctx);
      } break;
      default:
        break;
    }

    return Status::NotImplemented(
        "Unsupported types for intersect operation: "
        "values=",
        args[0].ToString(), args[1].ToString());
  }

 private:
  Result<Datum> Intersect(const Array& arr1, const Array& arr2, ExecContext* ctx) const {
    Datum output;
    Intersector intersector(ctx, arr1, arr2, &output);
    ARROW_RETURN_NOT_OK(intersector.Run());
    return output;
  }
};
}  // namespace

void RegisterVectorIntersect(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<IntersectMetaFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
