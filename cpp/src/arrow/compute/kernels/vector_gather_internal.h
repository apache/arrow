#pragma once

#include "arrow/compute/function.h"

namespace arrow::compute::internal {

// class GatherMetaFunction : public MetaFunction {
//  public:
//   GatherMetaFunction() : MetaFunction("gather", Arity::VarArgs(3), gather_doc) {}

//   Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
//                             const FunctionOptions* options,
//                             ExecContext* ctx) const override {
//     for (const auto& arg : args) {
//       if (arg.kind() != Datum::ARRAY && arg.kind() != Datum::CHUNKED_ARRAY) {
//         return Status::TypeError("Gather arguments should be array-like");
//       }
//     }

//     if (!is_integer(*args[0].type())) {
//       return Status::NotImplemented("Indices to gather must be integer type");
//     }

//     if (args[0].kind() == Datum::RECORD_BATCH) {
//       ARROW_ASSIGN_OR_RAISE(
//           std::shared_ptr<RecordBatch> out_batch,
//           FilterRecordBatch(*args[0].record_batch(), args[1], options, ctx));
//       return Datum(out_batch);
//     } else if (args[0].kind() == Datum::TABLE) {
//       ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> out_table,
//                             FilterTable(*args[0].table(), args[1], options, ctx));
//       return Datum(out_table);
//     } else {
//       return CallFunction("array_filter", args, options, ctx);
//     }
//   }
// };

}  // namespace arrow::compute::internal
