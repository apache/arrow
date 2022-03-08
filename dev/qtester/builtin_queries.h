#pragma once

#include <functional>
#include <memory>
#include <unordered_map>

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>

namespace arrow::qtest {

using QueryPlanFactory = std::function<Result<std::shared_ptr<compute::ExecPlan>>(
    std::shared_ptr<compute::SinkNodeConsumer>)>;

const std::unordered_map<std::string, QueryPlanFactory>& GetBuiltinQueries();

}  // namespace arrow::qtest