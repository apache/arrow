#pragma once

#include <functional>
#include <memory>
#include <unordered_map>

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>

namespace arrow {
namespace qtest {

using QueryPlanFactory = std::function<Result<std::shared_ptr<compute::ExecPlan>>(
    std::shared_ptr<compute::SinkNodeConsumer>, compute::ExecContext*)>;

const std::unordered_map<std::string, QueryPlanFactory>& GetBuiltinQueries();

}  // namespace qtest
}  // namespace arrow
