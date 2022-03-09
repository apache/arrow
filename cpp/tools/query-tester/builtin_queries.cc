#include "builtin_queries.h"

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/tpch_node.h>

namespace cp = arrow::compute;

namespace arrow {
namespace qtest {

namespace {

Result<std::shared_ptr<cp::ExecPlan>> Tpch1(
    std::shared_ptr<cp::SinkNodeConsumer> consumer, cp::ExecContext* exec_context) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan,
                        cp::ExecPlan::Make(exec_context));
  ARROW_ASSIGN_OR_RAISE(cp::TpchGen gen, cp::TpchGen::Make(plan.get(), 1));

  ARROW_ASSIGN_OR_RAISE(
      cp::ExecNode * lineitem,
      gen.Lineitem({"L_QUANTITY", "L_EXTENDEDPRICE", "L_TAX", "L_DISCOUNT", "L_SHIPDATE",
                    "L_RETURNFLAG", "L_LINESTATUS"}));

  std::shared_ptr<Date32Scalar> sept_2_1998 = std::make_shared<Date32Scalar>(
      10471);  // September 2, 1998 is 10471 days after January 1, 1970
  cp::Expression filter =
      cp::less_equal(cp::field_ref("L_SHIPDATE"), cp::literal(std::move(sept_2_1998)));
  cp::FilterNodeOptions filter_opts(filter);

  cp::Expression l_returnflag = cp::field_ref("L_RETURNFLAG");
  cp::Expression l_linestatus = cp::field_ref("L_LINESTATUS");
  cp::Expression quantity = cp::field_ref("L_QUANTITY");
  cp::Expression base_price = cp::field_ref("L_EXTENDEDPRICE");

  std::shared_ptr<Decimal128Scalar> decimal_1 =
      std::make_shared<Decimal128Scalar>(Decimal128{0, 100}, decimal(12, 2));
  cp::Expression discount_multiplier =
      cp::call("subtract", {cp::literal(decimal_1), cp::field_ref("L_DISCOUNT")});
  cp::Expression tax_multiplier =
      cp::call("add", {cp::literal(decimal_1), cp::field_ref("L_TAX")});
  cp::Expression disc_price =
      cp::call("multiply", {cp::field_ref("L_EXTENDEDPRICE"), discount_multiplier});
  cp::Expression charge = cp::call(
      "multiply", {cp::call("cast",
                            {cp::call("multiply", {cp::field_ref("L_EXTENDEDPRICE"),
                                                   discount_multiplier})},
                            cp::CastOptions::Unsafe(decimal(12, 2))),
                   tax_multiplier});
  cp::Expression discount = cp::field_ref("L_DISCOUNT");

  std::vector<cp::Expression> projection_list = {l_returnflag, l_linestatus, quantity,
                                                 base_price,   disc_price,   charge,
                                                 quantity,     base_price,   discount};
  std::vector<std::string> project_names = {
      "l_returnflag", "l_linestatus", "sum_qty",   "sum_base_price", "sum_disc_price",
      "sum_charge",   "avg_qty",      "avg_price", "avg_disc"};
  cp::ProjectNodeOptions project_opts(std::move(projection_list));

  cp::ScalarAggregateOptions sum_opts = cp::ScalarAggregateOptions::Defaults();
  cp::CountOptions count_opts(cp::CountOptions::CountMode::ALL);
  std::vector<arrow::compute::internal::Aggregate> aggs = {
      {"hash_sum", &sum_opts},  {"hash_sum", &sum_opts},    {"hash_sum", &sum_opts},
      {"hash_sum", &sum_opts},  {"hash_mean", &sum_opts},   {"hash_mean", &sum_opts},
      {"hash_mean", &sum_opts}, {"hash_count", &count_opts}};

  std::vector<FieldRef> cols = {2, 3, 4, 5, 6, 7, 8, 2};

  std::vector<std::string> names = {"sum_qty",    "sum_base_price", "sum_disc_price",
                                    "sum_charge", "avg_qty",        "avg_price",
                                    "avg_disc",   "count_order"};

  std::vector<FieldRef> keys = {"L_RETURNFLAG", "L_LINESTATUS"};
  cp::AggregateNodeOptions agg_opts(aggs, cols, names, keys);

  cp::ConsumingSinkNodeOptions sink_opts(std::move(consumer));

  cp::Declaration filter_decl("filter", {cp::Declaration::Input(lineitem)}, filter_opts);
  cp::Declaration project_decl("project", project_opts);
  cp::Declaration aggregate_decl("aggregate", agg_opts);
  cp::Declaration sink_decl("consuming_sink", sink_opts);

  cp::Declaration q1 =
      cp::Declaration::Sequence({filter_decl, project_decl, aggregate_decl, sink_decl});
  std::ignore = *q1.AddToPlan(plan.get());
  return plan;
}

std::unordered_map<std::string, QueryPlanFactory> CreateBuiltinQueriesMap() {
  std::unordered_map<std::string, QueryPlanFactory> builtin_queries_map;
  builtin_queries_map.insert({"tpch-1", Tpch1});
  return builtin_queries_map;
}

}  // namespace

const std::unordered_map<std::string, QueryPlanFactory>& GetBuiltinQueries() {
  static std::unordered_map<std::string, QueryPlanFactory> builtin_queries_map =
      CreateBuiltinQueriesMap();
  return builtin_queries_map;
}

}  // namespace qtest
}  // namespace arrow
