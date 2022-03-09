#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/engine/api.h>

#include <chrono>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

namespace arrow {
namespace qtest {

struct QueryTestOptions {
  /// Name of the query to run, will look for a query input file in the queries folder
  std::string query_name;
  /// Number of CPU threads to initialize Arrow with.  By default Arrow will base this
  /// on std::thread::hardware_concurrency
  util::optional<int> cpu_threads;
  /// Number of I/O threads to initialize Arrow with.  By default Arrow will use 8
  util::optional<int> io_threads;
  /// Number of iterations of the query to run, defaults to a single run
  int num_iterations = 1;
  /// If true, validate the query results, if possible
  bool validate = false;
  /// Path to the query_tester executable, used to locate queries & datasets
  std::string executable_path;
};

struct QueryIterationResult {
  uint64_t num_rows_processed = 0;
  uint64_t num_bytes_processed = 0;
  std::chrono::high_resolution_clock::time_point start_time;
  std::chrono::high_resolution_clock::time_point end_time;

  double duration_seconds() const {
    return std::chrono::duration<double>(end_time - start_time).count();
  }
};

struct QueryTestResult {
  std::vector<QueryIterationResult> iterations;

  inline uint64_t total_bytes_processed() const {
    uint64_t sum = 0;
    for (const auto& iteration : iterations) {
      sum += iteration.num_bytes_processed;
    }
    return sum;
  }

  inline uint64_t total_rows_processed() const {
    uint64_t sum = 0;
    for (const auto& iteration : iterations) {
      sum += iteration.num_rows_processed;
    }
    return sum;
  }

  inline double total_duration_seconds() const {
    double sum = 0;
    for (const auto& iteration : iterations) {
      sum += iteration.duration_seconds();
    }
    return sum;
  }

  inline double average_duration_seconds() const {
    return total_duration_seconds() / iterations.size();
  }

  inline double stderr_duration_seconds() const {
    double avg = average_duration_seconds();
    double err_sum = 0;
    for (const auto& iteration : iterations) {
      err_sum += std::abs(iteration.duration_seconds() - avg);
    }
    return err_sum / iterations.size();
  }

  inline double average_bps() const {
    return total_bytes_processed() / total_duration_seconds();
  }

  inline double average_rps() const {
    return total_rows_processed() / total_duration_seconds();
  }
};

/// Load a query and return the execution plan
///
/// The folder ${CWD}/queries will be searched for a file whose basename (everything
/// before the first '.' matches query_name).  The extension will be used to figure
/// out how to convert the file to an execution plan.  Supported extensions are:
///
/// .substrait.pb.json - Loads a Substrait plan using the JSON protobuf format
/// .substrait.pb - Loads a Substrait plan using the binary protobuf format
Result<std::shared_ptr<compute::ExecPlan>> LoadQuery(
    const std::string& root_path, const std::string& query_name,
    const engine::ConsumerFactory& consumer_factory, compute::ExecContext* exec_context);
/// Validate the options (will be run automatically by RunQueryTest)
Status ValidateOptions(const QueryTestOptions& options);
/// Run a query test.
///
/// This will load the query, download and prepare any neccesary data,
/// run the query the specified number of times, and then generate a report
Result<QueryTestResult> RunQueryTest(const QueryTestOptions& options);

/// Print a query test result
Status ReportResult(const QueryTestResult& result);

}  // namespace qtest
}  // namespace arrow
