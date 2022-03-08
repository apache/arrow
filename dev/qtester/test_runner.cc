#include "test_runner.h"
#include "builtin_queries.h"

#include <arrow/filesystem/api.h>

#include <filesystem>
#include <iostream>
#include <mutex>

namespace std_fs = std::filesystem;
namespace cp = arrow::compute;

namespace arrow::qtest {

Status ValidateOptions(const QueryTestOptions& options) {
  if (options.cpu_threads && *options.cpu_threads <= 0) {
    return Status::Invalid("cpu-threads must be > 0");
  }
  if (options.io_threads && *options.io_threads <= 0) {
    return Status::Invalid("io-threads must be > 0");
  }
  if (options.num_iterations <= 0) {
    return Status::Invalid("num-iterations must be > 0");
  }
  if (options.validate) {
    return Status::NotImplemented("validation has not yet been implemented");
  }
  return Status::OK();
}

namespace {
Result<std::string> DoGetRootDirectory(const std::string& executable_path) {
  std_fs::path path = std_fs::absolute(std_fs::path(executable_path));
  while (true) {
    if (std_fs::is_directory(path / "queries") &&
        std_fs::is_directory(path / "datasets")) {
      return path;
    }
    if (path.has_parent_path() && path != path.parent_path()) {
      path = path.parent_path();
    } else {
      return Status::Invalid(
          "Could not locate the root directory.  Did you perhaps move or copy the "
          "query_tester executable outside of the project directory?");
    }
  }
}

Result<std::string> GetRootDirectory(const std::string& executable) {
  static Result<std::string> cached_root_directory = DoGetRootDirectory(executable);
  return cached_root_directory;
}

Result<std::shared_ptr<Buffer>> PathToBuffer(const std_fs::path& path) {
  fs::LocalFileSystem local_fs;
  ARROW_ASSIGN_OR_RAISE(fs::FileInfo file_info, local_fs.GetFileInfo(path));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<io::InputStream> in_stream,
                        local_fs.OpenInputStream(path));
  return in_stream->Read(file_info.size());
}

Result<std::shared_ptr<compute::ExecPlan>> DeclsToPlan(
    const std::vector<cp::Declaration>& decls) {
  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make());
  for (const auto& decl : decls) {
    ARROW_RETURN_NOT_OK(decl.AddToPlan(plan.get()));
  }
  return plan;
}

Result<std::shared_ptr<compute::ExecPlan>> LoadQueryFromSubstraitJson(
    const std_fs::path& path, const engine::ConsumerFactory& consumer_factory) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> json_bytes, PathToBuffer(path));
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> plan_bytes,
      engine::internal::SubstraitFromJSON("Plan", json_bytes->ToString()));
  ARROW_ASSIGN_OR_RAISE(std::vector<cp::Declaration> decls,
                        engine::DeserializePlan(*plan_bytes, consumer_factory));
  return DeclsToPlan(decls);
}

Result<std::shared_ptr<compute::ExecPlan>> LoadQueryFromSubstraitBinary(
    const std_fs::path& path, const engine::ConsumerFactory& consumer_factory) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> plan_bytes, PathToBuffer(path));
  ARROW_ASSIGN_OR_RAISE(std::vector<cp::Declaration> decls,
                        engine::DeserializePlan(*plan_bytes, consumer_factory));
  return DeclsToPlan(decls);
}

Result<std::shared_ptr<compute::ExecPlan>> LoadQueryFromPath(
    const std_fs::path& path, const std::string& extension,
    const engine::ConsumerFactory& consumer_factory) {
  if (extension == "substrait.pb.json") {
    return LoadQueryFromSubstraitJson(path, consumer_factory);
  }
  if (extension == "substrait.pb") {
    return LoadQueryFromSubstraitBinary(path, consumer_factory);
  }

  return Status::Invalid("No handler for query file format ", extension);
}

class QueryResultUpdatingConsumer : public cp::SinkNodeConsumer {
 public:
  explicit QueryResultUpdatingConsumer(QueryTestResult* result) : result_(result) {}

  arrow::Status Consume(cp::ExecBatch batch) override {
    std::lock_guard<std::mutex> lg(mutex_);
    result_->iterations[iteration_].num_rows_processed += batch.length;
    result_->iterations[iteration_].num_bytes_processed += batch.TotalBufferSize();
    return arrow::Status::OK();
  }

  arrow::Future<> Finish() override {
    result_->iterations[iteration_].end_time = std::chrono::high_resolution_clock::now();
    return arrow::Future<>::MakeFinished();
  }

  void Start(std::size_t iteration) {
    iteration_ = iteration;
    result_->iterations.emplace_back();
    result_->iterations[iteration_].start_time =
        std::chrono::high_resolution_clock::now();
  }

 private:
  QueryTestResult* result_;
  std::mutex mutex_;
  std::size_t iteration_ = 0;
};

Result<std::optional<std::shared_ptr<compute::ExecPlan>>> LoadQueryFromFiles(
    const std::string& root_path, const std::string& query_name,
    const engine::ConsumerFactory& consumer_factory) {
  for (const auto& entry :
       std_fs::directory_iterator(std_fs::path(root_path) / "queries")) {
    auto entry_path_str = entry.path().filename().string();
    auto first_dot_idx = entry_path_str.find('.');
    if (first_dot_idx != std::string::npos) {
      auto stem = entry_path_str.substr(0, first_dot_idx);
      if (stem == query_name) {
        auto extension = entry_path_str.substr(first_dot_idx + 1);
        return LoadQueryFromPath(entry.path(), extension, consumer_factory);
      }
    }
  }
  return std::nullopt;
}

Result<std::optional<std::shared_ptr<compute::ExecPlan>>> LoadQueryFromBuiltin(
    const std::string& query_name, const engine::ConsumerFactory& consumer_factory) {
  const auto& builtin_queries_map = GetBuiltinQueries();
  const auto& query = builtin_queries_map.find(query_name);
  if (query == builtin_queries_map.end()) {
    return std::nullopt;
  }
  std::shared_ptr<cp::SinkNodeConsumer> consumer = consumer_factory();
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<cp::ExecPlan> plan, query->second(consumer));
  return plan;
}

Status InitializeArrow(const QueryTestOptions& options) {
  if (options.cpu_threads) {
    ARROW_RETURN_NOT_OK(
        arrow::internal::GetCpuThreadPool()->SetCapacity(*options.cpu_threads));
  }
  if (options.io_threads) {
    ARROW_RETURN_NOT_OK(arrow::io::SetIOThreadPoolCapacity(*options.io_threads));
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<compute::ExecPlan>> LoadQuery(
    const std::string& root_path, const std::string& query_name,
    const engine::ConsumerFactory& consumer_factory) {
  ARROW_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<compute::ExecPlan>> maybe_query,
                        LoadQueryFromFiles(root_path, query_name, consumer_factory));
  if (maybe_query) {
    return *maybe_query;
  }

  ARROW_ASSIGN_OR_RAISE(maybe_query, LoadQueryFromBuiltin(query_name, consumer_factory));
  if (maybe_query) {
    return *maybe_query;
  }

  return Status::Invalid("Could not find any query file or builtin query named ",
                         query_name);
}

Result<QueryTestResult> RunQueryTest(const QueryTestOptions& options) {
  ARROW_ASSIGN_OR_RAISE(auto root_path, GetRootDirectory(options.executable_path));
  ARROW_RETURN_NOT_OK(ValidateOptions(options));
  ARROW_RETURN_NOT_OK(InitializeArrow(options));
  QueryTestResult result;
  auto consumer = std::make_shared<QueryResultUpdatingConsumer>(&result);
  auto consumer_factory = [consumer] { return consumer; };
  for (int i = 0; i < options.num_iterations; i++) {
    consumer->Start(i);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<compute::ExecPlan> plan,
                          LoadQuery(root_path, options.query_name, consumer_factory));
    ARROW_RETURN_NOT_OK(plan->StartProducing());
    ARROW_RETURN_NOT_OK(plan->finished().status());
  }

  return result;
}

Status ReportResult(const QueryTestResult& result) {
  std::cout << "Average       Duration: " << result.average_duration_seconds()
            << "s (+/- " << result.stderr_duration_seconds() << "s)" << std::endl;
  std::cout << "Average Output  Rows/S: " << result.average_rps() << "rps" << std::endl;
  std::cout << "Average Output Bytes/S: " << result.average_bps() << "bps" << std::endl;
  return Status::OK();
}

}  // namespace arrow::qtest