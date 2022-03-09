#include <argparse/argparse.hpp>

#include <filesystem>
#include <optional>

#include "test_runner.h"

template <typename T>
arrow::util::optional<T> ToArrow(std::optional<T> std_opt) {
  if (std_opt) {
    return *std_opt;
  }
  return arrow::util::nullopt;
}

int main(int argc, char* argv[]) {
  argparse::ArgumentParser program("query_tester");

  program.add_argument("query").required().help("name of the query to run");
  program.add_argument("--num-iterations").default_value(1).scan<'i', int>();
  program.add_argument("--cpu-threads")
      .help("size to use for the CPU thread pool, default controlled by Arrow")
      .scan<'i', int>();
  program.add_argument("--io-threads")
      .help("size to use for the I/O thread pool, default controlled by Arrow")
      .scan<'i', int>();
  program.add_argument("--validate")
      .help("if set the program will validate the query results")
      .default_value(false)
      .implicit_value(true);

  try {
    program.parse_args(argc, argv);
  } catch (const std::runtime_error& err) {
    std::cerr << err.what() << std::endl;
    std::cerr << program;
    return 1;
  }

  arrow::qtest::QueryTestOptions options;
  options.query_name = program.get<std::string>("query");
  options.cpu_threads = ToArrow(program.present<int>("--cpu-threads"));
  options.io_threads = ToArrow(program.present<int>("--io-threads"));
  options.validate = program.get<bool>("--validate");
  options.num_iterations = program.get<int>("--num-iterations");
  options.executable_path = std::filesystem::absolute(argv[0]);

  arrow::Result<arrow::qtest::QueryTestResult> result =
      arrow::qtest::RunQueryTest(options);
  if (!result.ok()) {
    std::cout << "Error encountered running test: " << result.status() << std::endl;
    return 1;
  }

  arrow::Status report_status = arrow::qtest::ReportResult(*result);
  if (!report_status.ok()) {
    std::cout << "Error encountered reporting status: " << result.status() << std::endl;
    return 1;
  }

  return 0;
}