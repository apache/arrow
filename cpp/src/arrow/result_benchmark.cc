#include <benchmark/benchmark.h>

#include <thread>

#include "result.h"
#include "status.h"

namespace arrow {

void OperateOn(const Status& status) {
  benchmark::DoNotOptimize(status.message());
  benchmark::ClobberMemory();
}

template <typename T>
void OperateOn(const Result<T>& result) {
  OperateOn(result.status());
}

static void BM_DefaultStatus(benchmark::State& state) {
  for (auto _ : state) {
    Status status;
    OperateOn(status);
  }
}
BENCHMARK(BM_DefaultStatus)->Threads(16);

static void BM_DefaultResult(benchmark::State& state) {
  for (auto _ : state) {
    Result<int> result;
    OperateOn(result);
  }
}
BENCHMARK(BM_DefaultResult)->Threads(16);

static void BM_ValuedResult(benchmark::State& state) {
  for (auto _ : state) {
    Result<int> result{42};
    OperateOn(result);
  }
}
BENCHMARK(BM_ValuedResult)->Threads(16);

static void BM_InvalidStatus(benchmark::State& state) {
  for (auto _ : state) {
    Status st = Status::Invalid("XYZ");
    OperateOn(st);
  }
}
BENCHMARK(BM_InvalidStatus)->Threads(16);

static void BM_InvalidResult(benchmark::State& state) {
  for (auto _ : state) {
    Result<int> result{Status::Invalid("XYZ")};
    OperateOn(result);
  }
}
BENCHMARK(BM_InvalidResult)->Threads(16);

}  // namespace arrow

BENCHMARK_MAIN();
