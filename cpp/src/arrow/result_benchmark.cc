#include <benchmark/benchmark.h>

#include <thread>

#include "result.h"
#include "status.h"

namespace arrow {

struct Pod {
  int x;
  int y;
};
constexpr Pod kPod{1, 2};

struct NonPod {
  int x;
  int y;
  NonPod() {
    x = 1;
    y = 1;
  }
};
const NonPod kNonPod;

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
BENCHMARK(BM_DefaultStatus);

template <typename T>
void BM_DefaultResult(benchmark::State& state) {
  for (auto _ : state) {
    Result<T> result;
    OperateOn(result);
  }
}
BENCHMARK_TEMPLATE(BM_DefaultResult, int);
BENCHMARK_TEMPLATE(BM_DefaultResult, Pod);
BENCHMARK_TEMPLATE(BM_DefaultResult, NonPod);

void BM_ValuedResultInt(benchmark::State& state) {
  for (auto _ : state) {
    Result<int> result(42);
    OperateOn(result);
  }
}
BENCHMARK(BM_ValuedResultInt);

void BM_ValuedResultPod(benchmark::State& state) {
  for (auto _ : state) {
    Result<Pod> result(kPod);
    OperateOn(result);
  }
}
BENCHMARK(BM_ValuedResultPod);

void BM_ValuedResultNonPod(benchmark::State& state) {
  for (auto _ : state) {
    Result<NonPod> result(kNonPod);
    OperateOn(result);
  }
}
BENCHMARK(BM_ValuedResultNonPod);

static void BM_InvalidStatus(benchmark::State& state) {
  for (auto _ : state) {
    Status st = Status::Invalid("XYZ");
    OperateOn(st);
  }
}
BENCHMARK(BM_InvalidStatus);

static void BM_InvalidResult(benchmark::State& state) {
  for (auto _ : state) {
    Result<int> result{Status::Invalid("XYZ")};
    OperateOn(result);
  }
}
BENCHMARK(BM_InvalidResult);

}  // namespace arrow

BENCHMARK_MAIN();
