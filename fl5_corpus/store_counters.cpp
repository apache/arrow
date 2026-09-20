// store_counters -- hardware counters for the two bit-unpacking layouts at a
// working set that does not fit in cache.
//
// output_width_matrix's residency sweep shows the interleaved container losing to
// Parquet's continuous stream once output plus input passes L2, and shows a
// per-row store-ordering barrier recovering it. That is a claim about the store
// path, and this file is the counters behind it: written bytes that turn into
// cache line fills, store instructions retired, backend stalls.
//
// Three arms, identical bytes out:
//   seq_simd  Arrow's shipped vectorized unpacker over Parquet's continuous
//             LSB-first stream.
//   intlv     FastLanes interleaved container, rows written in source order.
//   intlv_b   The same kernel with an empty asm and a memory clobber ending each
//             row, which gives up the loads the rows share to keep each row's
//             128 bytes of output as one contiguous run.
//
// perf(1) is not needed and is not used: counting is per-process and user-space
// only, which is what perf_event_paranoid=2 permits. Events are raw ARMv8 PMU
// numbers and are read in small groups, because this core has six programmable
// counters and a group larger than that never schedules. Each group re-runs the
// arm; the kernels are deterministic, so that costs time and nothing else. An
// event this core does not implement reads zero and is printed as a dash rather
// than as a measurement.
//
// Build with build.sh (./build.sh store_counters).

#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <random>
#include <vector>

#include <arrow/util/bpacking_internal.h>

constexpr size_t kBlk = 1024;
constexpr unsigned kLanes = 32, kRows = 32, kT = 32;

// 8192 blocks: 32 MiB of int32 output, 20 MiB of packed input at twenty bits. Well
// past this core's 2 MiB L2 and past the far end of the residency sweep, so the
// arms sit where they diverge.
constexpr size_t kBlocks = 8192;
constexpr size_t kN = kBlocks * kBlk;
constexpr unsigned kW = 20;

static uint32_t Mask() { return (1u << kW) - 1u; }

static void PackInterleaved(const uint32_t* in, uint32_t* out) {
  std::memset(out, 0, kW * kLanes * sizeof(uint32_t));
  for (unsigned row = 0; row < kRows; ++row) {
    for (unsigned lane = 0; lane < kLanes; ++lane) {
      const uint64_t v = in[row * kLanes + lane] & Mask();
      const unsigned startBit = row * kW, word = startBit / kT, shift = startBit % kT;
      out[word * kLanes + lane] |= static_cast<uint32_t>(v << shift);
      const unsigned endWord = (startBit + kW - 1) / kT;
      if (endWord != word) out[endWord * kLanes + lane] |= static_cast<uint32_t>(v >> (kT - shift));
    }
  }
}

template <bool kBarrier>
static void UnpackInterleaved(const uint32_t* __restrict packed,
                              uint32_t* __restrict out) {
#pragma GCC unroll 32
  for (unsigned row = 0; row < kRows; ++row) {
    const unsigned startBit = row * kW, word = startBit / kT, shift = startBit % kT;
    const unsigned endWord = (startBit + kW - 1) / kT;
    if (word == endWord) {
      for (unsigned lane = 0; lane < kLanes; ++lane)
        out[row * kLanes + lane] = (packed[word * kLanes + lane] >> shift) & Mask();
    } else {
      const unsigned lowBits = kT - shift;
      for (unsigned lane = 0; lane < kLanes; ++lane) {
        const uint32_t lo = packed[word * kLanes + lane] >> shift;
        const uint32_t hi = packed[endWord * kLanes + lane] << lowBits;
        out[row * kLanes + lane] = (lo | hi) & Mask();
      }
    }
    if constexpr (kBarrier) asm volatile("" ::: "memory");
  }
}

template <bool kBarrier>
__attribute__((noinline)) static void IntlvBlocks(const uint32_t* pk, uint32_t* out) {
  for (size_t b = 0; b < kBlocks; ++b)
    UnpackInterleaved<kBarrier>(pk + b * kW * kLanes, out + b * kBlk);
}

static std::vector<uint8_t> PackContinuous(const std::vector<uint32_t>& v) {
  std::vector<uint8_t> bytes(kN * kW / 8 + 64, 0);
  uint8_t* out = bytes.data();
  uint64_t acc = 0;
  int bits = 0;
  for (size_t i = 0; i < kN; ++i) {
    acc |= static_cast<uint64_t>(v[i] & Mask()) << bits;
    bits += static_cast<int>(kW);
    while (bits >= 8) {
      *out++ = static_cast<uint8_t>(acc);
      acc >>= 8;
      bits -= 8;
    }
  }
  if (bits) *out = static_cast<uint8_t>(acc);
  return bytes;
}

__attribute__((noinline)) static void SeqBlocks(const uint8_t* src, size_t total,
                                                uint32_t* out) {
  arrow::internal::UnpackOptions o;
  o.batch_size = static_cast<int>(kBlk);
  o.bit_width = static_cast<int>(kW);
  const size_t stride = kW * kBlk / 8;
  for (size_t b = 0; b < kBlocks; ++b) {
    o.max_read_bytes = static_cast<int>(total - b * stride);
    arrow::internal::unpack<uint32_t>(src + b * stride, out + b * kBlk, o);
  }
}

// ------------------------------------------------------------------- counters

struct Event {
  const char* name;
  uint64_t config;
};

// Raw ARMv8 PMU event numbers. Anything this core leaves unimplemented reads zero.
static const Event kCycles = {"cycles", 0x11};
static const std::vector<std::vector<Event>> kPasses = {
    {{"insts", 0x08}, {"ld_spec", 0x70}, {"st_spec", 0x71}, {"stall_be", 0x24}},
    {{"l1d_wr", 0x41}, {"l1d_rfl_wr", 0x43}, {"l1d_rfl", 0x03}, {"l1d_acc", 0x04}},
    {{"l2d_wr", 0x51}, {"l2d_rfl_wr", 0x53}, {"l2d_rfl", 0x17}, {"bus_wr", 0x61}},
};

static long PerfOpen(uint64_t config, int group_fd) {
  perf_event_attr a{};
  a.size = sizeof(a);
  a.type = PERF_TYPE_RAW;
  a.config = config;
  a.disabled = group_fd == -1 ? 1 : 0;
  a.exclude_kernel = 1;  // perf_event_paranoid=2 permits user-space counting only
  a.exclude_hv = 1;
  a.inherit = 0;
  return syscall(__NR_perf_event_open, &a, 0, -1, group_fd, 0);
}

// Counts one pass of events around run(). Returns false if the group could not be
// opened at all, which is a permissions or kernel question, not a missing event.
static bool Count(const std::vector<Event>& events, const std::function<void()>& run,
                  std::vector<uint64_t>* out, uint64_t* cycles) {
  const int leader = static_cast<int>(PerfOpen(kCycles.config, -1));
  if (leader < 0) {
    perror("perf_event_open");
    return false;
  }
  std::vector<int> fds;
  for (const Event& e : events) {
    const int fd = static_cast<int>(PerfOpen(e.config, leader));
    fds.push_back(fd);  // -1 means this core does not implement the event
  }
  ioctl(leader, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
  ioctl(leader, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
  run();
  ioctl(leader, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
  auto readOne = [](int fd) -> uint64_t {
    uint64_t v = 0;
    if (fd < 0 || read(fd, &v, sizeof(v)) != static_cast<ssize_t>(sizeof(v))) return 0;
    return v;
  };
  *cycles = readOne(leader);
  out->clear();
  for (int fd : fds) {
    out->push_back(readOne(fd));
    if (fd >= 0) close(fd);
  }
  close(leader);
  return true;
}

struct ArmResult {
  const char* name;
  uint64_t cycles;
  std::vector<std::pair<const char*, uint64_t>> counts;
};

static ArmResult Measure(const char* name, const std::function<void()>& run) {
  ArmResult r{name, 0, {}};
  run();  // page the buffers in, settle the frequency
  for (const auto& pass : kPasses) {
    std::vector<uint64_t> vals;
    uint64_t cycles = 0;
    if (!Count(pass, run, &vals, &cycles)) return r;
    r.cycles = std::max(r.cycles, cycles);
    for (size_t i = 0; i < pass.size(); ++i) r.counts.emplace_back(pass[i].name, vals[i]);
  }
  return r;
}

int main() {
  std::vector<uint32_t> ref(kN);
  std::mt19937 rng(7 + kW);
  for (auto& v : ref) v = rng() & Mask();
  std::vector<uint32_t> pk(kBlocks * kW * kLanes + 64, 0);
  for (size_t b = 0; b < kBlocks; ++b)
    PackInterleaved(ref.data() + b * kBlk, pk.data() + b * kW * kLanes);
  const std::vector<uint8_t> seq = PackContinuous(ref);
  std::vector<uint32_t> out(kN + 64, 0);

  std::vector<std::pair<const char*, std::function<void()>>> arms = {
      {"seq_simd", [&] { SeqBlocks(seq.data(), seq.size(), out.data()); }},
      {"intlv", [&] { IntlvBlocks<false>(pk.data(), out.data()); }},
      {"intlv_b", [&] { IntlvBlocks<true>(pk.data(), out.data()); }},
  };
  for (const auto& arm : arms) {
    std::memset(out.data(), 0xCD, kN * sizeof(uint32_t));
    arm.second();
    if (!std::equal(ref.begin(), ref.end(), out.begin())) {
      fprintf(stderr, "MISMATCH in %s\n", arm.first);
      return 1;
    }
  }

  printf("store_counters -- %u-bit values into 32-bit output, %zu values, "
         "%zu MiB out, %zu MiB in\n",
         kW, kN, kN * sizeof(uint32_t) / (1024 * 1024), kN * kW / 8 / (1024 * 1024));
  printf("Per 1024 values. A dash is an event this core does not implement.\n\n");

  std::vector<ArmResult> res;
  for (const auto& arm : arms) res.push_back(Measure(arm.first, arm.second));
  if (res[0].counts.empty()) {
    fprintf(stderr, "no counters available\n");
    return 1;
  }

  printf("%-10s %10s", "", "cyc");
  for (const auto& c : res[0].counts) printf(" %11s", c.first);
  printf("\n");
  const double per = static_cast<double>(kN) / 1024.0;
  for (const ArmResult& r : res) {
    printf("%-10s %10.1f", r.name, r.cycles / per);
    for (const auto& c : r.counts) {
      if (c.second == 0) printf(" %11s", "-");
      else printf(" %11.1f", c.second / per);
    }
    printf("\n");
  }
  return 0;
}
