#ifndef ARROW_UTIL_MEMORY_H
#define ARROW_UTIL_MEMORY_H

namespace arrow {

// A helper function for doing memcpy with multiple threads. This is required
// to saturate the memory bandwidth of modern cpus.
void parallel_memcopy(
    uint8_t* dst, const uint8_t* src, uint64_t nbytes, uint64_t block_size, uint64_t num_threads) {
  std::vector<std::thread> threadpool(num_threads);
  uint64_t src_address = reinterpret_cast<uint64_t>(src);
  uint64_t left_address = (src_address + block_size - 1) & ~(block_size - 1);
  uint64_t right_address = (src_address + nbytes) & ~(block_size - 1);
  uint64_t num_blocks = (right_address - left_address) / block_size;
  // Update right address
  right_address = right_address - (num_blocks % num_threads) * block_size;
  // Now we divide these blocks between available threads. The remainder is
  // handled on the main thread.

  uint64_t chunk_size = (right_address - left_address) / num_threads;
  uint64_t prefix = left_address - src_address;
  uint64_t suffix = src_address + nbytes - right_address;
  // Now the data layout is | prefix | k * num_threads * block_size | suffix |.
  // We have chunk_size = k * block_size, therefore the data layout is
  // | prefix | num_threads * chunk_size | suffix |.
  // Each thread gets a "chunk" of k blocks.

  // Start all threads first and handle leftovers while threads run.
  for (uint64_t i = 0; i < num_threads; i++) {
    threadpool[i] = std::thread(memcpy, dst + prefix + i * chunk_size,
        reinterpret_cast<uint8_t*>(left_address) + i * chunk_size, chunk_size);
  }

  memcpy(dst, src, prefix);
  memcpy(dst + prefix + num_threads * chunk_size,
      reinterpret_cast<uint8_t*>(right_address), suffix);

  for (auto& t : threadpool) {
    if (t.joinable()) { t.join(); }
  }
}

}

#endif  // ARROW_UTIL_MEMORY_H
