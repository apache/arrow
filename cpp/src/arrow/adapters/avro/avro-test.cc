//
// Created by powerless on 8/30/17.
//

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#ifndef _MSC_VER
#include <fcntl.h>
#endif
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"

#include "arrow/io/file.h"
#include "arrow/io/test-common.h"
#include "arrow/memory_pool.h"
#include "adapter.h"

namespace arrow {
namespace adapters {
namespace avro {

TEST(TestAvroReader, SIMPLE) {
  auto pool = default_memory_pool();
  auto reader = AvroArrowReader(pool);
  ASSERT_TRUE(true);
}

}
}
}
