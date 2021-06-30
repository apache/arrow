// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gandiva/lru_cache.h"
#include "gandiva/base_cache_key.h"

#include <map>
#include <string>
#include <typeinfo>

#include <gtest/gtest.h>

namespace gandiva {

/*class TestCacheKey {
 public:
  explicit TestCacheKey(int tmp) : tmp_(tmp) {}
  std::size_t Hash() const { return tmp_; }
  bool operator==(const TestCacheKey& other) const { return tmp_ == other.tmp_; }

 private:
  int tmp_;
}*/
;

class TestLruCache : public ::testing::Test {
 public:
  TestLruCache() : cache_(2, 256, 256){};

 protected:
  LruCache<BaseCacheKey, std::shared_ptr<llvm::MemoryBuffer>> cache_;
};

TEST_F(TestLruCache, TestEvict) {
  llvm::StringRef hello_ref("hello");
  std::unique_ptr<llvm::MemoryBuffer> hello_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(hello_ref);
  std::shared_ptr<llvm::MemoryBuffer> hello_buffer_sptr = std::move(hello_buffer_uptr);
  BaseCacheKey hello_key("test", "1");

  llvm::StringRef bye_ref("bye");
  std::unique_ptr<llvm::MemoryBuffer> bye_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(bye_ref);
  std::shared_ptr<llvm::MemoryBuffer> bye_buffer_sptr = std::move(bye_buffer_uptr);
  BaseCacheKey bye_key("test", "2");

  llvm::StringRef hey_ref("hey");
  std::unique_ptr<llvm::MemoryBuffer> hey_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(hey_ref);
  std::shared_ptr<llvm::MemoryBuffer> hey_buffer_sptr = std::move(hey_buffer_uptr);
  BaseCacheKey hey_key("test", "3");

  cache_.insert(hello_key, hello_buffer_sptr);
  cache_.insert(bye_key, bye_buffer_sptr);
  cache_.insert(hello_key, hello_buffer_sptr);
  cache_.insert(hey_key, hey_buffer_sptr);

  // should have evicted key 1
  ASSERT_EQ(2, cache_.size());
  ASSERT_EQ(cache_.get(hello_key), arrow::util::nullopt);
}

TEST_F(TestLruCache, TestLruBehavior) {
  llvm::StringRef hello_ref("hello");
  std::unique_ptr<llvm::MemoryBuffer> hello_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(hello_ref);
  std::shared_ptr<llvm::MemoryBuffer> hello_buffer_sptr = std::move(hello_buffer_uptr);
  BaseCacheKey hello_key("test", "1");

  llvm::StringRef bye_ref("bye");
  std::unique_ptr<llvm::MemoryBuffer> bye_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(bye_ref);
  std::shared_ptr<llvm::MemoryBuffer> bye_buffer_sptr = std::move(bye_buffer_uptr);
  BaseCacheKey bye_key("test", "2");

  llvm::StringRef hey_ref("hey");
  std::unique_ptr<llvm::MemoryBuffer> hey_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(hey_ref);
  std::shared_ptr<llvm::MemoryBuffer> hey_buffer_sptr = std::move(hey_buffer_uptr);
  BaseCacheKey hey_key("test", "3");

  cache_.insert(hello_key, hello_buffer_sptr);
  cache_.insert(bye_key, bye_buffer_sptr);
  cache_.get(hello_key);
  cache_.insert(hey_key, hey_buffer_sptr);

  // should have evicted key 2.
  ASSERT_EQ(cache_.get(hello_key), hello_buffer_sptr);
}

TEST_F(TestLruCache, TestEvictObject) {
  llvm::StringRef a_ref("a");
  std::unique_ptr<llvm::MemoryBuffer> a_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(a_ref);
  std::shared_ptr<llvm::MemoryBuffer> a_buffer_sptr = std::move(a_buffer_uptr);
  BaseCacheKey a_key("test", "1");

  llvm::StringRef b_ref("b");
  std::unique_ptr<llvm::MemoryBuffer> b_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(b_ref);
  std::shared_ptr<llvm::MemoryBuffer> b_buffer_sptr = std::move(b_buffer_uptr);
  BaseCacheKey b_key("test", "2");

  llvm::StringRef c_ref("c");
  std::unique_ptr<llvm::MemoryBuffer> c_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(c_ref);
  std::shared_ptr<llvm::MemoryBuffer> c_buffer_sptr = std::move(c_buffer_uptr);
  BaseCacheKey c_key("test", "3");

  cache_.insertObject(a_key, a_buffer_sptr, a_buffer_sptr->getBufferSize());
  cache_.insertObject(b_key, b_buffer_sptr, b_buffer_sptr->getBufferSize());
  cache_.insertObject(c_key, c_buffer_sptr, c_buffer_sptr->getBufferSize());

  // should have evicted key a and b
  ASSERT_EQ(1, cache_.size());
  ASSERT_EQ(cache_.get(a_key), arrow::util::nullopt);
  ASSERT_EQ(cache_.get(b_key), arrow::util::nullopt);

  // check for evicted objects that are now files
  bool a_file_is_cached = false;
  std::string a_file_name = std::to_string(a_key.Hash()) + ".cache";
  llvm::SmallString<128> a_cache_file = cache_.getCacheDir();
  llvm::sys::path::append(a_cache_file, a_file_name);

  std::ifstream a_cached_file(a_cache_file.c_str(), std::ios::binary);
  if (a_cached_file) {
    a_file_is_cached = true;
  }
  a_cached_file.close();
  ASSERT_TRUE(a_file_is_cached);

  bool b_file_is_cached = false;
  std::string b_file_name = std::to_string(b_key.Hash()) + ".cache";
  llvm::SmallString<128> b_cache_file = cache_.getCacheDir();
  llvm::sys::path::append(b_cache_file, a_file_name);

  std::ifstream b_cached_file(b_cache_file.c_str(), std::ios::binary);
  if (b_cached_file) {
    b_file_is_cached = true;
  }
  b_cached_file.close();
  ASSERT_TRUE(b_file_is_cached);
}

TEST_F(TestLruCache, TestReinsertObject) {
  llvm::StringRef d_ref("d");
  std::unique_ptr<llvm::MemoryBuffer> d_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(d_ref);
  std::shared_ptr<llvm::MemoryBuffer> d_buffer_sptr = std::move(d_buffer_uptr);
  BaseCacheKey d_key("test", "4");

  llvm::StringRef e_ref("e");
  std::unique_ptr<llvm::MemoryBuffer> e_buffer_uptr =
      llvm::MemoryBuffer::getMemBuffer(e_ref);
  std::shared_ptr<llvm::MemoryBuffer> e_buffer_sptr = std::move(e_buffer_uptr);
  BaseCacheKey e_key("test", "5");

  cache_.insertObject(d_key, d_buffer_sptr, d_buffer_sptr->getBufferSize());
  cache_.insertObject(e_key, e_buffer_sptr, e_buffer_sptr->getBufferSize());

  // should have evicted key d
  ASSERT_EQ(1, cache_.size());
  ASSERT_EQ(cache_.get(d_key), arrow::util::nullopt);

  // check for evicted d object that are now a file
  bool d_file_is_cached = false;
  std::string d_file_name = std::to_string(d_key.Hash()) + ".cache";
  llvm::SmallString<128> d_cache_file = cache_.getCacheDir();
  llvm::sys::path::append(d_cache_file, d_file_name);

  std::ifstream d_cached_file(d_cache_file.c_str(), std::ios::binary);
  if (d_cached_file) {
    d_file_is_cached = true;
  }
  d_cached_file.close();
  ASSERT_TRUE(d_file_is_cached);

  // reinsert d_object to cache
  cache_.reinsertObject(d_key, d_buffer_sptr, d_buffer_sptr->getBufferSize());
  ASSERT_EQ(cache_.get(d_key), d_buffer_sptr);
  ASSERT_EQ(cache_.get(e_key), arrow::util::nullopt);
}
}  // namespace gandiva
