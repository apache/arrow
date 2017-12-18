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

#ifndef ARROW_ORC_CONVERTER_H
#define ARROW_ORC_CONVERTER_H

#include <cstdint>
#include <list>
#include <memory>

#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace adapters {

namespace orc {

class ARROW_EXPORT ORCFileReader {
 public:
  ~ORCFileReader();
  static Status Open(const std::shared_ptr<io::ReadableFileInterface>& file,
                     MemoryPool* pool, std::unique_ptr<ORCFileReader>* reader);
  Status ReadSchema(std::shared_ptr<Schema>* out);
  Status Read(std::shared_ptr<RecordBatch>* out);
  Status Read(const std::list<uint64_t>& include_indices,
              std::shared_ptr<RecordBatch>* out);
  Status ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out);
  Status ReadStripe(int64_t stripe, const std::list<uint64_t>& include_indices,
                    std::shared_ptr<RecordBatch>* out);
  int64_t NumberOfStripes();
  int64_t NumberOfRows();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  explicit ORCFileReader(std::unique_ptr<Impl> impl);
};

}  // namespace orc

}  // namespace adapters

}  // namespace arrow

#endif  // ARROW_ORC_CONVERTER_H
