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

#ifndef ARROW_AVRO_CONVERTER_H
#define ARROW_AVRO_CONVERTER_H

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/builder.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#include "avro.h"

namespace arrow {

namespace adapters {

namespace avro {

/// Base class for reading from an avro datasource.
//
/// This class provides a facilities for incrementally building the null bitmap
/// (see Append methods) and as a side effect the current number of slots and
/// the null count.
class ARROW_EXPORT AvroArrowReader {
 public:
  explicit AvroArrowReader(MemoryPool* pool)
      : pool_(pool) {
  }

  virtual ~AvroArrowReader() = default;

  Status ReadFromFileName(std::string filename, std::shared_ptr<Array>* out);

  Status ReadFromAvroFile(avro_file_reader_t* file_reader,
                          std::shared_ptr<Array>* out);
 protected:
  AvroArrowReader();
  class ARROW_NO_EXPORT AvroArrowReaderImpl;
  std::shared_ptr<AvroArrowReaderImpl> impl_;
  MemoryPool* pool_;

  Status GenerateAvroSchema(avro_file_reader_t* file_reader,
                            std::shared_ptr<DataType>* out);

};

}

}

}

#endif //ARROW_AVRO_CONVERTER_H
