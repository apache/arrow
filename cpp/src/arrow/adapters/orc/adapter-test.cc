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

#include "adapter.h"
#include "arrow/array.h"
#include "arrow/io/api.h"
#include "orc/OrcFile.hh"

#include <gtest/gtest.h>
namespace liborc = orc;

namespace arrow {

const int DEFAULT_MEM_STREAM_SIZE = 100 * 1024 * 1024;

class MemoryOutputStream : public liborc::OutputStream {
  public:
    MemoryOutputStream(ssize_t capacity) : name("MemoryOutputStream") {
      data = new char[capacity];
      length = 0;
    }

    virtual ~MemoryOutputStream() override { delete[] data; }

    virtual uint64_t getLength() const override { return length; }

    virtual uint64_t getNaturalWriteSize() const override { return naturalWriteSize; }

    virtual void write(const void* buf, size_t size) override  {
      memcpy(data + length, buf, size);
      length += size;
    }

    virtual const std::string& getName() const override { return name; }

    const char * getData() const { return data; }

    void close() override {}

    void reset()  { length = 0; }

  private:
    char * data;
    std::string name;
    uint64_t length, naturalWriteSize;
  };

  std::unique_ptr<liborc::Writer> createWriter(
          uint64_t stripeSize,
          const liborc::Type& type,
          liborc::OutputStream* stream){
    liborc::WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(1024);
    options.setMemoryPool(liborc::getDefaultPool());
    options.setRowIndexStride(0);
    return liborc::createWriter(type, stream, options);
  }

  TEST(TestAdapter, readIntFileMultipleStripes) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    ORC_UNIQUE_PTR<liborc::Type> type(
            liborc::Type::buildTypeFromString("struct<col1:int,col2:string>"));

    const uint64_t stripeSize = 1024; // 1K
    const uint64_t stripeCount = 10;

    auto writer = createWriter(stripeSize, *type, &memStream);
    auto batch = writer->createRowBatch(65535);
    auto structBatch = dynamic_cast<liborc::StructVectorBatch*>(batch.get());
    auto longBatch = dynamic_cast<liborc::LongVectorBatch*>(structBatch->fields[0]);
    auto strBatch = dynamic_cast<liborc::StringVectorBatch*>(structBatch->fields[1]);
    int64_t accumulated = 0;


    for (uint64_t j = 0; j < stripeCount; ++j) {
      char dataBuffer[327675];
      uint64_t offset = 0;
      for (uint64_t i = 0; i < 65535; ++i) {
        std::ostringstream os;
        os << accumulated % 65535;
        longBatch->data[i] = static_cast<int64_t>(accumulated % 65535);
        strBatch->data[i] = dataBuffer + offset;
        strBatch->length[i] = static_cast<int64_t>(os.str().size());
        memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
        accumulated++;
        offset += os.str().size();
      }
      structBatch->numElements = 65535;
      longBatch->numElements = 65535;
      strBatch->numElements = 65535;

      writer->add(*batch);
    }

    writer->close();

    std::shared_ptr<io::ReadableFileInterface> inStream(
            new io::BufferReader(std::make_shared<Buffer>(
                    reinterpret_cast<const uint8_t*>(memStream.getData()),
                    static_cast<int64_t>(memStream.getLength()))));

    std::unique_ptr<adapters::orc::ORCFileReader> reader;
    EXPECT_EQ(Status::OK().code(),
            adapters::orc::ORCFileReader::Open(inStream, default_memory_pool(), &reader).code());

    EXPECT_EQ(655350, reader->NumberOfRows());
    EXPECT_EQ(stripeCount, reader->NumberOfStripes());
    accumulated = 0;
    std::shared_ptr<RecordBatchReader> stripeReader;
    EXPECT_TRUE(reader->NextStripeReader(1024, &stripeReader).ok());
    while (stripeReader) {
      std::shared_ptr<RecordBatch> recordBatch;
      EXPECT_TRUE(stripeReader->ReadNext(&recordBatch).ok());
      while (recordBatch) {
        auto int32Array = std::dynamic_pointer_cast<Int32Array>(recordBatch->column(0));
        auto strArray = std::dynamic_pointer_cast<StringArray>(recordBatch->column(1));
        for (int j = 0; j < recordBatch->num_rows(); ++j) {
          std::ostringstream os;
          os << accumulated % 65535;
          EXPECT_EQ(accumulated % 65535, int32Array->Value(j));
          EXPECT_EQ(os.str(), strArray->GetString(j));
          accumulated++;
        }
        EXPECT_TRUE(stripeReader->ReadNext(&recordBatch).ok());
      }
      EXPECT_TRUE(reader->NextStripeReader(1024, &stripeReader).ok());
    }

    // test seek operation
    int64_t startOffset = 830;
    EXPECT_TRUE(reader->Seek(65535 + startOffset).ok());

    EXPECT_TRUE(reader->NextStripeReader(1024, &stripeReader).ok());
    std::shared_ptr<RecordBatch> recordBatch;
    EXPECT_TRUE(stripeReader->ReadNext(&recordBatch).ok());
    while (recordBatch) {
      auto int32Array = std::dynamic_pointer_cast<Int32Array>(recordBatch->column(0));
      auto strArray = std::dynamic_pointer_cast<StringArray>(recordBatch->column(1));
      for (int j = 0; j < recordBatch->num_rows(); ++j) {
        std::ostringstream os;
        os << startOffset % 65535;
        EXPECT_EQ(startOffset % 65535, int32Array->Value(j));
        EXPECT_EQ(os.str(), strArray->GetString(j));
        startOffset++;
      }
      EXPECT_TRUE(stripeReader->ReadNext(&recordBatch).ok());
    }
  }
}

