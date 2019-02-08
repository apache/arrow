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
#include <avro/DataFile.hh>

#include "gtest/gtest.h"

#include "arrow/io/file.h"
#include "arrow/io/test-common.h"
#include "arrow/memory_pool.h"
#include "adapter.h"

#include "avro/DataFile.hh"
#include "avro/Compiler.hh"
#include "avro/Generic.hh"


namespace arrow {
namespace adapters {

static const char dsch[] = "{\"type\": \"record\","
    "\"name\":\"ComplexDouble\", \"fields\": ["
    "{\"name\":\"re\", \"type\":\"double\"},"
    "{\"name\":\"im\", \"type\":\"double\"}"
    "]}";

static avro::ValidSchema makeValidSchema(const char* schema)
{
  auto vs = avro::compileJsonSchemaFromString(schema);
  return vs;
}

typedef std::pair<avro::ValidSchema, avro::GenericDatum> AvroPair;

TEST(TestAvroReader, SIMPLE) {
  auto pool = default_memory_pool();
  auto reader = AvroArrowReader(pool);


    // write a single record
    auto filename = "test.avro";
    avro::ValidSchema writerSchema = makeValidSchema(dsch);
    avro::DataFileWriter<AvroPair> df(filename, writerSchema);
    auto rootSchema = writerSchema.root();
    auto p = AvroPair(writerSchema, avro::GenericDatum());

    avro::GenericDatum& c = p.second;
    c = avro::GenericDatum(rootSchema);
    auto& r = c.value<avro::GenericRecord>();

    for (int i = 0; i < 100; i++) {
      r.fieldAt(0) = i * 0.5;
      r.fieldAt(1) = i * 2.5;
      df.write(p);
    }

    df.flush();
    df.close();

    std::shared_ptr<RecordBatch> out;
    reader.ReadFromFileName("test.avro", &out);

}



}
}
