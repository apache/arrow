/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.AvroToArrow;
import org.apache.arrow.AvroToArrowConfig;
import org.apache.arrow.AvroToArrowConfigBuilder;
import org.apache.arrow.AvroToArrowVectorIterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for avro adapter.
 */
@State(Scope.Benchmark)
public class AvroAdapterBenchmarks {

  private final int valueCount = 3000;

  private AvroToArrowConfig config;

  private Schema schema;
  private BinaryDecoder decoder;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() throws Exception {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    config = new AvroToArrowConfigBuilder(allocator).build();

    String schemaStr = "{\n" + " \"namespace\": \"org.apache.arrow.avro\",\n" +
         " \"type\": \"record\",\n" + " \"name\": \"testBenchmark\",\n" + " \"fields\": [\n" +
         "    {\"name\": \"f0\", \"type\": \"string\"},\n" +
         "    {\"name\": \"f1\", \"type\": \"int\"},\n" +
         "    {\"name\": \"f2\", \"type\": \"long\"},\n" +
         "    {\"name\": \"f3\", \"type\": \"boolean\"},\n" +
         "    {\"name\": \"f4\", \"type\": \"float\"}\n" + "  ]\n" + "}";
    schema = new Schema.Parser().parse(schemaStr);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(out, null);
    DatumWriter writer = new GenericDatumWriter(schema);

    for (int i = 0; i < valueCount; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i);
      record.put(2, i + 1L);
      record.put(3, i % 2 == 0);
      record.put(4, i + 0.1f);
      writer.write(record, encoder);
    }

    decoder = new DecoderFactory().directBinaryDecoder(new ByteArrayInputStream(out.toByteArray()), null);
  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() {
    config.getAllocator().close();
  }

  /**
   * Test {@link AvroToArrow#avroToArrowIterator(Schema, Decoder, AvroToArrowConfig)}.
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testAvroToArrow() throws Exception {
    decoder.inputStream().reset();
    int sum = 0;
    try (AvroToArrowVectorIterator iter = AvroToArrow.avroToArrowIterator(schema, decoder, config)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        IntVector intVector = (IntVector) root.getVector("f1");
        for (int i = 0; i < intVector.getValueCount(); i++) {
          sum += intVector.get(i);
        }
        root.close();
      }
    }
    return sum;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(AvroAdapterBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
