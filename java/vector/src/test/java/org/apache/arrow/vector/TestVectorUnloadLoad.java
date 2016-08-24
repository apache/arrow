/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorUnloadLoad {

  static final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  @Test
  public void test() throws IOException {
    int count = 10000;
    Schema schema;

    try (
        BufferAllocator originalVectorsAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorsAllocator, null)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter rootWriter = writer.rootAsMap();
      IntWriter intWriter = rootWriter.integer("int");
      BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
      for (int i = 0; i < count; i++) {
        intWriter.setPosition(i);
        intWriter.writeInt(i);
        bigIntWriter.setPosition(i);
        bigIntWriter.writeBigInt(i);
      }
      writer.setValueCount(count);

      VectorUnloader vectorUnloader = new VectorUnloader((MapVector)parent.getChild("root"));
      schema = vectorUnloader.getSchema();

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
          MapVector newParent = new MapVector("parent", finalVectorsAllocator, null)) {
        MapVector root = newParent.addOrGet("root", MinorType.MAP, MapVector.class);
        VectorLoader vectorLoader = new VectorLoader(schema, root);

        vectorLoader.load(recordBatch);

        MapReader rootReader = new SingleMapReaderImpl(newParent).reader("root");
        for (int i = 0; i < count; i++) {
          rootReader.setPosition(i);
          Assert.assertEquals(i, rootReader.reader("int").readInteger().intValue());
          Assert.assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
        }
      }
    }
  }

  @AfterClass
  public static void afterClass() {
    allocator.close();
  }
}
