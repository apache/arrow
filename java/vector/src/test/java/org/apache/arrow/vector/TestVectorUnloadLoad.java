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
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorUnloadLoad {

  static final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  @Test
  public void testUnloadLoad() throws IOException {
    int count = 10000;
    Schema schema;

    try (
        BufferAllocator originalVectorsAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorsAllocator, null)) {

      // write some data
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

      // unload it
      FieldVector root = parent.getChild("root");
      schema = new Schema(root.getField().getChildren());
      VectorUnloader vectorUnloader = newVectorUnloader(root);
      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
          VectorSchemaRoot newRoot = new VectorSchemaRoot(schema, finalVectorsAllocator);
          ) {

        // load it
        VectorLoader vectorLoader = new VectorLoader(newRoot);

        vectorLoader.load(recordBatch);

        FieldReader intReader = newRoot.getVector("int").getReader();
        FieldReader bigIntReader = newRoot.getVector("bigInt").getReader();
        for (int i = 0; i < count; i++) {
          intReader.setPosition(i);
          Assert.assertEquals(i, intReader.readInteger().intValue());
          bigIntReader.setPosition(i);
          Assert.assertEquals(i, bigIntReader.readLong().longValue());
        }
      }
    }
  }

  public static VectorUnloader newVectorUnloader(FieldVector root) {
    Schema schema = new Schema(root.getField().getChildren());
    int valueCount = root.getAccessor().getValueCount();
    List<FieldVector> fields = root.getChildrenFromFields();
    return new VectorUnloader(schema, valueCount, fields);
  }

  @AfterClass
  public static void afterClass() {
    allocator.close();
  }
}
