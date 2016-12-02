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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
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
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

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

  /**
   * The validity buffer can be empty if:
   *  - all values are defined
   *  - all values are null
   * @throws IOException
   */
  @Test
  public void testLoadEmptyValidityBuffer() throws IOException {
    Schema schema = new Schema(asList(
        new Field("intDefined", true, new ArrowType.Int(32, true), Collections.<Field>emptyList()),
        new Field("intNull", true, new ArrowType.Int(32, true), Collections.<Field>emptyList())
        ));
    int count = 10;
    ArrowBuf validity = allocator.getEmpty();
    ArrowBuf values = allocator.buffer(count * 4); // integers
    for (int i = 0; i < count; i++) {
      values.setInt(i * 4, i);
    }
    try (
        ArrowRecordBatch recordBatch = new ArrowRecordBatch(count, asList(new ArrowFieldNode(count, 0), new ArrowFieldNode(count, count)), asList(validity, values, validity, values));
        BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        VectorSchemaRoot newRoot = new VectorSchemaRoot(schema, finalVectorsAllocator);
        ) {

      // load it
      VectorLoader vectorLoader = new VectorLoader(newRoot);

      vectorLoader.load(recordBatch);

      NullableIntVector intDefinedVector = (NullableIntVector)newRoot.getVector("intDefined");
      NullableIntVector intNullVector = (NullableIntVector)newRoot.getVector("intNull");
      for (int i = 0; i < count; i++) {
        assertFalse("#" + i, intDefinedVector.getAccessor().isNull(i));
        assertEquals("#" + i, i, intDefinedVector.getAccessor().get(i));
        assertTrue("#" + i, intNullVector.getAccessor().isNull(i));
      }
      intDefinedVector.getMutator().setSafe(count + 10, 1234);
      assertTrue(intDefinedVector.getAccessor().isNull(count + 1));
      // empty slots should still default to unset
      intDefinedVector.getMutator().setSafe(count + 1, 789);
      assertFalse(intDefinedVector.getAccessor().isNull(count + 1));
      assertEquals(789, intDefinedVector.getAccessor().get(count + 1));
      assertTrue(intDefinedVector.getAccessor().isNull(count));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 2));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 3));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 4));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 5));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 6));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 7));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 8));
      assertTrue(intDefinedVector.getAccessor().isNull(count + 9));
      assertFalse(intDefinedVector.getAccessor().isNull(count + 10));
      assertEquals(1234, intDefinedVector.getAccessor().get(count + 10));
    } finally {
      values.release();
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
