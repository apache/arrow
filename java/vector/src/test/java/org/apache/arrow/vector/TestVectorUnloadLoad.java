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

package org.apache.arrow.vector;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestVectorUnloadLoad {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testUnloadLoad() throws IOException {
    int count = 10000;
    Schema schema;

    try (
        BufferAllocator originalVectorsAllocator =
          allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", originalVectorsAllocator)) {

      // write some data
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
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
          VectorSchemaRoot newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator);
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

  @Test
  public void testUnloadLoadAddPadding() throws IOException {
    int count = 10000;
    Schema schema;
    try (
        BufferAllocator originalVectorsAllocator =
          allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", originalVectorsAllocator)) {

      // write some data
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      ListWriter list = rootWriter.list("list");
      IntWriter intWriter = list.integer();
      for (int i = 0; i < count; i++) {
        list.setPosition(i);
        list.startList();
        for (int j = 0; j < i % 4 + 1; j++) {
          intWriter.writeInt(i);
        }
        list.endList();
      }
      writer.setValueCount(count);

      // unload it
      FieldVector root = parent.getChild("root");
      schema = new Schema(root.getField().getChildren());
      VectorUnloader vectorUnloader = newVectorUnloader(root);
      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
          VectorSchemaRoot newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator);
      ) {
        List<ArrowBuf> oldBuffers = recordBatch.getBuffers();
        List<ArrowBuf> newBuffers = new ArrayList<>();
        for (ArrowBuf oldBuffer : oldBuffers) {
          int l = oldBuffer.readableBytes();
          if (l % 64 != 0) {
            // pad
            l = l + 64 - l % 64;
          }
          ArrowBuf newBuffer = allocator.buffer(l);
          for (int i = oldBuffer.readerIndex(); i < oldBuffer.writerIndex(); i++) {
            newBuffer.setByte(i - oldBuffer.readerIndex(), oldBuffer.getByte(i));
          }
          newBuffer.readerIndex(0);
          newBuffer.writerIndex(l);
          newBuffers.add(newBuffer);
        }

        try (ArrowRecordBatch newBatch =
               new ArrowRecordBatch(recordBatch.getLength(), recordBatch.getNodes(), newBuffers);) {
          // load it
          VectorLoader vectorLoader = new VectorLoader(newRoot);

          vectorLoader.load(newBatch);

          FieldReader reader = newRoot.getVector("list").getReader();
          for (int i = 0; i < count; i++) {
            reader.setPosition(i);
            List<Integer> expected = new ArrayList<>();
            for (int j = 0; j < i % 4 + 1; j++) {
              expected.add(i);
            }
            Assert.assertEquals(expected, reader.readObject());
          }
        }

        for (ArrowBuf newBuf : newBuffers) {
          newBuf.getReferenceManager().release();
        }
      }
    }
  }

  /**
   * The validity buffer can be empty if:
   * - all values are defined.
   * - all values are null.
   *
   * @throws IOException on error
   */
  @Test
  public void testLoadValidityBuffer() throws IOException {
    Schema schema = new Schema(asList(
        new Field("intDefined", FieldType.nullable(new ArrowType.Int(32, true)), Collections.<Field>emptyList()),
        new Field("intNull", FieldType.nullable(new ArrowType.Int(32, true)), Collections.<Field>emptyList())
    ));
    int count = 10;
    ArrowBuf[] values = new ArrowBuf[4];
    for (int i = 0; i < 4; i += 2) {
      ArrowBuf buf1 = allocator.buffer(BitVectorHelper.getValidityBufferSize(count));
      ArrowBuf buf2 = allocator.buffer(count * 4); // integers
      buf1.setZero(0, buf1.capacity());
      buf2.setZero(0, buf2.capacity());
      values[i] = buf1;
      values[i + 1] = buf2;
      for (int j = 0; j < count; j++) {
        if (i == 2) {
          BitVectorHelper.setValidityBit(buf1, j, 0);
        } else {
          BitVectorHelper.setValidityBitToOne(buf1, j);
        }

        buf2.setInt(j * 4, j);
      }
      buf1.writerIndex((int)Math.ceil(count / 8));
      buf2.writerIndex(count * 4);
    }

    /*
     * values[0] - validity buffer for first vector
     * values[1] - data buffer for first vector
     * values[2] - validity buffer for second vector
     * values[3] - data buffer for second vector
     */

    try (
        ArrowRecordBatch recordBatch = new ArrowRecordBatch(count, asList(new ArrowFieldNode(count, 0),
          new ArrowFieldNode(count, count)), asList(values[0], values[1], values[2], values[3]));
        BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        VectorSchemaRoot newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator);
    ) {

      // load it
      VectorLoader vectorLoader = new VectorLoader(newRoot);

      vectorLoader.load(recordBatch);

      IntVector intDefinedVector = (IntVector) newRoot.getVector("intDefined");
      IntVector intNullVector = (IntVector) newRoot.getVector("intNull");
      for (int i = 0; i < count; i++) {
        assertFalse("#" + i, intDefinedVector.isNull(i));
        assertEquals("#" + i, i, intDefinedVector.get(i));
        assertTrue("#" + i, intNullVector.isNull(i));
      }
      intDefinedVector.setSafe(count + 10, 1234);
      assertTrue(intDefinedVector.isNull(count + 1));
      // empty slots should still default to unset
      intDefinedVector.setSafe(count + 1, 789);
      assertFalse(intDefinedVector.isNull(count + 1));
      assertEquals(789, intDefinedVector.get(count + 1));
      assertTrue(intDefinedVector.isNull(count));
      assertTrue(intDefinedVector.isNull(count + 2));
      assertTrue(intDefinedVector.isNull(count + 3));
      assertTrue(intDefinedVector.isNull(count + 4));
      assertTrue(intDefinedVector.isNull(count + 5));
      assertTrue(intDefinedVector.isNull(count + 6));
      assertTrue(intDefinedVector.isNull(count + 7));
      assertTrue(intDefinedVector.isNull(count + 8));
      assertTrue(intDefinedVector.isNull(count + 9));
      assertFalse(intDefinedVector.isNull(count + 10));
      assertEquals(1234, intDefinedVector.get(count + 10));
    } finally {
      for (ArrowBuf arrowBuf : values) {
        arrowBuf.getReferenceManager().release();
      }
    }
  }

  @Test
  public void testUnloadLoadDuplicates() throws IOException {
    int count = 10;
    Schema schema = new Schema(asList(
        new Field("duplicate", FieldType.nullable(new ArrowType.Int(32, true)), Collections.<Field>emptyList()),
        new Field("duplicate", FieldType.nullable(new ArrowType.Int(32, true)), Collections.<Field>emptyList())
    ));

    try (
        BufferAllocator originalVectorsAllocator =
          allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
    ) {
      List<FieldVector> sources = new ArrayList<>();
      for (Field field : schema.getFields()) {
        FieldVector vector = field.createVector(originalVectorsAllocator);
        vector.allocateNew();
        sources.add(vector);
        IntVector intVector = (IntVector)vector;
        for (int i = 0; i < count; i++) {
          intVector.set(i, i);
        }
        intVector.setValueCount(count);
      }

      try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), sources, count)) {
        VectorUnloader vectorUnloader = new VectorUnloader(root);
        try (ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
             BufferAllocator finalVectorsAllocator =
               allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
             VectorSchemaRoot newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator);) {
          // load it
          VectorLoader vectorLoader = new VectorLoader(newRoot);
          vectorLoader.load(recordBatch);

          List<FieldVector> targets = newRoot.getFieldVectors();
          Assert.assertEquals(sources.size(), targets.size());
          for (int k = 0; k < sources.size(); k++) {
            IntVector src = (IntVector) sources.get(k);
            IntVector tgt = (IntVector) targets.get(k);
            Assert.assertEquals(src.getValueCount(), tgt.getValueCount());
            for (int i = 0; i < count; i++) {
              Assert.assertEquals(src.get(i), tgt.get(i));
            }
          }
        }
      }
    }
  }

  public static VectorUnloader newVectorUnloader(FieldVector root) {
    Schema schema = new Schema(root.getField().getChildren());
    int valueCount = root.getValueCount();
    List<FieldVector> fields = root.getChildrenFromFields();
    VectorSchemaRoot vsr = new VectorSchemaRoot(schema.getFields(), fields, valueCount);
    return new VectorUnloader(vsr);
  }
}
