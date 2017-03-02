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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

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
        BufferAllocator originalVectorsAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorsAllocator, null)) {

      // write some data
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter rootWriter = writer.rootAsMap();
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

        try (ArrowRecordBatch newBatch = new ArrowRecordBatch(recordBatch.getLength(), recordBatch.getNodes(), newBuffers);) {
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
          newBuf.release();
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
    ArrowBuf validity = allocator.buffer(10).slice(0, 0);
    ArrowBuf[] values = new ArrowBuf[2];
    for (int i = 0; i < values.length; i++) {
      ArrowBuf arrowBuf = allocator.buffer(count * 4); // integers
      values[i] = arrowBuf;
      for (int j = 0; j < count; j++) {
        arrowBuf.setInt(j * 4, j);
      }
      arrowBuf.writerIndex(count * 4);
    }
    try (
        ArrowRecordBatch recordBatch = new ArrowRecordBatch(count, asList(new ArrowFieldNode(count, 0), new ArrowFieldNode(count, count)), asList(validity, values[0], validity, values[1]));
        BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        VectorSchemaRoot newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator);
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
      for (ArrowBuf arrowBuf : values) {
        arrowBuf.release();
      }
      validity.release();
    }
  }

  public static VectorUnloader newVectorUnloader(FieldVector root) {
    Schema schema = new Schema(root.getField().getChildren());
    int valueCount = root.getAccessor().getValueCount();
    List<FieldVector> fields = root.getChildrenFromFields();
    VectorSchemaRoot vsr = new VectorSchemaRoot(schema.getFields(), fields, valueCount);
    return new VectorUnloader(vsr);
  }

  @Test
  public void testWithoutFieldNodes() throws Exception {
    BufferAllocator original = allocator.newChildAllocator("original", 0, Integer.MAX_VALUE);
    NullableIntVector intVector = new NullableIntVector("int", original, null);
    NullableVarCharVector varCharVector = new NullableVarCharVector("int", original, null);
    ListVector listVector = new ListVector("list", original, null, null);
    ListVector innerListVector = (ListVector) listVector.addOrGetVector(MinorType.LIST, null).getVector();

    intVector.allocateNew();;
    varCharVector.allocateNew();
    listVector.allocateNew();

    UnionListWriter listWriter = listVector.getWriter();

    for (int i = 0; i < 100; i++) {
      if (i % 3 == 0) {
        continue;
      }
      intVector.getMutator().setSafe(i, i);
      byte[] s = ("val" + i).getBytes();
      varCharVector.getMutator().setSafe(i, s, 0, s.length);
      listWriter.setPosition(i);
      listWriter.startList();
      ListWriter innerListWriter = listWriter.list();
      innerListWriter.startList();
      innerListWriter.integer().writeInt(i);
      innerListWriter.integer().writeInt(i + 1);
      innerListWriter.endList();
      innerListWriter.startList();
      innerListWriter.integer().writeInt(i + 2);
      innerListWriter.integer().writeInt(i + 3);
      innerListWriter.endList();
      listWriter.endList();
    }

    intVector.getMutator().setValueCount(100);
    varCharVector.getMutator().setValueCount(100);
    listVector.getMutator().setValueCount(100);

    ByteBuf[] bufs = FluentIterable.from(
            ImmutableList.<FieldVector>of(intVector, varCharVector, listVector))
            .transformAndConcat(new Function<FieldVector, Iterable<ArrowBuf>>() {
              @Override
              public Iterable<ArrowBuf> apply(FieldVector vector) {
                return Arrays.asList(vector.getBuffers(true));
              }
            }).toList().toArray(new ByteBuf[0]);

    RecordBatch recordBatch = RecordBatch.getRootAsRecordBatch(getArrowRecordBatch(bufs, 100));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();



    for (ByteBuf buf : bufs) {
      buf.readBytes(outputStream, buf.readableBytes());
      buf.release();
    }
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();

    BufferAllocator newAllocator = allocator.newChildAllocator("new allocator", 0, Integer.MAX_VALUE);

    ArrowBuf body = newAllocator.buffer(bytes.length);

    body.writeBytes(bytes);


    NullableIntVector newIntVector = new NullableIntVector("newInt", newAllocator, null);
    NullableVarCharVector newVarCharVector = new NullableVarCharVector("newVarChar", newAllocator, null);
    ListVector newListVector = new ListVector("newListVector", newAllocator, null, null);
    ((ListVector) newListVector.addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.INT, null);

    BuffersIterator buffersIterator = new BuffersIterator(recordBatch);

    newIntVector.loadFieldBuffers(buffersIterator, body);
    newIntVector.getMutator().setValueCount(100);
    newVarCharVector.loadFieldBuffers(buffersIterator, body);
    newVarCharVector.getMutator().setValueCount(100);
    newListVector.loadFieldBuffers(buffersIterator, body);
    newListVector.getMutator().setValueCount(100);

    body.release();

    for (int i = 0; i < recordBatch.length(); i++) {
      if (i %3 == 0) {
        Assert.assertNull(newIntVector.getAccessor().getObject(i));
        Assert.assertNull(newVarCharVector.getAccessor().getObject(i));
        Assert.assertNull(newListVector.getAccessor().getObject(i));
      } else {
        Assert.assertEquals(Integer.valueOf(i), newIntVector.getAccessor().getObject(i));
        Assert.assertEquals("val" + i, newVarCharVector.getAccessor().getObject(i).toString());
        Assert.assertEquals(ImmutableList.of(ImmutableList.of(i, i + 1), ImmutableList.of(i + 2, i + 3)), newListVector.getAccessor().getObject(i));
      }
    }

    newIntVector.clear();
    newVarCharVector.clear();
    newListVector.clear();
    original.close();
    newAllocator.close();
  }

  private ByteBuffer getArrowRecordBatch(ByteBuf[] buffers, int recordCount) {
    int length = 0;
    for (ByteBuf buf : buffers) {
      length += buf.writerIndex();
    }
    FlatBufferBuilder builder = new FlatBufferBuilder();
    RecordBatch.startNodesVector(builder, 0);
    int nodesOffset = builder.endVector();
    RecordBatch.startBuffersVector(builder, buffers.length);
    int offset = length;
    for (int i = buffers.length - 1; i >= 0; i--) {
      long currentLength = buffers[i].writerIndex();
      offset -= currentLength;
      Buffer.createBuffer(builder, 0, offset, currentLength);
    }
    int buffersOffset = builder.endVector();
    RecordBatch.startRecordBatch(builder);
    RecordBatch.addLength(builder, recordCount);
    RecordBatch.addNodes(builder, nodesOffset);
    RecordBatch.addBuffers(builder, buffersOffset);
    int recordBatch = RecordBatch.endRecordBatch(builder);
    builder.finish(recordBatch);
    return builder.dataBuffer();
  }

  @AfterClass
  public static void afterClass() {
    allocator.close();
  }
}