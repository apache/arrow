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

package org.apache.arrow.dataset.jni;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestUnsafeRecordBatchSerializer {

  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  private ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  private ArrowBuf intBuf(int[] ints) {
    ArrowBuf buffer = allocator.buffer(ints.length * 4L);
    for (int i : ints) {
      buffer.writeInt(i);
    }
    return buffer;
  }

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null, null), asList(children));
  }

  @Test
  public void testRoundTrip() throws IOException {
    final byte[] aValidity = new byte[]{(byte) 255, 0};
    final byte[] bValidity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    final int[] aValues = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    final int[] bValues = new int[]{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    final Integer[] aExpected =
        new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, null, null, null, null, null, null, null, null};
    final Integer[] bExpected =
        new Integer[]{16, 15, 14, 13, 12, 11, 10, 9, null, null, null, null, null, null, null, null};
    ArrowBuf validitya = buf(aValidity);
    ArrowBuf valuesa = intBuf(aValues);
    ArrowBuf validityb = buf(bValidity);
    ArrowBuf valuesb = intBuf(bValues);
    final ArrowRecordBatch batch = new ArrowRecordBatch(
        16, Arrays.asList(new ArrowFieldNode(16, 8), new ArrowFieldNode(16, 8)),
        Arrays.asList(validitya, valuesa, validityb, valuesb));
    final Schema schema = new org.apache.arrow.vector.types.pojo.Schema(asList(
        field("a", true, new ArrowType.Int(32, true)),
        field("b", true, new ArrowType.Int(32, true)))
    );

    byte[] reexported = JniWrapper.get().reexportUnsafeSerializedBatch(SchemaUtility.serialize(schema),
        UnsafeRecordBatchSerializer.serializeUnsafe(batch));
    final ArrowRecordBatch reexportedBatch = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator, reexported);

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    VectorLoader loader = new VectorLoader(root);
    loader.load(reexportedBatch);

    final List<FieldVector> fieldVectors = root.getFieldVectors();

    // check data
    Assert.assertEquals(2, fieldVectors.size());
    FieldVector aVector = fieldVectors.get(0);
    FieldVector bVector = fieldVectors.get(1);
    Assert.assertEquals(Types.MinorType.INT, aVector.getMinorType());
    Assert.assertEquals(Types.MinorType.INT, bVector.getMinorType());
    IntVector aIntVector = (IntVector) aVector;
    IntVector bIntVector = (IntVector) bVector;
    Assert.assertEquals(8, aIntVector.getNullCount());
    Assert.assertEquals(8, bIntVector.getNullCount());
    for (int i = 0; i < 16; i++) {
      Assert.assertEquals(aExpected[i], aIntVector.getObject(i));
      Assert.assertEquals(bExpected[i], bIntVector.getObject(i));
    }
    // check memory release
    root.close();
    reexportedBatch.close();
    batch.close();
  }
}
