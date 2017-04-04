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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFixedSizeListVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testIntType() {
    try (FixedSizeListVector vector = new FixedSizeListVector("list", allocator, 2, null, null)) {
      vector.allocateNew();
      UnionListWriter writer = new UnionListWriter(vector);
      for (int i = 0; i < 10; i++) {
        writer.setPosition(i);
        writer.startList();
        writer.writeInt(i);
        writer.writeInt(i + 10);
        writer.endList();
      }
      writer.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        Assert.assertTrue(reader.isSet());
        Assert.assertTrue(reader.next());
        Assert.assertEquals(i, reader.reader().readInteger().intValue());
        Assert.assertTrue(reader.next());
        Assert.assertEquals(i + 10, reader.reader().readInteger().intValue());
        Assert.assertFalse(reader.next());
        Assert.assertEquals(Lists.newArrayList(i, i + 10), reader.readObject());
      }
    }
  }

  @Test
  public void testFloatTypeNullable() {
    try (FixedSizeListVector vector = new FixedSizeListVector("list", allocator, 2, null, null)) {
      vector.allocateNew();
      UnionListWriter writer = new UnionListWriter(vector);
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          writer.setPosition(i);
          writer.startList();
          writer.writeFloat4(i + 0.1f);
          writer.writeFloat4(i + 10.1f);
          writer.endList();
        }
      }
      writer.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          Assert.assertTrue(reader.isSet());
          Assert.assertTrue(reader.next());
          Assert.assertEquals(i + 0.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertTrue(reader.next());
          Assert.assertEquals(i + 10.1f, reader.reader().readFloat(), 0.00001);
          Assert.assertFalse(reader.next());
          Assert.assertEquals(Lists.newArrayList(i + 0.1f, i + 10.1f), reader.readObject());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapType() {
    try (FixedSizeListVector vector = new FixedSizeListVector("list", allocator, 2, null, null)) {
      vector.allocateNew();
      UnionListWriter writer = new UnionListWriter(vector);
      MapWriter mapWriter = writer.map();
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          writer.setPosition(i);
          writer.startList();
          for (int j = 0; j < 2; j++) {
            mapWriter.start();
            mapWriter.integer("int").writeInt(i * 10 + j);
            mapWriter.bigInt("bigint").writeBigInt(i * 10 + j);
            mapWriter.end();
          }
          writer.endList();
        }
      }
      writer.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          Assert.assertTrue(reader.isSet());
          Assert.assertTrue(reader.next());
          Assert.assertEquals(i * 10, reader.reader().reader("int").readInteger().intValue());
          Assert.assertEquals(i * 10, reader.reader().reader("bigint").readLong().intValue());
          Assert.assertTrue(reader.next());
          Assert.assertEquals(i * 10 + 1, reader.reader().reader("int").readInteger().intValue());
          Assert.assertEquals(i * 10 + 1, reader.reader().reader("bigint").readLong().intValue());
          Assert.assertFalse(reader.next());
          Map<String, Object> expected0 = new JsonStringHashMap<>();
          expected0.put("int", i * 10);
          expected0.put("bigint", i * 10L);
          Map<String, Object> expected1 = new JsonStringHashMap<>();
          expected1.put("int", i * 10 + 1);
          expected1.put("bigint", i * 10L + 1);
          Assert.assertEquals(Lists.newArrayList(expected0, expected1), reader.readObject());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testListType() {
    try (FixedSizeListVector vector = new FixedSizeListVector("list", allocator, 2, null, null)) {
      vector.allocateNew();
      UnionListWriter writer = new UnionListWriter(vector);
      ListWriter listWriter = writer.list();
      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          writer.setPosition(i);
          writer.startList();
          for (int j = 0; j < 2; j++) {
            listWriter.startList();
            for (int k = 0; k < i % 7; k++) {
              listWriter.integer().writeInt(k + j);
            }
            listWriter.endList();
          }
          writer.endList();
        }
      }
      writer.setValueCount(10);

      UnionFixedSizeListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          Assert.assertTrue(reader.isSet());
          Assert.assertTrue(reader.next());
          FieldReader listReader = reader.reader();
          List<Object> expected0 = new ArrayList<>();
          for (int k = 0; k < i % 7; k++) {
            listReader.next();
            Assert.assertEquals(k, listReader.reader().readInteger().intValue());
            expected0.add(k);
          }
          Assert.assertTrue(reader.next());
          List<Object> expected1 = new ArrayList<>();
          for (int k = 0; k < i % 7; k++) {
            listReader.next();
            Assert.assertEquals(k + 1, listReader.reader().readInteger().intValue());
            expected1.add(k + 1);
          }
          Assert.assertFalse(reader.next());
          Assert.assertEquals(Lists.newArrayList(expected0, expected1), reader.readObject());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testNestedInList() {
    try (ListVector vector = new ListVector("list", allocator, null, null)) {
      vector.allocateNew();
      UnionListWriter writer = vector.getWriter();
      ListWriter listWriter = writer.list(2);

      for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
          writer.setPosition(i);
          writer.startList();
          for (int j = 0; j < i % 7; j++) {
            listWriter.startList();
            for (int k = 0; k < 2; k++) {
              listWriter.integer().writeInt(k + j);
            }
            listWriter.endList();
          }
          writer.endList();
        }
      }
      writer.setValueCount(10);

      UnionListReader reader = vector.getReader();
      for (int i = 0; i < 10; i++) {
        reader.setPosition(i);
        if (i % 2 == 0) {
          for (int j = 0; j < i % 7; j++) {
            Assert.assertTrue(reader.next());
            FieldReader innerListReader = reader.reader();
            for (int k = 0; k < 2; k++) {
              Assert.assertTrue(innerListReader.next());
              Assert.assertEquals(k + j, innerListReader.reader().readInteger().intValue());
            }
            Assert.assertFalse(innerListReader.next());
          }
          Assert.assertFalse(reader.next());
        } else {
          Assert.assertFalse(reader.isSet());
          Assert.assertNull(reader.readObject());
        }
      }
    }
  }

  @Test
  public void testCopyFrom() throws Exception {
    try (FixedSizeListVector inVector = new FixedSizeListVector("input", allocator, 2, null, null);
         FixedSizeListVector outVector = new FixedSizeListVector("output", allocator, 2, null, null)) {
      UnionListWriter writer = inVector.getWriter();
      writer.allocate();

      // populate input vector with the following records
      // [1, 2]
      // null
      // []
      writer.setPosition(0); // optional
      writer.startList();
      writer.bigInt().writeBigInt(1);
      writer.bigInt().writeBigInt(2);
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      writer.endList();

      writer.setValueCount(3);

      // copy values from input to output
      outVector.allocateNew();
      for (int i = 0; i < 3; i++) {
        outVector.copyFrom(i, i, inVector);
      }
      outVector.getMutator().setValueCount(3);

      // assert the output vector is correct
      FieldReader reader = outVector.getReader();
      reader.setPosition(0);
      Assert.assertTrue(reader.isSet());
      reader.setPosition(1);
      Assert.assertFalse(reader.isSet());
      reader.setPosition(2);
      Assert.assertTrue(reader.isSet());
    }
  }
}
