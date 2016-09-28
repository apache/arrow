/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionReader;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestComplexWriter {

  private static final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  private static final int COUNT = 100;

  @Test
  public void simpleNestedTypes() {
    MapVector parent = new MapVector("parent", allocator, null);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    for (int i = 0; i < COUNT; i++) {
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
    }
    writer.setValueCount(COUNT);
    MapReader rootReader = new SingleMapReaderImpl(parent).reader("root");
    for (int i = 0; i < COUNT; i++) {
      rootReader.setPosition(i);
      Assert.assertEquals(i, rootReader.reader("int").readInteger().intValue());
      Assert.assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
    }

    parent.close();
  }

  @Test
  public void nullableMap() {
    MapVector parent = new MapVector("parent", allocator, null);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    for (int i = 0; i < COUNT; i++) {
      rootWriter.setPosition(i);
      rootWriter.start();
      if (i % 2 == 0) {
        MapWriter mapWriter = rootWriter.map("map");
        mapWriter.setPosition(i);
        mapWriter.start();
        mapWriter.bigInt("nested").writeBigInt(i);
        mapWriter.end();
      }
      rootWriter.end();
    }
    writer.setValueCount(COUNT);
    MapReader rootReader = new SingleMapReaderImpl(parent).reader("root");
    for (int i = 0; i < COUNT; i++) {
      rootReader.setPosition(i);
      assertTrue("index is set: " + i, rootReader.isSet());
      FieldReader map = rootReader.reader("map");
      if (i % 2 == 0) {
        assertTrue("index is set: " + i, map.isSet());
        assertNotNull("index is set: " + i, map.readObject());
        assertEquals(i, map.reader("nested").readLong().longValue());
      } else {
        assertFalse("index is not set: " + i, map.isSet());
        assertNull("index is not set: " + i, map.readObject());
      }
    }
    parent.close();
  }

  @Test
  public void listOfLists() {
    MapVector parent = new MapVector("parent", allocator, null);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();

    rootWriter.start();
    rootWriter.bigInt("int").writeBigInt(0);
    rootWriter.list("list").startList();
    rootWriter.list("list").bigInt().writeBigInt(0);
    rootWriter.list("list").endList();
    rootWriter.end();

    rootWriter.setPosition(1);
    rootWriter.start();
    rootWriter.bigInt("int").writeBigInt(1);
    rootWriter.end();

    writer.setValueCount(2);

    MapReader rootReader = new SingleMapReaderImpl(parent).reader("root");

    rootReader.setPosition(0);
    assertTrue("row 0 list is not set", rootReader.reader("list").isSet());
    assertEquals(Long.valueOf(0), rootReader.reader("list").reader().readLong());

    rootReader.setPosition(1);
    assertFalse("row 1 list is set", rootReader.reader("list").isSet());
  }

  @Test
  public void listScalarType() {
    ListVector listVector = new ListVector("list", allocator, null);
    listVector.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(listVector);
    for (int i = 0; i < COUNT; i++) {
      listWriter.setPosition(i);
      listWriter.startList();
      for (int j = 0; j < i % 7; j++) {
        listWriter.writeInt(j);
      }
      listWriter.endList();
    }
    listWriter.setValueCount(COUNT);
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        assertEquals(j, listReader.reader().readInteger().intValue());
      }
    }
  }

  @Test
  public void listScalarTypeNullable() {
    ListVector listVector = new ListVector("list", allocator, null);
    listVector.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(listVector);
    for (int i = 0; i < COUNT; i++) {
      if (i % 2 == 0) {
        listWriter.setPosition(i);
        listWriter.startList();
        for (int j = 0; j < i % 7; j++) {
          listWriter.writeInt(j);
        }
        listWriter.endList();
      }
    }
    listWriter.setValueCount(COUNT);
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      if (i % 2 == 0) {
        assertTrue("index is set: " + i, listReader.isSet());
        assertEquals("correct length at: " + i, i % 7, ((List<?>)listReader.readObject()).size());
      } else {
        assertFalse("index is not set: " + i, listReader.isSet());
        assertNull("index is not set: " + i, listReader.readObject());
      }
    }
  }

  @Test
  public void listMapType() {
    ListVector listVector = new ListVector("list", allocator, null);
    listVector.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(listVector);
    MapWriter mapWriter = listWriter.map();
    for (int i = 0; i < COUNT; i++) {
      listWriter.setPosition(i);
      listWriter.startList();
      for (int j = 0; j < i % 7; j++) {
        mapWriter.start();
        mapWriter.integer("int").writeInt(j);
        mapWriter.bigInt("bigInt").writeBigInt(j);
        mapWriter.end();
      }
      listWriter.endList();
    }
    listWriter.setValueCount(COUNT);
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        Assert.assertEquals("record: " + i, j, listReader.reader().reader("int").readInteger().intValue());
        Assert.assertEquals(j, listReader.reader().reader("bigInt").readLong().longValue());
      }
    }
  }

  @Test
  public void listListType() {
    ListVector listVector = new ListVector("list", allocator, null);
    listVector.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(listVector);
    for (int i = 0; i < COUNT; i++) {
      listWriter.setPosition(i);
      listWriter.startList();
      for (int j = 0; j < i % 7; j++) {
        ListWriter innerListWriter = listWriter.list();
        innerListWriter.startList();
        for (int k = 0; k < i % 13; k++) {
          innerListWriter.integer().writeInt(k);
        }
        innerListWriter.endList();
      }
      listWriter.endList();
    }
    listWriter.setValueCount(COUNT);
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        FieldReader innerListReader = listReader.reader();
        for (int k = 0; k < i % 13; k++) {
          innerListReader.next();
          Assert.assertEquals("record: " + i, k, innerListReader.reader().readInteger().intValue());
        }
      }
    }
    listVector.clear();
  }

  @Test
  public void unionListListType() {
    ListVector listVector = new ListVector("list", allocator, null);
    listVector.allocateNew();
    UnionListWriter listWriter = new UnionListWriter(listVector);
    for (int i = 0; i < COUNT; i++) {
      listWriter.setPosition(i);
      listWriter.startList();
      for (int j = 0; j < i % 7; j++) {
        ListWriter innerListWriter = listWriter.list();
        innerListWriter.startList();
        for (int k = 0; k < i % 13; k++) {
          if (k % 2 == 0) {
            innerListWriter.integer().writeInt(k);
          } else {
            innerListWriter.bigInt().writeBigInt(k);
          }
        }
        innerListWriter.endList();
      }
      listWriter.endList();
    }
    listWriter.setValueCount(COUNT);
    UnionListReader listReader = new UnionListReader(listVector);
    for (int i = 0; i < COUNT; i++) {
      listReader.setPosition(i);
      for (int j = 0; j < i % 7; j++) {
        listReader.next();
        FieldReader innerListReader = listReader.reader();
        for (int k = 0; k < i % 13; k++) {
          innerListReader.next();
          if (k % 2 == 0) {
            Assert.assertEquals("record: " + i, k, innerListReader.reader().readInteger().intValue());
          } else {
            Assert.assertEquals("record: " + i, k, innerListReader.reader().readLong().longValue());
          }
        }
      }
    }
    listVector.clear();
  }

  @Test
  public void simpleUnion() {
    UnionVector vector = new UnionVector("union", allocator, null);
    UnionWriter unionWriter = new UnionWriter(vector);
    unionWriter.allocate();
    for (int i = 0; i < COUNT; i++) {
      unionWriter.setPosition(i);
      if (i % 2 == 0) {
        unionWriter.writeInt(i);
      } else {
        unionWriter.writeFloat4((float) i);
      }
    }
    vector.getMutator().setValueCount(COUNT);
    UnionReader unionReader = new UnionReader(vector);
    for (int i = 0; i < COUNT; i++) {
      unionReader.setPosition(i);
      if (i % 2 == 0) {
        Assert.assertEquals(i, i, unionReader.readInteger());
      } else {
        Assert.assertEquals((float) i, unionReader.readFloat(), 1e-12);
      }
    }
    vector.close();
  }

  @Test
  public void promotableWriter() {
    MapVector parent = new MapVector("parent", allocator, null);
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    for (int i = 0; i < 100; i++) {
      BigIntWriter bigIntWriter = rootWriter.bigInt("a");
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
    }
    Field field = parent.getField().getChildren().get(0).getChildren().get(0);
    Assert.assertEquals("a", field.getName());
    Assert.assertEquals(Int.TYPE_TYPE, field.getType().getTypeType());
    Int intType = (Int) field.getType();

    Assert.assertEquals(64, intType.getBitWidth());
    Assert.assertTrue(intType.getIsSigned());
    for (int i = 100; i < 200; i++) {
      VarCharWriter varCharWriter = rootWriter.varChar("a");
      varCharWriter.setPosition(i);
      byte[] bytes = Integer.toString(i).getBytes();
      ArrowBuf tempBuf = allocator.buffer(bytes.length);
      tempBuf.setBytes(0, bytes);
      varCharWriter.writeVarChar(0, bytes.length, tempBuf);
    }
    field = parent.getField().getChildren().get(0).getChildren().get(0);
    Assert.assertEquals("a", field.getName());
    Assert.assertEquals(Union.TYPE_TYPE, field.getType().getTypeType());
    Assert.assertEquals(Int.TYPE_TYPE, field.getChildren().get(0).getType().getTypeType());
    Assert.assertEquals(Utf8.TYPE_TYPE, field.getChildren().get(1).getType().getTypeType());
    MapReader rootReader = new SingleMapReaderImpl(parent).reader("root");
    for (int i = 0; i < 100; i++) {
      rootReader.setPosition(i);
      Assert.assertEquals(i, rootReader.reader("a").readLong().intValue());
    }
    for (int i = 100; i < 200; i++) {
      rootReader.setPosition(i);
      Assert.assertEquals(Integer.toString(i), rootReader.reader("a").readText().toString());
    }
  }
}