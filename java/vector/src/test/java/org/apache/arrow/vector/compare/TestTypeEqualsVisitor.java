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

package org.apache.arrow.vector.compare;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTypeEqualsVisitor {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  private static final Charset utf8Charset = Charset.forName("UTF-8");
  private static final byte[] STR1 = "AAAAA1".getBytes(utf8Charset);
  private static final byte[] STR2 = "BBBBBBBBB2".getBytes(utf8Charset);
  private static final byte[] STR3 = "CCCC3".getBytes(utf8Charset);

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testTypeEqualsWithName() {
    try (final IntVector right = new IntVector("int", allocator);
         final IntVector left1 = new IntVector("int", allocator);
        final IntVector left2 = new IntVector("int2", allocator)) {

      TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);
      assertTrue(visitor.equals(left1));
      assertFalse(visitor.equals(left2));
    }
  }

  @Test
  public void testTypeEqualsWithMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    FieldType typeWithoutMeta = new FieldType(true, new ArrowType.Int(32, true),
        null, null);
    FieldType typeWithMeta = new FieldType(true, new ArrowType.Int(32, true),
        null, metadata);

    try (IntVector right = (IntVector) typeWithoutMeta.createNewSingleVector("int", allocator, null);
         IntVector left1 = (IntVector) typeWithoutMeta.createNewSingleVector("int", allocator, null);
         IntVector left2 = (IntVector) typeWithMeta.createNewSingleVector("int", allocator, null)) {

      TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);
      assertTrue(visitor.equals(left1));
      assertFalse(visitor.equals(left2));
    }
  }

  @Test
  public void testListTypeEquals() {
    try (final ListVector right = ListVector.empty("list", allocator);
         final ListVector left1 = ListVector.empty("list", allocator);
         final ListVector left2 = ListVector.empty("list", allocator)) {

      right.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
      left1.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
      left2.addOrGetVector(FieldType.nullable(new ArrowType.FixedSizeBinary(2)));

      TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);
      assertTrue(visitor.equals(left1));
      assertFalse(visitor.equals(left2));
    }
  }

  @Test
  public void testStructTypeEquals() {
    try (final StructVector right = StructVector.empty("struct", allocator);
         final StructVector left1 = StructVector.empty("struct", allocator);
         final StructVector left2 = StructVector.empty("struct", allocator)) {

      right.addOrGet("child", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      left1.addOrGet("child", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      left2.addOrGet("child2", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);

      TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);
      assertTrue(visitor.equals(left1));
      assertFalse(visitor.equals(left2));
    }
  }

  @Test
  public void testUnionTypeEquals() {
    try (final UnionVector right = new UnionVector("union", allocator, /* field type */ null, /* call-back */ null);
         final UnionVector left1 = new UnionVector("union", allocator, /* field type */ null, /* call-back */ null);
         final UnionVector left2 = new UnionVector("union", allocator, /* field type */ null, /* call-back */ null)) {

      right.addVector(new IntVector("int", allocator));
      left1.addVector(new IntVector("int", allocator));
      left2.addVector(new BigIntVector("bigint", allocator));

      TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);
      assertTrue(visitor.equals(left1));
      assertFalse(visitor.equals(left2));
    }
  }

  @Test
  public void testDenseUnionTypeEquals() {
    try (DenseUnionVector vector1 = new DenseUnionVector("vector1", allocator, null, null);
         DenseUnionVector vector2 = new DenseUnionVector("vector2", allocator, null, null)) {
      vector1.allocateNew();
      vector2.allocateNew();

      // set children for vector1
      byte intTypeId = vector1.registerNewTypeId(Field.nullable("int", Types.MinorType.INT.getType()));
      byte longTypeId = vector1.registerNewTypeId(Field.nullable("long", Types.MinorType.BIGINT.getType()));
      byte floatTypeId = vector1.registerNewTypeId(Field.nullable("float", Types.MinorType.FLOAT4.getType()));
      byte doubleTypeId = vector1.registerNewTypeId(Field.nullable("double", Types.MinorType.FLOAT8.getType()));

      vector1.addVector(floatTypeId, new Float4Vector("", allocator));
      vector1.addVector(longTypeId, new BigIntVector("", allocator));
      vector1.addVector(intTypeId, new IntVector("", allocator));
      vector1.addVector(doubleTypeId, new Float8Vector("", allocator));

      // set children for vector2
      intTypeId = vector2.registerNewTypeId(Field.nullable("int", Types.MinorType.INT.getType()));
      longTypeId = vector2.registerNewTypeId(Field.nullable("long", Types.MinorType.BIGINT.getType()));
      floatTypeId = vector2.registerNewTypeId(Field.nullable("float", Types.MinorType.FLOAT4.getType()));
      doubleTypeId = vector2.registerNewTypeId(Field.nullable("double", Types.MinorType.FLOAT8.getType()));

      // add vectors in a different order
      vector2.addVector(intTypeId, new IntVector("", allocator));
      vector2.addVector(floatTypeId, new Float4Vector("", allocator));
      vector2.addVector(doubleTypeId, new Float8Vector("", allocator));
      vector2.addVector(longTypeId, new BigIntVector("", allocator));

      // compare ranges
      TypeEqualsVisitor typeVisitor =
          new TypeEqualsVisitor(vector2, /* check name */ false, /* check meta data */ true);
      assertTrue(typeVisitor.equals(vector1));

      // if we check names, the types should be different
      typeVisitor =
          new TypeEqualsVisitor(vector2, /* check name */ true, /* check meta data */ true);
      assertFalse(typeVisitor.equals(vector1));
    }
  }
}
