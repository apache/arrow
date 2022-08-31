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

package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcStructVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private StructVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcStructVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> new ArrowFlightJdbcStructVectorAccessor((StructVector) vector,
              getCurrentRow, (boolean wasNull) -> {
          });

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcStructVectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Before
  public void setUp() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata);
    vector = new StructVector("", rootAllocatorTestRule.getRootAllocator(), type, null);
    vector.allocateNew();

    IntVector intVector =
        vector.addOrGet("int", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
    Float8Vector float8Vector =
        vector.addOrGet("float8", FieldType.nullable(Types.MinorType.FLOAT8.getType()),
            Float8Vector.class);

    intVector.setSafe(0, 100);
    float8Vector.setSafe(0, 100.05);
    vector.setIndexDefined(0);
    intVector.setSafe(1, 200);
    float8Vector.setSafe(1, 200.1);
    vector.setIndexDefined(1);

    vector.setValueCount(2);
  }

  @After
  public void tearDown() throws Exception {
    vector.close();
  }

  @Test
  public void testShouldGetObjectClassReturnMapClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector,
        ArrowFlightJdbcStructVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(Map.class));
  }

  @Test
  public void testShouldGetObjectReturnValidMap() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcStructVectorAccessor::getObject,
        (accessor, currentRow) -> {
          Map<String, Object> expected = new HashMap<>();
          expected.put("int", 100 * (currentRow + 1));
          expected.put("float8", 100.05 * (currentRow + 1));

          return equalTo(expected);
        });
  }

  @Test
  public void testShouldGetObjectReturnNull() throws Exception {
    vector.setNull(0);
    vector.setNull(1);
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcStructVectorAccessor::getObject,
        (accessor, currentRow) -> nullValue());
  }

  @Test
  public void testShouldGetStructReturnValidStruct() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      Struct struct = accessor.getStruct();
      assert struct != null;

      Object[] expected = new Object[] {
          100 * (currentRow + 1),
          100.05 * (currentRow + 1)
      };

      collector.checkThat(struct.getAttributes(), equalTo(expected));
    });
  }

  @Test
  public void testShouldGetStructReturnNull() throws Exception {
    vector.setNull(0);
    vector.setNull(1);
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcStructVectorAccessor::getStruct,
        (accessor, currentRow) -> nullValue());
  }

  @Test
  public void testShouldGetObjectWorkWithNestedComplexData() throws SQLException {
    try (StructVector rootVector = StructVector.empty("",
        rootAllocatorTestRule.getRootAllocator())) {
      StructVector structVector = rootVector.addOrGetStruct("struct");

      FieldType intFieldType = FieldType.nullable(Types.MinorType.INT.getType());
      IntVector intVector = structVector.addOrGet("int", intFieldType, IntVector.class);
      FieldType float8FieldType = FieldType.nullable(Types.MinorType.FLOAT8.getType());
      Float8Vector float8Vector =
          structVector.addOrGet("float8", float8FieldType, Float8Vector.class);

      ListVector listVector = rootVector.addOrGetList("list");
      UnionListWriter listWriter = listVector.getWriter();
      listWriter.allocate();

      UnionVector unionVector = rootVector.addOrGetUnion("union");

      intVector.setSafe(0, 100);
      intVector.setValueCount(1);
      float8Vector.setSafe(0, 100.05);
      float8Vector.setValueCount(1);
      structVector.setIndexDefined(0);

      listWriter.setPosition(0);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(Long.MAX_VALUE);
      listWriter.bigInt().writeBigInt(Long.MIN_VALUE);
      listWriter.endList();
      listVector.setValueCount(1);

      unionVector.setType(0, Types.MinorType.BIT);
      NullableBitHolder holder = new NullableBitHolder();
      holder.isSet = 1;
      holder.value = 1;
      unionVector.setSafe(0, holder);
      unionVector.setValueCount(1);

      rootVector.setIndexDefined(0);
      rootVector.setValueCount(1);

      Map<String, Object> expected = new JsonStringHashMap<>();
      Map<String, Object> nestedStruct = new JsonStringHashMap<>();
      nestedStruct.put("int", 100);
      nestedStruct.put("float8", 100.05);
      expected.put("struct", nestedStruct);
      JsonStringArrayList<Object> nestedList = new JsonStringArrayList<>();
      nestedList.add(Long.MAX_VALUE);
      nestedList.add(Long.MIN_VALUE);
      expected.put("list", nestedList);
      expected.put("union", true);

      ArrowFlightJdbcStructVectorAccessor accessor =
          new ArrowFlightJdbcStructVectorAccessor(rootVector, () -> 0, (boolean wasNull) -> {
          });

      Assert.assertEquals(accessor.getObject(), expected);
      Assert.assertEquals(accessor.getString(), expected.toString());
    }
  }
}
