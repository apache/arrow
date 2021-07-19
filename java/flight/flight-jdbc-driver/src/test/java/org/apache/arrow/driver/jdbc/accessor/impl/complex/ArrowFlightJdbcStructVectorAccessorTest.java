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

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
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

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcStructVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcStructVectorAccessor((StructVector) vector, getCurrentRow);

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcStructVectorAccessor> accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Before
  public void setUp() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("k1", "v1");
    FieldType type = new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata);
    vector = new StructVector("", rootAllocatorTestRule.getRootAllocator(), type, null);
    vector.allocateNew();

    IntVector intVector = vector.addOrGet("int", FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
    Float8Vector float8Vector =
        vector.addOrGet("float8", FieldType.nullable(Types.MinorType.FLOAT8.getType()), Float8Vector.class);

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
  public void testShouldGetObjectClassReturnMapClass() {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcStructVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(Map.class));
  }

  @Test
  public void testShouldGetObjectReturnValidMap() {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcStructVectorAccessor::getObject,
        (accessor, currentRow) -> {
          Map<String, Object> expected = new HashMap<>();
          expected.put("int", 100 * (currentRow + 1));
          expected.put("float8", 100.05 * (currentRow + 1));

          return equalTo(expected);
        });
  }

  @Test
  public void testShouldGetObjectReturnNull() {
    vector.setNull(0);
    vector.setNull(1);
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcStructVectorAccessor::getObject,
        (accessor, currentRow) -> nullValue());
  }
}
