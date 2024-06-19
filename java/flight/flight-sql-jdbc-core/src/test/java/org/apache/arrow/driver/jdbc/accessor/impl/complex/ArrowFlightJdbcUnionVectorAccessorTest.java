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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcUnionVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private UnionVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcUnionVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) ->
              new ArrowFlightJdbcUnionVectorAccessor(
                  (UnionVector) vector, getCurrentRow, (boolean wasNull) -> {});

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcUnionVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  @BeforeEach
  public void setup() {
    this.vector = UnionVector.empty("", rootAllocatorTestExtension.getRootAllocator());
    this.vector.allocateNew();

    NullableBigIntHolder nullableBigIntHolder = new NullableBigIntHolder();
    nullableBigIntHolder.isSet = 1;
    nullableBigIntHolder.value = Long.MAX_VALUE;
    this.vector.setType(0, Types.MinorType.BIGINT);
    this.vector.setSafe(0, nullableBigIntHolder);

    NullableFloat8Holder nullableFloat4Holder = new NullableFloat8Holder();
    nullableFloat4Holder.isSet = 1;
    nullableFloat4Holder.value = Math.PI;
    this.vector.setType(1, Types.MinorType.FLOAT8);
    this.vector.setSafe(1, nullableFloat4Holder);

    NullableTimeStampMilliHolder nullableTimeStampMilliHolder = new NullableTimeStampMilliHolder();
    nullableTimeStampMilliHolder.isSet = 1;
    nullableTimeStampMilliHolder.value = 1625702400000L;
    this.vector.setType(2, Types.MinorType.TIMESTAMPMILLI);
    this.vector.setSafe(2, nullableTimeStampMilliHolder);

    nullableBigIntHolder.isSet = 0;
    this.vector.setType(3, Types.MinorType.BIGINT);
    this.vector.setSafe(3, nullableBigIntHolder);

    this.vector.setValueCount(5);
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void getObject() throws Exception {
    List<Object> result = accessorIterator.toList(vector);
    List<Object> expected =
        Arrays.asList(Long.MAX_VALUE, Math.PI, new Timestamp(1625702400000L), null, null);

    assertThat(result, is(expected));
  }

  @Test
  public void getObjectForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    accessorIterator.assertAccessorGetter(
        vector, AbstractArrowFlightJdbcUnionVectorAccessor::getObject, equalTo(null));
  }
}
