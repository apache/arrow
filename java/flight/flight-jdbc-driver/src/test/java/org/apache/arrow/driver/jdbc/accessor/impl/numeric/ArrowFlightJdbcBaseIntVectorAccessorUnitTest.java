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

package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import static org.hamcrest.CoreMatchers.equalTo;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArrowFlightJdbcBaseIntVectorAccessorUnitTest {

  private static UInt8Vector int8Vector;
  private static TinyIntVector tinyIntVector;
  private static SmallIntVector smallIntVector;
  private static IntVector intVector;
  private static BigIntVector bigIntVector;

  @ClassRule
  public static RootAllocatorTestRule rule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setup() {
    int8Vector = new UInt8Vector("ID", rule.getRootAllocator());
    int8Vector.setSafe(0, 0xFFFFFFFFFFFFFFFFL);
    int8Vector.setValueCount(1);

    tinyIntVector = new TinyIntVector("ID", rule.getRootAllocator());
    tinyIntVector.setSafe(0, 0xAA);
    tinyIntVector.setValueCount(1);

    smallIntVector = new SmallIntVector("ID", rule.getRootAllocator());
    smallIntVector.setSafe(0, 0xAABB);
    smallIntVector.setValueCount(1);

    intVector = new IntVector("ID", rule.getRootAllocator());
    intVector.setSafe(0, 0xAABBCCDD);
    intVector.setValueCount(1);

    bigIntVector = new BigIntVector("ID", rule.getRootAllocator());
    bigIntVector.setSafe(0, 0xAABBCCDDEEFFAABBL);
    bigIntVector.setValueCount(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    bigIntVector.close();
    intVector.close();
    smallIntVector.close();
    tinyIntVector.close();
    int8Vector.close();
    rule.close();
  }

  @Test
  public void testShouldGetStringFromUnsignedValue() throws Exception {
    AccessorTestUtils
        .iterateOnAccessor(int8Vector, ((vector1, getCurrentRow) -> new ArrowFlightJdbcBaseIntVectorAccessor(int8Vector,
              getCurrentRow)), ((accessor, currentRow) -> {
            collector.checkThat(accessor.getString(), equalTo("18446744073709551615"));
          })
    );
  }

  @Test
  public void testShouldGetBytesFromIntVector() throws Exception {
    byte[] value = new byte[] {(byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd};

    AccessorTestUtils
        .iterateOnAccessor(intVector, ((vector1, getCurrentRow) -> new ArrowFlightJdbcBaseIntVectorAccessor(intVector,
            getCurrentRow)), ((accessor, currentRow) -> {
          collector.checkThat(accessor.getBytes(), CoreMatchers.is(value));
          })
    );
  }

  @Test
  public void testShouldGetBytesFromSmallVector() throws Exception {
    byte[] value = new byte[] {(byte) 0xaa, (byte) 0xbb};

    AccessorTestUtils.iterateOnAccessor(smallIntVector, ((vector1, getCurrentRow) ->
        new ArrowFlightJdbcBaseIntVectorAccessor(smallIntVector,
            getCurrentRow)), ((accessor, currentRow) -> {
          collector.checkThat(accessor.getBytes(), CoreMatchers.is(value));
        })
    );
  }

  @Test
  public void testShouldGetBytesFromTinyIntVector() throws Exception {
    byte[] value = new byte[] {(byte) 0xaa};

    AccessorTestUtils.iterateOnAccessor(tinyIntVector,
        ((vector1, getCurrentRow) -> new ArrowFlightJdbcBaseIntVectorAccessor(tinyIntVector,
            getCurrentRow)), ((accessor, currentRow) -> {
          collector.checkThat(accessor.getBytes(), CoreMatchers.is(value));
        })
    );
  }

  @Test
  public void testShouldGetBytesFromBigIntVector() throws Exception {
    byte[] value =
        new byte[] {(byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff, (byte) 0xaa,
            (byte) 0xbb};

    AccessorTestUtils.iterateOnAccessor(bigIntVector,
        ((vector1, getCurrentRow) -> new ArrowFlightJdbcBaseIntVectorAccessor(bigIntVector,
            getCurrentRow)), ((accessor, currentRow) -> {
          collector.checkThat(accessor.getBytes(), CoreMatchers.is(value));
        })
    );
  }
}
