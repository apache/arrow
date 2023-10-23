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

package org.apache.arrow.vector.complex;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class TestDenseUnionBufferSize {
  @Test
  public void testBufferSize() {
    try (BufferAllocator allocator = new RootAllocator();
         DenseUnionVector duv = new DenseUnionVector("duv", allocator,
                 FieldType.nullable(new ArrowType.Union(UnionMode.Dense, null)), null)) {

      byte aTypeId = 42;
      byte bTypeId = 7;

      duv.addVector(aTypeId, new IntVector("a", FieldType.notNullable(new ArrowType.Int(32, true)), allocator));
      duv.addVector(bTypeId, new VarBinaryVector("b", FieldType.notNullable(new ArrowType.Binary()), allocator));

      NullableIntHolder intHolder = new NullableIntHolder();
      NullableVarBinaryHolder varBinaryHolder = new NullableVarBinaryHolder();

      int aCount = BaseValueVector.INITIAL_VALUE_ALLOCATION + 1;
      for (int i = 0; i < aCount; i++) {
        duv.setTypeId(i, aTypeId);
        duv.setSafe(i, intHolder);
      }

      int bCount = 2;
      for (int i = 0; i < bCount; i++) {
        duv.setTypeId(i + aCount, bTypeId);
        duv.setSafe(i + aCount, varBinaryHolder);
      }

      int count = aCount + bCount;
      duv.setValueCount(count);

      // will not necessarily see an error unless bounds checking is on.
      assertDoesNotThrow(duv::getBufferSize);

      IntVector intVector = duv.getIntVector(aTypeId);
      VarBinaryVector varBinaryVector = duv.getVarBinaryVector(bTypeId);

      long overhead = DenseUnionVector.TYPE_WIDTH + DenseUnionVector.OFFSET_WIDTH;

      assertEquals(overhead * count + intVector.getBufferSize() + varBinaryVector.getBufferSize(),
              duv.getBufferSize());

      assertEquals(overhead * (aCount + 1) + intVector.getBufferSizeFor(aCount) + varBinaryVector.getBufferSizeFor(1),
              duv.getBufferSizeFor(aCount + 1));

    }
  }
}
