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

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
//import static org.junit.Assert.*;

public class TestRunEndEncodedVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  /**
   * Create REE vector representing: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5].
   */
  @Test
  public void testBasicOperation() {
    try (RunEndEncodedVector reeVector = RunEndEncodedVector.empty("ree", allocator)) {
      int valueCount = 5;
      reeVector.allocateNew();
      reeVector.setInitialCapacity(valueCount);
      for (int i = 1; i <= valueCount ; i++) {
        //reeVector.set(i, i);
      }
      reeVector.setValueCount(valueCount);
      System.out.println("Created reeVector: " + reeVector);
    }
  }

  /**
   * Create REE vector representing: [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5]
   */
  //  @Test
  //  public void testBasicOperation() {
  //    int count = 5;
  //    try (RunEndEncodedVector reeVector = RunEndEncodedVector.empty("ree", allocator, false)) {
  //      reeVector.allocateNew();
  //      UnionRunEndEncodedWriter reeWriter = reeVector.getWriter();
  //      for (int i = 0; i < count; i++) {
  //        reeWriter.setPosition(i);
  //        reeWriter.run_length().integer().writeInt(1 + i);
  //        reeWriter.value().integer().writeInt(1 + i);
  //      }
  //      reeWriter.setValueCount(count);
  //      UnionRunEndEncodedReader reeReader = reeVector.getReader();
  //      for (int i = 0; i < count; i++) {
  //        reeReader.setPosition(i);
  //        assertEquals(i, reeReader.run_length().readInteger().intValue());
  //        assertEquals(i, reeReader.value().readInteger().intValue());
  //      }
  //    }
  //  }
}
