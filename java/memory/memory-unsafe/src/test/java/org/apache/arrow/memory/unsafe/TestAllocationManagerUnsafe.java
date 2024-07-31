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
package org.apache.arrow.memory.unsafe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.junit.jupiter.api.Test;

/** Test cases for {@link AllocationManager}. */
public class TestAllocationManagerUnsafe {

  @Test
  public void testAllocationManagerType() {

    // test unsafe allocation manager type
    System.setProperty(
        DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, "Unsafe");
    DefaultAllocationManagerOption.AllocationManagerType mgrType =
        DefaultAllocationManagerOption.getDefaultAllocationManagerType();

    assertEquals(DefaultAllocationManagerOption.AllocationManagerType.Unsafe, mgrType);
  }
}
