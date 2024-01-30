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

package org.apache.arrow.memory.netty;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.junit.Test;

/**
 * Test cases for {@link AllocationManager}.
 */
public class TestAllocationManagerNetty {

  @Test
  public void testAllocationManagerType() {
    // test netty allocation manager type
    System.setProperty(
        DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, "Netty");
    DefaultAllocationManagerOption.AllocationManagerType mgrType =
        DefaultAllocationManagerOption.getDefaultAllocationManagerType();

    assertEquals(DefaultAllocationManagerOption.AllocationManagerType.Netty, mgrType);
  }
}
