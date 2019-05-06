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

package org.apache.arrow.memory;

import org.junit.Assert;
import org.junit.Test;

public class TestBoundaryChecking {

  @Test
  public void testDefaultValue() {
    BoundsChecking.evaluate();
    Assert.assertTrue(BoundsChecking.BOUNDS_CHECKING_ENABLED);
  }

  @Test
  public void testEnableOldProperty() {
    String savedOldProperty = System.getProperty("drill.enable_unsafe_memory_access");

    System.setProperty("drill.enable_unsafe_memory_access", "true");
    BoundsChecking.evaluate();

    Assert.assertFalse(BoundsChecking.BOUNDS_CHECKING_ENABLED);

    // restore system property
    if (savedOldProperty != null) {
      System.setProperty("drill.enable_unsafe_memory_access", savedOldProperty);
    } else {
      System.clearProperty("drill.enable_unsafe_memory_access");
    }
  }

  @Test
  public void testEnableNewProperty() {
    String savedNewProperty = System.getProperty("arrow.enable_unsafe_memory_access");

    System.setProperty("arrow.enable_unsafe_memory_access", "true");
    BoundsChecking.evaluate();

    Assert.assertFalse(BoundsChecking.BOUNDS_CHECKING_ENABLED);

    // restore system property
    if (savedNewProperty != null) {
      System.setProperty("arrow.enable_unsafe_memory_access", savedNewProperty);
    } else {
      System.clearProperty("arrow.enable_unsafe_memory_access");
    }
  }

  @Test
  public void testEnableBothProperties() {
    String savedOldProperty = System.getProperty("drill.enable_unsafe_memory_access");
    String savedNewProperty = System.getProperty("arrow.enable_unsafe_memory_access");

    System.setProperty("drill.enable_unsafe_memory_access", "false");
    System.setProperty("arrow.enable_unsafe_memory_access", "true");

    BoundsChecking.evaluate();

    // new property takes precedence.
    Assert.assertFalse(BoundsChecking.BOUNDS_CHECKING_ENABLED);

    // restore system property
    if (savedOldProperty != null) {
      System.setProperty("drill.enable_unsafe_memory_access", savedOldProperty);
    } else {
      System.clearProperty("drill.enable_unsafe_memory_access");
    }

    if (savedNewProperty != null) {
      System.setProperty("arrow.enable_unsafe_memory_access", savedNewProperty);
    } else {
      System.clearProperty("arrow.enable_unsafe_memory_access");
    }
  }
}
