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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Test;

public class TestOpens {
  /** Instantiating the RootAllocator should poke MemoryUtil and fail. */
  @Test
  public void testMemoryUtilFailsLoudly() {
    // This test is configured by Maven to run WITHOUT add-opens. So this should fail on JDK16+
    // (where JEP396 means that add-opens is required to access JDK internals).
    // The test will likely fail in your IDE if it doesn't correctly pick this up.
    Throwable e = assertThrows(Throwable.class, () -> {
      BufferAllocator allocator = new RootAllocator();
      allocator.close();
    });
    boolean found = false;
    while (e != null) {
      e = e.getCause();
      if (e instanceof RuntimeException && e.getMessage().contains("Failed to initialize MemoryUtil")) {
        found = true;
        break;
      }
    }
    assertTrue(found, "Expected exception as not thrown");
  }
}
