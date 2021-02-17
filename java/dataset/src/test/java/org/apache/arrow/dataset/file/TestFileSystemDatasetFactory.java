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

package org.apache.arrow.dataset.file;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

public class TestFileSystemDatasetFactory {

  @Test
  public void testErrorHandling() {
    RuntimeException e = assertThrows(RuntimeException.class, () -> {
      new FileSystemDatasetFactory(new RootAllocator(Long.MAX_VALUE), NativeMemoryPool.getDefault(),
          FileFormat.NONE, "file:///NON_EXIST_FILE");
    });
    assertEquals("illegal file format id: -1", e.getMessage());
  }

  @Test
  public void testCloseAgain() {
    assertDoesNotThrow(() -> {
      FileSystemDatasetFactory factory = new FileSystemDatasetFactory(new RootAllocator(Long.MAX_VALUE),
          NativeMemoryPool.getDefault(), FileFormat.PARQUET, "file:///NON_EXIST_FILE");
      factory.close();
      factory.close();
    });
  }
}
