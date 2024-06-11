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
package org.apache.arrow.tools;

import static org.apache.arrow.tools.ArrowFileTestFixtures.validateOutput;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeInput;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFileRoundtrip {

  @TempDir public File testFolder;

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void test() throws Exception {
    File testInFile = new File(testFolder, "testIn.arrow");
    File testOutFile = new File(testFolder, "testOut.arrow");

    writeInput(testInFile, allocator);

    if (!testOutFile.exists()) {
      if (!testOutFile.createNewFile()) {
        throw new IOException("Failed to create file: " + testOutFile);
      }
    }

    String[] args = {"-i", testInFile.getAbsolutePath(), "-o", testOutFile.getAbsolutePath()};
    int result = new FileRoundtrip(System.err).run(args);
    assertEquals(0, result);

    validateOutput(testOutFile, allocator);
  }

  @Test
  public void testDiffFolder() throws Exception {
    File testInFile = testFolder.newFile("testIn.arrow");
    File testOutFile = testAnotherFolder.newFile("testOut.arrow");

    writeInput(testInFile, allocator);

    String[] args = {"-i", testInFile.getAbsolutePath(), "-o", testOutFile.getAbsolutePath()};
    int result = new FileRoundtrip(System.err).run(args);
    assertEquals(0, result);

    validateOutput(testOutFile, allocator);
  }

  @Test
  public void testNotPreparedInput() throws Exception {
    File testInFile = testFolder.newFile("testIn.arrow");
    File testOutFile = testFolder.newFile("testOut.arrow");

    String[] args = {"-i", testInFile.getAbsolutePath(), "-o", testOutFile.getAbsolutePath()};

    // In JUnit 5, since the file itself is not created, the exception and message will be
    // different.
    Exception exception =
        assertThrows(
            InvalidArrowFileException.class,
            () -> {
              new FileRoundtrip(System.err).run(args);
            });
    assertEquals("file too small: 0", exception.getMessage());
  }
}
