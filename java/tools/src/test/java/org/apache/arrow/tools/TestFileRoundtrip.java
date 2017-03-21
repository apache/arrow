/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.arrow.tools;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.arrow.tools.ArrowFileTestFixtures.validateOutput;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeInput;
import static org.junit.Assert.assertEquals;

public class TestFileRoundtrip {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void test() throws Exception {
    File testInFile = testFolder.newFile("testIn.arrow");
    File testOutFile = testFolder.newFile("testOut.arrow");

    writeInput(testInFile, allocator);

    String[] args = {"-i", testInFile.getAbsolutePath(), "-o", testOutFile.getAbsolutePath()};
    int result = new FileRoundtrip(System.out, System.err).run(args);
    assertEquals(0, result);

    validateOutput(testOutFile, allocator);
  }

}
