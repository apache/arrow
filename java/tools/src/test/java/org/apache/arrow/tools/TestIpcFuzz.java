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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIpcFuzz {

  static final List<String> WHITE_LIST = new ArrayList<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(TestIpcFuzz.class);

  static {
    // test files that are expected to finish successfully should be included here.
    WHITE_LIST.add("clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5707423356813312");
  }

  private void readIpcFile(File ipcFile) {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    try (ArrowFileReader reader = new ArrowFileReader(new FileInputStream(ipcFile).getChannel(), allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      // validate schema
      ValueVectorUtility.validate(root);

      while (reader.loadNextBatch()) {
        ValueVectorUtility.validateFull(root);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void readIpcStream(File ipcFile) {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    try (ArrowStreamReader reader = new ArrowStreamReader(new FileInputStream(ipcFile).getChannel(), allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      // validate schema
      ValueVectorUtility.validate(root);

      while (reader.loadNextBatch()) {
        ValueVectorUtility.validateFull(root);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private File[] getTestFiles(String testFilePath) {
    int idx = testFilePath.lastIndexOf(File.separator);
    File directory = new File(testFilePath.substring(0, idx));
    String filter = testFilePath.substring(idx + 1);
    FileFilter fileFilter = new WildcardFileFilter(filter);
    return directory.listFiles(fileFilter);
  }

  private void testFuzz(boolean stream, File test) {
    if (stream) {
      readIpcStream(test);
    } else {
      readIpcFile(test);
    }
  }

  private void testFuzzData(boolean stream, String path) {
    File[] testFiles = getTestFiles(path);
    for (File test : testFiles) {
      LOGGER.info("Testing file " + test.getName());
      if (WHITE_LIST.contains(test.getName())) {
        testFuzz(stream, test);
        LOGGER.info("Test finished successfully.");
      } else {
        Exception e = assertThrows(Exception.class,
            () -> testFuzz(stream, test));
        LOGGER.info("Exception thrown: " + e);
      }
    }
  }

  private String getTestDataPath() {
    String tdPath = System. getenv("ARROW_TEST_DATA");
    if (tdPath == null || tdPath.isEmpty()) {
      // suppose the test is running from the project root path
      File tdFile = new File("../../testing/data");
      if (!tdFile.exists()) {
        throw new IllegalStateException("Please specify the test data path through environmental " +
             "variable ARROW_TEST_DATA, or run the test from project root with test data properly installed.");
      }
      tdPath = tdFile.getAbsolutePath();
    }
    return tdPath;
  }

  @Test
  public void testStreamFuzz() {
    final String dataPath = getTestDataPath();
    testFuzzData(true, dataPath + "/arrow-ipc-stream/crash-*");
    testFuzzData(true, dataPath + "/arrow-ipc-stream/*-testcase-*");
  }

  @Test
  public void testFileFuzz() {
    final String dataPath = getTestDataPath();
    testFuzzData(false, dataPath + "/arrow-ipc-file/*-testcase-*");
  }
}
