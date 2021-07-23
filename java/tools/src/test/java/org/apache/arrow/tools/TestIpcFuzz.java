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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIpcFuzz {

  static final List<String> WHITE_LIST = new ArrayList<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(TestIpcFuzz.class);

  static {
    WHITE_LIST.add("clusterfuzz-testcase-minimized-arrow-ipc-file-fuzz-5707423356813312");
  }

  static void readIpcFile(File ipcFile) {
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

  static void readIpcStream(File ipcFile) {
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

  static File[] getTestFiles(String testFilePath) {
    int idx = testFilePath.lastIndexOf(File.separator);
    File directory = new File(testFilePath.substring(0, idx));
    String filter = testFilePath.substring(idx + 1);
    FileFilter fileFilter = new WildcardFileFilter(filter);
    return directory.listFiles(fileFilter);
  }

  static void testFuzz(boolean stream, File test) {
    if (stream) {
      readIpcStream(test);
    } else {
      readIpcFile(test);
    }
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      LOGGER.error("Usage: <cmd> [stream|file] <test file path>");
      System.exit(1);
    }

    final boolean stream;
    if (args[0].equalsIgnoreCase("file")) {
      stream = false;
    } else if (args[0].equalsIgnoreCase("stream")) {
      stream = true;
    } else {
      throw new IllegalArgumentException("The first argument must be file or stream");
    }

    File[] testFiles = getTestFiles(args[1]);
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
}
