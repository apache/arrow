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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.scanner.ArrowScannerReader;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.SchemaUtility;

import java.util.Iterator;

/**
 * JNI-based utility to write datasets into files. It internally depends on C++ static method
 * FileSystemDataset::Write.
 */
public class DatasetFileWriter {

  /**
   * Scan over an input {@link Scanner} then write all record batches to file.
   *
   * @param reader the datasource for writing
   * @param format target file format
   * @param uri target file uri
   * @param maxPartitions maximum partitions to be included in written files
   * @param partitionColumns columns used to partition output files. Empty to disable partitioning
   * @param baseNameTemplate file name template used to make partitions. E.g. "dat_{i}", i is current partition
   *                         ID around all written files.
   */
  public static void write(BufferAllocator allocator, ArrowReader reader, FileFormat format, String uri,
                           String[] partitionColumns, int maxPartitions, String baseNameTemplate) {
    RuntimeException throwableWrapper = null;
    try {
      ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
      Data.exportArrayStream(allocator, reader, stream);
      JniWrapper.get().writeFromScannerToFile(stream.memoryAddress(),
          format.id(), uri, partitionColumns, maxPartitions, baseNameTemplate);
      stream.close();
    } catch (Throwable t) {
      throwableWrapper = new RuntimeException(t);
      throw throwableWrapper;
    }
  }

  /**
   * Scan over an input {@link Scanner} then write all record batches to file, with default partitioning settings.
   *
   * @param reader the datasource for writing
   * @param format target file format
   * @param uri target file uri
   */
  public static void write(BufferAllocator allocator, ArrowReader reader, FileFormat format, String uri) {
    write(allocator, reader, format, uri, new String[0], 1024, "dat_{i}");
  }
}
