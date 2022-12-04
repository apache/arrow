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
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * JNI-based utility to write datasets into files. It internally depends on C++ static method
 * FileSystemDataset::Write.
 */
public class DatasetFileWriter {

  /**
   * Write the contents of an ArrowReader as a dataset.
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
    try (final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, stream);
      JniWrapper.get().writeFromScannerToFile(stream.memoryAddress(),
          format.id(), uri, partitionColumns, maxPartitions, baseNameTemplate);
    }
  }

  /**
   * Write the contents of an ArrowReader as a dataset, with default partitioning settings.
   *
   * @param reader the datasource for writing
   * @param format target file format
   * @param uri target file uri
   */
  public static void write(BufferAllocator allocator, ArrowReader reader, FileFormat format, String uri) {
    write(allocator, reader, format, uri, new String[0], 1024, "data_{i}");
  }
}
