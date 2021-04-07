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

import org.apache.arrow.dataset.jni.NativeSerializedRecordBatchIterator;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * JNI-based utility to write datasets into files. It internally depends on C++ static method
 * FileSystemDataset::Write.
 */
public class DatasetFileWriter {

  /**
   * Scan over an input {@link Scanner} then write all record batches to file.
   *
   * @param scanner the source scanner for writing
   * @param format target file format
   * @param uri target file uri
   * @param maxPartitions maximum partitions to be included in written files
   * @param partitionColumns columns used to partition output files. Empty to disable partitioning
   * @param baseNameTemplate file name template used to make partitions. E.g. "dat_{i}", i is current partition
   *                         ID around all written files.
   */
  public static void write(Scanner scanner, FileFormat format, String uri,
      String[] partitionColumns, int maxPartitions, String baseNameTemplate) {
    final NativeScannerAdaptorImpl adaptor = new NativeScannerAdaptorImpl(scanner);
    final NativeSerializedRecordBatchIterator itr = adaptor.scan();
    RuntimeException throwableWrapper = null;
    try {
      JniWrapper.get().writeFromScannerToFile(itr, SchemaUtility.serialize(scanner.schema()),
              format.id(), uri, partitionColumns, maxPartitions, baseNameTemplate);
    } catch (Throwable t) {
      throwableWrapper = new RuntimeException(t);
      throw throwableWrapper;
    } finally {
      try {
        AutoCloseables.close(itr);
      } catch (Exception e) {
        if (throwableWrapper != null) {
          throwableWrapper.addSuppressed(e);
        }
      }
    }
  }

  /**
   * Scan over an input {@link Scanner} then write all record batches to file, with default partitioning settings.
   *
   * @param scanner the source scanner for writing
   * @param format target file format
   * @param uri target file uri
   */
  public static void write(Scanner scanner, FileFormat format, String uri) {
    write(scanner, format, uri, new String[0], 1024, "dat_{i}");
  }
}
