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

import org.apache.arrow.dataset.jni.JniLoader;

/**
 * JniWrapper for filesystem based {@link org.apache.arrow.dataset.source.Dataset} implementations.
 */
public class JniWrapper {

  private static final JniWrapper INSTANCE = new JniWrapper();

  public static JniWrapper get() {
    JniLoader.get().ensureLoaded();
    return INSTANCE;
  }

  private JniWrapper() {
  }

  /**
   * Create FileSystemDatasetFactory and return its native pointer. The pointer is pointing to a
   * intermediate shared_ptr of the factory instance.
   *
   * @param uri file uri to read, either a file or a directory
   * @param fileFormat file format ID
   * @return the native pointer of the arrow::dataset::FileSystemDatasetFactory instance.
   * @see FileFormat
   */
  public native long makeFileSystemDatasetFactory(String uri, int fileFormat);

  /**
   * Create FileSystemDatasetFactory and return its native pointer. The pointer is pointing to a
   * intermediate shared_ptr of the factory instance.
   *
   * @param uris List of file uris to read, each path pointing to an individual file
   * @param fileFormat file format ID
   * @return the native pointer of the arrow::dataset::FileSystemDatasetFactory instance.
   * @see FileFormat
   */
  public native long makeFileSystemDatasetFactory(String[] uris, int fileFormat);

  /**
   * Write the content in a {@link org.apache.arrow.c.ArrowArrayStream} into files. This internally
   * depends on C++ write API: FileSystemDataset::Write.
   *
   * @param streamAddress the ArrowArrayStream address
   * @param fileFormat target file format (ID)
   * @param uri target file uri
   * @param partitionColumns columns used to partition output files
   * @param maxPartitions maximum partitions to be included in written files
   * @param baseNameTemplate file name template used to make partitions. E.g. "dat_{i}", i is current partition
   *                         ID around all written files.
   */
  public native void writeFromScannerToFile(long streamAddress,
                                            long fileFormat,
                                            String uri,
                                            String[] partitionColumns,
                                            int maxPartitions,
                                            String baseNameTemplate);

}
