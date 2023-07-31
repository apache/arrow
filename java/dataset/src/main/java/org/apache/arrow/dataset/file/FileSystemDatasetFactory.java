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

import org.apache.arrow.dataset.jni.NativeDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Java binding of the C++ FileSystemDatasetFactory.
 */
public class FileSystemDatasetFactory extends NativeDatasetFactory {

  public FileSystemDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat format,
      String uri) {
    super(allocator, memoryPool, createNative(format, uri));
  }

  public FileSystemDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat format,
                                  String[] uris) {
    super(allocator, memoryPool, createNative(format, uris));
  }

  private static long createNative(FileFormat format, String uri) {
    return JniWrapper.get().makeFileSystemDatasetFactory(uri, format.id());
  }

  private static long createNative(FileFormat format, String[] uris) {
    return JniWrapper.get().makeFileSystemDatasetFactory(uris, format.id());
  }

}
