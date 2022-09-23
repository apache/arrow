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

import java.io.IOException;
import java.util.Iterator;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.jni.CRecordBatchIterator;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public class CRecordBatchIteratorImpl implements CRecordBatchIterator {

  private final Scanner scanner;
  private final BufferAllocator allocator;

  private Iterator<? extends ScanTask> taskIterators;
  private ArrowReader currentReader = null;

  public CRecordBatchIteratorImpl(Scanner scanner,
                                       BufferAllocator allocator) {
    this.scanner = scanner;
    this.allocator = allocator;
    this.taskIterators = scanner.scan().iterator();
  }

  @Override
  public void close() throws Exception {
    scanner.close();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (taskIterators.hasNext()) {
      return true;
    } else {
      return false;
    }
  }

  public void next(long cStreamPointer) throws IOException {
    currentReader = taskIterators.next().execute();
    try (final ArrowArrayStream stream = ArrowArrayStream.wrap(cStreamPointer)) {

      Data.exportArrayStream(allocator, currentReader, stream);
    } finally {
      currentReader.close();
    }
  }
}
