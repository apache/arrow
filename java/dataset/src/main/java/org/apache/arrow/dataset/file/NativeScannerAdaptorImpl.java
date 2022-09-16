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
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.jni.NativeRecordBatchIterator;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Default implementation of {@link NativeScannerAdaptor}.
 */
public class NativeScannerAdaptorImpl implements NativeScannerAdaptor, AutoCloseable {

  private final Scanner scanner;
  private final BufferAllocator allocator;

  /**
   * Constructor.
   *
   * @param scanner the delegated scanner.
   */
  public NativeScannerAdaptorImpl(Scanner scanner, BufferAllocator allocator) {
    this.scanner = scanner;
    this.allocator = allocator;
  }

  @Override
  public NativeRecordBatchIterator scan() {
    final Iterable<? extends ScanTask> tasks = scanner.scan();
    return new IteratorImpl(tasks, allocator);
  }

  @Override
  public void close() throws Exception {
    scanner.close();
  }

  private static class IteratorImpl implements NativeRecordBatchIterator {

    private final Iterator<? extends ScanTask> taskIterator;

    private ScanTask currentTask = null;
    private ArrowReader reader = null;

    private BufferAllocator allocator = null;

    public IteratorImpl(Iterable<? extends ScanTask> tasks,
                        BufferAllocator allocator) {
      this.taskIterator = tasks.iterator();
      this.allocator = allocator;
    }

    @Override
    public void close() throws Exception {
      closeCurrent();
    }

    private void closeCurrent() throws Exception {
      if (currentTask == null) {
        return;
      }
      currentTask.close();
      reader.close();
    }

    private boolean advance() {
      if (!taskIterator.hasNext()) {
        return false;
      }
      try {
        closeCurrent();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      currentTask = taskIterator.next();
      reader = currentTask.execute();
      return true;
    }
    @Override
    public boolean hasNext() {

      if (currentTask == null) {
        if (!advance()) {
          return false;
        }
      }
      try {
        if (!reader.loadNextBatch()) {
          if (!advance()) {
            return false;
          }
        }
        return true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

   byte[] longtoBytes(long data) {
     return new byte[]{
         (byte) ((data >> 0) & 0xff),
         (byte) ((data >> 8) & 0xff),
         (byte) ((data >> 16) & 0xff),
         (byte) ((data >> 24) & 0xff),
         (byte) ((data >> 32) & 0xff),
         (byte) ((data >> 40) & 0xff),
         (byte) ((data >> 48) & 0xff),
         (byte) ((data >> 56) & 0xff),
     };
    }

    @Override
    public byte[] next() {
      ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
      try {
        Data.exportVectorSchemaRoot(allocator, reader.getVectorSchemaRoot(), reader, arrowArray);
        return longtoBytes(arrowArray.memoryAddress());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
