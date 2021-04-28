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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.arrow.dataset.jni.NativeSerializedRecordBatchIterator;
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer;
import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 * Default implementation of {@link NativeScannerAdaptor}.
 */
public class NativeScannerAdaptorImpl implements NativeScannerAdaptor, AutoCloseable {

  private final Scanner scanner;

  /**
   * Constructor.
   *
   * @param scanner the delegated scanner.
   */
  public NativeScannerAdaptorImpl(Scanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public NativeSerializedRecordBatchIterator scan() {
    final Iterable<? extends ScanTask> tasks = scanner.scan();
    return new IteratorImpl(tasks);
  }

  @Override
  public void close() throws Exception {
    scanner.close();
  }

  private static class IteratorImpl implements NativeSerializedRecordBatchIterator {

    private final Iterator<? extends ScanTask> taskIterator;

    private ScanTask currentTask = null;
    private ScanTask.BatchIterator currentBatchIterator = null;

    public IteratorImpl(Iterable<? extends ScanTask> tasks) {
      this.taskIterator = tasks.iterator();
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
      currentBatchIterator.close();
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
      currentBatchIterator = currentTask.execute();
      return true;
    }

    @Override
    public boolean hasNext() {
      if (currentTask == null) {
        if (!advance()) {
          return false;
        }
      }
      if (!currentBatchIterator.hasNext()) {
        if (!advance()) {
          return false;
        }
      }
      return currentBatchIterator.hasNext();
    }

    @Override
    public byte[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return serialize(currentBatchIterator.next());
    }

    private byte[] serialize(ArrowRecordBatch batch) {
      return UnsafeRecordBatchSerializer.serializeUnsafe(batch);
    }
  }
}
