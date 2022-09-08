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

package org.apache.arrow.dataset.jni;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Native implementation of {@link Scanner}. Note that it currently emits only a single scan task of type
 * {@link NativeScanTask}, which is internally a combination of all scan task instances returned by the
 * native scanner.
 */
public class NativeScanner implements Scanner {

  private final AtomicBoolean executed = new AtomicBoolean(false);
  private final NativeContext context;
  private final long scannerId;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock writeLock = lock.writeLock();
  private final Lock readLock = lock.readLock();
  private boolean closed = false;

  public NativeScanner(NativeContext context, long scannerId) {
    this.context = context;
    this.scannerId = scannerId;
  }

  ArrowReader execute() {
    if (closed) {
      throw new NativeInstanceReleasedException();
    }
    if (!executed.compareAndSet(false, true)) {
      throw new UnsupportedOperationException("NativeScanner cannot be executed more than once. Consider creating " +
          "new scanner instead");
    }
    return new NativeReader(context.getAllocator());
  }

  @Override
  public Iterable<? extends NativeScanTask> scan() {
    if (closed) {
      throw new NativeInstanceReleasedException();
    }
    return Collections.singletonList(new NativeScanTask(this));
  }

  @Override
  public Schema schema() {
    readLock.lock();
    try {
      if (closed) {
        throw new NativeInstanceReleasedException();
      }
      return SchemaUtility.deserialize(JniWrapper.get().getSchemaFromScanner(scannerId), context.getAllocator());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void close() {
    writeLock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      JniWrapper.get().closeScanner(scannerId);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * {@link ArrowReader} implementation for NativeDataset.
   */
  public class NativeReader extends ArrowReader {

    private NativeReader(BufferAllocator allocator) {
      super(allocator);
    }

    @Override
    protected void loadRecordBatch(ArrowRecordBatch batch) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void loadDictionary(ArrowDictionaryBatch dictionaryBatch) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      readLock.lock();
      try {
        if (closed) {
          throw new NativeInstanceReleasedException();
        }
        try (ArrowArray arrowArray = ArrowArray.allocateNew(context.getAllocator())) {
          if (!JniWrapper.get().nextRecordBatch(scannerId, arrowArray.memoryAddress())) {
            return false;
          }
          final VectorSchemaRoot vsr = getVectorSchemaRoot();
          Data.importIntoVectorSchemaRoot(context.getAllocator(), arrowArray, vsr, this);
        }
      } finally {
        readLock.unlock();
      }
      return true;
    }

    @Override
    public long bytesRead() {
      return 0L;
    }

    @Override
    protected void closeReadSource() throws IOException {
      // no-op
    }

    @Override
    protected Schema readSchema() throws IOException {
      return schema();
    }
  }
}
