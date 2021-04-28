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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
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

  ScanTask.BatchIterator execute() {
    if (closed) {
      throw new NativeInstanceReleasedException();
    }
    if (!executed.compareAndSet(false, true)) {
      throw new UnsupportedOperationException("NativeScanner cannot be executed more than once. Consider creating " +
          "new scanner instead");
    }
    return new ScanTask.BatchIterator() {
      private ArrowRecordBatch peek = null;

      @Override
      public void close() {
        NativeScanner.this.close();
      }

      @Override
      public boolean hasNext() {
        if (peek != null) {
          return true;
        }
        final byte[] bytes;
        readLock.lock();
        try {
          if (closed) {
            throw new NativeInstanceReleasedException();
          }
          bytes = JniWrapper.get().nextRecordBatch(scannerId);
        } finally {
          readLock.unlock();
        }
        if (bytes == null) {
          return false;
        }
        peek = UnsafeRecordBatchSerializer.deserializeUnsafe(context.getAllocator(), bytes);
        return true;
      }

      @Override
      public ArrowRecordBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        try {
          return peek;
        } finally {
          peek = null;
        }
      }
    };
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
}
