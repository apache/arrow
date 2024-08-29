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

import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Native implementation of {@link DatasetFactory}.
 */
public class NativeDatasetFactory implements DatasetFactory {
  private final long datasetFactoryId;
  private final NativeMemoryPool memoryPool;
  private final BufferAllocator allocator;

  private boolean closed = false;

  /**
   * Constructor.
   *
   * @param allocator a context allocator associated with this factory. Any buffer that will be created natively will
   *                  be then bound to this allocator.
   * @param memoryPool the native memory pool associated with this factory. Any buffer created natively should request
   *                   for memory spaces from this memory pool. This is a mapped instance of c++ arrow::MemoryPool.
   * @param datasetFactoryId an ID, at the same time the native pointer of the underlying native instance of this
   *                         factory. Make sure in c++ side  the pointer is pointing to the shared pointer wrapping
   *                         the actual instance so we could successfully decrease the reference count once
   *                         {@link #close} is called.
   * @see #close()
   */
  public NativeDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, long datasetFactoryId) {
    this.allocator = allocator;
    this.memoryPool = memoryPool;
    this.datasetFactoryId = datasetFactoryId;
  }

  @Override
  public Schema inspect() {
    final byte[] buffer;
    synchronized (this) {
      if (closed) {
        throw new NativeInstanceReleasedException();
      }
      buffer = JniWrapper.get().inspectSchema(datasetFactoryId);
    }
    try {
      return SchemaUtility.deserialize(buffer, allocator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public NativeDataset finish() {
    return finish(inspect());
  }

  @Override
  public NativeDataset finish(Schema schema) {
    try {
      byte[] serialized = SchemaUtility.serialize(schema);
      synchronized (this) {
        if (closed) {
          throw new NativeInstanceReleasedException();
        }
        return new NativeDataset(new NativeContext(allocator, memoryPool),
            JniWrapper.get().createDataset(datasetFactoryId, serialized));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Close this factory by release the pointer of the native instance.
   */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    JniWrapper.get().closeDatasetFactory(datasetFactoryId);
  }
}
