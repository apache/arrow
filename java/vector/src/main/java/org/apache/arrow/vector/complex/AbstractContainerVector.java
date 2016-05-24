/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex;

import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.CallBack;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Base class for composite vectors.
 *
 * This class implements common functionality of composite vectors.
 */
public abstract class AbstractContainerVector implements ValueVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);

  protected final String name;
  protected final BufferAllocator allocator;
  protected final CallBack callBack;

  protected AbstractContainerVector(String name, BufferAllocator allocator, CallBack callBack) {
    this.name = name;
    this.allocator = allocator;
    this.callBack = callBack;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException();
    }
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Returns a {@link org.apache.arrow.vector.ValueVector} corresponding to the given field name if exists or null.
   */
  public ValueVector getChild(String name) {
    return getChild(name, ValueVector.class);
  }

  /**
   * Clears out all underlying child vectors.
   */
 @Override
  public void close() {
    for (ValueVector vector:(Iterable<ValueVector>)this) {
      vector.close();
    }
  }

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    if (clazz.isAssignableFrom(v.getClass())) {
      return (T) v;
    }
    throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s]. Arrow doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
  }

  protected boolean supportsDirectRead() {
    return false;
  }

  // return the number of child vectors
  public abstract int size();

  // add a new vector with the input MajorType or return the existing vector if we already added one with the same type
  public abstract <T extends ValueVector> T addOrGet(String name, MinorType minorType, Class<T> clazz, int... precisionScale);

  // return the child vector with the input name
  public abstract <T extends ValueVector> T getChild(String name, Class<T> clazz);

  // return the child vector's ordinal in the composite container
  public abstract VectorWithOrdinal getChildVectorWithOrdinal(String name);
}