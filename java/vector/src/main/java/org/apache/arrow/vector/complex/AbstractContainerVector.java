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

package org.apache.arrow.vector.complex;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.DensityAwareVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

/**
 * Base class for composite vectors.
 *
 * <p>This class implements common functionality of composite vectors.
 */
public abstract class AbstractContainerVector implements ValueVector, DensityAwareVector {
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
   *
   * @param name the name of the child to return
   * @return the corresponding FieldVector
   */
  public FieldVector getChild(String name) {
    return getChild(name, FieldVector.class);
  }

  /**
   * Clears out all underlying child vectors.
   */
  @Override
  public void close() {
    for (ValueVector vector : (Iterable<ValueVector>) this) {
      vector.close();
    }
  }

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    if (clazz.isAssignableFrom(v.getClass())) {
      return clazz.cast(v);
    }
    throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s]. Arrow " +
      "doesn't yet support heterogenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
  }

  protected boolean supportsDirectRead() {
    return false;
  }

  // return the number of child vectors
  public abstract int size();

  // add a new vector with the input FieldType or return the existing vector if we already added one with the same name
  public abstract <T extends FieldVector> T addOrGet(String name, FieldType fieldType, Class<T> clazz);

  // return the child vector with the input name
  public abstract <T extends FieldVector> T getChild(String name, Class<T> clazz);

  // return the child vector's ordinal in the composite container
  public abstract VectorWithOrdinal getChildVectorWithOrdinal(String name);

  public StructVector addOrGetStruct(String name) {
    return addOrGet(name, FieldType.nullable(new Struct()), StructVector.class);
  }

  public ListVector addOrGetList(String name) {
    return addOrGet(name, FieldType.nullable(new List()), ListVector.class);
  }

  public UnionVector addOrGetUnion(String name) {
    return addOrGet(name, FieldType.nullable(MinorType.UNION.getType()), UnionVector.class);
  }

  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }
}
