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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.MapWithOrdinal;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

/*
 * Base class for MapVectors. Currently used by RepeatedMapVector and MapVector
 */
public abstract class AbstractMapVector extends AbstractContainerVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);

  // Maintains a map with key as field name and value is the vector itself
  private final MapWithOrdinal<String, FieldVector> vectors =  new MapWithOrdinal<>();

  protected AbstractMapVector(String name, BufferAllocator allocator, CallBack callBack) {
    super(name, allocator, callBack);
  }

  @Override
  public void close() {
    for(final ValueVector valueVector : vectors.values()) {
      valueVector.close();
    }
    vectors.clear();

    super.close();
  }

  @Override
  public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      for (final ValueVector v : vectors.values()) {
        if (!v.allocateNewSafe()) {
          return false;
        }
      }
      success = true;
    } finally {
      if (!success) {
        clear();
      }
    }
    return true;
  }

  /**
   * Adds a new field with the given parameters or replaces the existing one and consequently returns the resultant
   * {@link org.apache.arrow.vector.ValueVector}.
   *
   * Execution takes place in the following order:
   * <ul>
   *   <li>
   *     if field is new, create and insert a new vector of desired type.
   *   </li>
   *   <li>
   *     if field exists and existing vector is of desired vector type, return the vector.
   *   </li>
   *   <li>
   *     if field exists and null filled, clear the existing vector; create and insert a new vector of desired type.
   *   </li>
   *   <li>
   *     otherwise, throw an {@link java.lang.IllegalStateException}
   *   </li>
   * </ul>
   *
   * @param name the name of the field
   * @param minorType the minorType for the vector
   * @param clazz class of expected vector type
   * @param <T> class type of expected vector type
   * @throws java.lang.IllegalStateException raised if there is a hard schema change
   *
   * @return resultant {@link org.apache.arrow.vector.ValueVector}
   */
  @Override
  public <T extends FieldVector> T addOrGet(String name, MinorType minorType, Class<T> clazz, int... precisionScale) {
    final ValueVector existing = getChild(name);
    boolean create = false;
    if (existing == null) {
      create = true;
    } else if (clazz.isAssignableFrom(existing.getClass())) {
      return clazz.cast(existing);
    } else if (nullFilled(existing)) {
      existing.clear();
      create = true;
    }
    if (create) {
      final T vector = clazz.cast(minorType.getNewVector(name, allocator, callBack, precisionScale));
      putChild(name, vector);
      if (callBack!=null) {
        callBack.doWork();
      }
      return vector;
    }
    final String message = "Arrow does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), clazz.getSimpleName()));
  }

  private boolean nullFilled(ValueVector vector) {
    for (int r = 0; r < vector.getAccessor().getValueCount(); r++) {
      if (!vector.getAccessor().isNull(r)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a {@link org.apache.arrow.vector.ValueVector} corresponding to the given ordinal identifier.
   */
  public ValueVector getChildByOrdinal(int id) {
    return vectors.getByOrdinal(id);
  }

  /**
   * Returns a {@link org.apache.arrow.vector.ValueVector} instance of subtype of <T> corresponding to the given
   * field name if exists or null.
   */
  @Override
  public <T extends FieldVector> T getChild(String name, Class<T> clazz) {
    final ValueVector v = vectors.get(name);
    if (v == null) {
      return null;
    }
    return typeify(v, clazz);
  }

  protected ValueVector add(String name, MinorType minorType, int... precisionScale) {
    final ValueVector existing = getChild(name);
    if (existing != null) {
      throw new IllegalStateException(String.format("Vector already exists: Existing[%s], Requested[%s] ", existing.getClass().getSimpleName(), minorType));
    }
    FieldVector vector = minorType.getNewVector(name, allocator, callBack, precisionScale);
    putChild(name, vector);
    if (callBack!=null) {
      callBack.doWork();
    }
    return vector;
  }

  /**
   * Inserts the vector with the given name if it does not exist else replaces it with the new value.
   *
   * Note that this method does not enforce any vector type check nor throws a schema change exception.
   */
  protected void putChild(String name, FieldVector vector) {
    putVector(name, vector);
  }

  /**
   * Inserts the input vector into the map if it does not exist, replaces if it exists already
   * @param name  field name
   * @param vector  vector to be inserted
   */
  protected void putVector(String name, FieldVector vector) {
    final ValueVector old = vectors.put(
        Preconditions.checkNotNull(name, "field name cannot be null"),
        Preconditions.checkNotNull(vector, "vector cannot be null")
    );
    if (old != null && old != vector) {
      logger.debug("Field [{}] mutated from [{}] to [{}]", name, old.getClass().getSimpleName(),
                   vector.getClass().getSimpleName());
    }
  }

  /**
   * Returns a sequence of underlying child vectors.
   */
  protected List<FieldVector> getChildren() {
    int size = vectors.size();
    List<FieldVector> children = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      children.add(vectors.getByOrdinal(i));
    }
    return children;
  }

  protected List<String> getChildFieldNames() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (ValueVector child : getChildren()) {
      builder.add(child.getField().getName());
    }
    return builder.build();
  }

  /**
   * Returns the number of underlying child vectors.
   */
  @Override
  public int size() {
    return vectors.size();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>unmodifiableCollection(vectors.values()).iterator();
  }

  /**
   * Returns a list of scalar child vectors recursing the entire vector hierarchy.
   */
  public List<ValueVector> getPrimitiveVectors() {
    final List<ValueVector> primitiveVectors = Lists.newArrayList();
    for (final ValueVector v : vectors.values()) {
      if (v instanceof AbstractMapVector) {
        AbstractMapVector mapVector = (AbstractMapVector) v;
        primitiveVectors.addAll(mapVector.getPrimitiveVectors());
      } else {
        primitiveVectors.add(v);
      }
    }
    return primitiveVectors;
  }

  /**
   * Returns a vector with its corresponding ordinal mapping if field exists or null.
   */
  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    final int ordinal = vectors.getOrdinal(name);
    if (ordinal < 0) {
      return null;
    }
    final ValueVector vector = vectors.getByOrdinal(ordinal);
    return new VectorWithOrdinal(vector, ordinal);
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final List<ArrowBuf> buffers = Lists.newArrayList();

    for (final ValueVector vector : vectors.values()) {
      for (final ArrowBuf buf : vector.getBuffers(false)) {
        buffers.add(buf);
        if (clear) {
          buf.retain(1);
        }
      }
      if (clear) {
        vector.clear();
      }
    }

    return buffers.toArray(new ArrowBuf[buffers.size()]);
  }

  @Override
  public int getBufferSize() {
    int actualBufSize = 0 ;

    for (final ValueVector v : vectors.values()) {
      for (final ArrowBuf buf : v.getBuffers(false)) {
        actualBufSize += buf.writerIndex();
      }
    }
    return actualBufSize;
  }
}
