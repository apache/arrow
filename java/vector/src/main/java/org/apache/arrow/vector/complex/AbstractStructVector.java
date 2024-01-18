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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.PromotableMultiMapWithOrdinal;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * Base class for StructVectors. Currently used by NonNullableStructVector
 */
public abstract class AbstractStructVector extends AbstractContainerVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);
  private static final String STRUCT_CONFLICT_POLICY_ENV = "ARROW_STRUCT_CONFLICT_POLICY";
  private static final String STRUCT_CONFLICT_POLICY_JVM = "arrow.struct.conflict.policy";
  private static final ConflictPolicy DEFAULT_CONFLICT_POLICY;
  // Maintains a map with key as field name and value is the vector itself
  private final PromotableMultiMapWithOrdinal<String, FieldVector> vectors;
  protected final boolean allowConflictPolicyChanges;
  private ConflictPolicy conflictPolicy;


  static {
    String conflictPolicyStr = System.getProperty(STRUCT_CONFLICT_POLICY_JVM,
        ConflictPolicy.CONFLICT_REPLACE.toString());
    if (conflictPolicyStr == null) {
      conflictPolicyStr = System.getenv(STRUCT_CONFLICT_POLICY_ENV);
    }
    ConflictPolicy conflictPolicy;
    try {
      conflictPolicy = ConflictPolicy.valueOf(conflictPolicyStr.toUpperCase(Locale.ROOT));
    } catch (Exception e) {
      conflictPolicy = ConflictPolicy.CONFLICT_REPLACE;
    }
    DEFAULT_CONFLICT_POLICY = conflictPolicy;
  }

  /**
   * Policy to determine how to react when duplicate columns are encountered.
   */
  public enum ConflictPolicy {
    // Ignore the conflict and append the field. This is the default behaviour
    CONFLICT_APPEND,
    // Keep the existing field and ignore the newer one.
    CONFLICT_IGNORE,
    // Replace the existing field with the newer one.
    CONFLICT_REPLACE,
    // Refuse the new field and error out.
    CONFLICT_ERROR
  }

  /**
   *  Base constructor that sets default conflict policy to APPEND.
   */
  protected AbstractStructVector(String name,
                                 BufferAllocator allocator,
                                 CallBack callBack,
                                 ConflictPolicy conflictPolicy,
                                 boolean allowConflictPolicyChanges) {
    super(name, allocator, callBack);
    this.conflictPolicy = conflictPolicy == null ? DEFAULT_CONFLICT_POLICY : conflictPolicy;
    this.vectors = new PromotableMultiMapWithOrdinal<>(allowConflictPolicyChanges, this.conflictPolicy);
    this.allowConflictPolicyChanges = allowConflictPolicyChanges;
  }

  /**
   * Set conflict policy and return last conflict policy state.
   */
  public ConflictPolicy setConflictPolicy(ConflictPolicy conflictPolicy) {
    ConflictPolicy tmp = this.conflictPolicy;
    this.conflictPolicy = conflictPolicy;
    this.vectors.setConflictPolicy(conflictPolicy);
    return tmp;
  }

  public ConflictPolicy getConflictPolicy() {
    return conflictPolicy;
  }

  @Override
  public void close() {
    for (final ValueVector valueVector : vectors.values()) {
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

  @Override
  public void reAlloc() {
    for (final ValueVector v : vectors.values()) {
      v.reAlloc();
    }
  }

  /**
   * Adds a new field with the given parameters or replaces the existing one and consequently returns the resultant
   * {@link org.apache.arrow.vector.ValueVector}.
   *
   * <p>Execution takes place in the following order:
   * <ul>
   * <li>
   * if field is new, create and insert a new vector of desired type.
   * </li>
   * <li>
   * if field exists and existing vector is of desired vector type, return the vector.
   * </li>
   * <li>
   * if field exists and null filled, clear the existing vector; create and insert a new vector of desired type.
   * </li>
   * <li>
   * otherwise, throw an {@link java.lang.IllegalStateException}
   * </li>
   * </ul>
   *
   * @param childName the name of the field
   * @param fieldType the type for the vector
   * @param clazz     class of expected vector type
   * @param <T>       class type of expected vector type
   * @return resultant {@link org.apache.arrow.vector.ValueVector}
   * @throws java.lang.IllegalStateException raised if there is a hard schema change
   */
  @Override
  public <T extends FieldVector> T addOrGet(String childName, FieldType fieldType, Class<T> clazz) {
    final ValueVector existing = getChild(childName);
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
      final T vector = clazz.cast(fieldType.createNewSingleVector(childName, allocator, callBack));
      putChild(childName, vector);
      if (callBack != null) {
        callBack.doWork();
      }
      return vector;
    }
    final String message = "Arrow does not support schema change yet. Existing[%s] and desired[%s] vector types " +
        "mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), clazz.getSimpleName()));
  }

  private boolean nullFilled(ValueVector vector) {
    return BitVectorHelper.checkAllBitsEqualTo(vector.getValidityBuffer(), vector.getValueCount(), false);
  }

  /**
   * Returns a {@link org.apache.arrow.vector.ValueVector} corresponding to the given ordinal identifier.
   *
   * @param id the ordinal of the child to return
   * @return the corresponding child
   */
  public ValueVector getChildByOrdinal(int id) {
    return vectors.getByOrdinal(id);
  }

  /**
   * Returns a {@link org.apache.arrow.vector.ValueVector} instance of subtype of T corresponding to the given
   * field name if exists or null.
   *
   * If there is more than one element for name this will return the first inserted.
   *
   * @param name  the name of the child to return
   * @param clazz the expected type of the child
   * @return the child corresponding to this name
   */
  @Override
  public <T extends FieldVector> T getChild(String name, Class<T> clazz) {
    final FieldVector f = vectors.get(name);
    if (f == null) {
      return null;
    }
    return typeify(f, clazz);
  }

  protected ValueVector add(String childName, FieldType fieldType) {
    FieldVector vector = fieldType.createNewSingleVector(childName, allocator, callBack);
    putChild(childName, vector);
    if (callBack != null) {
      callBack.doWork();
    }
    return vector;
  }

  /**
   * Inserts the vector with the given name if it does not exist else replaces it with the new value.
   *
   * <p>Note that this method does not enforce any vector type check nor throws a schema change exception.
   *
   * @param name   the name of the child to add
   * @param vector the vector to add as a child
   */
  protected void putChild(String name, FieldVector vector) {
    putVector(name, vector);
  }

  private void put(String name, FieldVector vector, boolean overwrite) {
    final boolean old = vectors.put(
        Preconditions.checkNotNull(name, "field name cannot be null"),
        Preconditions.checkNotNull(vector, "vector cannot be null"),
        overwrite
    );
    if (old) {
      logger.debug("Field [{}] mutated to [{}] ", name,
          vector.getClass().getSimpleName());
    }
  }

  /**
   * Inserts the input vector into the map if it does not exist.
   *
   * <p>
   * If the field name already exists the conflict is handled according to the currently set ConflictPolicy
   * </p>
   *
   * @param name   field name
   * @param vector vector to be inserted
   */
  protected void putVector(String name, FieldVector vector) {
    switch (conflictPolicy) {
      case CONFLICT_APPEND:
        put(name, vector, false);
        break;
      case CONFLICT_IGNORE:
        if (!vectors.containsKey(name)) {
          put(name, vector, false);
        }
        break;
      case CONFLICT_REPLACE:
        if (vectors.containsKey(name)) {
          vectors.removeAll(name);
        }
        put(name, vector, true);
        break;
      case CONFLICT_ERROR:
        if (vectors.containsKey(name)) {
          throw new IllegalStateException(String.format("Vector already exists: Existing[%s], Requested[%s] ",
            vector.getClass().getSimpleName(), vector.getField().getFieldType()));
        }
        put(name, vector, false);
        break;
      default:
        throw new IllegalStateException(String.format("%s type not a valid conflict state", conflictPolicy));
    }

  }

  /**
   * Get child vectors.
   * @return a sequence of underlying child vectors.
   */
  protected List<FieldVector> getChildren() {
    int size = vectors.size();
    List<FieldVector> children = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      children.add(vectors.getByOrdinal(i));
    }
    return children;
  }

  /**
   * Get child field names.
   */
  public List<String> getChildFieldNames() {
    return getChildren().stream()
        .map(child -> child.getField().getName())
        .collect(Collectors.toList());
  }

  /**
   * Get the number of child vectors.
   * @return the number of underlying child vectors.
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
   * Get primitive child vectors.
   * @return a list of scalar child vectors recursing the entire vector hierarchy.
   */
  public List<ValueVector> getPrimitiveVectors() {
    final List<ValueVector> primitiveVectors = new ArrayList<>();
    for (final FieldVector v : vectors.values()) {
      primitiveVectors.addAll(getPrimitiveVectors(v));
    }
    return primitiveVectors;
  }

  private List<ValueVector> getPrimitiveVectors(FieldVector v) {
    final List<ValueVector> primitives = new ArrayList<>();
    if (v instanceof AbstractStructVector) {
      AbstractStructVector structVector = (AbstractStructVector) v;
      primitives.addAll(structVector.getPrimitiveVectors());
    } else if (v instanceof ListVector) {
      ListVector listVector = (ListVector) v;
      primitives.addAll(getPrimitiveVectors(listVector.getDataVector()));
    } else if (v instanceof FixedSizeListVector) {
      FixedSizeListVector listVector = (FixedSizeListVector) v;
      primitives.addAll(getPrimitiveVectors(listVector.getDataVector()));
    } else if (v instanceof UnionVector) {
      UnionVector unionVector = (UnionVector) v;
      for (final FieldVector vector : unionVector.getChildrenFromFields()) {
        primitives.addAll(getPrimitiveVectors(vector));
      }
    } else {
      primitives.add(v);
    }
    return primitives;
  }

  /**
   * Get a child vector by name. If duplicate names this returns the first inserted.
   * @param name the name of the child to return
   * @return a vector with its corresponding ordinal mapping if field exists or null.
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
    final List<ArrowBuf> buffers = new ArrayList<>();

    for (final ValueVector vector : vectors.values()) {
      for (final ArrowBuf buf : vector.getBuffers(false)) {
        buffers.add(buf);
        if (clear) {
          buf.getReferenceManager().retain(1);
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
    int actualBufSize = 0;

    for (final ValueVector v : vectors.values()) {
      for (final ArrowBuf buf : v.getBuffers(false)) {
        actualBufSize += (int) buf.writerIndex();
      }
    }
    return actualBufSize;
  }

  @Override
  public String toString() {
    return ValueVectorUtility.getToString(this, 0 , getValueCount());
  }

}
