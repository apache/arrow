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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import io.netty.buffer.ArrowBuf;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.TransferPair;

public class MapVector extends AbstractMapVector {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapVector.class);

  public static MapVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    return new MapVector(name, allocator, fieldType, null);
  }

  private final SingleMapReaderImpl reader = new SingleMapReaderImpl(this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  protected final FieldType fieldType;
  public int valueCount;

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public MapVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null, null), callBack);
  }

  public MapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, callBack);
    this.fieldType = checkNotNull(fieldType);
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  transient private MapTransferPair ephPair;

  public void copyFromSafe(int fromIndex, int thisIndex, MapVector from) {
    if (ephPair == null || ephPair.from != from) {
      ephPair = (MapTransferPair) from.makeTransferPair(this);
    }
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  protected boolean supportsDirectRead() {
    return true;
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      v.setInitialCapacity(numRecords);
    }
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0 || size() == 0) {
      return 0;
    }
    long buffer = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      buffer += v.getBufferSize();
    }

    return (int) buffer;
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new MapTransferPair(this, new MapVector(name, allocator, fieldType, callBack), false);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new MapTransferPair(this, (MapVector) to);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new MapTransferPair(this, new MapVector(ref, allocator, fieldType, callBack), false);
  }

  protected static class MapTransferPair implements TransferPair {
    private final TransferPair[] pairs;
    private final MapVector from;
    private final MapVector to;

    public MapTransferPair(MapVector from, MapVector to) {
      this(from, to, true);
    }

    protected MapTransferPair(MapVector from, MapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      this.to.ephPair = null;

      int i = 0;
      FieldVector vector;
      for (String child : from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        //DRILL-1872: we add the child fields for the vector, looking up the field by name. For a map vector,
        // the child fields may be nested fields of the top level child. For example if the structure
        // of a child field is oa.oab.oabc then we add oa, then add oab to oa then oabc to oab.
        // But the children member of a Materialized field is a HashSet. If the fields are added in the
        // children HashSet, and the hashCode of the Materialized field includes the hash code of the
        // children, the hashCode value of oa changes *after* the field has been added to the HashSet.
        // (This is similar to what happens in ScanBatch where the children cannot be added till they are
        // read). To take care of this, we ensure that the hashCode of the MaterializedField does not
        // include the hashCode of the children but is based only on MaterializedField$key.
        final FieldVector newVector = to.addOrGet(child, vector.getField().getFieldType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    @Override
    public void transfer() {
      for (final TransferPair p : pairs) {
        p.transfer();
      }
      to.valueCount = from.valueCount;
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(from, to);
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.getMutator().setValueCount(length);
    }
  }

  @Override
  public int getValueCapacity() {
    if (size() == 0) {
      return 0;
    }

    final Ordering<ValueVector> natural = new Ordering<ValueVector>() {
      @Override
      public int compare(@Nullable ValueVector left, @Nullable ValueVector right) {
        return Ints.compare(
            checkNotNull(left).getValueCapacity(),
            checkNotNull(right).getValueCapacity()
        );
      }
    };

    return natural.min(getChildren()).getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class Accessor extends BaseValueVector.BaseAccessor {

    @Override
    public Object getObject(int index) {
      Map<String, Object> vv = new JsonStringHashMap<>();
      for (String child : getChildFieldNames()) {
        ValueVector v = getChild(child);
        if (v instanceof  NullableVarCharVector || v instanceof  NullableIntVector) {
          if (v != null && index < v.getValueCount()) {
            Object value = v.getObject(index);
            if (value != null) {
              vv.put(child, value);
            }
          }
        } else {
          if (v != null && index < v.getAccessor().getValueCount()) {
            Object value = v.getAccessor().getObject(index);
            if (value != null) {
              vv.put(child, value);
            }
          }
        }
      }
      return vv;
    }

    public void get(int index, ComplexHolder holder) {
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

  public ValueVector getVectorById(int id) {
    return getChildByOrdinal(id);
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    @Override
    public void setValueCount(int valueCount) {
      for (final ValueVector v : getChildren()) {
        if (v instanceof NullableIntVector || v instanceof NullableVarCharVector) {
          v.setValueCount(valueCount);
        } else {
          v.getMutator().setValueCount(valueCount);
        }
      }
      MapVector.this.valueCount = valueCount;
    }

    @Override
    public void reset() {
    }

    @Override
    public void generateTestData(int values) {
    }
  }

  @Override
  public void clear() {
    for (final ValueVector v : getChildren()) {
      v.clear();
    }
    valueCount = 0;
  }

  @Override
  public Field getField() {
    List<Field> children = new ArrayList<>();
    for (ValueVector child : getChildren()) {
      children.add(child.getField());
    }
    return new Field(name, fieldType, children);
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.MAP;
  }

  @Override
  public void close() {
    final Collection<FieldVector> vectors = getChildren();
    for (final FieldVector v : vectors) {
      v.close();
    }
    vectors.clear();

    valueCount = 0;

    super.close();
  }

  public void initializeChildrenFromFields(List<Field> children) {
    for (Field field : children) {
      FieldVector vector = (FieldVector) this.add(field.getName(), field.getFieldType());
      vector.initializeChildrenFromFields(field.getChildren());
    }
  }

  public List<FieldVector> getChildrenFromFields() {
    return getChildren();
  }

  public int getValueCount() { return 0; }

  public void setValueCount(int valueCount) { }

  public Object getObject(int index) { return null; }

}
