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

import io.netty.buffer.ArrowBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorDescriptor;
import org.apache.arrow.vector.complex.impl.NullReader;
import org.apache.arrow.vector.complex.impl.RepeatedMapReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.holders.RepeatedMapHolder;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RepeatedMapVector extends AbstractMapVector
    implements RepeatedValueVector, RepeatedFixedWidthVectorLike {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedMapVector.class);

  public final static MajorType TYPE = new MajorType(MinorType.MAP, DataMode.REPEATED);

  final UInt4Vector offsets;   // offsets to start of each record (considering record indices are 0-indexed)
  private final RepeatedMapReaderImpl reader = new RepeatedMapReaderImpl(RepeatedMapVector.this);
  private final RepeatedMapAccessor accessor = new RepeatedMapAccessor();
  private final Mutator mutator = new Mutator();
  private final EmptyValuePopulator emptyPopulator;

  public RepeatedMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack){
    super(field, allocator, callBack);
    this.offsets = new UInt4Vector(BaseRepeatedValueVector.OFFSETS_FIELD, allocator);
    this.emptyPopulator = new EmptyValuePopulator(offsets);
  }

  @Override
  public UInt4Vector getOffsetVector() {
    return offsets;
  }

  @Override
  public ValueVector getDataVector() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsets.setInitialCapacity(numRecords + 1);
    for(final ValueVector v : (Iterable<ValueVector>) this) {
      v.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
    }
  }

  @Override
  public RepeatedMapReaderImpl getReader() {
    return reader;
  }

  @Override
  public void allocateNew(int groupCount, int innerValueCount) {
    clear();
    try {
      offsets.allocateNew(groupCount + 1);
      for (ValueVector v : getChildren()) {
        AllocationHelper.allocatePrecomputedChildCount(v, groupCount, 50, innerValueCount);
      }
    } catch (OutOfMemoryException e){
      clear();
      throw e;
    }
    offsets.zeroVector();
    mutator.reset();
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public List<ValueVector> getPrimitiveVectors() {
    final List<ValueVector> primitiveVectors = super.getPrimitiveVectors();
    primitiveVectors.add(offsets);
    return primitiveVectors;
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    long bufferSize = offsets.getBufferSize();
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSize();
    }
    return (int) bufferSize;
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
  public void close() {
    offsets.close();
    super.close();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new RepeatedMapTransferPair(this, getField().getPath(), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new RepeatedMapTransferPair(this, (RepeatedMapVector)to);
  }

  MapSingleCopier makeSingularCopier(MapVector to) {
    return new MapSingleCopier(this, to);
  }

  protected static class MapSingleCopier {
    private final TransferPair[] pairs;
    public final RepeatedMapVector from;

    public MapSingleCopier(RepeatedMapVector from, MapVector to) {
      this.from = from;
      this.pairs = new TransferPair[from.size()];

      int i = 0;
      ValueVector vector;
      for (final String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    public void copySafe(int fromSubIndex, int toIndex) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(fromSubIndex, toIndex);
      }
    }
  }

  public TransferPair getTransferPairToSingleMap(String reference, BufferAllocator allocator) {
    return new SingleMapTransferPair(this, reference, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new RepeatedMapTransferPair(this, ref, allocator);
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
      if (!offsets.allocateNewSafe()) {
        return false;
      }
      success =  super.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    offsets.zeroVector();
    return success;
  }

  protected static class SingleMapTransferPair implements TransferPair {
    private final TransferPair[] pairs;
    private final RepeatedMapVector from;
    private final MapVector to;
    private static final MajorType MAP_TYPE = new MajorType(MinorType.MAP, DataMode.REQUIRED);

    public SingleMapTransferPair(RepeatedMapVector from, String path, BufferAllocator allocator) {
      this(from, new MapVector(MaterializedField.create(path, MAP_TYPE), allocator, from.callBack), false);
    }

    public SingleMapTransferPair(RepeatedMapVector from, MapVector to) {
      this(from, to, true);
    }

    public SingleMapTransferPair(RepeatedMapVector from, MapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      int i = 0;
      ValueVector vector;
      for (final String child : from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }


    @Override
    public void transfer() {
      for (TransferPair p : pairs) {
        p.transfer();
      }
      to.getMutator().setValueCount(from.getAccessor().getValueCount());
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

  private static class RepeatedMapTransferPair implements TransferPair{

    private final TransferPair[] pairs;
    private final RepeatedMapVector to;
    private final RepeatedMapVector from;

    public RepeatedMapTransferPair(RepeatedMapVector from, String path, BufferAllocator allocator) {
      this(from, new RepeatedMapVector(MaterializedField.create(path, TYPE), allocator, from.callBack), false);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to) {
      this(from, to, true);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      this.to.ephPair = null;

      int i = 0;
      ValueVector vector;
      for (final String child : from.getChildFieldNames()) {
        final int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }

        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (to.size() != preSize) {
          newVector.allocateNew();
        }

        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    @Override
    public void transfer() {
      from.offsets.transferTo(to.offsets);
      for (TransferPair p : pairs) {
        p.transfer();
      }
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int srcIndex, int destIndex) {
      RepeatedMapHolder holder = new RepeatedMapHolder();
      from.getAccessor().get(srcIndex, holder);
      to.emptyPopulator.populate(destIndex + 1);
      int newIndex = to.offsets.getAccessor().get(destIndex);
      //todo: make these bulk copies
      for (int i = holder.start; i < holder.end; i++, newIndex++) {
        for (TransferPair p : pairs) {
          p.copyValueSafe(i, newIndex);
        }
      }
      to.offsets.getMutator().setSafe(destIndex + 1, newIndex);
    }

    @Override
    public void splitAndTransfer(final int groupStart, final int groups) {
      final UInt4Vector.Accessor a = from.offsets.getAccessor();
      final UInt4Vector.Mutator m = to.offsets.getMutator();

      final int startPos = a.get(groupStart);
      final int endPos = a.get(groupStart + groups);
      final int valuesToCopy = endPos - startPos;

      to.offsets.clear();
      to.offsets.allocateNew(groups + 1);

      int normalizedPos;
      for (int i = 0; i < groups + 1; i++) {
        normalizedPos = a.get(groupStart + i) - startPos;
        m.set(i, normalizedPos);
      }

      m.setValueCount(groups + 1);
      to.emptyPopulator.populate(groups);

      for (final TransferPair p : pairs) {
        p.splitAndTransfer(startPos, valuesToCopy);
      }
    }
  }


  transient private RepeatedMapTransferPair ephPair;

  public void copyFromSafe(int fromIndex, int thisIndex, RepeatedMapVector from) {
    if (ephPair == null || ephPair.from != from) {
      ephPair = (RepeatedMapTransferPair) from.makeTransferPair(this);
    }
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public int getValueCapacity() {
    return Math.max(offsets.getValueCapacity() - 1, 0);
  }

  @Override
  public RepeatedMapAccessor getAccessor() {
    return accessor;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final int expectedBufferSize = getBufferSize();
    final int actualBufferSize = super.getBufferSize();

    Preconditions.checkArgument(expectedBufferSize == actualBufferSize + offsets.getBufferSize());
    return ArrayUtils.addAll(offsets.getBuffers(clear), super.getBuffers(clear));
  }


//  @Override
//  public void load(SerializedField metadata, DrillBuf buffer) {
//    final List<SerializedField> children = metadata.getChildList();
//
//    final SerializedField offsetField = children.get(0);
//    offsets.load(offsetField, buffer);
//    int bufOffset = offsetField.getBufferLength();
//
//    for (int i = 1; i < children.size(); i++) {
//      final SerializedField child = children.get(i);
//      final MaterializedField fieldDef = SerializedFieldHelper.create(child);
//      ValueVector vector = getChild(fieldDef.getLastName());
//      if (vector == null) {
        // if we arrive here, we didn't have a matching vector.
//        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
//        putChild(fieldDef.getLastName(), vector);
//      }
//      final int vectorLength = child.getBufferLength();
//      vector.load(child, buffer.slice(bufOffset, vectorLength));
//      bufOffset += vectorLength;
//    }
//
//    assert bufOffset == buffer.capacity();
//  }
//
//
//  @Override
//  public SerializedField getMetadata() {
//    SerializedField.Builder builder = getField() //
//        .getAsBuilder() //
//        .setBufferLength(getBufferSize()) //
        // while we don't need to actually read this on load, we need it to make sure we don't skip deserialization of this vector
//        .setValueCount(accessor.getValueCount());
//    builder.addChild(offsets.getMetadata());
//    for (final ValueVector child : getChildren()) {
//      builder.addChild(child.getMetadata());
//    }
//    return builder.build();
//  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class RepeatedMapAccessor implements RepeatedAccessor {
    @Override
    public Object getObject(int index) {
      final List<Object> list = new JsonStringArrayList<>();
      final int end = offsets.getAccessor().get(index+1);
      String fieldName;
      for (int i =  offsets.getAccessor().get(index); i < end; i++) {
        final Map<String, Object> vv = Maps.newLinkedHashMap();
        for (final MaterializedField field : getField().getChildren()) {
          if (!field.equals(BaseRepeatedValueVector.OFFSETS_FIELD)) {
            fieldName = field.getLastName();
            final Object value = getChild(fieldName).getAccessor().getObject(i);
            if (value != null) {
              vv.put(fieldName, value);
            }
          }
        }
        list.add(vv);
      }
      return list;
    }

    @Override
    public int getValueCount() {
      return Math.max(offsets.getAccessor().getValueCount() - 1, 0);
    }

    @Override
    public int getInnerValueCount() {
      final int valueCount = getValueCount();
      if (valueCount == 0) {
        return 0;
      }
      return offsets.getAccessor().get(valueCount);
    }

    @Override
    public int getInnerValueCountAt(int index) {
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }

    @Override
    public boolean isEmpty(int index) {
      return false;
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }

    public void get(int index, RepeatedMapHolder holder) {
      assert index < getValueCapacity() :
        String.format("Attempted to access index %d when value capacity is %d",
            index, getValueCapacity());
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      holder.start = offsetsAccessor.get(index);
      holder.end = offsetsAccessor.get(index + 1);
    }

    public void get(int index, ComplexHolder holder) {
      final FieldReader reader = getReader();
      reader.setPosition(index);
      holder.reader = reader;
    }

    public void get(int index, int arrayIndex, ComplexHolder holder) {
      final RepeatedMapHolder h = new RepeatedMapHolder();
      get(index, h);
      final int offset = h.start + arrayIndex;

      if (offset >= h.end) {
        holder.reader = NullReader.INSTANCE;
      } else {
        reader.setSinglePosition(index, arrayIndex);
        holder.reader = reader;
      }
    }
  }

  public class Mutator implements RepeatedMutator {
    @Override
    public void startNewValue(int index) {
      emptyPopulator.populate(index + 1);
      offsets.getMutator().setSafe(index + 1, offsets.getAccessor().get(index));
    }

    @Override
    public void setValueCount(int topLevelValueCount) {
      emptyPopulator.populate(topLevelValueCount);
      offsets.getMutator().setValueCount(topLevelValueCount == 0 ? 0 : topLevelValueCount + 1);
      int childValueCount = offsets.getAccessor().get(topLevelValueCount);
      for (final ValueVector v : getChildren()) {
        v.getMutator().setValueCount(childValueCount);
      }
    }

    @Override
    public void reset() {}

    @Override
    public void generateTestData(int values) {}

    public int add(int index) {
      final int prevEnd = offsets.getAccessor().get(index + 1);
      offsets.getMutator().setSafe(index + 1, prevEnd + 1);
      return prevEnd;
    }
  }

  @Override
  public void clear() {
    getMutator().reset();

    offsets.clear();
    for(final ValueVector vector : getChildren()) {
      vector.clear();
    }
  }
}
