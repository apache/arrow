/*******************************************************************************

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
 ******************************************************************************/
package org.apache.arrow.vector.complex;

import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;
import io.netty.buffer.ArrowBuf;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;

import com.google.common.collect.ObjectArrays;

public class ListVector extends BaseRepeatedValueVector {

  UInt4Vector offsets;
  final UInt1Vector bits;
  private Mutator mutator = new Mutator();
  private Accessor accessor = new Accessor();
  private UnionListWriter writer;
  private UnionListReader reader;
  private CallBack callBack;

  public ListVector(String name, BufferAllocator allocator, CallBack callBack) {
    super(name, allocator);
    this.bits = new UInt1Vector("$bits$", allocator);
    offsets = getOffsetVector();
    this.writer = new UnionListWriter(this);
    this.reader = new UnionListReader(this);
    this.callBack = callBack;
  }

  public UnionListWriter getWriter() {
    return writer;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    super.allocateNewSafe();
    bits.allocateNewSafe();
  }

  public void copyFromSafe(int inIndex, int outIndex, ListVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public void copyFrom(int inIndex, int outIndex, ListVector from) {
    FieldReader in = from.getReader();
    in.setPosition(inIndex);
    FieldWriter out = getWriter();
    out.setPosition(outIndex);
    ComplexCopier.copy(in, out);
  }

  @Override
  public ValueVector getDataVector() {
    return vector;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((ListVector) target);
  }

  private class TransferImpl implements TransferPair {

    ListVector to;
    TransferPair pairs[] = new TransferPair[3];

    public TransferImpl(String name, BufferAllocator allocator) {
      this(new ListVector(name, allocator, null));
    }

    public TransferImpl(ListVector to) {
      this.to = to;
      to.addOrGetVector(vector.getMinorType());
      pairs[0] = offsets.makeTransferPair(to.offsets);
      pairs[1] = bits.makeTransferPair(to.bits);
      if (to.getDataVector() instanceof ZeroVector) {
        to.addOrGetVector(vector.getMinorType());
      }
      pairs[2] = getDataVector().makeTransferPair(to.getDataVector());
    }

    @Override
    public void transfer() {
      for (TransferPair pair : pairs) {
        pair.transfer();
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.allocateNew();
      for (int i = 0; i < length; i++) {
        copyValueSafe(startIndex + i, i);
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, ListVector.this);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    return reader;
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
      success = vector.allocateNewSafe();
      success = success && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    if (success) {
      offsets.zeroVector();
      bits.zeroVector();
    }
    return success;
  }

  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(MinorType minorType) {
    AddOrGetResult<T> result = super.addOrGetVector(minorType);
    reader = new UnionListReader(this);
    return result;
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return offsets.getBufferSize() + bits.getBufferSize() + vector.getBufferSize();
  }

  @Override
  public Field getField() {
    return new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.List(),
            ImmutableList.of(getDataVector().getField()));
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.LIST;
  }

  @Override
  public void clear() {
    offsets.clear();
    vector.clear();
    bits.clear();
    lastSet = 0;
    super.clear();
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(offsets.getBuffers(false), ObjectArrays.concat(bits.getBuffers(false),
            vector.getBuffers(false), ArrowBuf.class), ArrowBuf.class);
    if (clear) {
      for (ArrowBuf buffer:buffers) {
        buffer.retain();
      }
      clear();
    }
    return buffers;
  }

  public UnionVector promoteToUnion() {
    UnionVector vector = new UnionVector(name, allocator, null);
    replaceDataVector(vector);
    reader = new UnionListReader(this);
    return vector;
  }

  private int lastSet;

  public class Accessor extends BaseRepeatedAccessor {

    @Override
    public Object getObject(int index) {
      if (isNull(index)) {
        return null;
      }
      final List<Object> vals = new JsonStringArrayList<>();
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      final int start = offsetsAccessor.get(index);
      final int end = offsetsAccessor.get(index + 1);
      final ValueVector.Accessor valuesAccessor = getDataVector().getAccessor();
      for(int i = start; i < end; i++) {
        vals.add(valuesAccessor.getObject(i));
      }
      return vals;
    }

    @Override
    public boolean isNull(int index) {
      return bits.getAccessor().get(index) == 0;
    }
  }

  public class Mutator extends BaseRepeatedMutator {
    public void setNotNull(int index) {
      bits.getMutator().setSafe(index, 1);
      lastSet = index + 1;
    }

    @Override
    public void startNewValue(int index) {
      for (int i = lastSet; i <= index; i++) {
        offsets.getMutator().setSafe(i + 1, offsets.getAccessor().get(i));
      }
      setNotNull(index);
      lastSet = index + 1;
    }

    @Override
    public void setValueCount(int valueCount) {
      // TODO: populate offset end points
      if (valueCount == 0) {
        offsets.getMutator().setValueCount(0);
      } else {
        for (int i = lastSet; i < valueCount; i++) {
          offsets.getMutator().setSafe(i + 1, offsets.getAccessor().get(i));
        }
        offsets.getMutator().setValueCount(valueCount + 1);
      }
      final int childValueCount = valueCount == 0 ? 0 : offsets.getAccessor().get(valueCount);
      vector.getMutator().setValueCount(childValueCount);
      bits.getMutator().setValueCount(valueCount);
    }
  }
}
