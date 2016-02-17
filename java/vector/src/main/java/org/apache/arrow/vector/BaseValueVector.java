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
package org.apache.arrow.vector;

import io.netty.buffer.ArrowBuf;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseValueVector implements ValueVector {
  private static final Logger logger = LoggerFactory.getLogger(BaseValueVector.class);

  public static final int MAX_ALLOCATION_SIZE = Integer.MAX_VALUE;
  public static final int INITIAL_VALUE_ALLOCATION = 4096;

  protected final BufferAllocator allocator;
  protected final MaterializedField field;

  protected BaseValueVector(MaterializedField field, BufferAllocator allocator) {
    this.field = Preconditions.checkNotNull(field, "field cannot be null");
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
  }

  @Override
  public String toString() {
    return super.toString() + "[field = " + field + ", ...]";
  }

  @Override
  public void clear() {
    getMutator().reset();
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  public MaterializedField getField(String ref){
    return getField().withPath(ref);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(getField().getPath(), allocator);
  }

//  public static SerializedField getMetadata(BaseValueVector vector) {
//    return getMetadataBuilder(vector).build();
//  }
//
//  protected static SerializedField.Builder getMetadataBuilder(BaseValueVector vector) {
//    return SerializedFieldHelper.getAsBuilder(vector.getField())
//        .setValueCount(vector.getAccessor().getValueCount())
//        .setBufferLength(vector.getBufferSize());
//  }

  public abstract static class BaseAccessor implements ValueVector.Accessor {
    protected BaseAccessor() { }

    @Override
    public boolean isNull(int index) {
      return false;
    }
  }

  public abstract static class BaseMutator implements ValueVector.Mutator {
    protected BaseMutator() { }

    @Override
    public void generateTestData(int values) {}

    //TODO: consider making mutator stateless(if possible) on another issue.
    public void reset() {}
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Iterators.emptyIterator();
  }

  public static boolean checkBufRefs(final ValueVector vv) {
    for(final ArrowBuf buffer : vv.getBuffers(false)) {
      if (buffer.refCnt() <= 0) {
        throw new IllegalStateException("zero refcount");
      }
    }

    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }
}

