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

import static java.util.Collections.singletonList;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.DATA_VECTOR_NAME;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;
import org.apache.arrow.vector.util.TransferPair;

public class LegacyFixedSizeListVector extends BaseValueVector implements FieldVector, PromotableVector {

   public static LegacyFixedSizeListVector empty(String name, int size, BufferAllocator allocator) {
      FieldType fieldType = FieldType.nullable(new ArrowType.FixedSizeList(size));
      return new LegacyFixedSizeListVector(name, allocator, fieldType, null);
   }

   private Mutator mutator = new Mutator();
   private Accessor accessor = new Accessor();
   /* delegate */
   private final FixedSizeListVector fixedSizeListVector;

   // deprecated, use FieldType or static constructor instead
   @Deprecated
   public LegacyFixedSizeListVector(String name,
                              BufferAllocator allocator,
                              int listSize,
                              DictionaryEncoding dictionary,
                              CallBack schemaChangeCallback) {
      super(name, allocator);
      fixedSizeListVector = new FixedSizeListVector(name, allocator, listSize, dictionary, schemaChangeCallback);
   }

   public LegacyFixedSizeListVector(String name,
                              BufferAllocator allocator,
                              FieldType fieldType,
                              CallBack schemaChangeCallback) {
      super(name, allocator);
      fixedSizeListVector = new FixedSizeListVector(name, allocator, fieldType, schemaChangeCallback);
   }

   @Override
   public Field getField() {
      return fixedSizeListVector.getField();
   }

   @Override
   public MinorType getMinorType() {
      return MinorType.FIXED_SIZE_LIST;
   }

   public int getListSize() {
      return fixedSizeListVector.getListSize();
   }

   @Override
   public void initializeChildrenFromFields(List<Field> children) {
      fixedSizeListVector.initializeChildrenFromFields(children);
   }

   @Override
   public List<FieldVector> getChildrenFromFields() {
      return fixedSizeListVector.getChildrenFromFields();
   }

   @Override
   public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
      fixedSizeListVector.loadFieldBuffers(fieldNode, ownBuffers);
   }

   @Override
   public List<ArrowBuf> getFieldBuffers() {
      return fixedSizeListVector.getFieldBuffers();
   }

   @Override
   public List<BufferBacked> getFieldInnerVectors() {
      return fixedSizeListVector.getFieldInnerVectors();
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
   public UnionFixedSizeListReader getReader() {
      return fixedSizeListVector.getReader();
   }

   @Override
   public void allocateNew() throws OutOfMemoryException {
     fixedSizeListVector.allocateNew();
   }

   @Override
   public boolean allocateNewSafe() {
    return fixedSizeListVector.allocateNewSafe();
   }

   @Override
   public void reAlloc() {
      fixedSizeListVector.reAlloc();
   }

   public FieldVector getDataVector() {
      return fixedSizeListVector.getDataVector();
   }

   @Override
   public void setInitialCapacity(int numRecords) {
      fixedSizeListVector.setInitialCapacity(numRecords);
   }

   @Override
   public int getValueCapacity() {
     return fixedSizeListVector.getValueCapacity();
   }

   @Override
   public int getBufferSize() {
     return fixedSizeListVector.getBufferSize();
   }

   @Override
   public int getBufferSizeFor(int valueCount) {
    return fixedSizeListVector.getBufferSizeFor(valueCount);
   }

   @Override
   public Iterator<ValueVector> iterator() {
      return fixedSizeListVector.iterator();
   }

   @Override
   public void clear() {
     fixedSizeListVector.clear();
   }

   @Override
   public ArrowBuf[] getBuffers(boolean clear) {
      return fixedSizeListVector.getBuffers(clear);
   }

   /**
    * @return 1 if inner vector is explicitly set via #addOrGetVector else 0
    */
   public int size() {
      return fixedSizeListVector.size();
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType type) {
      return fixedSizeListVector.addOrGetVector(type);
   }

   public void copyFromSafe(int inIndex, int outIndex, FixedSizeListVector from) {
      fixedSizeListVector.copyFromSafe(inIndex, outIndex, from);
   }

   public void copyFrom(int fromIndex, int thisIndex, FixedSizeListVector from) {
     fixedSizeListVector.copyFrom(fromIndex, thisIndex, from);
   }

   @Override
   public UnionVector promoteToUnion() {
      return fixedSizeListVector.promoteToUnion();
   }

   @Override
   public long getValidityBufferAddress() {
      return fixedSizeListVector.getValidityBufferAddress();
   }

   @Override
   public long getDataBufferAddress() {
      throw new UnsupportedOperationException();
   }

   @Override
   public long getOffsetBufferAddress() {
      throw new UnsupportedOperationException();
   }

   @Override
   public ArrowBuf getValidityBuffer() {
      return fixedSizeListVector.getValidityBuffer();
   }

   @Override
   public ArrowBuf getDataBuffer() {
      throw new UnsupportedOperationException();
   }

   @Override
   public ArrowBuf getOffsetBuffer() {
      throw new UnsupportedOperationException();
   }

   public class Accessor extends BaseValueVector.BaseAccessor {

      @Override
      public Object getObject(int index) {
         return fixedSizeListVector.getObject(index);
      }

      @Override
      public boolean isNull(int index) {
         return fixedSizeListVector.isNull(index);
      }

      @Override
      public int getNullCount() {
         return fixedSizeListVector.getNullCount();
      }

      @Override
      public int getValueCount() {
         return fixedSizeListVector.getValueCount();
      }
   }

   public class Mutator extends BaseValueVector.BaseMutator {

      public void setNull(int index) {
         fixedSizeListVector.setNull(index);
      }

      public void setNotNull(int index) {
         fixedSizeListVector.setNotNull(index);
      }

      @Override
      public void setValueCount(int valueCount) {
         fixedSizeListVector.setValueCount(valueCount);
      }
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
      return fixedSizeListVector.getTransferPair(ref, allocator);
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
      return fixedSizeListVector.getTransferPair(ref, allocator, callBack);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector target) {
      return fixedSizeListVector.makeTransferPair(((LegacyFixedSizeListVector)target).fixedSizeListVector);
   }
}
