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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.list;
import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;

public class LegacyListVector extends LegacyBaseRepeatedValueVector implements FieldVector, PromotableVector {

   public static LegacyListVector empty(String name, BufferAllocator allocator) {
      return new LegacyListVector(name, allocator, FieldType.nullable(ArrowType.List.INSTANCE), null);
   }

   private Mutator mutator = new Mutator();
   private Accessor accessor = new Accessor();

   private final ListVector listVector;

   // deprecated, use FieldType or static constructor instead
   @Deprecated
   public LegacyListVector(String name, BufferAllocator allocator, CallBack callBack) {
      super(name, allocator, callBack);
      listVector = new ListVector(name, allocator, callBack);
   }

   // deprecated, use FieldType or static constructor instead
   @Deprecated
   public LegacyListVector(String name, BufferAllocator allocator, DictionaryEncoding dictionary, CallBack callBack) {
      super(name, allocator, callBack);
      listVector = new ListVector(name, allocator, dictionary, callBack);
   }

   public LegacyListVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
      super(name, allocator, callBack);
      listVector = new ListVector(name, allocator, fieldType, callBack);
   }

   @Override
   public void initializeChildrenFromFields(List<Field> children) {
      listVector.initializeChildrenFromFields(children);
   }

   @Override
   public List<FieldVector> getChildrenFromFields() {
      return listVector.getChildrenFromFields();
   }

   @Override
   public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
      listVector.loadFieldBuffers(fieldNode, ownBuffers);
   }

   @Override
   public List<ArrowBuf> getFieldBuffers() {
      return listVector.getFieldBuffers();
   }

   @Override
   public List<BufferBacked> getFieldInnerVectors() {
      return listVector.getFieldInnerVectors();
   }

   public UnionListWriter getWriter() {
      return listVector.getWriter();
   }

   @Override
   public void allocateNew() throws OutOfMemoryException {
      listVector.allocateNew();
   }

   @Override
   public void reAlloc() {
      listVector.reAlloc();
   }

   public void copyFromSafe(int inIndex, int outIndex, ListVector from) {
      listVector.copyFrom(inIndex, outIndex, from);
   }

   public void copyFrom(int inIndex, int outIndex, ListVector from) {
      listVector.copyFrom(inIndex, outIndex, from);
   }

   @Override
   public FieldVector getDataVector() {
      return listVector.getDataVector();
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
      return listVector.getTransferPair(ref, allocator);
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
      return listVector.getTransferPair(ref, allocator, callBack);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector target) {
      return listVector.makeTransferPair(((LegacyListVector)target).listVector);
   }

   @Override
   public long getValidityBufferAddress() {
      return listVector.getValidityBufferAddress();
   }

   @Override
   public long getDataBufferAddress() {
      throw new UnsupportedOperationException();
   }

   @Override
   public long getOffsetBufferAddress() {
      return listVector.getOffsetBufferAddress();
   }

   @Override
   public ArrowBuf getValidityBuffer() {
      return listVector.getValidityBuffer();
   }

   @Override
   public ArrowBuf getDataBuffer() {
      throw new UnsupportedOperationException();
   }

   @Override
   public ArrowBuf getOffsetBuffer() {
      return listVector.getOffsetBuffer();
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
   public UnionListReader getReader() {
      return listVector.getReader();
   }

   @Override
   public boolean allocateNewSafe() {
      return listVector.allocateNewSafe();
   }

   public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
      return listVector.addOrGetVector(fieldType);
   }

   @Override
   public int getBufferSize() {
      return listVector.getBufferSize();
   }

   @Override
   public Field getField() {
     return listVector.getField();
   }

   @Override
   public MinorType getMinorType() {
      return MinorType.LIST;
   }

   @Override
   public void clear() {
      listVector.clear();
   }

   @Override
   public ArrowBuf[] getBuffers(boolean clear) {
     return listVector.getBuffers(clear);
   }

   @Override
   public UnionVector promoteToUnion() {
     return listVector.promoteToUnion();
   }

   private int lastSet = 0;

   public class Accessor extends LegacyBaseRepeatedAccessor {

      @Override
      public Object getObject(int index) {
        return listVector.getObject(index);
      }

      @Override
      public boolean isNull(int index) {
         return listVector.isNull(index);
      }

      @Override
      public int getNullCount() {
         return listVector.getNullCount();
      }
   }

   public class Mutator extends LegacyBaseRepeatedMutator {
      public void setNotNull(int index) {
        listVector.setNotNull(index);
      }

      @Override
      public int startNewValue(int index) {
         return listVector.startNewValue(index);
      }

      /**
       * End the current value
       *
       * @param index index of the value to end
       * @param size  number of elements in the list that was written
       */
      public void endValue(int index, int size) {
         listVector.endValue(index, size);
      }

      @Override
      public void setValueCount(int valueCount) {
        listVector.setValueCount(valueCount);
      }

      public void setLastSet(int value) {
         listVector.setLastSet(value);
      }

      public int getLastSet() {
         return listVector.getLastSet();
      }
   }

}
