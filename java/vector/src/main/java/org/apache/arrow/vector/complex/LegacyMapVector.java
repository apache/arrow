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

public class LegacyMapVector extends AbstractMapVector {

   /* delegate */
   private final MapVector mapVector;

   public static LegacyMapVector empty(String name, BufferAllocator allocator) {
      FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
      return new LegacyMapVector(name, allocator, fieldType, null);
   }

   private final Accessor accessor = new Accessor();
   private final Mutator mutator = new Mutator();

   @Deprecated
   public LegacyMapVector(String name, BufferAllocator allocator, CallBack callBack) {
      super(name, allocator, callBack);
      mapVector = new MapVector(name, allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null, null), callBack);
   }

   public LegacyMapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
      super(name, allocator, callBack);
      mapVector = new MapVector(name, allocator, fieldType, callBack);
   }

   @Override
   public FieldReader getReader() {
      return mapVector.getReader();
   }

   public void copyFromSafe(int fromIndex, int thisIndex, MapVector from) {
     mapVector.copyFromSafe(fromIndex, thisIndex, from);
   }

   @Override
   protected boolean supportsDirectRead() {
      return true;
   }

   public Iterator<String> fieldNameIterator() {
      return mapVector.fieldNameIterator();
   }

   @Override
   public void setInitialCapacity(int numRecords) {
     mapVector.setInitialCapacity(numRecords);
   }

   @Override
   public int getBufferSize() {
     return mapVector.getBufferSize();
   }

   @Override
   public int getBufferSizeFor(final int valueCount) {
      return (int) mapVector.getBufferSizeFor(valueCount);
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
      return mapVector.getTransferPair(ref, allocator, callBack);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector to) {
      return mapVector.makeTransferPair(((LegacyMapVector)to).mapVector);
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
      return mapVector.getTransferPair(ref, allocator);
   }

   @Override
   public int getValueCapacity() {
      return mapVector.getValueCapacity();
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
         return mapVector.getObject(index);
      }

      public void get(int index, ComplexHolder holder) {
        mapVector.get(index, holder);
      }

      @Override
      public int getValueCount() {
         return mapVector.getValueCount();
      }
   }

   public ValueVector getVectorById(int id) {
      return mapVector.getVectorById(id);
   }

   public class Mutator extends BaseValueVector.BaseMutator {

      @Override
      public void setValueCount(int valueCount) {
        mapVector.setValueCount(valueCount);
      }

      @Override
      public void reset() { }

      @Override
      public void generateTestData(int values) { }
   }

   @Override
   public void clear() {
     mapVector.clear();
   }

   @Override
   public Field getField() {
     return mapVector.getField();
   }

   @Override
   public MinorType getMinorType() {
      return MinorType.MAP;
   }

   @Override
   public void close() {
      mapVector.close();
   }

   public void initializeChildrenFromFields(List<Field> children) {
      mapVector.initializeChildrenFromFields(children);
   }

   public List<FieldVector> getChildrenFromFields() {
      return mapVector.getChildren();
   }

   public boolean isNull(int index) { return false; }

   public int getNullCount() { return  0; }

   public int getValueCount() { return 0; }

   public void setValueCount(int valueCount) { }

   public Object getObject(int index) { return null; }
}
