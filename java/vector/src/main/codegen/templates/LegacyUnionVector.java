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
<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/LegacyUnionVector.java" />


<#include "/@includes/license.ftl" />

        package org.apache.arrow.vector.complex;

<#include "/@includes/vv_imports.ftl" />
        import com.google.common.collect.ImmutableList;
        import java.util.ArrayList;
        import java.util.Collections;
        import java.util.Iterator;
        import org.apache.arrow.vector.BaseDataValueVector;
        import org.apache.arrow.vector.complex.impl.ComplexCopier;
        import org.apache.arrow.vector.util.CallBack;
        import org.apache.arrow.vector.schema.ArrowFieldNode;

        import static org.apache.arrow.vector.types.UnionMode.Sparse;



/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")


/**
 * A vector which can hold values of different types. It does so by using a MapVector which contains a vector for each
 * primitive type that is stored. MapVector is used in order to take advantage of its serialization/deserialization methods,
 * as well as the addOrGet method.
 *
 * For performance reasons, UnionVector stores a cached reference to each subtype vector, to avoid having to do the map lookup
 * each time the vector is accessed.
 * Source code generated using FreeMarker template ${.template_name}
 */
public class LegacyUnionVector implements FieldVector {

   private Accessor accessor = new Accessor();
   private Mutator mutator = new Mutator();
   private final UnionVector unionVector;

   public LegacyUnionVector(String name, BufferAllocator allocator, CallBack callBack) {
     unionVector = new UnionVector(name, allocator, callBack);
   }

   public BufferAllocator getAllocator() {
      return unionVector.getAllocator();
   }

   @Override
   public MinorType getMinorType() {
      return MinorType.UNION;
   }

   @Override
   public void initializeChildrenFromFields(List<Field> children) {
      unionVector.initializeChildrenFromFields(children);
   }

   @Override
   public List<FieldVector> getChildrenFromFields() {
      return unionVector.getChildrenFromFields();
   }

   @Override
   public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
      unionVector.loadFieldBuffers(fieldNode, ownBuffers);
   }

   @Override
   public List<ArrowBuf> getFieldBuffers() {
      return unionVector.getFieldBuffers();
   }

   @Override
   public List<BufferBacked> getFieldInnerVectors() {
      return unionVector.getFieldInnerVectors();
   }

   @Override
   public long getValidityBufferAddress() {
      return unionVector.getValidityBufferAddress();
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
      return unionVector.getValidityBuffer();
   }

   @Override
   public ArrowBuf getDataBuffer() { throw new UnsupportedOperationException(); }

   @Override
   public ArrowBuf getOffsetBuffer() { throw new UnsupportedOperationException(); }

   public NullableMapVector getMap() {
      return unionVector.getMap();
   }
  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#assign lowerCaseName = name?lower_case/>
      <#if !minor.typeParams?? >

   private Nullable${name}Vector ${uncappedName}Vector;

   public Nullable${name}Vector get${name}Vector() {
      return unionVector.get${name}Vector();
   }
      </#if>
    </#list>
  </#list>

   public ListVector getList() {
      return unionVector.getList();
   }

   public int getTypeValue(int index) {
      return unionVector.getTypeValue(index);
   }

   @Override
   public void allocateNew() throws OutOfMemoryException {
     unionVector.allocateNew();
   }

   @Override
   public boolean allocateNewSafe() {
     return unionVector.allocateNewSafe();
   }

   @Override
   public void reAlloc() {
      unionVector.reAlloc();
   }

   @Override
   public void setInitialCapacity(int numRecords) {
   }

   @Override
   public int getValueCapacity() {
      return unionVector.getValueCapacity();
   }

   @Override
   public void close() {
     unionVector.close();
   }

   @Override
   public void clear() {
      unionVector.clear();
   }

   @Override
   public Field getField() {
      return unionVector.getField();
   }

   @Override
   public TransferPair getTransferPair(BufferAllocator allocator) {
      return unionVector.getTransferPair(allocator);
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
      return unionVector.getTransferPair(ref, allocator);
   }

   @Override
   public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
      return unionVector.getTransferPair(ref, allocator, callBack);
   }

   @Override
   public TransferPair makeTransferPair(ValueVector target) {
      return unionVector.makeTransferPair(((LegacyUnionVector)target).unionVector);
   }

   public void copyFrom(int inIndex, int outIndex, UnionVector from) {
      unionVector.copyFrom(inIndex, outIndex, from);
   }

   public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
      unionVector.copyFromSafe(inIndex, outIndex, from);
   }

   public FieldVector addVector(FieldVector v) {
     return unionVector.addVector(v);
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
      return unionVector.getReader();
   }

   public FieldWriter getWriter() {
      return unionVector.getWriter();
   }

   @Override
   public int getBufferSize() {
      return unionVector.getBufferSize();
   }

   @Override
   public int getBufferSizeFor(final int valueCount) {
     return unionVector.getBufferSizeFor(valueCount);
   }

   @Override
   public ArrowBuf[] getBuffers(boolean clear) {
     return unionVector.getBuffers(clear);
   }

   @Override
   public Iterator<ValueVector> iterator() {
      return unionVector.iterator();
   }

   public class Accessor extends BaseValueVector.BaseAccessor {

      @Override
      public Object getObject(int index) {
        return unionVector.getObject(index);
      }

      public byte[] get(int index) {
         return unionVector.get(index);
      }

      public void get(int index, ComplexHolder holder) {
      }

      public void get(int index, UnionHolder holder) {
         unionVector.get(index, holder);
      }

      public int getNullCount() {
         return unionVector.getNullCount();
      }

      @Override
      public int getValueCount() {
         return unionVector.getValueCount();
      }

      @Override
      public boolean isNull(int index) {
         return unionVector.isNull(index);
      }

      public int isSet(int index) {
         return unionVector.isSet(index);
      }
   }

   public class Mutator extends BaseValueVector.BaseMutator {

      UnionWriter writer;

      @Override
      public void setValueCount(int valueCount) {
         unionVector.setValueCount(valueCount);
      }

      public void setSafe(int index, UnionHolder holder) {
        unionVector.setSafe(index, holder);
      }
    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.typeParams?? >
      public void setSafe(int index, Nullable${name}Holder holder) {
         unionVector.setSafe(index, holder);
      }

        </#if>
      </#list>
    </#list>

      public void setType(int index, MinorType type) {
         unionVector.setType(index, type);
      }

      @Override
      public void reset() { }

      @Override
      public void generateTestData(int values) { }
   }

   @Override
   @Deprecated
   public int getValueCount() { return getAccessor().getValueCount(); }

   @Override
   @Deprecated
   public void setValueCount(int valueCount) { getMutator().setValueCount(valueCount);}

   @Override
   @Deprecated
   public Object getObject(int index) { return getAccessor().getObject(index); }

   @Override
   @Deprecated
   public int getNullCount() { return getAccessor().getNullCount(); }

   @Override
   @Deprecated
   public boolean isNull(int index) { return getAccessor().isNull(index); }
}
