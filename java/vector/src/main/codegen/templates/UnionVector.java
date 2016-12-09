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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/UnionVector.java" />


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
 */
public class UnionVector implements FieldVector {

  private String name;
  private BufferAllocator allocator;
  private Accessor accessor = new Accessor();
  private Mutator mutator = new Mutator();
  int valueCount;

  MapVector internalMap;
  UInt1Vector typeVector;

  private NullableMapVector mapVector;
  private ListVector listVector;

  private FieldReader reader;

  private int singleType = 0;
  private ValueVector singleVector;

  private final CallBack callBack;
  private final List<BufferBacked> innerVectors;

  public UnionVector(String name, BufferAllocator allocator, CallBack callBack) {
    this.name = name;
    this.allocator = allocator;
    this.internalMap = new MapVector("internal", allocator, callBack);
    this.typeVector = new UInt1Vector("types", allocator);
    this.callBack = callBack;
    this.innerVectors = Collections.unmodifiableList(Arrays.<BufferBacked>asList(typeVector));
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.UNION;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    internalMap.initializeChildrenFromFields(children);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return internalMap.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    // truncate types vector buffer to size (#0)
    org.apache.arrow.vector.BaseDataValueVector.truncateBufferBasedOnSize(ownBuffers, 0, typeVector.getBufferSizeFor(fieldNode.getLength()));
    BaseDataValueVector.load(fieldNode, getFieldInnerVectors(), ownBuffers);
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return BaseDataValueVector.unload(getFieldInnerVectors());
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
     return this.innerVectors;
  }
  
  public NullableMapVector getMap() {
    if (mapVector == null) {
      int vectorCount = internalMap.size();
      mapVector = internalMap.addOrGet("map", MinorType.MAP, NullableMapVector.class);
      if (internalMap.size() > vectorCount) {
        mapVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return mapVector;
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#if !minor.class?starts_with("Decimal")>

  private Nullable${name}Vector ${uncappedName}Vector;

  public Nullable${name}Vector get${name}Vector() {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalMap.size();
      ${uncappedName}Vector = internalMap.addOrGet("${uncappedName}", MinorType.${name?upper_case}, Nullable${name}Vector.class);
      if (internalMap.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return ${uncappedName}Vector;
  }

  </#if>

  </#list></#list>

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalMap.size();
      listVector = internalMap.addOrGet("list", MinorType.LIST, ListVector.class);
      if (internalMap.size() > vectorCount) {
        listVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return listVector;
  }

  public int getTypeValue(int index) {
    return typeVector.getAccessor().get(index);
  }

  public UInt1Vector getTypeVector() {
    return typeVector;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    internalMap.allocateNew();
    typeVector.allocateNew();
    if (typeVector != null) {
      typeVector.zeroVector();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean safe = internalMap.allocateNewSafe();
    safe = safe && typeVector.allocateNewSafe();
    if (safe) {
      if (typeVector != null) {
        typeVector.zeroVector();
      }
    }
    return safe;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
  }

  @Override
  public int getValueCapacity() {
    return Math.min(typeVector.getValueCapacity(), internalMap.getValueCapacity());
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public void clear() {
    typeVector.clear();
    internalMap.clear();
  }

  @Override
  public Field getField() {
    List<org.apache.arrow.vector.types.pojo.Field> childFields = new ArrayList<>();
    List<FieldVector> children = internalMap.getChildren();
    int[] typeIds = new int[children.size()];
    for (ValueVector v : children) {
      typeIds[childFields.size()] = v.getMinorType().ordinal();
      childFields.add(v.getField());
    }
    return new Field(name, true, new ArrowType.Union(Sparse, typeIds), childFields);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  public void copyFrom(int inIndex, int outIndex, UnionVector from) {
    from.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(from.reader, mutator.writer);
  }

  public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public FieldVector addVector(FieldVector v) {
    String name = v.getMinorType().name().toLowerCase();
    Preconditions.checkState(internalMap.getChild(name) == null, String.format("%s vector already exists", name));
    final FieldVector newVector = internalMap.addOrGet(name, v.getMinorType(), v.getClass());
    v.makeTransferPair(newVector).transfer();
    internalMap.putChild(name, newVector);
    if (callBack != null) {
      callBack.doWork();
    }
    return newVector;
  }

  private class TransferImpl implements TransferPair {
    private final TransferPair internalMapVectorTransferPair;
    private final TransferPair typeVectorTransferPair;
    private final UnionVector to;

    public TransferImpl(String name, BufferAllocator allocator) {
      to = new UnionVector(name, allocator, null);
      internalMapVectorTransferPair = internalMap.makeTransferPair(to.internalMap);
      typeVectorTransferPair = typeVector.makeTransferPair(to.typeVector);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
      internalMapVectorTransferPair = internalMap.makeTransferPair(to.internalMap);
      typeVectorTransferPair = typeVector.makeTransferPair(to.typeVector);
    }

    @Override
    public void transfer() {
      internalMapVectorTransferPair.transfer();
      typeVectorTransferPair.transfer();
      to.valueCount = valueCount;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.allocateNew();
      for (int i = 0; i < length; i++) {
        to.copyFromSafe(startIndex + i, i, org.apache.arrow.vector.complex.UnionVector.this);
      }
      to.getMutator().setValueCount(length);
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, UnionVector.this);
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
    if (reader == null) {
      reader = new UnionReader(this);
    }
    return reader;
  }

  public FieldWriter getWriter() {
    if (mutator.writer == null) {
      mutator.writer = new UnionWriter(this);
    }
    return mutator.writer;
  }

  @Override
  public int getBufferSize() {
    return typeVector.getBufferSize() + internalMap.getBufferSize();
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
  public ArrowBuf[] getBuffers(boolean clear) {
    ImmutableList.Builder<ArrowBuf> builder = ImmutableList.builder();
    builder.add(typeVector.getBuffers(clear));
    builder.add(internalMap.getBuffers(clear));
    List<ArrowBuf> list = builder.build();
    return list.toArray(new ArrowBuf[list.size()]);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    List<ValueVector> vectors = Lists.newArrayList(internalMap.iterator());
    vectors.add(typeVector);
    return vectors.iterator();
  }

  public class Accessor extends BaseValueVector.BaseAccessor {


    @Override
    public Object getObject(int index) {
      int type = typeVector.getAccessor().get(index);
      switch (MinorType.values()[type]) {
      case NULL:
        return null;
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case ${name?upper_case}:
        return get${name}Vector().getAccessor().getObject(index);
      </#if>

      </#list></#list>
      case MAP:
        return getMap().getAccessor().getObject(index);
      case LIST:
        return getList().getAccessor().getObject(index);
      default:
        throw new UnsupportedOperationException("Cannot support type: " + MinorType.values()[type]);
      }
    }

    public byte[] get(int index) {
      return null;
    }

    public void get(int index, ComplexHolder holder) {
    }

    public void get(int index, UnionHolder holder) {
      FieldReader reader = new UnionReader(UnionVector.this);
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }

    @Override
    public boolean isNull(int index) {
      return typeVector.getAccessor().get(index) == 0;
    }

    public int isSet(int index) {
      return isNull(index) ? 0 : 1;
    }
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    UnionWriter writer;

    @Override
    public void setValueCount(int valueCount) {
      UnionVector.this.valueCount = valueCount;
      typeVector.getMutator().setValueCount(valueCount);
      internalMap.getMutator().setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getMinorType();
      switch (type) {
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case ${name?upper_case}:
        Nullable${name}Holder ${uncappedName}Holder = new Nullable${name}Holder();
        reader.read(${uncappedName}Holder);
        setSafe(index, ${uncappedName}Holder);
        break;
      </#if>
      </#list></#list>
      case MAP: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      case LIST: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      default:
        throw new UnsupportedOperationException();
      }
    }

    <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
    <#assign fields = minor.fields!type.fields />
    <#assign uncappedName = name?uncap_first/>
    <#if !minor.class?starts_with("Decimal")>
    public void setSafe(int index, Nullable${name}Holder holder) {
      setType(index, MinorType.${name?upper_case});
      get${name}Vector().getMutator().setSafe(index, holder);
    }

    </#if>
    </#list></#list>

    public void setType(int index, MinorType type) {
      typeVector.getMutator().setSafe(index, (byte) type.ordinal());
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }
}
