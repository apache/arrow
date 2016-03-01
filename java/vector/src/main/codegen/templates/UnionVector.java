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
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.BasicTypeHelper;

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
public class UnionVector implements ValueVector {

  private MaterializedField field;
  private BufferAllocator allocator;
  private Accessor accessor = new Accessor();
  private Mutator mutator = new Mutator();
  int valueCount;

  MapVector internalMap;
  private UInt1Vector typeVector;

  private MapVector mapVector;
  private ListVector listVector;

  private FieldReader reader;
  private NullableBitVector bit;

  private int singleType = 0;
  private ValueVector singleVector;
  private MajorType majorType;

  private final CallBack callBack;

  public UnionVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = field.clone();
    this.allocator = allocator;
    this.internalMap = new MapVector("internal", allocator, callBack);
    this.typeVector = internalMap.addOrGet("types", new MajorType(MinorType.UINT1, DataMode.REQUIRED), UInt1Vector.class);
    this.field.addChild(internalMap.getField().clone());
    this.majorType = field.getType();
    this.callBack = callBack;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public List<MinorType> getSubTypes() {
    return majorType.getSubTypes();
  }

  public void addSubType(MinorType type) {
    if (majorType.getSubTypes().contains(type)) {
      return;
    }
    List<MinorType> subTypes = this.majorType.getSubTypes();
    List<MinorType> newSubTypes = new ArrayList<>(subTypes);
    newSubTypes.add(type);
    majorType =  new MajorType(this.majorType.getMinorType(), this.majorType.getMode(), this.majorType.getPrecision(),
            this.majorType.getScale(), this.majorType.getTimezone(), newSubTypes);
    field = MaterializedField.create(field.getName(), majorType);
    if (callBack != null) {
      callBack.doWork();
    }
  }

  private static final MajorType MAP_TYPE = new MajorType(MinorType.MAP, DataMode.OPTIONAL);

  public MapVector getMap() {
    if (mapVector == null) {
      int vectorCount = internalMap.size();
      mapVector = internalMap.addOrGet("map", MAP_TYPE, MapVector.class);
      addSubType(MinorType.MAP);
      if (internalMap.size() > vectorCount) {
        mapVector.allocateNew();
      }
    }
    return mapVector;
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#if !minor.class?starts_with("Decimal")>

  private Nullable${name}Vector ${uncappedName}Vector;
  private static final MajorType ${name?upper_case}_TYPE = new MajorType(MinorType.${name?upper_case}, DataMode.OPTIONAL);

  public Nullable${name}Vector get${name}Vector() {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalMap.size();
      ${uncappedName}Vector = internalMap.addOrGet("${uncappedName}", ${name?upper_case}_TYPE, Nullable${name}Vector.class);
      addSubType(MinorType.${name?upper_case});
      if (internalMap.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
      }
    }
    return ${uncappedName}Vector;
  }

  </#if>

  </#list></#list>

  private static final MajorType LIST_TYPE = new MajorType(MinorType.LIST, DataMode.OPTIONAL);

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalMap.size();
      listVector = internalMap.addOrGet("list", LIST_TYPE, ListVector.class);
      addSubType(MinorType.LIST);
      if (internalMap.size() > vectorCount) {
        listVector.allocateNew();
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
    if (typeVector != null) {
      typeVector.zeroVector();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean safe = internalMap.allocateNewSafe();
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
  }

  @Override
  public void clear() {
    internalMap.clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(field.withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  public void transferTo(UnionVector target) {
    internalMap.makeTransferPair(target.internalMap).transfer();
    target.valueCount = valueCount;
    target.majorType = majorType;
  }

  public void copyFrom(int inIndex, int outIndex, UnionVector from) {
    from.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(from.reader, mutator.writer);
  }

  public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public ValueVector addVector(ValueVector v) {
    String name = v.getField().getType().getMinorType().name().toLowerCase();
    MajorType type = v.getField().getType();
    Preconditions.checkState(internalMap.getChild(name) == null, String.format("%s vector already exists", name));
    final ValueVector newVector = internalMap.addOrGet(name, type, (Class<ValueVector>) BasicTypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
    v.makeTransferPair(newVector).transfer();
    internalMap.putChild(name, newVector);
    addSubType(v.getField().getType().getMinorType());
    return newVector;
  }

  private class TransferImpl implements TransferPair {

    UnionVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      to = new UnionVector(field, allocator, null);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {

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

//  @Override
//  public UserBitShared.SerializedField getMetadata() {
//    SerializedField.Builder b = getField() //
//            .getAsBuilder() //
//            .setBufferLength(getBufferSize()) //
//            .setValueCount(valueCount);
//
//    b.addChild(internalMap.getMetadata());
//    return b.build();
//  }

  @Override
  public int getBufferSize() {
    return internalMap.getBufferSize();
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
    return internalMap.getBuffers(clear);
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
      case LATE:
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
      internalMap.getMutator().setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getType().getMinorType();
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
      typeVector.getMutator().setSafe(index, type.ordinal());
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }
}
