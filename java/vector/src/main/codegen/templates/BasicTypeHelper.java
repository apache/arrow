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
<@pp.changeOutputFile name="/org/apache/arrow/vector/util/BasicTypeHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.util;

<#include "/@includes/vv_imports.ftl" />
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.RepeatedMapVector;
import org.apache.arrow.vector.util.CallBack;

public class BasicTypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicTypeHelper.class);

  private static final int WIDTH_ESTIMATE = 50;

  // Default length when casting to varchar : 65536 = 2^16
  // This only defines an absolute maximum for values, setting
  // a high value like this will not inflate the size for small values
  public static final int VARCHAR_DEFAULT_CAST_LEN = 65536;

  protected static String buildErrorMessage(final String operation, final MinorType type, final DataMode mode) {
    return String.format("Unable to %s for minor type [%s] and mode [%s]", operation, type, mode);
  }

  protected static String buildErrorMessage(final String operation, final MajorType type) {
    return buildErrorMessage(operation, type.getMinorType(), type.getMode());
  }

  public static int getSize(MajorType major) {
    switch (major.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case}:
      return ${type.width}<#if minor.class?substring(0, 3) == "Var" ||
                               minor.class?substring(0, 3) == "PRO" ||
                               minor.class?substring(0, 3) == "MSG"> + WIDTH_ESTIMATE</#if>;
  </#list>
</#list>
//      case FIXEDCHAR: return major.getWidth();
//      case FIXED16CHAR: return major.getWidth();
//      case FIXEDBINARY: return major.getWidth();
    }
    throw new UnsupportedOperationException(buildErrorMessage("get size", major));
  }

  public static ValueVector getNewVector(String name, BufferAllocator allocator, MajorType type, CallBack callback){
    MaterializedField field = MaterializedField.create(name, type);
    return getNewVector(field, allocator, callback);
  }
  
  
  public static Class<?> getValueVectorClass(MinorType type, DataMode mode){
    switch (type) {
    case UNION:
      return UnionVector.class;
    case MAP:
      switch (mode) {
      case OPTIONAL:
      case REQUIRED:
        return MapVector.class;
      case REPEATED:
        return RepeatedMapVector.class;
      }
      
    case LIST:
      switch (mode) {
      case REPEATED:
        return RepeatedListVector.class;
      case REQUIRED:
      case OPTIONAL:
        return ListVector.class;
      }
    
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}Vector.class;
          case OPTIONAL:
            return Nullable${minor.class}Vector.class;
          case REPEATED:
            return Repeated${minor.class}Vector.class;
        }
  </#list>
</#list>
    case GENERIC_OBJECT      :
      return ObjectVector.class  ;
    default:
      break;
    }
    throw new UnsupportedOperationException(buildErrorMessage("get value vector class", type, mode));
  }
  public static Class<?> getReaderClassName( MinorType type, DataMode mode, boolean isSingularRepeated){
    switch (type) {
    case MAP:
      switch (mode) {
      case REQUIRED:
        if (!isSingularRepeated)
          return SingleMapReaderImpl.class;
        else
          return SingleLikeRepeatedMapReaderImpl.class;
      case REPEATED: 
          return RepeatedMapReaderImpl.class;
      }
    case LIST:
      switch (mode) {
      case REQUIRED:
        return SingleListReaderImpl.class;
      case REPEATED:
        return RepeatedListReaderImpl.class;
      }
      
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}ReaderImpl.class;
          case OPTIONAL:
            return Nullable${minor.class}ReaderImpl.class;
          case REPEATED:
            return Repeated${minor.class}ReaderImpl.class;
        }
  </#list>
</#list>
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get reader class name", type, mode));
  }
  
  public static Class<?> getWriterInterface( MinorType type, DataMode mode){
    switch (type) {
    case UNION: return UnionWriter.class;
    case MAP: return MapWriter.class;
    case LIST: return ListWriter.class;
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}: return ${minor.class}Writer.class;
  </#list>
</#list>
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get writer interface", type, mode));
  }
  
  public static Class<?> getWriterImpl( MinorType type, DataMode mode){
    switch (type) {
    case UNION:
      return UnionWriter.class;
    case MAP:
      switch (mode) {
      case REQUIRED:
      case OPTIONAL:
        return SingleMapWriter.class;
      case REPEATED:
        return RepeatedMapWriter.class;
      }
    case LIST:
      switch (mode) {
      case REQUIRED:
      case OPTIONAL:
        return UnionListWriter.class;
      case REPEATED:
        return RepeatedListWriter.class;
      }
      
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}WriterImpl.class;
          case OPTIONAL:
            return Nullable${minor.class}WriterImpl.class;
          case REPEATED:
            return Repeated${minor.class}WriterImpl.class;
        }
  </#list>
</#list>
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get writer implementation", type, mode));
  }

  public static Class<?> getHolderReaderImpl( MinorType type, DataMode mode){
    switch (type) {      
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}HolderReaderImpl.class;
          case OPTIONAL:
            return Nullable${minor.class}HolderReaderImpl.class;
          case REPEATED:
            return Repeated${minor.class}HolderReaderImpl.class;
        }
  </#list>
</#list>
      default:
        break;
      }
      throw new UnsupportedOperationException(buildErrorMessage("get holder reader implementation", type, mode));
  }
  
  public static ValueVector getNewVector(MaterializedField field, BufferAllocator allocator){
    return getNewVector(field, allocator, null);
  }
  public static ValueVector getNewVector(MaterializedField field, BufferAllocator allocator, CallBack callBack){
    MajorType type = field.getType();

    switch (type.getMinorType()) {
    
    case UNION:
      return new UnionVector(field, allocator, callBack);

    case MAP:
      switch (type.getMode()) {
      case REQUIRED:
      case OPTIONAL:
        return new MapVector(field, allocator, callBack);
      case REPEATED:
        return new RepeatedMapVector(field, allocator, callBack);
      }
    case LIST:
      switch (type.getMode()) {
      case REPEATED:
        return new RepeatedListVector(field, allocator, callBack);
      case OPTIONAL:
      case REQUIRED:
        return new ListVector(field, allocator, callBack);
      }
<#list vv.  types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (type.getMode()) {
        case REQUIRED:
          return new ${minor.class}Vector(field, allocator);
        case OPTIONAL:
          return new Nullable${minor.class}Vector(field, allocator);
        case REPEATED:
          return new Repeated${minor.class}Vector(field, allocator);
      }
  </#list>
</#list>
    case GENERIC_OBJECT:
      return new ObjectVector(field, allocator)        ;
    default:
      break;
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(buildErrorMessage("get new vector", type));
  }

  public static ValueHolder getValue(ValueVector vector, int index) {
    MajorType type = vector.getField().getType();
    ValueHolder holder;
    switch(type.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case} :
      <#if minor.class?starts_with("Var") || minor.class == "IntervalDay" || minor.class == "Interval" ||
        minor.class?starts_with("Decimal28") ||  minor.class?starts_with("Decimal38")>
        switch (type.getMode()) {
          case REQUIRED:
            holder = new ${minor.class}Holder();
            ((${minor.class}Vector) vector).getAccessor().get(index, (${minor.class}Holder)holder);
            return holder;
          case OPTIONAL:
            holder = new Nullable${minor.class}Holder();
            ((Nullable${minor.class}Holder)holder).isSet = ((Nullable${minor.class}Vector) vector).getAccessor().isSet(index);
            if (((Nullable${minor.class}Holder)holder).isSet == 1) {
              ((Nullable${minor.class}Vector) vector).getAccessor().get(index, (Nullable${minor.class}Holder)holder);
            }
            return holder;
        }
      <#else>
      switch (type.getMode()) {
        case REQUIRED:
          holder = new ${minor.class}Holder();
          ((${minor.class}Holder)holder).value = ((${minor.class}Vector) vector).getAccessor().get(index);
          return holder;
        case OPTIONAL:
          holder = new Nullable${minor.class}Holder();
          ((Nullable${minor.class}Holder)holder).isSet = ((Nullable${minor.class}Vector) vector).getAccessor().isSet(index);
          if (((Nullable${minor.class}Holder)holder).isSet == 1) {
            ((Nullable${minor.class}Holder)holder).value = ((Nullable${minor.class}Vector) vector).getAccessor().get(index);
          }
          return holder;
      }
      </#if>
  </#list>
</#list>
    case GENERIC_OBJECT:
      holder = new ObjectHolder();
      ((ObjectHolder)holder).obj = ((ObjectVector) vector).getAccessor().getObject(index)         ;
      break;
    }

    throw new UnsupportedOperationException(buildErrorMessage("get value", type));
  }

  public static void setValue(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case} :
      switch (type.getMode()) {
        case REQUIRED:
          ((${minor.class}Vector) vector).getMutator().setSafe(index, (${minor.class}Holder) holder);
          return;
        case OPTIONAL:
          if (((Nullable${minor.class}Holder) holder).isSet == 1) {
            ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (Nullable${minor.class}Holder) holder);
          }
          return;
      }
  </#list>
</#list>
    case GENERIC_OBJECT:
      ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
      return;
    default:
      throw new UnsupportedOperationException(buildErrorMessage("set value", type));
    }
  }

  public static void setValueSafe(ValueVector vector, int index, ValueHolder holder) {
    MajorType type = vector.getField().getType();

    switch(type.getMinorType()) {
      <#list vv.types as type>
      <#list type.minor as minor>
      case ${minor.class?upper_case} :
      switch (type.getMode()) {
        case REQUIRED:
          ((${minor.class}Vector) vector).getMutator().setSafe(index, (${minor.class}Holder) holder);
          return;
        case OPTIONAL:
          if (((Nullable${minor.class}Holder) holder).isSet == 1) {
            ((Nullable${minor.class}Vector) vector).getMutator().setSafe(index, (Nullable${minor.class}Holder) holder);
          } else {
            ((Nullable${minor.class}Vector) vector).getMutator().isSafe(index);
          }
          return;
      }
      </#list>
      </#list>
      case GENERIC_OBJECT:
        ((ObjectVector) vector).getMutator().setSafe(index, (ObjectHolder) holder);
      default:
        throw new UnsupportedOperationException(buildErrorMessage("set value safe", type));
    }
  }

  public static boolean compareValues(ValueVector v1, int v1index, ValueVector v2, int v2index) {
    MajorType type1 = v1.getField().getType();
    MajorType type2 = v2.getField().getType();

    if (type1.getMinorType() != type2.getMinorType()) {
      return false;
    }

    switch(type1.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case} :
      if ( ((${minor.class}Vector) v1).getAccessor().get(v1index) == 
           ((${minor.class}Vector) v2).getAccessor().get(v2index) ) 
        return true;
      break;
  </#list>
</#list>
    default:
      break;
    }
    return false;
  }

  /**
   *  Create a ValueHolder of MajorType.
   * @param type
   * @return
   */
  public static ValueHolder createValueHolder(MajorType type) {
    switch(type.getMinorType()) {
      <#list vv.types as type>
      <#list type.minor as minor>
      case ${minor.class?upper_case} :

        switch (type.getMode()) {
          case REQUIRED:
            return new ${minor.class}Holder();
          case OPTIONAL:
            return new Nullable${minor.class}Holder();
          case REPEATED:
            return new Repeated${minor.class}Holder();
        }
      </#list>
      </#list>
      case GENERIC_OBJECT:
        return new ObjectHolder();
      default:
        throw new UnsupportedOperationException(buildErrorMessage("create value holder", type));
    }
  }

  public static boolean isNull(ValueHolder holder) {
    MajorType type = getValueHolderType(holder);

    switch(type.getMinorType()) {
      <#list vv.types as type>
      <#list type.minor as minor>
      case ${minor.class?upper_case} :

      switch (type.getMode()) {
        case REQUIRED:
          return true;
        case OPTIONAL:
          return ((Nullable${minor.class}Holder) holder).isSet == 0;
        case REPEATED:
          return true;
      }
      </#list>
      </#list>
      default:
        throw new UnsupportedOperationException(buildErrorMessage("check is null", type));
    }
  }

  public static ValueHolder deNullify(ValueHolder holder) {
    MajorType type = getValueHolderType(holder);

    switch(type.getMinorType()) {
      <#list vv.types as type>
      <#list type.minor as minor>
      case ${minor.class?upper_case} :

        switch (type.getMode()) {
          case REQUIRED:
            return holder;
          case OPTIONAL:
            if( ((Nullable${minor.class}Holder) holder).isSet == 1) {
              ${minor.class}Holder newHolder = new ${minor.class}Holder();

              <#assign fields = minor.fields!type.fields />
              <#list fields as field>
              newHolder.${field.name} = ((Nullable${minor.class}Holder) holder).${field.name};
              </#list>

              return newHolder;
            } else {
              throw new UnsupportedOperationException("You can not convert a null value into a non-null value!");
            }
          case REPEATED:
            return holder;
        }
      </#list>
      </#list>
      default:
        throw new UnsupportedOperationException(buildErrorMessage("deNullify", type));
    }
  }

  public static ValueHolder nullify(ValueHolder holder) {
    MajorType type = getValueHolderType(holder);

    switch(type.getMinorType()) {
      <#list vv.types as type>
      <#list type.minor as minor>
      case ${minor.class?upper_case} :
        switch (type.getMode()) {
          case REQUIRED:
            Nullable${minor.class}Holder newHolder = new Nullable${minor.class}Holder();
            newHolder.isSet = 1;
            <#assign fields = minor.fields!type.fields />
            <#list fields as field>
            newHolder.${field.name} = ((${minor.class}Holder) holder).${field.name};
            </#list>
            return newHolder;
          case OPTIONAL:
            return holder;
          case REPEATED:
            throw new UnsupportedOperationException("You can not convert repeated type " + type + " to nullable type!");
        }
      </#list>
      </#list>
      default:
        throw new UnsupportedOperationException(buildErrorMessage("nullify", type));
    }
  }

  public static MajorType getValueHolderType(ValueHolder holder) {

    if (0 == 1) {
      return null;
    }
    <#list vv.types as type>
    <#list type.minor as minor>
      else if (holder instanceof ${minor.class}Holder) {
        return ((${minor.class}Holder) holder).TYPE;
      } else if (holder instanceof Nullable${minor.class}Holder) {
      return ((Nullable${minor.class}Holder) holder).TYPE;
      }
    </#list>
    </#list>

    throw new UnsupportedOperationException("ValueHolder is not supported for 'getValueHolderType' method.");

  }

}
