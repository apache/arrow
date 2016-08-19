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
<#list vv.types as type>
<#list type.minor as minor>
<#list ["", "Nullable"] as holderMode>
<#assign nullMode = holderMode />

<#assign lowerName = minor.class?uncap_first />
<#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
<#assign name = minor.class?cap_first />
<#assign javaType = (minor.javaType!type.javaType) />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#assign safeType=friendlyType />
<#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>
<#assign fields = minor.fields!type.fields />

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${holderMode}${name}HolderReaderImpl.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.Period;

// Source code generated using FreeMarker template ${.template_name}

@SuppressWarnings("unused")
public class ${holderMode}${name}HolderReaderImpl extends AbstractFieldReader {

  private ${nullMode}${name}Holder holder;
  public ${holderMode}${name}HolderReaderImpl(${holderMode}${name}Holder holder) {
    this.holder = holder;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("You can't call size on a Holder value reader.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");

  }

  @Override
  public void setPosition(int index) {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");
  }

  @Override
  public MinorType getMinorType() {
        return MinorType.${name?upper_case};
  }

  @Override
  public boolean isSet() {
    <#if holderMode == "Nullable">
    return this.holder.isSet == 1;
    <#else>
    return true;
    </#if>
    
  }

@Override
  public void read(${name}Holder h) {
  <#list fields as field>
    h.${field.name} = holder.${field.name};
  </#list>
  }

  @Override
  public void read(Nullable${name}Holder h) {
  <#list fields as field>
    h.${field.name} = holder.${field.name};
  </#list>
    h.isSet = isSet() ? 1 : 0;
  }


  @Override
  public ${friendlyType} read${safeType}(){
<#if nullMode == "Nullable">
    if (!isSet()) {
      return null;
    }
</#if>

<#if type.major == "VarLen">

      int length = holder.end - holder.start;
      byte[] value = new byte [length];
      holder.buffer.getBytes(holder.start, value, 0, length);

<#if minor.class == "VarBinary">
      return value;
<#elseif minor.class == "Var16Char">
      return new String(value);
<#elseif minor.class == "VarChar">
      Text text = new Text();
      text.set(value);
      return text;
</#if>

<#elseif minor.class == "Interval">
      Period p = new Period();
      return p.plusMonths(holder.months).plusDays(holder.days).plusMillis(holder.milliseconds);

<#elseif minor.class == "IntervalDay">
      Period p = new Period();
      return p.plusDays(holder.days).plusMillis(holder.milliseconds);

<#elseif minor.class == "Bit" >
      return new Boolean(holder.value != 0);
<#elseif minor.class == "Decimal" >
        return (BigDecimal) readSingleObject();
<#else>
      ${friendlyType} value = new ${friendlyType}(this.holder.value);
      return value;
</#if>

  }

  @Override
  public Object readObject() {
    return readSingleObject();
  }

  private Object readSingleObject() {
<#if nullMode == "Nullable">
    if (!isSet()) {
      return null;
    }
</#if>

<#if type.major == "VarLen">
      <#if minor.class != "Decimal">
      int length = holder.end - holder.start;
      byte[] value = new byte [length];
      holder.buffer.getBytes(holder.start, value, 0, length);
      </#if>

<#if minor.class == "VarBinary">
      return value;
<#elseif minor.class == "Var16Char">
      return new String(value);
<#elseif minor.class == "VarChar">
      Text text = new Text();
      text.set(value);
      return text;
<#elseif minor.class == "Decimal" >
      return org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(holder.buffer, holder.start, holder.scale);
</#if>

<#elseif minor.class == "Interval">
      Period p = new Period();
      return p.plusMonths(holder.months).plusDays(holder.days).plusMillis(holder.milliseconds);

<#elseif minor.class == "IntervalDay">
      Period p = new Period();
      return p.plusDays(holder.days).plusMillis(holder.milliseconds);

<#elseif minor.class == "Decimal28Dense" ||
         minor.class == "Decimal38Dense">
      return org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromDense(holder.buffer,
                                                                                holder.start,
                                                                                holder.nDecimalDigits,
                                                                                holder.scale,
                                                                                holder.maxPrecision,
                                                                                holder.WIDTH);

<#elseif minor.class == "Decimal28Sparse" ||
         minor.class == "Decimal38Sparse">
      return org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromSparse(holder.buffer,
                                                                                 holder.start,
                                                                                 holder.nDecimalDigits,
                                                                                 holder.scale);

<#elseif minor.class == "Bit" >
      return new Boolean(holder.value != 0);
<#elseif minor.class == "Decimal">
        byte[] bytes = new byte[${type.width}];
        holder.buffer.getBytes(holder.start, bytes, 0, ${type.width});
        ${friendlyType} value = new BigDecimal(new BigInteger(bytes), holder.scale);
        return value;
<#else>
      ${friendlyType} value = new ${friendlyType}(this.holder.value);
      return value;
</#if>
  }

<#if nullMode != "Nullable">
  public void copyAsValue(${minor.class?cap_first}Writer writer){
    writer.write(holder);
  }
</#if>
}

</#list>
</#list>
</#list>
