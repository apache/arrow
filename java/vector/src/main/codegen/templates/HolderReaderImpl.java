/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
<#assign fields = (minor.fields!type.fields) + minor.typeParams![]/>

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${holderMode}${name}HolderReaderImpl.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

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

  // read friendly type
  @Override
  public ${friendlyType} read${safeType}() {
  <#if nullMode == "Nullable">
    if (!isSet()) {
      return null;
    }
  </#if>

  <#if type.major == "VarLen">
    <#if type.width == 4>
    int length = holder.end - holder.start;
    <#elseif type.width == 8>
    int length = (int) (holder.end - holder.start);
    </#if>
    byte[] value = new byte [length];
    holder.buffer.getBytes(holder.start, value, 0, length);
    <#if minor.class == "VarBinary" || minor.class == "LargeVarBinary" || minor.class == "ViewVarBinary">
    return value;
    <#elseif minor.class == "VarChar" || minor.class == "LargeVarChar" || minor.class == "ViewVarChar">
    Text text = new Text();
    text.set(value);
    return text;
    </#if>
  <#elseif minor.class == "IntervalDay">
    return Duration.ofDays(holder.days).plusMillis(holder.milliseconds);
  <#elseif minor.class == "IntervalYear">
    return Period.ofMonths(holder.value);
  <#elseif minor.class == "IntervalMonthDayNano">
    return new PeriodDuration(Period.ofMonths(holder.months).plusDays(holder.days),
        Duration.ofNanos(holder.nanoseconds));
  <#elseif minor.class == "Duration">
    return DurationVector.toDuration(holder.value, holder.unit);
  <#elseif minor.class == "Bit" >
    return new Boolean(holder.value != 0);
  <#elseif minor.class == "Decimal">
    byte[] bytes = new byte[${type.width}];
    holder.buffer.getBytes(holder.start, bytes, 0, ${type.width});
    ${friendlyType} value = new BigDecimal(new BigInteger(bytes), holder.scale);
    return value;
  <#elseif minor.class == "Decimal256">
    byte[] bytes = new byte[${type.width}];
    holder.buffer.getBytes(holder.start, bytes, 0, ${type.width});
    ${friendlyType} value = new BigDecimal(new BigInteger(bytes), holder.scale);
    return value;
  <#elseif minor.class == "FixedSizeBinary">
    byte[] value = new byte [holder.byteWidth];
    holder.buffer.getBytes(0, value, 0, holder.byteWidth);
    return value;
  <#elseif minor.class == "TimeStampSec">
    final long millis = java.util.concurrent.TimeUnit.SECONDS.toMillis(holder.value);
    return DateUtility.getLocalDateTimeFromEpochMilli(millis);
  <#elseif minor.class == "TimeStampMilli" || minor.class == "DateMilli" || minor.class == "TimeMilli">
    return DateUtility.getLocalDateTimeFromEpochMilli(holder.value);
  <#elseif minor.class == "TimeStampMicro">
    return DateUtility.getLocalDateTimeFromEpochMicro(holder.value);
  <#elseif minor.class == "TimeStampNano">
    return DateUtility.getLocalDateTimeFromEpochNano(holder.value);
  <#else>
    ${friendlyType} value = new ${friendlyType}(this.holder.value);
    return value;
  </#if>
  }

  @Override
  public Object readObject() {
    return read${safeType}();
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
