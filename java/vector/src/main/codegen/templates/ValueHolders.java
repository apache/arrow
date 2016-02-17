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
<#list vv.modes as mode>
<#list vv.types as type>
<#list type.minor as minor>

<#assign className="${mode.prefix}${minor.class}Holder" />
<@pp.changeOutputFile name="/org/apache/arrow/vector/holders/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.holders;

<#include "/@includes/vv_imports.ftl" />

public final class ${className} implements ValueHolder{
  
  public static final MajorType TYPE = new MajorType(MinorType.${minor.class?upper_case}, DataMode.${mode.name?upper_case});

  public MajorType getType() {return TYPE;}
  
    <#if mode.name == "Repeated">
    
    /** The first index (inclusive) into the Vector. **/
    public int start;
    
    /** The last index (exclusive) into the Vector. **/
    public int end;
    
    /** The Vector holding the actual values. **/
    public ${minor.class}Vector vector;
    
    <#else>
    public static final int WIDTH = ${type.width};
    
    <#if mode.name == "Optional">public int isSet;</#if>
    <#assign fields = minor.fields!type.fields />
    <#list fields as field>
    public ${field.type} ${field.name};
    </#list>
    
    <#if minor.class.startsWith("Decimal")>
    public static final int maxPrecision = ${minor.maxPrecisionDigits};
    <#if minor.class.startsWith("Decimal28") || minor.class.startsWith("Decimal38")>
    public static final int nDecimalDigits = ${minor.nDecimalDigits};
    
    public static int getInteger(int index, int start, ArrowBuf buffer) {
      int value = buffer.getInt(start + (index * 4));

      if (index == 0) {
          /* the first byte contains sign bit, return value without it */
          <#if minor.class.endsWith("Sparse")>
          value = (value & 0x7FFFFFFF);
          <#elseif minor.class.endsWith("Dense")>
          value = (value & 0x0000007F);
          </#if>
      }
      return value;
    }

    public static void setInteger(int index, int value, int start, ArrowBuf buffer) {
        buffer.setInt(start + (index * 4), value);
    }
  
    public static void setSign(boolean sign, int start, ArrowBuf buffer) {
      // Set MSB to 1 if sign is negative
      if (sign == true) {
        int value = getInteger(0, start, buffer);
        setInteger(0, (value | 0x80000000), start, buffer);
      }
    }
  
    public static boolean getSign(int start, ArrowBuf buffer) {
      return ((buffer.getInt(start) & 0x80000000) != 0);
    }
    </#if></#if>
    
    @Deprecated
    public int hashCode(){
      throw new UnsupportedOperationException();
    }

    /*
     * Reason for deprecation is that ValueHolders are potential scalar replacements
     * and hence we don't want any methods to be invoked on them.
     */
    @Deprecated
    public String toString(){
      throw new UnsupportedOperationException();
    }
    </#if>
    
    
    
    
}

</#list>
</#list>
</#list>