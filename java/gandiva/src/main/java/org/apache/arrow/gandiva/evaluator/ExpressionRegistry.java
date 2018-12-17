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

package org.apache.arrow.gandiva.evaluator;

import java.util.List;
import java.util.Set;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.gandiva.ipc.GandivaTypes.ExtGandivaType;
import org.apache.arrow.gandiva.ipc.GandivaTypes.GandivaDataTypes;
import org.apache.arrow.gandiva.ipc.GandivaTypes.GandivaFunctions;
import org.apache.arrow.gandiva.ipc.GandivaTypes.GandivaType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Used to get the functions and data types supported by
 * Gandiva.
 * All types are in Arrow namespace.
 */
public class ExpressionRegistry {

  private static final int BIT_WIDTH8 = 8;
  private static final int BIT_WIDTH_16 = 16;
  private static final int BIT_WIDTH_32 = 32;
  private static final int BIT_WIDTH_64 = 64;
  private static final boolean IS_SIGNED_FALSE = false;
  private static final boolean IS_SIGNED_TRUE = true;

  private final Set<ArrowType> supportedTypes;
  private final Set<FunctionSignature> functionSignatures;

  private static volatile ExpressionRegistry INSTANCE;

  private ExpressionRegistry(Set<ArrowType> supportedTypes,
                             Set<FunctionSignature> functionSignatures) {
    this.supportedTypes = supportedTypes;
    this.functionSignatures = functionSignatures;
  }

  /**
   * Returns a singleton instance of the class.
   * @return singleton instance
   * @throws GandivaException if error in Gandiva Library integration.
   */
  public static ExpressionRegistry getInstance() throws GandivaException {
    if (INSTANCE == null) {
      synchronized (ExpressionRegistry.class) {
        if (INSTANCE == null) {
          // ensure library is setup.
          JniLoader.getInstance();
          Set<ArrowType> typesFromGandiva = getSupportedTypesFromGandiva();
          Set<FunctionSignature> functionsFromGandiva = getSupportedFunctionsFromGandiva();
          INSTANCE = new ExpressionRegistry(typesFromGandiva, functionsFromGandiva);
        }
      }
    }
    return INSTANCE;
  }

  public Set<FunctionSignature> getSupportedFunctions() {
    return functionSignatures;
  }

  public Set<ArrowType> getSupportedTypes() {
    return supportedTypes;
  }

  private static Set<ArrowType> getSupportedTypesFromGandiva() throws GandivaException {
    Set<ArrowType> supportedTypes = Sets.newHashSet();
    try {
      byte[] gandivaSupportedDataTypes = new ExpressionRegistryJniHelper()
              .getGandivaSupportedDataTypes();
      GandivaDataTypes gandivaDataTypes = GandivaDataTypes.parseFrom(gandivaSupportedDataTypes);
      for (ExtGandivaType type : gandivaDataTypes.getDataTypeList()) {
        supportedTypes.add(getArrowType(type));
      }
    } catch (InvalidProtocolBufferException invalidProtException) {
      throw new GandivaException("Could not get supported types.", invalidProtException);
    }
    return supportedTypes;
  }

  private static Set<FunctionSignature> getSupportedFunctionsFromGandiva() throws
          GandivaException {
    Set<FunctionSignature> supportedTypes = Sets.newHashSet();
    try {
      byte[] gandivaSupportedFunctions = new ExpressionRegistryJniHelper()
              .getGandivaSupportedFunctions();
      GandivaFunctions gandivaFunctions = GandivaFunctions.parseFrom(gandivaSupportedFunctions);
      for (GandivaTypes.FunctionSignature protoFunctionSignature
              : gandivaFunctions.getFunctionList()) {

        String functionName = protoFunctionSignature.getName();
        ArrowType returnType = getArrowType(protoFunctionSignature.getReturnType());
        List<ArrowType> paramTypes = Lists.newArrayList();
        for (ExtGandivaType type : protoFunctionSignature.getParamTypesList()) {
          paramTypes.add(getArrowType(type));
        }
        FunctionSignature functionSignature = new FunctionSignature(functionName,
                                                                    returnType, paramTypes);
        supportedTypes.add(functionSignature);
      }
    } catch (InvalidProtocolBufferException invalidProtException) {
      throw new GandivaException("Could not get supported functions.", invalidProtException);
    }
    return supportedTypes;
  }

  private static ArrowType getArrowType(ExtGandivaType type) {
    switch (type.getType().getNumber()) {
      case GandivaType.BOOL_VALUE:
        return ArrowType.Bool.INSTANCE;
      case GandivaType.UINT8_VALUE:
        return new ArrowType.Int(BIT_WIDTH8, IS_SIGNED_FALSE);
      case GandivaType.INT8_VALUE:
        return new ArrowType.Int(BIT_WIDTH8, IS_SIGNED_TRUE);
      case GandivaType.UINT16_VALUE:
        return new ArrowType.Int(BIT_WIDTH_16, IS_SIGNED_FALSE);
      case GandivaType.INT16_VALUE:
        return new ArrowType.Int(BIT_WIDTH_16, IS_SIGNED_TRUE);
      case GandivaType.UINT32_VALUE:
        return new ArrowType.Int(BIT_WIDTH_32, IS_SIGNED_FALSE);
      case GandivaType.INT32_VALUE:
        return new ArrowType.Int(BIT_WIDTH_32, IS_SIGNED_TRUE);
      case GandivaType.UINT64_VALUE:
        return new ArrowType.Int(BIT_WIDTH_64, IS_SIGNED_FALSE);
      case GandivaType.INT64_VALUE:
        return new ArrowType.Int(BIT_WIDTH_64, IS_SIGNED_TRUE);
      case GandivaType.HALF_FLOAT_VALUE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
      case GandivaType.FLOAT_VALUE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case GandivaType.DOUBLE_VALUE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case GandivaType.UTF8_VALUE:
        return new ArrowType.Utf8();
      case GandivaType.BINARY_VALUE:
        return new ArrowType.Binary();
      case GandivaType.DATE32_VALUE:
        return new ArrowType.Date(DateUnit.DAY);
      case GandivaType.DATE64_VALUE:
        return new ArrowType.Date(DateUnit.MILLISECOND);
      case GandivaType.TIMESTAMP_VALUE:
        return new ArrowType.Timestamp(mapArrowTimeUnit(type.getTimeUnit()), null);
      case GandivaType.TIME32_VALUE:
        return new ArrowType.Time(mapArrowTimeUnit(type.getTimeUnit()),
                BIT_WIDTH_32);
      case GandivaType.TIME64_VALUE:
        return new ArrowType.Time(mapArrowTimeUnit(type.getTimeUnit()),
                BIT_WIDTH_64);
      case GandivaType.NONE_VALUE:
        return new ArrowType.Null();
      case GandivaType.DECIMAL_VALUE:
        return new ArrowType.Decimal(0,0);
      case GandivaType.FIXED_SIZE_BINARY_VALUE:
      case GandivaType.MAP_VALUE:
      case GandivaType.INTERVAL_VALUE:
      case GandivaType.DICTIONARY_VALUE:
      case GandivaType.LIST_VALUE:
      case GandivaType.STRUCT_VALUE:
      case GandivaType.UNION_VALUE:
      default:
        assert false;
    }
    return null;
  }

  private static TimeUnit mapArrowTimeUnit(GandivaTypes.TimeUnit timeUnit) {
    switch (timeUnit.getNumber()) {
      case GandivaTypes.TimeUnit.MICROSEC_VALUE:
        return TimeUnit.MICROSECOND;
      case GandivaTypes.TimeUnit.MILLISEC_VALUE:
        return TimeUnit.MILLISECOND;
      case GandivaTypes.TimeUnit.NANOSEC_VALUE:
        return TimeUnit.NANOSECOND;
      case GandivaTypes.TimeUnit.SEC_VALUE:
        return TimeUnit.SECOND;
      default:
        return null;
    }
  }
}

