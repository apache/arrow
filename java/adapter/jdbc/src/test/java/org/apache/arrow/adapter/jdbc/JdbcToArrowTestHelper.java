/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.Charset;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * This is a Helper class which has functionalities to read and assert the values from the given FieldVector object
 */
public class JdbcToArrowTestHelper {

  public static void assertIntVectorValues(IntVector intVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, intVector.getValueCount());

    for (int j = 0; j < intVector.getValueCount(); j++) {
      assertEquals(values[j].intValue(), intVector.get(j));
    }
  }

  public static void assertBooleanVectorValues(BitVector bitVector, int rowCount, Boolean[] values) {
    assertEquals(rowCount, bitVector.getValueCount());

    for (int j = 0; j < bitVector.getValueCount(); j++) {
      assertEquals(values[j].booleanValue(), bitVector.get(j) == 1);
    }
  }

  public static void assertBitVectorValues(BitVector bitVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, bitVector.getValueCount());

    for (int j = 0; j < bitVector.getValueCount(); j++) {
      assertEquals(values[j].intValue(), bitVector.get(j));
    }
  }

  public static void assertTinyIntVectorValues(TinyIntVector tinyIntVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, tinyIntVector.getValueCount());

    for (int j = 0; j < tinyIntVector.getValueCount(); j++) {
      assertEquals(values[j].intValue(), tinyIntVector.get(j));
    }
  }

  public static void assertSmallIntVectorValues(SmallIntVector smallIntVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, smallIntVector.getValueCount());

    for (int j = 0; j < smallIntVector.getValueCount(); j++) {
      assertEquals(values[j].intValue(), smallIntVector.get(j));
    }
  }

  public static void assertBigIntVectorValues(BigIntVector bigIntVector, int rowCount, Long[] values) {
    assertEquals(rowCount, bigIntVector.getValueCount());

    for (int j = 0; j < bigIntVector.getValueCount(); j++) {
      assertEquals(values[j].longValue(), bigIntVector.get(j));
    }
  }

  public static void assertDecimalVectorValues(DecimalVector decimalVector, int rowCount, BigDecimal[] values) {
    assertEquals(rowCount, decimalVector.getValueCount());

    for (int j = 0; j < decimalVector.getValueCount(); j++) {
      assertNotNull(decimalVector.getObject(j));
      assertEquals(values[j].doubleValue(), decimalVector.getObject(j).doubleValue(), 0);
    }
  }

  public static void assertFloat8VectorValues(Float8Vector float8Vector, int rowCount, Double[] values) {
    assertEquals(rowCount, float8Vector.getValueCount());

    for (int j = 0; j < float8Vector.getValueCount(); j++) {
      assertEquals(values[j], float8Vector.get(j), 0.01);
    }
  }

  public static void assertFloat4VectorValues(Float4Vector float4Vector, int rowCount, Float[] values) {
    assertEquals(rowCount, float4Vector.getValueCount());

    for (int j = 0; j < float4Vector.getValueCount(); j++) {
      assertEquals(values[j], float4Vector.get(j), 0.01);
    }
  }

  public static void assertTimeVectorValues(TimeMilliVector timeMilliVector, int rowCount, Long[] values) {
    assertEquals(rowCount, timeMilliVector.getValueCount());

    for (int j = 0; j < timeMilliVector.getValueCount(); j++) {
      assertEquals(values[j].longValue(), timeMilliVector.get(j));
    }
  }

  public static void assertDateVectorValues(DateMilliVector dateMilliVector, int rowCount, Long[] values) {
    assertEquals(rowCount, dateMilliVector.getValueCount());

    for (int j = 0; j < dateMilliVector.getValueCount(); j++) {
      assertEquals(values[j].longValue(), dateMilliVector.get(j));
    }
  }

  public static void assertTimeStampVectorValues(TimeStampVector timeStampVector, int rowCount, Long[] values) {
    assertEquals(rowCount, timeStampVector.getValueCount());

    for (int j = 0; j < timeStampVector.getValueCount(); j++) {
      assertEquals(values[j].longValue(), timeStampVector.get(j));
    }
  }

  public static void assertVarBinaryVectorValues(VarBinaryVector varBinaryVector, int rowCount, byte[][] values) {
    assertEquals(rowCount, varBinaryVector.getValueCount());

    for (int j = 0; j < varBinaryVector.getValueCount(); j++) {
      assertArrayEquals(values[j], varBinaryVector.get(j));
    }
  }

  public static void assertVarcharVectorValues(VarCharVector varCharVector, int rowCount, byte[][] values) {
    assertEquals(rowCount, varCharVector.getValueCount());

    for (int j = 0; j < varCharVector.getValueCount(); j++) {
      assertArrayEquals(values[j], varCharVector.get(j));
    }
  }

  public static void assertNullValues(BaseValueVector vector, int rowCount) {
    assertEquals(rowCount, vector.getValueCount());

    for (int j = 0; j < vector.getValueCount(); j++) {
      assertTrue(vector.isNull(j));
    }
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
              + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static Integer[] getIntValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Integer[] valueArr = new Integer[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = Integer.parseInt(data);
    }
    return valueArr;
  }

  public static Boolean[] getBooleanValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Boolean[] valueArr = new Boolean[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = data.trim().equals("1");
    }
    return valueArr;
  }

  public static BigDecimal[] getDecimalValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    BigDecimal[] valueArr = new BigDecimal[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = new BigDecimal(data);
    }
    return valueArr;
  }

  public static Double[] getDoubleValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Double[] valueArr = new Double[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = Double.parseDouble(data);
    }
    return valueArr;
  }

  public static Float[] getFloatValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Float[] valueArr = new Float[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = Float.parseFloat(data);
    }
    return valueArr;
  }

  public static Long[] getLongValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Long[] valueArr = new Long[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = Long.parseLong(data);
    }
    return valueArr;
  }

  public static byte[][] getCharArray(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    byte[][] valueArr = new byte[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = data.trim().getBytes();
    }
    return valueArr;
  }

  public static byte[][] getCharArrayWithCharSet(String[] values, String dataType, Charset charSet) {
    String[] dataArr = getValues(values, dataType);
    byte[][] valueArr = new byte[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = data.trim().getBytes(charSet);
    }
    return valueArr;
  }

  public static byte[][] getBinaryValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    byte[][] valueArr = new byte[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = hexStringToByteArray(data.trim());
    }
    return valueArr;
  }

  public static String[] getValues(String[] values, String dataType) {
    String value = "";
    for (String val : values) {
      if (val.startsWith(dataType)) {
        value = val.split("=")[1];
        break;
      }
    }
    return value.split(",");
  }
}
