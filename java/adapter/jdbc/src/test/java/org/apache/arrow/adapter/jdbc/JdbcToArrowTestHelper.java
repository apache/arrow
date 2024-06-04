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

package org.apache.arrow.adapter.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.ObjectMapperFactory;
import org.apache.arrow.vector.util.Text;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This is a Helper class which has functionalities to read and assert the values from the given FieldVector object.
 */
public class JdbcToArrowTestHelper {

  public static void assertIntVectorValues(IntVector intVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, intVector.getValueCount());

    for (int j = 0; j < intVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(intVector.isNull(j));
      } else {
        assertEquals(values[j].intValue(), intVector.get(j));
      }
    }
  }

  public static void assertBooleanVectorValues(BitVector bitVector, int rowCount, Boolean[] values) {
    assertEquals(rowCount, bitVector.getValueCount());

    for (int j = 0; j < bitVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(bitVector.isNull(j));
      } else {
        assertEquals(values[j].booleanValue(), bitVector.get(j) == 1);
      }
    }
  }

  public static void assertBitVectorValues(BitVector bitVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, bitVector.getValueCount());

    for (int j = 0; j < bitVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(bitVector.isNull(j));
      } else {
        assertEquals(values[j].intValue(), bitVector.get(j));
      }
    }
  }

  public static void assertTinyIntVectorValues(TinyIntVector tinyIntVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, tinyIntVector.getValueCount());

    for (int j = 0; j < tinyIntVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(tinyIntVector.isNull(j));
      } else {
        assertEquals(values[j].intValue(), tinyIntVector.get(j));
      }
    }
  }

  public static void assertSmallIntVectorValues(SmallIntVector smallIntVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, smallIntVector.getValueCount());

    for (int j = 0; j < smallIntVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(smallIntVector.isNull(j));
      } else {
        assertEquals(values[j].intValue(), smallIntVector.get(j));
      }
    }
  }

  public static void assertBigIntVectorValues(BigIntVector bigIntVector, int rowCount, Long[] values) {
    assertEquals(rowCount, bigIntVector.getValueCount());

    for (int j = 0; j < bigIntVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(bigIntVector.isNull(j));
      } else {
        assertEquals(values[j].longValue(), bigIntVector.get(j));
      }
    }
  }

  public static void assertDecimalVectorValues(DecimalVector decimalVector, int rowCount, BigDecimal[] values) {
    assertEquals(rowCount, decimalVector.getValueCount());

    for (int j = 0; j < decimalVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(decimalVector.isNull(j));
      } else {
        assertEquals(values[j].doubleValue(), decimalVector.getObject(j).doubleValue(), 0);
      }
    }
  }

  public static void assertFloat8VectorValues(Float8Vector float8Vector, int rowCount, Double[] values) {
    assertEquals(rowCount, float8Vector.getValueCount());

    for (int j = 0; j < float8Vector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(float8Vector.isNull(j));
      } else {
        assertEquals(values[j], float8Vector.get(j), 0.01);
      }
    }
  }

  public static void assertFloat4VectorValues(Float4Vector float4Vector, int rowCount, Float[] values) {
    assertEquals(rowCount, float4Vector.getValueCount());

    for (int j = 0; j < float4Vector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(float4Vector.isNull(j));
      } else {
        assertEquals(values[j], float4Vector.get(j), 0.01);
      }
    }
  }

  public static void assertTimeVectorValues(TimeMilliVector timeMilliVector, int rowCount, Long[] values) {
    assertEquals(rowCount, timeMilliVector.getValueCount());

    for (int j = 0; j < timeMilliVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(timeMilliVector.isNull(j));
      } else {
        assertEquals(values[j].longValue(), timeMilliVector.get(j));
      }
    }
  }

  public static void assertDateVectorValues(DateDayVector dateDayVector, int rowCount, Integer[] values) {
    assertEquals(rowCount, dateDayVector.getValueCount());

    for (int j = 0; j < dateDayVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(dateDayVector.isNull(j));
      } else {
        assertEquals(values[j].longValue(), dateDayVector.get(j));
      }
    }
  }

  public static void assertTimeStampVectorValues(TimeStampVector timeStampVector, int rowCount, Long[] values) {
    assertEquals(rowCount, timeStampVector.getValueCount());

    for (int j = 0; j < timeStampVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(timeStampVector.isNull(j));
      } else {
        assertEquals(values[j].longValue(), timeStampVector.get(j));
      }
    }
  }

  public static void assertVarBinaryVectorValues(VarBinaryVector varBinaryVector, int rowCount, byte[][] values) {
    assertEquals(rowCount, varBinaryVector.getValueCount());

    for (int j = 0; j < varBinaryVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(varBinaryVector.isNull(j));
      } else {
        assertArrayEquals(values[j], varBinaryVector.get(j));
      }
    }
  }

  public static void assertVarcharVectorValues(VarCharVector varCharVector, int rowCount, byte[][] values) {
    assertEquals(rowCount, varCharVector.getValueCount());

    for (int j = 0; j < varCharVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(varCharVector.isNull(j));
      } else {
        assertArrayEquals(values[j], varCharVector.get(j));
      }
    }
  }

  public static void assertNullVectorValues(NullVector vector, int rowCount) {
    assertEquals(rowCount, vector.getValueCount());
  }

  public static void assertListVectorValues(ListVector listVector, int rowCount, Integer[][] values) {
    assertEquals(rowCount, listVector.getValueCount());

    for (int j = 0; j < listVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(listVector.isNull(j));
      } else {
        List<Integer> list = (List<Integer>) listVector.getObject(j);
        assertEquals(Arrays.asList(values[j]), list);
      }
    }
  }

  public static void assertMapVectorValues(MapVector mapVector, int rowCount, Map<String, String>[] values) {
    assertEquals(rowCount, mapVector.getValueCount());

    for (int j = 0; j < mapVector.getValueCount(); j++) {
      if (values[j] == null) {
        assertTrue(mapVector.isNull(j));
      } else {
        JsonStringArrayList<JsonStringHashMap<String, Text>> actualSource =
            (JsonStringArrayList<JsonStringHashMap<String, Text>>) mapVector.getObject(j);
        Map<String, String> actualMap = null;
        if (actualSource != null && !actualSource.isEmpty()) {
          actualMap = actualSource.stream().map(entry ->
            new AbstractMap.SimpleEntry<>(entry.get("key").toString(),
                    entry.get("value") != null ? entry.get("value").toString() : null))
          .collect(HashMap::new, (collector, val) -> collector.put(val.getKey(), val.getValue()), HashMap::putAll);
        }
        assertEquals(values[j], actualMap);
      }
    }
  }

  public static Map<String, String>[] getMapValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Map<String, String>[] maps = new Map[dataArr.length];
    ObjectMapper objectMapper = ObjectMapperFactory.newObjectMapper();
    TypeReference<Map<String, String>> typeReference = new TypeReference<Map<String, String>>() {};
    for (int idx = 0; idx < dataArr.length; idx++) {
      String jsonString = dataArr[idx].replace("|", ",");
      if (!jsonString.isEmpty()) {
        try {
          maps[idx] = objectMapper.readValue(jsonString, typeReference);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return maps;
  }

  public static void assertNullValues(BaseValueVector vector, int rowCount) {
    assertEquals(rowCount, vector.getValueCount());

    for (int j = 0; j < vector.getValueCount(); j++) {
      assertTrue(vector.isNull(j));
    }
  }

  public static void assertFieldMetadataIsEmpty(VectorSchemaRoot schema) {
    assertNotNull(schema);
    assertNotNull(schema.getSchema());
    assertNotNull(schema.getSchema().getFields());

    for (Field field : schema.getSchema().getFields()) {
      assertNotNull(field.getMetadata());
      assertEquals(0, field.getMetadata().size());
    }
  }

  public static void assertFieldMetadataMatchesResultSetMetadata(ResultSetMetaData rsmd, Schema schema)
      throws SQLException {
    assertNotNull(schema);
    assertNotNull(schema.getFields());
    assertNotNull(rsmd);

    List<Field> fields = schema.getFields();

    assertEquals(rsmd.getColumnCount(), fields.size());

    // Vector columns are created in the same order as ResultSet columns.
    for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
      Map<String, String> metadata = fields.get(i - 1).getMetadata();

      assertNotNull(metadata);
      assertEquals(5, metadata.size());

      assertEquals(rsmd.getCatalogName(i), metadata.get(Constants.SQL_CATALOG_NAME_KEY));
      assertEquals(rsmd.getSchemaName(i), metadata.get(Constants.SQL_SCHEMA_NAME_KEY));
      assertEquals(rsmd.getTableName(i), metadata.get(Constants.SQL_TABLE_NAME_KEY));
      assertEquals(rsmd.getColumnLabel(i), metadata.get(Constants.SQL_COLUMN_NAME_KEY));
      assertEquals(rsmd.getColumnTypeName(i), metadata.get(Constants.SQL_TYPE_KEY));
    }
  }

  public static Integer[] getIntValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Integer[] valueArr = new Integer[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : Integer.parseInt(data);
    }
    return valueArr;
  }

  public static Boolean[] getBooleanValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Boolean[] valueArr = new Boolean[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : data.trim().equals("1");
    }
    return valueArr;
  }

  public static BigDecimal[] getDecimalValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    BigDecimal[] valueArr = new BigDecimal[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : new BigDecimal(data);
    }
    return valueArr;
  }

  public static Double[] getDoubleValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Double[] valueArr = new Double[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : Double.parseDouble(data);
    }
    return valueArr;
  }

  public static Float[] getFloatValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Float[] valueArr = new Float[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : Float.parseFloat(data);
    }
    return valueArr;
  }

  public static Long[] getLongValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    Long[] valueArr = new Long[dataArr.length];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : Long.parseLong(data);
    }
    return valueArr;
  }

  public static byte[][] getCharArray(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    byte[][] valueArr = new byte[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : data.trim().getBytes(StandardCharsets.UTF_8);
    }
    return valueArr;
  }

  public static byte[][] getCharArrayWithCharSet(String[] values, String dataType, Charset charSet) {
    String[] dataArr = getValues(values, dataType);
    byte[][] valueArr = new byte[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : data.trim().getBytes(charSet);
    }
    return valueArr;
  }

  public static byte[][] getBinaryValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    byte[][] valueArr = new byte[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      valueArr[i++] = "null".equals(data.trim()) ? null : data.trim().getBytes(StandardCharsets.UTF_8);
    }
    return valueArr;
  }

  @SuppressWarnings("StringSplitter")
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

  public static Integer[][] getListValues(String[] values, String dataType) {
    String[] dataArr = getValues(values, dataType);
    return getListValues(dataArr);
  }

  @SuppressWarnings("StringSplitter")
  public static Integer[][] getListValues(String[] dataArr) {
    Integer[][] valueArr = new Integer[dataArr.length][];
    int i = 0;
    for (String data : dataArr) {
      if ("null".equals(data.trim())) {
        valueArr[i++] = null;
      } else if ("()".equals(data.trim())) {
        valueArr[i++] = new Integer[0];
      } else {
        String[] row = data.replace("(", "").replace(")", "").split(";");
        Integer[] arr = new Integer[row.length];
        for (int j = 0; j < arr.length; j++) {
          arr[j] = "null".equals(row[j]) ? null : Integer.parseInt(row[j]);
        }
        valueArr[i++] = arr;
      }
    }
    return valueArr;
  }
}
