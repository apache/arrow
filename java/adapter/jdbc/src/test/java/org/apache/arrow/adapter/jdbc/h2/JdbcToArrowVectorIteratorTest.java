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

package org.apache.arrow.adapter.jdbc.h2;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class JdbcToArrowVectorIteratorTest extends JdbcToArrowTest {

  /**
   * Constructor which populate table object for each test iteration.
   *
   * @param table Table object
   */
  public JdbcToArrowVectorIteratorTest(Table table) {
    super(table);
  }

  @Override
  public void testJdbcToArroValues() throws SQLException, IOException {

    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE),
        Calendar.getInstance()).setTargetBatchSize(3).build();

    ArrowVectorIterator iterator =
        JdbcToArrow.sqlToArrowVectorIterator(conn.createStatement().executeQuery(table.getQuery()), config);

    validate(iterator);
  }

  private void validate(ArrowVectorIterator iterator) throws SQLException, IOException {

    List<BigIntVector> bigIntVectors = new ArrayList<>();
    List<TinyIntVector> tinyIntVectors = new ArrayList<>();
    List<IntVector> intVectors = new ArrayList<>();
    List<SmallIntVector> smallIntVectors = new ArrayList<>();
    List<VarBinaryVector> vectorsForBinary = new ArrayList<>();
    List<VarBinaryVector> vectorsForBlob = new ArrayList<>();
    List<VarCharVector> vectorsForClob = new ArrayList<>();
    List<VarCharVector> vectorsForVarChar = new ArrayList<>();
    List<VarCharVector> vectorsForChar = new ArrayList<>();
    List<BitVector> vectorsForBit = new ArrayList<>();
    List<BitVector> vectorsForBool = new ArrayList<>();
    List<DateMilliVector> dateMilliVectors = new ArrayList<>();
    List<TimeMilliVector> timeMilliVectors = new ArrayList<>();
    List<TimeStampVector> timeStampVectors = new ArrayList<>();
    List<DecimalVector> decimalVectors = new ArrayList<>();
    List<Float4Vector> float4Vectors = new ArrayList<>();
    List<Float8Vector> float8Vectors = new ArrayList<>();

    List<VectorSchemaRoot> roots = new ArrayList<>();
    while (iterator.hasNext()) {
      VectorSchemaRoot root = iterator.next();
      roots.add(root);
      JdbcToArrowTestHelper.assertFieldMetadataIsEmpty(root);

      bigIntVectors.add((BigIntVector) root.getVector(BIGINT));
      tinyIntVectors.add((TinyIntVector) root.getVector(TINYINT));
      intVectors.add((IntVector) root.getVector(INT));
      smallIntVectors.add((SmallIntVector) root.getVector(SMALLINT));
      vectorsForBinary.add((VarBinaryVector) root.getVector(BINARY));
      vectorsForBlob.add((VarBinaryVector) root.getVector(BLOB));
      vectorsForClob.add((VarCharVector) root.getVector(CLOB));
      vectorsForVarChar.add((VarCharVector) root.getVector(VARCHAR));
      vectorsForChar.add((VarCharVector) root.getVector(CHAR));
      vectorsForBit.add((BitVector) root.getVector(BIT));
      vectorsForBool.add((BitVector) root.getVector(BOOL));
      dateMilliVectors.add((DateMilliVector) root.getVector(DATE));
      timeMilliVectors.add((TimeMilliVector) root.getVector(TIME));
      timeStampVectors.add((TimeStampVector) root.getVector(TIMESTAMP));
      decimalVectors.add((DecimalVector) root.getVector(DECIMAL));
      float4Vectors.add((Float4Vector) root.getVector(REAL));
      float8Vectors.add((Float8Vector) root.getVector(DOUBLE));

    }
    assertBigIntVectorValues(bigIntVectors, table.getRowCount(), getLongValues(table.getValues(), BIGINT));
    assertTinyIntVectorValues(tinyIntVectors, table.getRowCount(), getIntValues(table.getValues(), TINYINT));
    assertIntVectorValues(intVectors, table.getRowCount(), getIntValues(table.getValues(), INT));
    assertSmallIntVectorValues(smallIntVectors, table.getRowCount(), getIntValues(table.getValues(), SMALLINT));
    assertBinaryVectorValues(vectorsForBinary, table.getRowCount(), getBinaryValues(table.getValues(), BINARY));
    assertBinaryVectorValues(vectorsForBlob, table.getRowCount(), getBinaryValues(table.getValues(), BLOB));
    assertVarCharVectorValues(vectorsForClob, table.getRowCount(), getCharArray(table.getValues(), CLOB));
    assertVarCharVectorValues(vectorsForVarChar, table.getRowCount(), getCharArray(table.getValues(), VARCHAR));
    assertVarCharVectorValues(vectorsForChar, table.getRowCount(), getCharArray(table.getValues(), CHAR));
    assertBitVectorValues(vectorsForBit, table.getRowCount(), getIntValues(table.getValues(), BIT));
    assertBooleanVectorValues(vectorsForBool, table.getRowCount(), getBooleanValues(table.getValues(), BOOL));
    assertDateMilliVectorValues(dateMilliVectors, table.getRowCount(), getLongValues(table.getValues(), DATE));
    assertTimeMilliVectorValues(timeMilliVectors, table.getRowCount(), getLongValues(table.getValues(), TIME));
    assertTimeStampVectorValues(timeStampVectors, table.getRowCount(), getLongValues(table.getValues(), TIMESTAMP));
    assertDecimalVectorValues(decimalVectors, table.getRowCount(), getDecimalValues(table.getValues(), DECIMAL));
    assertFloat4VectorValues(float4Vectors, table.getRowCount(), getFloatValues(table.getValues(), REAL));
    assertFloat8VectorValues(float8Vectors, table.getRowCount(), getDoubleValues(table.getValues(), DOUBLE));

    roots.forEach(root -> root.close());
  }

  private void assertFloat8VectorValues(List<Float8Vector> vectors, int rowCount, Double[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (Float8Vector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].doubleValue(), vector.get(i), 0.01);
      }
    }
  }

  private void assertFloat4VectorValues(List<Float4Vector> vectors, int rowCount, Float[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (Float4Vector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].floatValue(), vector.get(i), 0.01);
      }
    }
  }

  private void assertDecimalVectorValues(List<DecimalVector> vectors, int rowCount, BigDecimal[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (DecimalVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertNotNull(vector.getObject(i));
        assertEquals(values[index++].doubleValue(), vector.getObject(i).doubleValue(), 0);
      }
    }
  }

  private void assertTimeStampVectorValues(List<TimeStampVector> vectors, int rowCount, Long[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (TimeStampVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].longValue(), vector.get(i));
      }
    }
  }

  private void assertTimeMilliVectorValues(List<TimeMilliVector> vectors, int rowCount, Long[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (TimeMilliVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].longValue(), vector.get(i));
      }
    }
  }

  private void assertDateMilliVectorValues(List<DateMilliVector> vectors, int rowCount, Long[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (DateMilliVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].longValue(), vector.get(i));
      }
    }
  }

  private void assertBitVectorValues(List<BitVector> vectors, int rowCount, Integer[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (BitVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].intValue(), vector.get(i));
      }
    }
  }

  private void assertBooleanVectorValues(List<BitVector> vectors, int rowCount, Boolean[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (BitVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++], vector.get(i) == 1);
      }
    }
  }

  private void assertVarCharVectorValues(List<VarCharVector> vectors, int rowCount, byte[][] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (VarCharVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertArrayEquals(values[index++], vector.get(i));
      }
    }
  }

  private void assertBinaryVectorValues(List<VarBinaryVector> vectors, int rowCount, byte[][] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (VarBinaryVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertArrayEquals(values[index++], vector.get(i));
      }
    }
  }

  private void assertSmallIntVectorValues(List<SmallIntVector> vectors, int rowCount, Integer[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (SmallIntVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].intValue(), vector.get(i));
      }
    }
  }

  private void assertTinyIntVectorValues(List<TinyIntVector> vectors, int rowCount, Integer[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (TinyIntVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].intValue(), vector.get(i));
      }
    }
  }

  private void assertBigIntVectorValues(List<BigIntVector> vectors, int rowCount, Long[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (BigIntVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].longValue(), vector.get(i));
      }
    }
  }

  private void assertIntVectorValues(List<IntVector> vectors, int rowCount, Integer[] values) {
    int valueCount = vectors.stream().mapToInt(ValueVector::getValueCount).sum();
    assertEquals(rowCount, valueCount);

    int index = 0;
    for (IntVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(values[index++].intValue(), vector.get(i));
      }
    }
  }
}
