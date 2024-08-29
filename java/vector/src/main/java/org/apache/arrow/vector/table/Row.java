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

package org.apache.arrow.vector.table;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableDurationHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeSecHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecTZHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;

/**
 * Row is a positionable, immutable cursor backed by a {@link Table}.
 *
 * <p>Getters are provided for most vector types. The exceptions being {@link org.apache.arrow.vector.NullVector},
 * which only contains null values and has no getter, and {@link org.apache.arrow.vector.ZeroVector},
 * which is a zero-length vector of any type
 *
 * <p>
 * This API is EXPERIMENTAL.
 */
public class Row implements Iterator<Row> {

  /**
   * Returns the standard character set to use for decoding strings. The Arrow format only supports UTF-8.
   */
  private static final Charset DEFAULT_CHARACTER_SET = StandardCharsets.UTF_8;

  /** The table we're enumerating. */
  protected final BaseTable table;
  /** the current row number. */
  protected int rowNumber = -1;
  /** Indicates whether the next non-deleted row has been determined yet. */
  private boolean nextRowSet;

  /**
   * An iterator that returns every row in the table, deleted or not. The implemented next() and
   * hasNext() methods in Row wrap it with a filter to get only the non-deleted ones.
   */
  private final Iterator<Integer> iterator = intIterator();

  /**
   * Constructs a new Row backed by the given table.
   *
   * @param table the table that this Row object represents
   */
  public Row(BaseTable table) {
    this.table = table;
  }

  /**
   * Resets the current row to -1 and returns this object.
   */
  public Row resetPosition() {
    rowNumber = -1;
    return this;
  }

  /**
   * Moves this Row to the given 0-based row index.
   *
   * @return this Row for chaining
   */
  public Row setPosition(int rowNumber) {
    this.rowNumber = rowNumber;
    this.nextRowSet = false;
    return this;
  }

  /**
   * For vectors other than Union and DenseUnion, returns true if the value at columnName is null,
   * and false otherwise.
   *
   * <p>UnionVector#isNull always returns false, but the underlying vector may hold null values.
   */
  public boolean isNull(String columnName) {
    ValueVector vector = table.getVector(columnName);
    return vector.isNull(rowNumber);
  }

  /**
   * For vectors other than Union and DenseUnion, returns true if the value at columnIndex is null,
   * and false otherwise.
   *
   * <p>UnionVector#isNull always returns false, but the underlying vector may hold null values.
   */
  public boolean isNull(int columnIndex) {
    ValueVector vector = table.getVector(columnIndex);
    return vector.isNull(rowNumber);
  }

  /**
   * Returns an object representing the value in the ExtensionTypeVector at the currentRow and vectorIndex. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if the type is incorrect.
   */
  public Object getExtensionType(int vectorIndex) {
    FieldVector vector = table.getVector(vectorIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an object representing the value in the named ExtensionTypeVector at the currentRow. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type.
   *
   * @param columnName The name of the vector providing the result
   * @return The object in the named column at the current row
   */
  public Object getExtensionType(String columnName) {
    FieldVector vector = table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a Map from the column of the given vectorIndex at the current row. An IllegalArgumentException is
   * thrown if the column is not present in the Row and a ClassCastException is thrown if
   * it has a different type.
   */
  public List<?> getMap(int vectorIndex) {
    ListVector vector = (ListVector) table.getVector(vectorIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a Map from the column of the given name at the current row. An IllegalArgumentException is
   * thrown if the column is not present in the Row and a ClassCastException is thrown if
   * it has a different type
   */
  public List<?> getMap(String columnName) {
    ListVector vector = (ListVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an Object from the column at vectorIndex at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public Object getStruct(int vectorIndex) {
    StructVector vector = (StructVector) table.getVector(vectorIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an Object from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public Object getStruct(String columnName) {
    StructVector vector = (StructVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an Object from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public Object getUnion(int vectorIndex) {
    UnionVector vector = (UnionVector) table.getVector(vectorIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an Object from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public Object getUnion(String columnName) {
    UnionVector vector = (UnionVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an Object from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public Object getDenseUnion(String columnName) {
    DenseUnionVector vector = (DenseUnionVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an Object from the column with the given vectorIndex at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public Object getDenseUnion(int vectorIndex) {
    DenseUnionVector vector = (DenseUnionVector) table.getVector(vectorIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a List from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   */
  public List<?> getList(String columnName) {
    ListVector vector = (ListVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a List from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present and a ClassCastException is
   * thrown if it has a different type
   */
  public List<?> getList(int columnIndex) {
    ListVector vector = (ListVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an int from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   */
  public int getInt(String columnName) {
    IntVector vector = (IntVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present and a ClassCastException is
   * thrown if it has a different type
   */
  public int getInt(int columnIndex) {
    IntVector vector = (IntVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public void getInt(String columnName, NullableIntHolder holder) {
    IntVector vector = (IntVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present and a ClassCastException is
   * thrown if it has a different type
   */
  public void getInt(int columnIndex, NullableIntHolder holder) {
    IntVector vector = (IntVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns an int from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   */
  public int getUInt4(String columnName) {
    UInt4Vector vector = (UInt4Vector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present and a ClassCastException is
   * thrown if it has a different type
   */
  public int getUInt4(int columnIndex) {
    UInt4Vector vector = (UInt4Vector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value at the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public void getUInt4(String columnName, NullableUInt4Holder holder) {
    UInt4Vector vector = (UInt4Vector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value at the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present and a ClassCastException is
   * thrown if it has a different type
   */
  public void getUInt4(int columnIndex, NullableUInt4Holder holder) {
    UInt4Vector vector = (UInt4Vector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a short from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public short getSmallInt(String columnName) {
    SmallIntVector vector = (SmallIntVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a short from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public short getSmallInt(int columnIndex) {
    SmallIntVector vector = (SmallIntVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getSmallInt(String columnName, NullableSmallIntHolder holder) {
    SmallIntVector vector = (SmallIntVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getSmallInt(int columnIndex, NullableSmallIntHolder holder) {
    SmallIntVector vector = (SmallIntVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a char from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public char getUInt2(String columnName) {
    UInt2Vector vector = (UInt2Vector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a char from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public char getUInt2(int columnIndex) {
    UInt2Vector vector = (UInt2Vector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getUInt2(String columnName, NullableUInt2Holder holder) {
    UInt2Vector vector = (UInt2Vector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getUInt2(int columnIndex, NullableUInt2Holder holder) {
    UInt2Vector vector = (UInt2Vector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a byte from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public byte getTinyInt(String columnName) {
    TinyIntVector vector = (TinyIntVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public byte getTinyInt(int columnIndex) {
    TinyIntVector vector = (TinyIntVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTinyInt(String columnName, NullableTinyIntHolder holder) {
    TinyIntVector vector = (TinyIntVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column at the given index and current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTinyInt(int columnIndex, NullableTinyIntHolder holder) {
    TinyIntVector vector = (TinyIntVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a byte from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public byte getUInt1(String columnName) {
    UInt1Vector vector = (UInt1Vector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public byte getUInt1(int columnIndex) {
    UInt1Vector vector = (UInt1Vector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getUInt1(String columnName, NullableUInt1Holder holder) {
    UInt1Vector vector = (UInt1Vector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getUInt1(int columnIndex, NullableUInt1Holder holder) {
    UInt1Vector vector = (UInt1Vector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getBigInt(String columnName) {
    BigIntVector vector = (BigIntVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getBigInt(int columnIndex) {
    BigIntVector vector = (BigIntVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getBigInt(String columnName, NullableBigIntHolder holder) {
    BigIntVector vector = (BigIntVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getBigInt(int columnIndex, NullableBigIntHolder holder) {
    BigIntVector vector = (BigIntVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getUInt8(String columnName) {
    UInt8Vector vector = (UInt8Vector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getUInt8(int columnIndex) {
    UInt8Vector vector = (UInt8Vector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getUInt8(String columnName, NullableUInt8Holder holder) {
    UInt8Vector vector = (UInt8Vector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getUInt8(int columnIndex, NullableUInt8Holder holder) {
    UInt8Vector vector = (UInt8Vector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a float from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public float getFloat4(String columnName) {
    Float4Vector vector = (Float4Vector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a float from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public float getFloat4(int columnIndex) {
    Float4Vector vector = (Float4Vector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getFloat4(String columnName, NullableFloat4Holder holder) {
    Float4Vector vector = (Float4Vector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getFloat4(int columnIndex, NullableFloat4Holder holder) {
    Float4Vector vector = (Float4Vector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a double from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public double getFloat8(String columnName) {
    Float8Vector vector = (Float8Vector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a double from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public double getFloat8(int columnIndex) {
    Float8Vector vector = (Float8Vector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown
   * if it is present but has a different type
   */
  public void getFloat8(String columnName, NullableFloat8Holder holder) {
    Float8Vector vector = (Float8Vector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getFloat8(int columnIndex, NullableFloat8Holder holder) {
    Float8Vector vector = (Float8Vector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns an int from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public int getBit(String columnName) {
    BitVector vector = (BitVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public int getBit(int columnIndex) {
    BitVector vector = (BitVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getBit(String columnName, NullableBitHolder holder) {
    BitVector vector = (BitVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getBit(int columnIndex, NullableBitHolder holder) {
    BitVector vector = (BitVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public long getDateMilli(String columnName) {
    DateMilliVector vector = (DateMilliVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public long getDateMilli(int columnIndex) {
    DateMilliVector vector = (DateMilliVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDateMilli(String columnName, NullableDateMilliHolder holder) {
    DateMilliVector vector = (DateMilliVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDateMilli(int columnIndex, NullableDateMilliHolder holder) {
    DateMilliVector vector = (DateMilliVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns an int from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public int getDateDay(String columnName) {
    DateDayVector vector = (DateDayVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public int getDateDay(int columnIndex) {
    DateDayVector vector = (DateDayVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }


  /**
   * Updates the holder with the value in the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDateDay(String columnName, NullableDateDayHolder holder) {
    DateDayVector vector = (DateDayVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDateDay(int columnIndex, NullableDateDayHolder holder) {
    DateDayVector vector = (DateDayVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeNano(String columnName) {
    TimeNanoVector vector = (TimeNanoVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeNano(int columnIndex) {
    TimeNanoVector vector = (TimeNanoVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value in the column with the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown
   * if it is present but has a different type
   */
  public void getTimeNano(String columnName, NullableTimeNanoHolder holder) {
    TimeNanoVector vector = (TimeNanoVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value in the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public void getTimeNano(int columnIndex, NullableTimeNanoHolder holder) {
    TimeNanoVector vector = (TimeNanoVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public long getTimeMicro(String columnName) {
    TimeMicroVector vector = (TimeMicroVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public long getTimeMicro(int columnIndex) {
    TimeMicroVector vector = (TimeMicroVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public void getTimeMicro(String columnName, NullableTimeMicroHolder holder) {
    TimeMicroVector vector = (TimeMicroVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public void getTimeMicro(int columnIndex, NullableTimeMicroHolder holder) {
    TimeMicroVector vector = (TimeMicroVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns an int from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public int getTimeMilli(String columnName) {
    TimeMilliVector vector = (TimeMilliVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public int getTimeMilli(int columnIndex) {
    TimeMilliVector vector = (TimeMilliVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public void getTimeMilli(String columnName, NullableTimeMilliHolder holder) {
    TimeMilliVector vector = (TimeMilliVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public void getTimeMilli(int columnIndex, NullableTimeMilliHolder holder) {
    TimeMilliVector vector = (TimeMilliVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a LocalDateTime from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public LocalDateTime getTimeMilliObj(String columnName) {
    TimeMilliVector vector = (TimeMilliVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a LocalDateTime from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public LocalDateTime getTimeMilliObj(int columnIndex) {
    TimeMilliVector vector = (TimeMilliVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an int from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public int getTimeSec(String columnName) {
    TimeSecVector vector = (TimeSecVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public int getTimeSec(int columnIndex) {
    TimeSecVector vector = (TimeSecVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public void getTimeSec(String columnName, NullableTimeSecHolder holder) {
    TimeSecVector vector = (TimeSecVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type.
   */
  public void getTimeSec(int columnIndex, NullableTimeSecHolder holder) {
    TimeSecVector vector = (TimeSecVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type.
   */
  public long getTimeStampSec(String columnName) {
    TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampSec(int columnIndex) {
    TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampSec(String columnName, NullableTimeStampSecHolder holder) {
    TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampSec(int columnIndex, NullableTimeStampSecHolder holder) {
    TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a LocalDateTime from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public LocalDateTime getTimeStampSecObj(String columnName) {
    TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a LocalDateTime from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampSecObj(int columnIndex) {
    TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampSecTZ(String columnName) {
    TimeStampSecTZVector vector = (TimeStampSecTZVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampSecTZ(int columnIndex) {
    TimeStampSecTZVector vector = (TimeStampSecTZVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampSecTZ(String columnName, NullableTimeStampSecTZHolder holder) {
    TimeStampSecTZVector vector = (TimeStampSecTZVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row.
   * An IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampSecTZ(int columnIndex, NullableTimeStampSecTZHolder holder) {
    TimeStampSecTZVector vector = (TimeStampSecTZVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampNano(String columnName) {
    TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampNano(int columnIndex) {
    TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampNano(String columnName, NullableTimeStampNanoHolder holder) {
    TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampNano(int columnIndex, NullableTimeStampNanoHolder holder) {
    TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a LocalDateTime from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampNanoObj(String columnName) {
    TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a LocalDateTime from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampNanoObj(int columnIndex) {
    TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampNanoTZ(String columnName) {
    TimeStampNanoTZVector vector = (TimeStampNanoTZVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampNanoTZ(int columnIndex) {
    TimeStampNanoTZVector vector = (TimeStampNanoTZVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampNanoTZ(String columnName, NullableTimeStampNanoTZHolder holder) {
    TimeStampNanoTZVector vector = (TimeStampNanoTZVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampNanoTZ(int columnIndex, NullableTimeStampNanoTZHolder holder) {
    TimeStampNanoTZVector vector = (TimeStampNanoTZVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampMilli(String columnName) {
    TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampMilli(int columnIndex) {
    TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampMilli(String columnName, NullableTimeStampMilliHolder holder) {
    TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampMilli(int columnIndex, NullableTimeStampMilliHolder holder) {
    TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a LocalDateTime from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampMilliObj(String columnName) {
    TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a LocalDateTime from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampMilliObj(int columnIndex) {
    TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampMilliTZ(String columnName) {
    TimeStampMilliTZVector vector = (TimeStampMilliTZVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampMilliTZ(int columnIndex) {
    TimeStampMilliTZVector vector = (TimeStampMilliTZVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different types
   */
  public void getTimeStampMilliTZ(String columnName, NullableTimeStampMilliTZHolder holder) {
    TimeStampMilliTZVector vector = (TimeStampMilliTZVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampMilliTZ(int columnIndex, NullableTimeStampMilliTZHolder holder) {
    TimeStampMilliTZVector vector = (TimeStampMilliTZVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampMicro(String columnName) {
    TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampMicro(int columnIndex) {
    TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampMicro(String columnName, NullableTimeStampMicroHolder holder) {
    TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampMicro(int columnIndex, NullableTimeStampMicroHolder holder) {
    TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a LocalDateTime from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampMicroObj(String columnName) {
    TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a LocalDateTime from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public LocalDateTime getTimeStampMicroObj(int columnIndex) {
    TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a long from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public long getTimeStampMicroTZ(String columnName) {
    TimeStampMicroTZVector vector = (TimeStampMicroTZVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a long from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public long getTimeStampMicroTZ(int columnIndex) {
    TimeStampMicroTZVector vector = (TimeStampMicroTZVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public void getTimeStampMicroTZ(String columnName, NullableTimeStampMicroTZHolder holder) {
    TimeStampMicroTZVector vector = (TimeStampMicroTZVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getTimeStampMicroTZ(int columnIndex, NullableTimeStampMicroTZHolder holder) {
    TimeStampMicroTZVector vector = (TimeStampMicroTZVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a Duration from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public Duration getDurationObj(String columnName) {
    DurationVector vector = (DurationVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a Duration from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public Duration getDurationObj(int columnIndex) {
    DurationVector vector = (DurationVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getDuration(String columnName) {
    DurationVector vector = (DurationVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getDuration(int columnIndex) {
    DurationVector vector = (DurationVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDuration(String columnName, NullableDurationHolder holder) {
    DurationVector vector = (DurationVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDuration(int columnIndex, NullableDurationHolder holder) {
    DurationVector vector = (DurationVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a PeriodDuration from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public PeriodDuration getIntervalMonthDayNanoObj(String columnName) {
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a PeriodDuration from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public PeriodDuration getIntervalMonthDayNanoObj(int columnIndex) {
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getIntervalMonthDayNano(String columnName) {
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getIntervalMonthDayNano(int columnIndex) {
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getIntervalMonthDayNano(
      String columnName, NullableIntervalMonthDayNanoHolder holder) {
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getIntervalMonthDayNano(int columnIndex, NullableIntervalMonthDayNanoHolder holder) {
    IntervalMonthDayNanoVector vector = (IntervalMonthDayNanoVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns an ArrowBuf from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getIntervalDay(String columnName) {
    IntervalDayVector vector = (IntervalDayVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getIntervalDay(int columnIndex) {
    IntervalDayVector vector = (IntervalDayVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getIntervalDay(String columnName, NullableIntervalDayHolder holder) {
    IntervalDayVector vector = (IntervalDayVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getIntervalDay(int columnIndex, NullableIntervalDayHolder holder) {
    IntervalDayVector vector = (IntervalDayVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a Duration from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public Duration getIntervalDayObj(int columnIndex) {
    IntervalDayVector vector = (IntervalDayVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a Duration from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public Duration getIntervalDayObj(String columnName) {
    IntervalDayVector vector = (IntervalDayVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a Period from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   *
   * @return a Period of n MONTHS, not YEARS
   */
  public Period getIntervalYearObj(String columnName) {
    IntervalYearVector vector = (IntervalYearVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a Period from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   *
   * @return a Period of n MONTHS, not YEARS
   */
  public Period getIntervalYearObj(int columnIndex) {
    IntervalYearVector vector = (IntervalYearVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an int from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   *
   * @return the number of MONTHS in the interval (not YEARS)
   */
  public int getIntervalYear(String columnName) {
    IntervalYearVector vector = (IntervalYearVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an int from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   *
   * @return the number of MONTHS in the interval (not YEARS)
   */
  public int getIntervalYear(int columnIndex) {
    IntervalYearVector vector = (IntervalYearVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Updates the holder with the value from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   *
   * @param holder  a holder to store the interval. Note that the value of the holder represents MONTHS not years
   */
  public void getIntervalYear(String columnName, NullableIntervalYearHolder holder) {
    IntervalYearVector vector = (IntervalYearVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the holder with the value from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   *
   * @param holder  a holder to store the interval. Note that the value of the holder represents MONTHS not years
   */
  public void getIntervalYear(int columnIndex, NullableIntervalYearHolder holder) {
    IntervalYearVector vector = (IntervalYearVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the value of the holder with data from vector at the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDecimal(int columnIndex, NullableDecimalHolder holder) {
    DecimalVector vector = (DecimalVector) table.getVector(columnIndex);
    vector.get(rowNumber, holder);
  }

  /**
   * Updates the value of the holder with data from the vector with given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public void getDecimal(String columnName, NullableDecimalHolder holder) {
    DecimalVector vector = (DecimalVector) table.getVector(columnName);
    vector.get(rowNumber, holder);
  }

  /**
   * Returns a BigDecimal from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public BigDecimal getDecimalObj(String columnName) {
    DecimalVector vector = (DecimalVector) table.getVector(columnName);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns a BigDecimal from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public BigDecimal getDecimalObj(int columnIndex) {
    DecimalVector vector = (DecimalVector) table.getVector(columnIndex);
    return vector.getObject(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column of the given name at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getDecimal(String columnName) {
    DecimalVector vector = (DecimalVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns an ArrowBuf from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public ArrowBuf getDecimal(int columnIndex) {
    DecimalVector vector = (DecimalVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public byte[] getVarBinary(String columnName) {
    VarBinaryVector vector = (VarBinaryVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public byte[] getVarBinary(int columnIndex) {
    VarBinaryVector vector = (VarBinaryVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public byte[] getFixedSizeBinary(String columnName) {
    FixedSizeBinaryVector vector = (FixedSizeBinaryVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public byte[] getFixedSizeBinary(int columnIndex) {
    FixedSizeBinaryVector vector = (FixedSizeBinaryVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present, and a ClassCastException is thrown if it is
   * present but has a different type
   */
  public byte[] getLargeVarBinary(String columnName) {
    LargeVarBinaryVector vector = (LargeVarBinaryVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present, and a ClassCastException
   * is thrown if it is present but has a different type
   */
  public byte[] getLargeVarBinary(int columnIndex) {
    LargeVarBinaryVector vector = (LargeVarBinaryVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Returns a String from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   *
   * <p>StandardCharsets.UTF_8 is used as the charset
   */
  public String getVarCharObj(String columnName) {
    VarCharVector vector = (VarCharVector) table.getVector(columnName);
    return new String(vector.get(rowNumber), getDefaultCharacterSet());
  }

  /**
   * Returns a String from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   *
   * @param columnIndex the index of the FieldVector holding the value
   */
  public String getVarCharObj(int columnIndex) {
    VarCharVector vector = (VarCharVector) table.getVector(columnIndex);
    return new String(vector.get(rowNumber), getDefaultCharacterSet());
  }

  /**
   * Returns a byte[] from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   *
   * <p>StandardCharsets.UTF_8 is used as the charset
   */
  public byte[] getVarChar(String columnName) {
    VarCharVector vector = (VarCharVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   *
   * @param columnIndex the index of the FieldVector holding the value
   */
  public byte[] getVarChar(int columnIndex) {
    VarCharVector vector = (VarCharVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Returns a String from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   *
   * <p>StandardCharsets.UTF_8 is used as the charset, unless this cursor was created with a default
   * Charset
   */
  public String getLargeVarCharObj(String columnName) {
    LargeVarCharVector vector = (LargeVarCharVector) table.getVector(columnName);
    return new String(vector.get(rowNumber), getDefaultCharacterSet());
  }

  /**
   * Returns a String from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public String getLargeVarCharObj(int columnIndex) {
    LargeVarCharVector vector = (LargeVarCharVector) table.getVector(columnIndex);
    return new String(vector.get(rowNumber), getDefaultCharacterSet());
  }

  /**
   * Returns a byte[] from the column of the given name at the current row. An IllegalArgumentException
   * is thrown if the column is not present in the Row and a ClassCastException is thrown
   * if it has a different type
   *
   * <p>StandardCharsets.UTF_8 is used as the charset, unless this cursor was created with a default
   * Charset
   */
  public byte[] getLargeVarChar(String columnName) {
    LargeVarCharVector vector = (LargeVarCharVector) table.getVector(columnName);
    return vector.get(rowNumber);
  }

  /**
   * Returns a byte[] from the column with the given index at the current row. An
   * IllegalArgumentException is thrown if the column is not present in the Row and a
   * ClassCastException is thrown if it has a different type
   */
  public byte[] getLargeVarChar(int columnIndex) {
    LargeVarCharVector vector = (LargeVarCharVector) table.getVector(columnIndex);
    return vector.get(rowNumber);
  }

  /**
   * Returns true if there is at least one more non-deleted row in the table that has yet to be
   * processed.
   */
  @Override
  public boolean hasNext() {
    return nextRowSet || setNextObject();
  }

  /**
   * Returns the next non-deleted row in the table.
   *
   * @throws NoSuchElementException if there are no more rows
   */
  @Override
  public Row next() {
    if (!nextRowSet && !setNextObject()) {
      throw new NoSuchElementException();
    }
    nextRowSet = false;
    return this;
  }

  /**
   * Set rowNumber to the next non-deleted row. If there are no more rows return false. Otherwise,
   * return true.
   */
  private boolean setNextObject() {
    while (iterator.hasNext()) {
      final int row = iterator.next();
      if (!rowIsDeleted(row)) {
        rowNumber = row;
        nextRowSet = true;
        return true;
      }
    }
    return false;
  }

  /**
   * Returns new internal iterator that processes every row, deleted or not. Use the
   * wrapping next() and hasNext() methods rather than using this iterator directly, unless you want
   * to see any deleted rows.
   */
  private Iterator<Integer> intIterator() {
    return new Iterator<Integer>() {

      @Override
      public boolean hasNext() {
        return rowNumber < table.getRowCount() - 1;
      }

      @Override
      public Integer next() {
        rowNumber++;
        return rowNumber;
      }
    };
  }

  public int getRowNumber() {
    return rowNumber;
  }

  private boolean rowIsDeleted(int rowNumber) {
    return table.isRowDeleted(rowNumber);
  }

  /**
   * Returns the default character set for use with character vectors.
   */
  public Charset getDefaultCharacterSet() {
    return DEFAULT_CHARACTER_SET;
  }
}
