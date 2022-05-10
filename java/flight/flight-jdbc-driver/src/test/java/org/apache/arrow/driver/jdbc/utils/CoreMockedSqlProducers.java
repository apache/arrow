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

package org.apache.arrow.driver.jdbc.utils;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;

/**
 * Standard {@link MockFlightSqlProducer} instances for tests.
 */
// TODO Remove this once all tests are refactor to use only the queries they need.
public final class CoreMockedSqlProducers {

  public static final String LEGACY_REGULAR_SQL_CMD = "SELECT * FROM TEST";
  public static final String LEGACY_METADATA_SQL_CMD = "SELECT * FROM METADATA";
  public static final String LEGACY_CANCELLATION_SQL_CMD = "SELECT * FROM TAKES_FOREVER";

  private CoreMockedSqlProducers() {
    // Prevent instantiation.
  }

  /**
   * Gets the {@link MockFlightSqlProducer} for legacy tests and backward compatibility.
   *
   * @return a new producer.
   */
  public static MockFlightSqlProducer getLegacyProducer() {

    final MockFlightSqlProducer producer = new MockFlightSqlProducer();
    addLegacyRegularSqlCmdSupport(producer);
    addLegacyMetadataSqlCmdSupport(producer);
    addLegacyCancellationSqlCmdSupport(producer);
    return producer;
  }

  private static void addLegacyRegularSqlCmdSupport(final MockFlightSqlProducer producer) {
    final Schema querySchema = new Schema(ImmutableList.of(
        new Field(
            "ID",
            new FieldType(true, new ArrowType.Int(64, true),
                null),
            null),
        new Field(
            "Name",
            new FieldType(true, new ArrowType.Utf8(), null),
            null),
        new Field(
            "Age",
            new FieldType(true, new ArrowType.Int(32, false),
                null),
            null),
        new Field(
            "Salary",
            new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                null),
            null),
        new Field(
            "Hire Date",
            new FieldType(true, new ArrowType.Date(DateUnit.DAY), null),
            null),
        new Field(
            "Last Sale",
            new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
                null),
            null)
    ));
    final List<Consumer<ServerStreamListener>> resultProducers = new ArrayList<>();
    IntStream.range(0, 10).forEach(page -> {
      resultProducers.add(listener -> {
        final int rowsPerPage = 5000;
        try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             final VectorSchemaRoot root = VectorSchemaRoot.create(querySchema, allocator)) {
          root.allocateNew();
          listener.start(root);
          int batchSize = 500;
          int indexOnBatch = 0;
          int resultsOffset = page * rowsPerPage;
          for (int i = 0; i < rowsPerPage; i++) {
            ((BigIntVector) root.getVector("ID"))
                .setSafe(indexOnBatch, (long) Integer.MAX_VALUE + 1 + i + resultsOffset);
            ((VarCharVector) root.getVector("Name"))
                .setSafe(indexOnBatch, new Text("Test Name #" + (resultsOffset + i)));
            ((UInt4Vector) root.getVector("Age"))
                .setSafe(indexOnBatch, (int) Short.MAX_VALUE + 1 + i + resultsOffset);
            ((Float8Vector) root.getVector("Salary"))
                .setSafe(indexOnBatch,
                    Math.scalb((double) (i + resultsOffset) / 2, i + resultsOffset));
            ((DateDayVector) root.getVector("Hire Date"))
                .setSafe(indexOnBatch, i + resultsOffset);
            ((TimeStampMilliVector) root.getVector("Last Sale"))
                .setSafe(indexOnBatch, Long.MAX_VALUE - i - resultsOffset);
            indexOnBatch++;
            if (indexOnBatch == batchSize) {
              root.setRowCount(indexOnBatch);
              if (listener.isCancelled()) {
                return;
              }
              listener.putNext();
              root.allocateNew();
              indexOnBatch = 0;
            }
          }
          if (listener.isCancelled()) {
            return;
          }
          root.setRowCount(indexOnBatch);
          listener.putNext();
        } finally {
          listener.completed();
        }
      });
    });
    producer.addSelectQuery(LEGACY_REGULAR_SQL_CMD, querySchema, resultProducers);
  }

  private static void addLegacyMetadataSqlCmdSupport(final MockFlightSqlProducer producer) {
    final Schema metadataSchema = new Schema(ImmutableList.of(
        new Field(
            "integer0",
            new FieldType(true, new ArrowType.Int(64, true),
                null, new FlightSqlColumnMetadata.Builder()
                .catalogName("CATALOG_NAME_1")
                .schemaName("SCHEMA_NAME_1")
                .tableName("TABLE_NAME_1")
                .typeName("TYPE_NAME_1")
                .precision(10)
                .scale(0)
                .isAutoIncrement(true)
                .isCaseSensitive(false)
                .isReadOnly(true)
                .isSearchable(true)
                .build().getMetadataMap()),
            null),
        new Field(
            "string1",
            new FieldType(true, new ArrowType.Utf8(),
                null, new FlightSqlColumnMetadata.Builder()
                .catalogName("CATALOG_NAME_2")
                .schemaName("SCHEMA_NAME_2")
                .tableName("TABLE_NAME_2")
                .typeName("TYPE_NAME_2")
                .precision(65535)
                .scale(0)
                .isAutoIncrement(false)
                .isCaseSensitive(true)
                .isReadOnly(false)
                .isSearchable(true)
                .build().getMetadataMap()),
            null),
        new Field(
            "float2",
            new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                null, new FlightSqlColumnMetadata.Builder()
                .catalogName("CATALOG_NAME_3")
                .schemaName("SCHEMA_NAME_3")
                .tableName("TABLE_NAME_3")
                .typeName("TYPE_NAME_3")
                .precision(15)
                .scale(20)
                .isAutoIncrement(false)
                .isCaseSensitive(false)
                .isReadOnly(false)
                .isSearchable(true)
                .build().getMetadataMap()),
            null)));
    final Consumer<ServerStreamListener> formula = listener -> {
      try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
           final VectorSchemaRoot root = VectorSchemaRoot.create(metadataSchema, allocator)) {
        root.allocateNew();
        ((BigIntVector) root.getVector("integer0")).setSafe(0, 1);
        ((VarCharVector) root.getVector("string1")).setSafe(0, new Text("teste"));
        ((Float4Vector) root.getVector("float2")).setSafe(0, (float) 4.1);
        root.setRowCount(1);
        listener.start(root);
        listener.putNext();
      } finally {
        listener.completed();
      }
    };
    producer.addSelectQuery(LEGACY_METADATA_SQL_CMD, metadataSchema,
        Collections.singletonList(formula));
  }

  private static void addLegacyCancellationSqlCmdSupport(final MockFlightSqlProducer producer) {
    producer.addSelectQuery(
        LEGACY_CANCELLATION_SQL_CMD,
        new Schema(Collections.singletonList(new Field(
            "integer0",
            new FieldType(true, new ArrowType.Int(64, true), null),
            null))),
        Collections.singletonList(listener -> {
          // Should keep hanging until canceled.
        }));
  }

  /**
   * Asserts that the values in the provided {@link ResultSet} are expected for the
   * legacy {@link MockFlightSqlProducer}.
   *
   * @param resultSet the result set.
   * @param collector the {@link ErrorCollector} to use.
   * @throws SQLException on error.
   */
  public static void assertLegacyRegularSqlResultSet(final ResultSet resultSet,
                                                     final ErrorCollector collector)
      throws SQLException {
    final int expectedRowCount = 50_000;

    final long[] expectedIds = new long[expectedRowCount];
    final List<String> expectedNames = new ArrayList<>(expectedRowCount);
    final int[] expectedAges = new int[expectedRowCount];
    final double[] expectedSalaries = new double[expectedRowCount];
    final List<Date> expectedHireDates = new ArrayList<>(expectedRowCount);
    final List<Timestamp> expectedLastSales = new ArrayList<>(expectedRowCount);

    final long[] actualIds = new long[expectedRowCount];
    final List<String> actualNames = new ArrayList<>(expectedRowCount);
    final int[] actualAges = new int[expectedRowCount];
    final double[] actualSalaries = new double[expectedRowCount];
    final List<Date> actualHireDates = new ArrayList<>(expectedRowCount);
    final List<Timestamp> actualLastSales = new ArrayList<>(expectedRowCount);

    int actualRowCount = 0;

    for (; resultSet.next(); actualRowCount++) {
      expectedIds[actualRowCount] = (long) Integer.MAX_VALUE + 1 + actualRowCount;
      expectedNames.add(format("Test Name #%d", actualRowCount));
      expectedAges[actualRowCount] = (int) Short.MAX_VALUE + 1 + actualRowCount;
      expectedSalaries[actualRowCount] = Math.scalb((double) actualRowCount / 2, actualRowCount);
      expectedHireDates.add(new Date(86_400_000L * actualRowCount));
      expectedLastSales.add(new Timestamp(Long.MAX_VALUE - actualRowCount));

      actualIds[actualRowCount] = (long) resultSet.getObject(1);
      actualNames.add((String) resultSet.getObject(2));
      actualAges[actualRowCount] = (int) resultSet.getObject(3);
      actualSalaries[actualRowCount] = (double) resultSet.getObject(4);
      actualHireDates.add((Date) resultSet.getObject(5));
      actualLastSales.add((Timestamp) resultSet.getObject(6));
    }
    collector.checkThat(actualRowCount, is(equalTo(expectedRowCount)));
    collector.checkThat(actualIds, is(expectedIds));
    collector.checkThat(actualNames, is(expectedNames));
    collector.checkThat(actualAges, is(expectedAges));
    collector.checkThat(actualSalaries, is(expectedSalaries));
    collector.checkThat(actualHireDates, is(expectedHireDates));
    collector.checkThat(actualLastSales, is(expectedLastSales));
  }
}
