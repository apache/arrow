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

package org.apache.arrow.driver.jdbc.test.adhoc;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
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
   * @param random    the {@link Random} instance to use.
   * @param allocator the {@link BufferAllocator} to use.
   * @return a new producer.
   */
  public static MockFlightSqlProducer getLegacyProducer(final Random random) {

    final MockFlightSqlProducer producer = new MockFlightSqlProducer();
    addLegacyRegularSqlCmdSupport(producer, random);
    addLegacyMetadataSqlCmdSupport(producer);
    addLegacyCancellationSqlCmdSupport(producer);
    return producer;
  }

  private static void addLegacyRegularSqlCmdSupport(final MockFlightSqlProducer producer, final Random random) {
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
            ((BigIntVector) root.getVector("ID")).setSafe(indexOnBatch, random.nextLong());
            ((VarCharVector) root.getVector("Name"))
                .setSafe(indexOnBatch, new Text("Test Name #" + (resultsOffset + i)));
            ((UInt4Vector) root.getVector("Age")).setSafe(indexOnBatch, random.nextInt(Integer.MAX_VALUE));
            ((Float8Vector) root.getVector("Salary")).setSafe(indexOnBatch, random.nextDouble());
            ((DateDayVector) root.getVector("Hire Date"))
                .setSafe(indexOnBatch, random.nextInt(Integer.MAX_VALUE));
            ((TimeStampMilliVector) root.getVector("Last Sale"))
                .setSafe(indexOnBatch, Instant.now().toEpochMilli());
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
    producer.addQuery(LEGACY_REGULAR_SQL_CMD, querySchema, resultProducers);
  }

  private static void addLegacyMetadataSqlCmdSupport(final MockFlightSqlProducer producer) {
    final Schema metadataSchema = new Schema(ImmutableList.of(
        new Field(
            "integer0",
            new FieldType(true, new ArrowType.Int(64, true),
                null),
            null),
        new Field(
            "string1",
            new FieldType(true, new ArrowType.Utf8(),
                null),
            null),
        new Field(
            "float2",
            new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                null),
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
    producer.addQuery(LEGACY_METADATA_SQL_CMD, metadataSchema, Collections.singletonList(formula));
  }

  private static void addLegacyCancellationSqlCmdSupport(final MockFlightSqlProducer producer) {
    producer.addQuery(
        LEGACY_CANCELLATION_SQL_CMD,
        new Schema(Collections.emptyList()),
        Collections.singletonList(listener -> {
          // Should keep hanging until canceled.
        }));
  }

  public static void assertLegacyRegularSqlResultSet(final ResultSet resultSet, final ErrorCollector collector) throws
      SQLException {
    int count = 0;
    int expectedRows = 50000;

    Set<String> testNames =
        IntStream.range(0, expectedRows).mapToObj(i -> "Test Name #" + i).collect(Collectors.toSet());

    for (; resultSet.next(); count++) {
      collector.checkThat(resultSet.getObject(1), instanceOf(Long.class));
      collector.checkThat(testNames.remove(resultSet.getString(2)), is(true));
      collector.checkThat(resultSet.getObject(3), instanceOf(Integer.class));
      collector.checkThat(resultSet.getObject(4), instanceOf(Double.class));
      collector.checkThat(resultSet.getObject(5), instanceOf(Date.class));
      collector.checkThat(resultSet.getObject(6), instanceOf(Timestamp.class));
    }

    collector.checkThat(testNames.isEmpty(), is(true));
    collector.checkThat(expectedRows, is(count));
  }
}
