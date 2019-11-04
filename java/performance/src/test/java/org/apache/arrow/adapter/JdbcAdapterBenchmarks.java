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

package org.apache.arrow.adapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for avro adapter.
 */
@State(Scope.Benchmark)
public class JdbcAdapterBenchmarks {

  private final int valueCount = 3000;
  private JdbcToArrowConfig config;

  private Connection conn = null;
  private ResultSet resultSet = null;

  private static final String CREATE_STATEMENT =
      "CREATE TABLE test_table (f0 INT, f1 LONG, f2 VARCHAR, f3 BOOLEAN);";
  private static final String INSERT_STATEMENT =
      "INSERT INTO test_table (f0, f1, f2, f3) VALUES (?, ?, ?, ?);";
  private static final String QUERY = "SELECT f0, f1, f2, f3 FROM test_table;";
  private static final String DROP_STATEMENT = "DROP TABLE test_table;";

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() throws Exception {
    BaseAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    config = new JdbcToArrowConfigBuilder().setAllocator(allocator).setTargetBatchSize(1024).build();

    String url = "jdbc:h2:mem:JdbcAdapterBenchmarks";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(CREATE_STATEMENT);
    }

    for (int i = 0; i < valueCount; i++) {
      // Insert data
      try (PreparedStatement stmt = conn.prepareStatement(INSERT_STATEMENT)) {

        stmt.setInt(1, i);
        stmt.setLong(2, i);
        stmt.setString(3, "test" + i);
        stmt.setBoolean(4, i % 2 == 0);
        stmt.executeUpdate();
      }
    }

    resultSet = conn.createStatement().executeQuery(QUERY);

  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(DROP_STATEMENT);
    } finally {
      if (conn != null) {
        conn.close();
        conn = null;
      }
    }
  }

  /**
   * Test {@link JdbcToArrow#sqlToArrowVectorIterator(ResultSet, JdbcToArrowConfig)}
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testJdbcToArrow() throws Exception {
    int valueCount = 0;
    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(resultSet, config)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        IntVector intVector = (IntVector) root.getFieldVectors().get(0);
        valueCount += intVector.getValueCount();
        root.close();
      }
    }
    return valueCount;
  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(JdbcAdapterBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
