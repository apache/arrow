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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.adapter.jdbc.consumer.BigIntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.BitConsumer;
import org.apache.arrow.adapter.jdbc.consumer.IntConsumer;
import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.VarCharConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
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
 * Benchmarks for Jdbc adapter.
 */
public class JdbcAdapterBenchmarks {

  private static final int VALUE_COUNT = 3000;

  private static final String CREATE_STATEMENT =
          "CREATE TABLE test_table (f0 INT, f1 LONG, f2 VARCHAR, f3 BOOLEAN);";
  private static final String INSERT_STATEMENT =
          "INSERT INTO test_table (f0, f1, f2, f3) VALUES (?, ?, ?, ?);";
  private static final String QUERY = "SELECT f0, f1, f2, f3 FROM test_table;";
  private static final String DROP_STATEMENT = "DROP TABLE test_table;";

  private static final String URL = "jdbc:h2:mem:JdbcAdapterBenchmarks";
  private static final String DRIVER = "org.h2.Driver";

  /**
   * State object for the jdbc e2e benchmark.
   */
  @State(Scope.Benchmark)
  public static class JdbcState {

    private Connection conn = null;

    private ResultSet resultSet = null;

    private BufferAllocator allocator;

    private Statement statement;

    private JdbcToArrowConfig config;

    @Setup(Level.Trial)
    public void prepareState() throws Exception {
      allocator = new RootAllocator(Integer.MAX_VALUE);
      config = new JdbcToArrowConfigBuilder().setAllocator(allocator).setTargetBatchSize(1024).build();
      Class.forName(DRIVER);
      conn = DriverManager.getConnection(URL);

      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(CREATE_STATEMENT);
      }

      for (int i = 0; i < VALUE_COUNT; i++) {
        // Insert data
        try (PreparedStatement stmt = conn.prepareStatement(INSERT_STATEMENT)) {

          stmt.setInt(1, i);
          stmt.setLong(2, i);
          stmt.setString(3, "test" + i);
          stmt.setBoolean(4, i % 2 == 0);
          stmt.executeUpdate();
        }
      }
    }

    @Setup(Level.Invocation)
    public void prepareInvoke() throws Exception {
      statement = conn.createStatement();
      resultSet = statement.executeQuery(QUERY);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvoke() throws Exception {
      resultSet.close();
      statement.close();
    }

    @TearDown(Level.Trial)
    public void tearDownState() throws Exception {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(DROP_STATEMENT);
      }
      allocator.close();
    }
  }

  /**
   * State object for the consume benchmark.
   */
  @State(Scope.Benchmark)
  public static class ConsumeState {

    private static final boolean NULLABLE = true;

    private Connection conn = null;

    private ResultSet resultSet = null;

    private BufferAllocator allocator;

    private Statement statement;

    private IntVector intVector;

    private BigIntVector longVector;

    private VarCharVector varCharVector;

    private BitVector bitVector;

    private JdbcConsumer<IntVector> intConsumer;

    private JdbcConsumer<BigIntVector> longConsumer;

    private JdbcConsumer<VarCharVector> varCharConsumer;

    private JdbcConsumer<BitVector> bitConsumer;

    private JdbcToArrowConfig config;

    @Setup(Level.Trial)
    public void prepare() throws Exception {
      allocator = new RootAllocator(Integer.MAX_VALUE);
      config = new JdbcToArrowConfigBuilder().setAllocator(allocator).setTargetBatchSize(1024).build();

      Class.forName(DRIVER);
      conn = DriverManager.getConnection(URL);
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(CREATE_STATEMENT);
      }

      for (int i = 0; i < VALUE_COUNT; i++) {
        // Insert data
        try (PreparedStatement stmt = conn.prepareStatement(INSERT_STATEMENT)) {

          stmt.setInt(1, i);
          stmt.setLong(2, i);
          stmt.setString(3, "test" + i);
          stmt.setBoolean(4, i % 2 == 0);
          stmt.executeUpdate();
        }
      }

      statement = conn.createStatement();
      resultSet = statement.executeQuery(QUERY);
      resultSet.next();

      intVector = new IntVector("", allocator);
      intVector.allocateNew(VALUE_COUNT);
      intConsumer = IntConsumer.createConsumer(intVector, 1, NULLABLE);

      longVector = new BigIntVector("", allocator);
      longVector.allocateNew(VALUE_COUNT);
      longConsumer = BigIntConsumer.createConsumer(longVector, 2, NULLABLE);

      varCharVector = new VarCharVector("", allocator);
      varCharVector.allocateNew(VALUE_COUNT);
      varCharConsumer = VarCharConsumer.createConsumer(varCharVector, 3, NULLABLE);

      bitVector = new BitVector("", allocator);
      bitVector.allocateNew(VALUE_COUNT);
      bitConsumer = BitConsumer.createConsumer(bitVector, 4, NULLABLE);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(DROP_STATEMENT);
      }

      resultSet.close();
      statement.close();
      conn.close();

      intVector.close();
      intConsumer.close();

      longVector.close();
      longConsumer.close();

      varCharVector.close();
      varCharConsumer.close();

      bitVector.close();
      bitConsumer.close();

      allocator.close();
    }
  }

  /**
   * State object for the jdbc row consume benchmark.
   */
  @State(Scope.Benchmark)
  public static class RowConsumeState {

    private Connection conn = null;

    private ResultSet resultSet = null;

    private BufferAllocator allocator;

    private Statement statement;

    private JdbcToArrowConfig config;

    private ArrowVectorIterator iter;

    private VectorSchemaRoot root;

    @Setup(Level.Trial)
    public void prepareState() throws Exception {
      allocator = new RootAllocator(Integer.MAX_VALUE);
      config = new JdbcToArrowConfigBuilder().setAllocator(allocator).setTargetBatchSize(VALUE_COUNT).build();
      Class.forName(DRIVER);
      conn = DriverManager.getConnection(URL);

      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(CREATE_STATEMENT);
      }

      for (int i = 0; i < VALUE_COUNT; i++) {
        // Insert data
        try (PreparedStatement stmt = conn.prepareStatement(INSERT_STATEMENT)) {

          stmt.setInt(1, i);
          stmt.setLong(2, i);
          stmt.setString(3, "test" + i);
          stmt.setBoolean(4, i % 2 == 0);
          stmt.executeUpdate();
        }
      }
    }

    @Setup(Level.Invocation)
    public void prepareInvoke() throws Exception {
      statement = conn.createStatement();
      resultSet = statement.executeQuery(QUERY);

      iter = JdbcToArrow.sqlToArrowVectorIterator(resultSet, config);
      root = iter.next();
      iter.compositeConsumer.resetVectorSchemaRoot(root);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvoke() throws Exception {
      resultSet.close();
      statement.close();
      iter.close();
    }

    @TearDown(Level.Trial)
    public void tearDownState() throws Exception {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(DROP_STATEMENT);
      }
      allocator.close();
    }
  }

  /**
   * Test {@link JdbcToArrow#sqlToArrowVectorIterator(ResultSet, JdbcToArrowConfig)}.
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int testJdbcToArrow(JdbcState state) throws Exception {
    int valueCount = 0;
    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(state.resultSet, state.config)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        IntVector intVector = (IntVector) root.getFieldVectors().get(0);
        valueCount += intVector.getValueCount();
        root.close();
      }
    }
    return valueCount;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void consumeBenchmark(ConsumeState state) throws Exception {
    state.intConsumer.resetValueVector(state.intVector);
    state.longConsumer.resetValueVector(state.longVector);
    state.varCharConsumer.resetValueVector(state.varCharVector);
    state.bitConsumer.resetValueVector(state.bitVector);
    for (int i = 0; i < VALUE_COUNT; i++) {
      state.intConsumer.consume(state.resultSet);
      state.longConsumer.consume(state.resultSet);
      state.varCharConsumer.consume(state.resultSet);
      state.bitConsumer.consume(state.resultSet);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void consumeRowsBenchmark(RowConsumeState state) throws Exception {
    for (int i = 0; i < VALUE_COUNT; i++) {
      state.iter.compositeConsumer.consume(state.resultSet);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(JdbcAdapterBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}

