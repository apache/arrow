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

package org.apache.arrow.driver.jdbc;

import static java.util.Optional.ofNullable;
import static org.apache.arrow.driver.jdbc.utils.FlightStreamQueue.createNewQueue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.driver.jdbc.utils.FlightStreamQueue;
import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

/**
 * {@link ResultSet} implementation for Arrow Flight used to access the results of multiple {@link FlightStream}
 * objects.
 */
public final class ArrowFlightJdbcFlightStreamResultSet
    extends ArrowFlightJdbcVectorSchemaRootResultSet {
  private static final String BLOCKING_QUEUE_PARAM = "buffersize";
  private static final int DEFAULT_BLOCKING_QUEUE_CAPACITY = 5;
  private final ArrowFlightConnection connection;
  private FlightStream currentFlightStream;
  private FlightStreamQueue flightStreamQueue;
  private VectorSchemaRootTransformer transformer;
  private BlockingQueue<VectorSchemaRoot> vectorSchemaRoots;
  private VectorSchemaRoot currentRoot;
  private Schema schema;
  private boolean streamHasNext;
  private BufferAllocator allocator;


  ArrowFlightJdbcFlightStreamResultSet(final AvaticaStatement statement,
                                       final QueryState state,
                                       final Meta.Signature signature,
                                       final ResultSetMetaData resultSetMetaData,
                                       final TimeZone timeZone,
                                       final Meta.Frame firstFrame) throws SQLException {
    super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = (ArrowFlightConnection) statement.connection;
    initializeVectorSchemaRootsQueue();
  }

  ArrowFlightJdbcFlightStreamResultSet(final ArrowFlightConnection connection,
                                       final QueryState state,
                                       final Meta.Signature signature,
                                       final ResultSetMetaData resultSetMetaData,
                                       final TimeZone timeZone,
                                       final Meta.Frame firstFrame) throws SQLException {
    super(null, state, signature, resultSetMetaData, timeZone, firstFrame);
    this.connection = connection;
    initializeVectorSchemaRootsQueue();
  }

  /**
   * Create a {@link ResultSet} which pulls data from given {@link FlightInfo}.
   *
   * @param connection  The connection linked to the returned ResultSet.
   * @param flightInfo  The FlightInfo from which data will be iterated by the returned ResultSet.
   * @param transformer Optional transformer for processing VectorSchemaRoot before access from ResultSet
   * @return A ResultSet which pulls data from given FlightInfo.
   */
  static ArrowFlightJdbcFlightStreamResultSet fromFlightInfo(
      final ArrowFlightConnection connection,
      final FlightInfo flightInfo,
      final VectorSchemaRootTransformer transformer) throws SQLException {
    // Similar to how org.apache.calcite.avatica.util.ArrayFactoryImpl does

    final TimeZone timeZone = TimeZone.getDefault();
    final QueryState state = new QueryState();

    final Meta.Signature signature = ArrowFlightMetaImpl.newSignature(null);

    final AvaticaResultSetMetaData resultSetMetaData =
        new AvaticaResultSetMetaData(null, null, signature);
    final ArrowFlightJdbcFlightStreamResultSet resultSet =
        new ArrowFlightJdbcFlightStreamResultSet(connection, state, signature, resultSetMetaData,
            timeZone, null);
    resultSet.transformer = transformer;

    resultSet.execute(flightInfo);
    return resultSet;
  }

  private void initializeVectorSchemaRootsQueue() throws SQLException {
    if (vectorSchemaRoots == null) {
      try {
        int blockingQueueCapacity = ofNullable(connection.getClientInfo().getProperty(BLOCKING_QUEUE_PARAM))
                .map(Integer::parseInt)
                .filter(s -> s > 0)
                .orElse(DEFAULT_BLOCKING_QUEUE_CAPACITY);

        vectorSchemaRoots = new LinkedBlockingQueue<>(blockingQueueCapacity);
      } catch (java.lang.NumberFormatException e) {
        throw new SQLException("Invalid value for 'buffersize' was provided", e);
      }
    }
  }

  @Override
  protected AvaticaResultSet execute() throws SQLException {
    final FlightInfo flightInfo = ((ArrowFlightInfoStatement) statement).executeFlightInfoQuery();

    if (flightInfo != null) {
      schema = flightInfo.getSchema();
      execute(flightInfo);
    }
    return this;
  }

  private void execute(final FlightInfo flightInfo) throws SQLException {
    // load new FlightStreamQueue
    ofNullable(flightStreamQueue).ifPresent(AutoCloseables::closeNoChecked);
    flightStreamQueue = createNewQueue(connection.getExecutorService());

    // load new FlightStream
    flightStreamQueue.enqueue(connection.getClientHandler().getStreams(flightInfo));
    ofNullable(currentFlightStream).ifPresent(AutoCloseables::closeNoChecked);
    currentFlightStream = getNextFlightStream(true);

    // Ownership of the root will be passed onto the cursor.
    if (currentFlightStream != null) {
      storeRootsFromStreamAsync();
      executeNextRoot();
    }
  }

  private BufferAllocator getAllocator() {
    if (allocator == null) {
      allocator = connection.getBufferAllocator().newChildAllocator("vsr-copier", 0, Long.MAX_VALUE);
    }

    return allocator;
  }

  private VectorSchemaRoot cloneRoot(VectorSchemaRoot originalRoot) {
    VectorSchemaRoot theRoot = VectorSchemaRoot.create(originalRoot.getSchema(), getAllocator());
    VectorLoader loader = new VectorLoader(theRoot);
    VectorUnloader unloader = new VectorUnloader(originalRoot);
    try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
      loader.load(recordBatch);
    }
    return theRoot;
  }

  private void storeRoot(VectorSchemaRoot originalRoot) throws SQLException {
    VectorSchemaRoot theRoot = cloneRoot(originalRoot);
    VectorSchemaRoot transformedRoot = null;
    if (transformer != null) {
      try {
        transformedRoot = transformer.transform(theRoot, null);
        theRoot.close();
      } catch (final Exception e) {
        throw new SQLException("Failed to transform VectorSchemaRoot.", e);
      }
    }

    try {
      vectorSchemaRoots.put(ofNullable(transformedRoot).orElse(theRoot));
    } catch (InterruptedException e) {
      throw new SQLException("Could not put root to the queue", e);
    }
  }

  private void executeNextRoot() throws SQLException {
    try {
      ofNullable(currentRoot).ifPresent(AutoCloseables::closeNoChecked);
      currentRoot = vectorSchemaRoots.poll(10, TimeUnit.SECONDS);
      execute(currentRoot, schema);
    } catch (InterruptedException e) {
      throw new SQLException("Could not take root from the queue", e);
    }
  }

  private void storeRootsFromStreamAsync() {
    CompletableFuture.runAsync(() -> {
      while (vectorSchemaRoots.remainingCapacity() > 0) {
        try {
          currentFlightStream = ofNullable(currentFlightStream).orElse(getNextFlightStream(false));
          streamHasNext = currentFlightStream.next();
          if (!streamHasNext) {
            flightStreamQueue.enqueue(currentFlightStream);
          }
          storeRoot(currentFlightStream.getRoot());
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  @Override
  public boolean next() throws SQLException {
    if (vectorSchemaRoots.isEmpty() && currentFlightStream == null) {
      return false;
    }
    while (true) {
      final boolean hasNext = super.next();
      final int maxRows = statement != null ? statement.getMaxRows() : 0;
      if (maxRows != 0 && this.getRow() > maxRows) {
        if (statement.isCloseOnCompletion()) {
          statement.close();
        }
        return false;
      }

      if (hasNext) {
        storeRootsFromStreamAsync();
        return true;
      }

      storeRootsFromStreamAsync();
      executeNextRoot();
      if (streamHasNext) {
        continue;
      }

      if (statement != null && statement.isCloseOnCompletion()) {
        statement.close();
      }

      return false;
    }
  }

  private void cleanUpResources() throws Exception {
    if (flightStreamQueue != null) {
      // flightStreamQueue should close currentFlightStream internally
      flightStreamQueue.close();
    } else if (currentFlightStream != null) {
      // close is only called for currentFlightStream if there's no queue
      currentFlightStream.close();
    }

    List<VectorSchemaRoot> roots = new LinkedList<>();
    vectorSchemaRoots.drainTo(roots);
    roots.forEach(AutoCloseables::closeNoChecked);
    ofNullable(currentRoot).ifPresent(AutoCloseables::closeNoChecked);
  }

  @Override
  protected void cancel() {
    super.cancel();
    final FlightStream currentFlightStream = this.currentFlightStream;
    if (currentFlightStream != null) {
      currentFlightStream.cancel("Cancel", null);
    }

    try {
      cleanUpResources();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void close() {
    try {
      cleanUpResources();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      super.close();
    }
  }

  private FlightStream getNextFlightStream(final boolean isExecution) throws SQLException {
    if (isExecution) {
      final int statementTimeout = statement != null ? statement.getQueryTimeout() : 0;
      return statementTimeout != 0 ?
          flightStreamQueue.next(statementTimeout, TimeUnit.SECONDS) : flightStreamQueue.next();
    } else {
      return flightStreamQueue.next();
    }
  }
}
