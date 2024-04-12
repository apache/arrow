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

package org.apache.arrow.flight.sql.example;

import static java.lang.String.format;
import static org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;
import static org.apache.arrow.flight.sql.impl.FlightSql.*;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.slf4j.Logger;

import com.google.protobuf.ByteString;

/**
 * Example {@link FlightSqlProducer} implementation showing an Apache Derby backed Flight SQL server that generally
 * supports all current features of Flight SQL.
 */
public class FlightSqlStatelessExample extends FlightSqlExample {
  private static final Logger LOGGER = getLogger(FlightSqlStatelessExample.class);

  public static void main(String[] args) throws Exception {
    Location location = Location.forGrpcInsecure("localhost", 55555);
    final FlightSqlStatelessExample example = new FlightSqlStatelessExample(location);
    Location listenLocation = Location.forGrpcInsecure("0.0.0.0", 55555);
    try (final BufferAllocator allocator = new RootAllocator();
         final FlightServer server = FlightServer.builder(allocator, listenLocation, example).build()) {
      server.start();
      server.awaitTermination();
    }
  }

  public FlightSqlStatelessExample(final Location location) {
    super(location);
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command, CallContext context,
                                                  FlightStream flightStream, StreamListener<PutResult> ackStream) {

    return () -> {
      try {
        final String query = new String(command.getPreparedStatementHandle().toString("UTF-8"));
        final Connection connection = dataSource.getConnection();
        final PreparedStatement preparedStatement = connection.prepareStatement(query,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        while (flightStream.next()) {
          final VectorSchemaRoot root = flightStream.getRoot();
          final JdbcParameterBinder binder = JdbcParameterBinder.builder(preparedStatement, root).bindAll().build();
          while (binder.next()) {
            // Do not execute() - will be done in a getStream call
          }

          final ByteArrayOutputStream parametersStream = new ByteArrayOutputStream();
          try (ArrowFileWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(parametersStream))
          ) {
            writer.start();
            writer.writeBatch();
          }

          if (parametersStream.size() > 0) {
            final DoPutPreparedStatementResultPOJO doPutPreparedStatementResultPOJO =
                    new DoPutPreparedStatementResultPOJO(query, parametersStream.toByteArray());
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
              oos.writeObject((Object) doPutPreparedStatementResultPOJO);
              final byte[] doPutPreparedStatementResultPOJOArr = bos.toByteArray();
              final DoPutPreparedStatementResult doPutPreparedStatementResult =
                      DoPutPreparedStatementResult.newBuilder()
                              .setPreparedStatementHandle(
                                      ByteString.copyFrom(ByteBuffer.wrap(doPutPreparedStatementResultPOJOArr)))
                              .build();

              try (final ArrowBuf buffer = rootAllocator.buffer(doPutPreparedStatementResult.getSerializedSize())) {
                buffer.writeBytes(doPutPreparedStatementResult.toByteArray());
                ackStream.onNext(PutResult.metadata(buffer));
              }
            }
          }
        }

      } catch (SQLException | IOException e) {
        ackStream.onError(CallStatus.INTERNAL
            .withDescription("Failed to bind parameters: " + e.getMessage())
            .withCause(e)
            .toRuntimeException());
        return;
      }

      ackStream.onCompleted();
    };
  }

  @Override
  public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
                                         final ServerStreamListener listener) {
    try {
      final byte[] handle = command.getPreparedStatementHandle().asReadOnlyByteBuffer().array();
      try (ByteArrayInputStream bis = new ByteArrayInputStream(handle);
           ObjectInputStream ois = new ObjectInputStream(bis)) {
        final DoPutPreparedStatementResultPOJO doPutPreparedStatementResultPOJO =
                (DoPutPreparedStatementResultPOJO) ois.readObject();
        final String query = doPutPreparedStatementResultPOJO.getQuery();
        final Connection connection = dataSource.getConnection();
        final PreparedStatement statement = connection.prepareStatement(query,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        try (ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(
                new ByteArrayReadableSeekableByteChannel(
                        doPutPreparedStatementResultPOJO.getParameters())), rootAllocator)) {

          for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
            reader.loadRecordBatch(arrowBlock);
            VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
            JdbcParameterBinder binder = JdbcParameterBinder.builder(statement, vectorSchemaRootRecover)
                    .bindAll().build();

            while (binder.next()) {
              try (final ResultSet resultSet = statement.executeQuery()) {
                final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR);
                try (final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
                  final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
                  listener.start(vectorSchemaRoot);

                  final ArrowVectorIterator iterator = sqlToArrowVectorIterator(resultSet, rootAllocator);
                  while (iterator.hasNext()) {
                    final VectorSchemaRoot batch = iterator.next();
                    if (batch.getRowCount() == 0) {
                      break;
                    }
                    final VectorUnloader unloader = new VectorUnloader(batch);
                    loader.load(unloader.getRecordBatch());
                    listener.putNext();
                    vectorSchemaRoot.clear();
                  }
                  listener.putNext();
                }
              }
            }
          }
        }
      }
    } catch (final SQLException | IOException | ClassNotFoundException e) {
      LOGGER.error(format("Failed to getStreamPreparedStatement: <%s>.", e.getMessage()), e);
      listener.error(CallStatus.INTERNAL.withDescription("Failed to prepare statement: " + e).toRuntimeException());
    } finally {
      listener.completed();
    }
  }
}
