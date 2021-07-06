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

package org.apache.arrow.flight.sql;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

/**
 * Proof of concept {@link FlightSqlProducer} implementation showing an Apache Derby backed Flight SQL server capable
 * of the following workflows:
 * - returning a list of tables from the action "GetTables".
 * - creation of a prepared statement from the action "GetPreparedStatement".
 * - execution of a prepared statement by using a {@link CommandPreparedStatementQuery} with getFlightInfo and
 * getStream.
 */
public class FlightSqlExample extends FlightSqlProducer implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlightSqlExample.class);

  private static final int BIT_WIDTH_8 = 8;
  private static final int BIT_WIDTH_16 = 16;
  private static final int BIT_WIDTH_32 = 32;
  private static final int BIT_WIDTH_64 = 64;
  private static final boolean IS_SIGNED_TRUE = true;

  private static final int BATCH_ROW_SIZE = 1000;

  private final Location location;
  private final PoolingDataSource<PoolableConnection> dataSource;

  private final LoadingCache<CommandPreparedStatementQuery, ResultSet> commandExecutePreparedStatementLoadingCache;
  private final LoadingCache<PreparedStatementCacheKey, PreparedStatementContext> preparedStatementLoadingCache;

  public FlightSqlExample(Location location) {
    removeDerbyDatabaseIfExists();
    populateDerbyDatabase();

    final ConnectionFactory connectionFactory =
            new DriverManagerConnectionFactory("jdbc:derby:target/derbyDB", null);
    final PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
    final ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
    poolableConnectionFactory.setPool(connectionPool);

    // PoolingDataSource takes ownership of connectionPool.
    dataSource = new PoolingDataSource<>(connectionPool);

    preparedStatementLoadingCache =
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(10, java.util.concurrent.TimeUnit.MINUTES)
                    .removalListener(new PreparedStatementRemovalListener())
                    .build(new PreparedStatementCacheLoader(dataSource));

    commandExecutePreparedStatementLoadingCache =
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(10, java.util.concurrent.TimeUnit.MINUTES)
                    .removalListener(new CommandExecutePreparedStatementRemovalListener())
                    .build(new CommandExecutePreparedStatementCacheLoader(preparedStatementLoadingCache));

    this.location = location;
  }

  @Override
  public void getTables(FlightSql.ActionGetTablesRequest request, CallContext context,
          StreamListener<Result> listener) {
    try {
      final String catalog = (request.getCatalog().isEmpty() ? null : request.getCatalog());

      final String schemaFilterPattern =
              (request.getSchemaFilterPattern().isEmpty() ? null : request.getSchemaFilterPattern());

      final String tableFilterPattern =
              (request.getTableNameFilterPattern().isEmpty() ? null : request.getTableNameFilterPattern());

      final String[] tableTypes = request.getTableTypesList().size() == 0 ? null :
              request.getTableTypesList().toArray(new String[request.getTableTypesList().size()]);

      try (final Connection connection = dataSource.getConnection();
           final ResultSet tables = connection.getMetaData().getTables(
                   catalog,
                   schemaFilterPattern,
                   tableFilterPattern,
                   tableTypes)) {
        while (tables.next()) {
          listener.onNext(getTableResult(tables, request.getIncludeSchema()));
        }
      }
    } catch (SQLException e) {
      listener.onError(e);
    } finally {
      listener.onCompleted();
    }
  }

  private Result getTableResult(final ResultSet tables, boolean includeSchema) throws SQLException {

    final String catalog = tables.getString("TABLE_CAT");
    final String schema = tables.getString("TABLE_SCHEM");
    final String table = tables.getString("TABLE_NAME");
    final String tableType = tables.getString("TABLE_TYPE");

    final ActionGetTablesResult.Builder builder = ActionGetTablesResult.newBuilder()
            .setCatalog(catalog)
            .setSchema(schema)
            .setTable(table)
            .setTableType(tableType);

    if (includeSchema) {
      final Schema pojoSchema = buildSchema(catalog, schema, table);
      builder.setArrowMetadata(ByteString.copyFrom(pojoSchema.toByteArray()));
    }

    return new Result(Any.pack(builder.build()).toByteArray());
  }

  @Override
  public void getPreparedStatement(FlightSql.ActionGetPreparedStatementRequest request, CallContext context,
          StreamListener<Result> listener) {
    final PreparedStatementCacheKey handle = new PreparedStatementCacheKey(
            UUID.randomUUID().toString(), request.getQuery());

    try {
      final PreparedStatementContext preparedStatementContext = preparedStatementLoadingCache.get(handle);
      final PreparedStatement preparedStatement = preparedStatementContext.getPreparedStatement();

      // todo
      final Schema pojoParameterMetaDataSchema = buildSchema(preparedStatement.getParameterMetaData());
      final Schema pojoResultSetSchema = buildSchema(preparedStatement.getMetaData());

      listener.onNext(new Result(
              Any.pack(ActionGetPreparedStatementResult.newBuilder()
                      .setDatasetSchema(ByteString.copyFrom(pojoResultSetSchema.toByteArray()))
                      .setParameterSchema(ByteString.copyFrom(pojoParameterMetaDataSchema.toByteArray()))
                      .setPreparedStatementHandle(handle.toProtocol())
                      .build())
                      .toByteArray()));

    } catch (Throwable e) {
      listener.onError(e);
    } finally {
      listener.onCompleted();
    }
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(CommandPreparedStatementQuery command, FlightDescriptor descriptor,
          CallContext context) {
    try {
      final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command);
      final Schema schema = buildSchema(resultSet.getMetaData());

      final List<FlightEndpoint> endpoints = ImmutableList
              .of(new FlightEndpoint(new Ticket(Any.pack(command).toByteArray()), location));

      return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    } catch (Throwable e) {
      logger.error("There was a problem executing the prepared statement", e);
      throw new FlightRuntimeException(new CallStatus(FlightStatusCode.INTERNAL, e, e.getMessage(), null));
    }
  }

  private Schema buildSchema(String catalog, String schema, String table) throws SQLException {
    final List<Field> fields = new ArrayList<>();

    try (final Connection connection = dataSource.getConnection();
         final ResultSet columns = connection.getMetaData().getColumns(
                 catalog,
                 schema,
                 table,
                 null);) {

      while (columns.next()) {
        final String columnName = columns.getString("COLUMN_NAME");
        final int jdbcDataType = columns.getInt("DATA_TYPE");
        final String jdbcDataTypeName = columns.getString("TYPE_NAME");
        final String jdbcIsNullable = columns.getString("IS_NULLABLE");
        final boolean arrowIsNullable = jdbcIsNullable.equals("YES");

        final int precision = columns.getInt("DECIMAL_DIGITS");
        final int scale = columns.getInt("COLUMN_SIZE");
        final ArrowType arrowType = FlightSqlUtils.getArrowTypeFromJDBCType(jdbcDataType, precision, scale);

        final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/null);
        fields.add(new Field(columnName, fieldType, null));
      }
    }

    return new Schema(fields);
  }

  @Override
  public void getStreamPreparedStatement(CommandPreparedStatementQuery command, CallContext context, Ticket ticket,
          ServerStreamListener listener) {
    try {
      final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command);
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      final Schema schema = buildSchema(resultSetMetaData);
      final DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();

      try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
           final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

        listener.start(root, dictionaryProvider);
        final int columnCount = resultSetMetaData.getColumnCount();

        while (resultSet.next()) {
          final int rowCounter = readBatch(resultSet, resultSetMetaData, root, columnCount);

          for (int resultSetColumnCounter = 1; resultSetColumnCounter <= columnCount; resultSetColumnCounter++) {
            final String columnName = resultSetMetaData.getColumnName(resultSetColumnCounter);
            root.getVector(columnName).setValueCount(rowCounter);
          }

          root.setRowCount(rowCounter);
          listener.putNext();
        }
      }
    } catch (Throwable e) {
      listener.error(e);
    } finally {
      listener.completed();
      commandExecutePreparedStatementLoadingCache.invalidate(command);
    }
  }

  private int readBatch(ResultSet resultSet, ResultSetMetaData resultSetMetaData, VectorSchemaRoot root,
          int columnCount) throws SQLException {
    int rowCounter = 0;
    do {
      for (int resultSetColumnCounter = 1; resultSetColumnCounter <= columnCount; resultSetColumnCounter++) {
        final String columnName = resultSetMetaData.getColumnName(resultSetColumnCounter);

        final FieldVector fieldVector = root.getVector(columnName);

        if (fieldVector instanceof VarCharVector) {
          final String value = resultSet.getString(resultSetColumnCounter);
          if (resultSet.wasNull()) {
            // TODO handle null
          } else {
            ((VarCharVector) fieldVector).setSafe(rowCounter, value.getBytes(), 0, value.length());
          }
        } else if (fieldVector instanceof IntVector) {
          final int value = resultSet.getInt(resultSetColumnCounter);

          if (resultSet.wasNull()) {
            // TODO handle null
          } else {
            ((IntVector) fieldVector).setSafe(rowCounter, value);
          }
        } else {
          throw new UnsupportedOperationException();
        }
      }
      rowCounter++;
    }
    while (rowCounter < BATCH_ROW_SIZE && resultSet.next());

    return rowCounter;
  }


  @Override
  public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context,
          StreamListener<Result> listener) {
    try {
      preparedStatementLoadingCache.invalidate(
              PreparedStatementCacheKey.fromProtocol(request.getPreparedStatementHandleBytes()));
    } catch (InvalidProtocolBufferException e) {
      listener.onError(e);
    } finally {
      listener.onCompleted();
    }
  }

  private Schema buildSchema(ResultSetMetaData resultSetMetaData) throws SQLException {
    Preconditions.checkNotNull(resultSetMetaData, "ResultSetMetaData object can't be null");
    final List<Field> resultSetFields = new ArrayList<>();

    for (int resultSetCounter = 1; resultSetCounter <= resultSetMetaData.getColumnCount(); resultSetCounter++) {
      final String name = resultSetMetaData.getColumnName(resultSetCounter);

      final int jdbcDataType = resultSetMetaData.getColumnType(resultSetCounter);

      final int jdbcIsNullable = resultSetMetaData.isNullable(resultSetCounter);
      final boolean arrowIsNullable = jdbcIsNullable == ResultSetMetaData.columnNullable;

      final int precision = resultSetMetaData.getPrecision(resultSetCounter);
      final int scale = resultSetMetaData.getScale(resultSetCounter);

      final ArrowType arrowType = getArrowTypeFromJDBCType(jdbcDataType, precision, scale);

      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/null);
      resultSetFields.add(new Field(name, fieldType, null));
    }
    final Schema pojoResultSetSchema = new Schema(resultSetFields);
    return pojoResultSetSchema;
  }

  private Schema buildSchema(ParameterMetaData parameterMetaData) throws SQLException {
    Preconditions.checkNotNull(parameterMetaData, "ParameterMetaData object can't be null");
    final List<Field> parameterFields = new ArrayList<>();

    for (int parameterCounter = 1; parameterCounter <= parameterMetaData.getParameterCount(); parameterCounter++) {
      final int jdbcDataType = parameterMetaData.getParameterType(parameterCounter);

      final int jdbcIsNullable = parameterMetaData.isNullable(parameterCounter);
      final boolean arrowIsNullable = jdbcIsNullable == ParameterMetaData.parameterNullable;

      final int precision = parameterMetaData.getPrecision(parameterCounter);
      final int scale = parameterMetaData.getScale(parameterCounter);

      final ArrowType arrowType = getArrowTypeFromJDBCType(jdbcDataType, precision, scale);

      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/null);
      parameterFields.add(new Field(null, fieldType, null));
    }
    final Schema pojoParameterMetaDataSchema = new Schema(parameterFields);
    return pojoParameterMetaDataSchema;
  }

  @Override
  public void close() throws Exception {
    try {
      commandExecutePreparedStatementLoadingCache.cleanUp();
    } catch (Throwable e) {
      // Swallow
    }

    try {
      preparedStatementLoadingCache.cleanUp();
    } catch (Throwable e) {
      // Swallow
    }

    AutoCloseables.close(dataSource);
  }

  private static class CommandExecutePreparedStatementRemovalListener
          implements RemovalListener<CommandPreparedStatementQuery, ResultSet> {
    @Override
    public void onRemoval(RemovalNotification<CommandPreparedStatementQuery, ResultSet> notification) {
      try {
        AutoCloseables.close(notification.getValue());
      } catch (Throwable e) {
        // Swallow
      }
    }
  }

  private static class CommandExecutePreparedStatementCacheLoader
          extends CacheLoader<CommandPreparedStatementQuery, ResultSet> {

    private final LoadingCache<PreparedStatementCacheKey, PreparedStatementContext> preparedStatementLoadingCache;

    private CommandExecutePreparedStatementCacheLoader(LoadingCache<PreparedStatementCacheKey,
            PreparedStatementContext> preparedStatementLoadingCache) {
      this.preparedStatementLoadingCache = preparedStatementLoadingCache;
    }

    @Override
    public ResultSet load(CommandPreparedStatementQuery commandExecutePreparedStatement)
            throws SQLException, InvalidProtocolBufferException, ExecutionException {
      final PreparedStatementCacheKey preparedStatementCacheKey =
              PreparedStatementCacheKey.fromProtocol(commandExecutePreparedStatement.getPreparedStatementHandle());
      final PreparedStatementContext preparedStatementContext = preparedStatementLoadingCache
              .get(preparedStatementCacheKey);
      return preparedStatementContext.getPreparedStatement().executeQuery();
    }
  }


  private static class PreparedStatementRemovalListener implements RemovalListener<PreparedStatementCacheKey,
          PreparedStatementContext> {
    @Override
    public void onRemoval(RemovalNotification<PreparedStatementCacheKey, PreparedStatementContext> notification) {
      try {
        AutoCloseables.close(notification.getValue());
      } catch (Throwable e) {
        // swallow
      }
    }
  }

  private static class PreparedStatementCacheLoader extends CacheLoader<PreparedStatementCacheKey,
          PreparedStatementContext> {

    // Owned by parent class.
    private final PoolingDataSource<PoolableConnection> dataSource;

    private PreparedStatementCacheLoader(PoolingDataSource<PoolableConnection> dataSource) {
      this.dataSource = dataSource;
    }

    @Override
    public PreparedStatementContext load(PreparedStatementCacheKey key) throws SQLException {

      // Ownership of the connection will be passed to the context.
      final Connection connection = dataSource.getConnection();
      try {
        final PreparedStatement preparedStatement = connection.prepareStatement(key.getSql());
        return new PreparedStatementContext(connection, preparedStatement);
      } catch (SQLException e) {
        connection.close();
        throw e;
      }
    }
  }

  private static void removeDerbyDatabaseIfExists() {
    final Path path = Paths.get("target" + File.separator + "derbyDB");

    try (final Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
    } catch (NoSuchFileException e) {
      // Ignore as there was no data directory to clean up.
    } catch (IOException e) {
      throw new RuntimeException("Failed to remove derby data directory.", e);
    }
  }

  private static void populateDerbyDatabase() {
    try (final Connection conn = DriverManager.getConnection("jdbc:derby:target/derbyDB;create=true")) {
      conn.createStatement().execute("CREATE TABLE intTable (keyName varchar(100), value int)");
      conn.createStatement().execute("INSERT INTO intTable (keyName, value) VALUES ('one', 1)");
      conn.createStatement().execute("INSERT INTO intTable (keyName, value) VALUES ('zero', 0)");
      conn.createStatement().execute("INSERT INTO intTable (keyName, value) VALUES ('negative one', -1)");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create derby database.", e);
    }
  }


  @Override
  public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoStatement(CommandStatementQuery command, FlightDescriptor descriptor,
          CallContext context) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getSqlInfo(CallContext context, StreamListener<Result> listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getCatalogs(FlightSql.ActionGetCatalogsRequest request, CallContext context,
          StreamListener<Result> listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getSchemas(FlightSql.ActionGetSchemasRequest request, CallContext context,
          StreamListener<Result> listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getTableTypes(CallContext context, StreamListener<Result> listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public SchemaResult getSchemaStatement(CommandStatementQuery command, FlightDescriptor descriptor,
          CallContext context) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate command,
          CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command, CallContext context,
          FlightStream flightStream, StreamListener<PutResult> ackStream) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command, CallContext context,
          FlightStream flightStream, StreamListener<PutResult> ackStream) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamStatement(CommandStatementQuery command, CallContext context, Ticket ticket,
          ServerStreamListener listener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }


  /**
   * Converts {@link java.sql.Types} values returned from JDBC Apis to Arrow types.
   *
   * @param jdbcDataType {@link java.sql.Types} value.
   * @param precision    Precision of the type.
   * @param scale        Scale of the type.
   * @return The Arrow equivalent type.
   */
  static ArrowType getArrowTypeFromJDBCType(int jdbcDataType, int precision, int scale) {
    switch (jdbcDataType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case Types.TINYINT:
        return new ArrowType.Int(BIT_WIDTH_8, IS_SIGNED_TRUE);
      case Types.SMALLINT:
        return new ArrowType.Int(BIT_WIDTH_16, IS_SIGNED_TRUE);
      case Types.INTEGER:
        return new ArrowType.Int(BIT_WIDTH_32, IS_SIGNED_TRUE);
      case Types.BIGINT:
        return new ArrowType.Int(BIT_WIDTH_64, IS_SIGNED_TRUE);
      case Types.FLOAT:
      case Types.REAL:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case Types.DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return new ArrowType.Decimal(precision, scale);
      case Types.DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case Types.TIME:
        return new ArrowType.Time(TimeUnit.MILLISECOND, BIT_WIDTH_32);
      case Types.TIMESTAMP:
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return ArrowType.Binary.INSTANCE;
      case Types.NULL:
        return ArrowType.Null.INSTANCE;

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.CLOB:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NCLOB:

      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.BLOB:
      case Types.REF:
      case Types.DATALINK:
      case Types.ROWID:
      case Types.SQLXML:
      case Types.REF_CURSOR:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP_WITH_TIMEZONE:
      default:
        return ArrowType.Utf8.INSTANCE;
    }
  }
}
