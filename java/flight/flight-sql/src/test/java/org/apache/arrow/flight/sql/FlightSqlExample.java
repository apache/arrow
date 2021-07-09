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

import static io.grpc.Status.UNIMPLEMENTED;
import static java.io.File.separator;
import static java.lang.String.format;
import static java.nio.file.Files.walk;
import static java.sql.DriverManager.getConnection;
import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DISTINCT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.REAL;
import static java.sql.Types.REF;
import static java.sql.Types.REF_CURSOR;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;
import static java.util.Comparator.reverseOrder;
import static java.util.Optional.empty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.management.ObjectName.WILDCARD;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;
import static org.apache.arrow.vector.types.DateUnit.DAY;
import static org.apache.arrow.vector.types.TimeUnit.MILLISECOND;
import static org.apache.arrow.vector.types.pojo.ArrowType.Null.INSTANCE;
import static org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetForeignKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
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
import org.slf4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Proof of concept {@link FlightSqlProducer} implementation showing an Apache Derby backed Flight SQL server capable
 * of the following workflows:
 * <!--
 * TODO Revise summary: is it still matching?
 * -->
 * - returning a list of tables from the action `GetTables`.
 * - creation of a prepared statement from the action `CreatePreparedStatement`.
 * - execution of a prepared statement by using a {@link CommandPreparedStatementQuery}
 * with {@link #getFlightInfo} and {@link #getStream}.
 */
public class FlightSqlExample extends FlightSqlProducer implements AutoCloseable {
  public static final String DATABASE_URI = "jdbc:derby:target/derbyDB";
  private static final Logger LOGGER = getLogger(FlightSqlExample.class);
  private static final int BIT_WIDTH_8 = 8;
  private static final int BIT_WIDTH_16 = 16;
  private static final int BIT_WIDTH_32 = 32;
  private static final int BIT_WIDTH_64 = 64;
  private static final boolean IS_SIGNED_TRUE = true;
  private static final int BATCH_ROW_SIZE = 1000;
  @SuppressWarnings("unused") // TODO Verify whether this is needed.
  private final Location location;
  private final PoolingDataSource<PoolableConnection> dataSource;

  private final LoadingCache<CommandPreparedStatementQuery, ResultSet> commandExecutePreparedStatementLoadingCache;
  private final LoadingCache<PreparedStatementCacheKey, PreparedStatementContext> preparedStatementLoadingCache;

  public FlightSqlExample(final Location location) {
    checkState(
        removeDerbyDatabaseIfExists() && populateDerbyDatabase(),
        "Failed to reset Derby database!");

    final ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(DATABASE_URI, new Properties());
    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, WILDCARD);
    final ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);

    poolableConnectionFactory.setPool(connectionPool);
    // PoolingDataSource takes ownership of `connectionPool`
    dataSource = new PoolingDataSource<>(connectionPool);

    preparedStatementLoadingCache =
        CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, MINUTES)
            .removalListener(new PreparedStatementRemovalListener())
            .build(new PreparedStatementCacheLoader(dataSource));

    commandExecutePreparedStatementLoadingCache =
        CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, MINUTES)
            .removalListener(new CommandExecutePreparedStatementRemovalListener())
            .build(new CommandExecutePreparedStatementCacheLoader(preparedStatementLoadingCache));

    this.location = location;
  }

  private static boolean removeDerbyDatabaseIfExists() {
    boolean wasSuccess;
    final Path path = Paths.get("target" + separator + "derbyDB");

    try (final Stream<Path> walk = walk(path)) {
      /*
       * Iterate over all paths to delete, mapping each path to the outcome of its own
       * deletion as a boolean representing whether or not each individual operation was
       * successful; then reduce all booleans into a single answer, and store that into
       * `wasSuccess`, which will later be returned by this method.
       * If for whatever reason the resulting `Stream<Boolean>` is empty, throw an `IOException`;
       * this not expected.
       */
      wasSuccess = walk.sorted(reverseOrder())
          .map(pathToDelete -> pathToDelete.toFile().delete())
          .reduce(Boolean::logicalAnd).orElseThrow(IOException::new);
    } catch (IOException e) {
      /*
       * The only acceptable scenario for an `IOException` to be thrown here is if
       * an attempt to delete an non-existing file takes place -- which should be
       * alright, since they would be deleted anyway.
       */
      if (!(wasSuccess = e instanceof NoSuchFileException)) {
        LOGGER.error(format("Failed attempt to clear DerbyDB: <%s>", e.getMessage()), e);
      }
    }

    return wasSuccess;
  }

  private static boolean populateDerbyDatabase() {
    Optional<SQLException> exception = empty();
    try (final Connection connection = getConnection("jdbc:derby:target/derbyDB;create=true");
         Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE intTable (keyName varchar(100), value int)");
      statement.execute("INSERT INTO intTable (keyName, value) VALUES ('one', 1)");
      statement.execute("INSERT INTO intTable (keyName, value) VALUES ('zero', 0)");
      statement.execute("INSERT INTO intTable (keyName, value) VALUES ('negative one', -1)");
    } catch (SQLException e) {
      LOGGER.error(
          format("Failed attempt to populate DerbyDB: <%s>", e.getMessage()),
          (exception = Optional.of(e)).get());
    }

    return !exception.isPresent();
  }

  /**
   * Converts {@link Types} values returned from JDBC Apis to Arrow types.
   *
   * @param jdbcDataType {@link Types} value.
   * @param precision    Precision of the type.
   * @param scale        Scale of the type.
   * @return The Arrow equivalent type.
   */
  static ArrowType getArrowTypeFromJdbcType(int jdbcDataType, int precision, int scale) {
    switch (jdbcDataType) {
      case BIT:
      case BOOLEAN:
        return Bool.INSTANCE;
      case TINYINT:
        // sint8
        return new Int(BIT_WIDTH_8, IS_SIGNED_TRUE);
      case SMALLINT:
        // sint16
        return new Int(BIT_WIDTH_16, IS_SIGNED_TRUE);
      case INTEGER:
        // sint32
        return new Int(BIT_WIDTH_32, IS_SIGNED_TRUE);
      case BIGINT:
        // sint64
        return new Int(BIT_WIDTH_64, IS_SIGNED_TRUE);
      case FLOAT:
      case REAL:
        return new FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new FloatingPoint(FloatingPointPrecision.DOUBLE);
      case NUMERIC:
      case DECIMAL:
        return new Decimal(precision, scale);
      case DATE:
        return new Date(DAY);
      case TIME:
        // millis as int32
        return new Time(MILLISECOND, BIT_WIDTH_32);
      case TIMESTAMP:
        return new Timestamp(MILLISECOND, null);
      case BINARY:
      case VARBINARY:
      case LONGVARBINARY:
        return Binary.INSTANCE;
      case NULL:
        return INSTANCE;

      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
      case CLOB:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
      case NCLOB:

      case OTHER:
      case JAVA_OBJECT:
      case DISTINCT:
      case STRUCT:
      case ARRAY:
      case BLOB:
      case REF:
      case DATALINK:
      case ROWID:
      case SQLXML:
      case REF_CURSOR:
      case TIME_WITH_TIMEZONE:
      case TIMESTAMP_WITH_TIMEZONE:
      default:
        return Utf8.INSTANCE;
    }
  }

  private Result getTableResult(final ResultSet tables, boolean includeSchema) throws SQLException {
    // TODO
    throw UNIMPLEMENTED.asRuntimeException();
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
        @SuppressWarnings("unused") // TODO Investigate why this might be here.
        final String jdbcDataTypeName = columns.getString("TYPE_NAME");
        final String jdbcIsNullable = columns.getString("IS_NULLABLE");
        final boolean arrowIsNullable = "YES".equals(jdbcIsNullable);

        final int precision = columns.getInt("DECIMAL_DIGITS");
        final int scale = columns.getInt("COLUMN_SIZE");
        final ArrowType arrowType = getArrowTypeFromJdbcType(jdbcDataType, precision, scale);

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
      final DictionaryProvider dictionaryProvider = new MapDictionaryProvider();

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
    } catch (Throwable t) {
      listener.error(t);
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

        try (final FieldVector vector = root.getVector(columnName)) {
          if (vector instanceof VarCharVector) {
            final String value = resultSet.getString(resultSetColumnCounter);
            if (resultSet.wasNull()) {
              // TODO handle null
            } else {
              ((VarCharVector) vector).setSafe(rowCounter, value.getBytes(), 0, value.length());
            }
          } else if (vector instanceof IntVector) {
            final int value = resultSet.getInt(resultSetColumnCounter);

            if (resultSet.wasNull()) {
              // TODO handle null
            } else {
              ((IntVector) vector).setSafe(rowCounter, value);
            }
          } else {
            throw new UnsupportedOperationException();
          }
        }
      }
      rowCounter++;
    }
    while (rowCounter < BATCH_ROW_SIZE && resultSet.next());

    return rowCounter;
  }

  @Override
  public void closePreparedStatement(ActionClosePreparedStatementRequest request, CallContext context,
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

  @Override
  public FlightInfo getFlightInfoStatement(final CommandStatementQuery command, final CallContext context,
                                           final FlightDescriptor descriptor) {
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
                                                   final CallContext context,
                                                   final FlightDescriptor descriptor) {
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
                                         final FlightDescriptor descriptor) {
    throw UNIMPLEMENTED.asRuntimeException();
  }

  private Schema buildSchema(ResultSetMetaData resultSetMetaData) throws SQLException {
    final List<Field> resultSetFields = new ArrayList<>();

    for (int resultSetCounter = 1;
         resultSetCounter <= checkNotNull(resultSetMetaData, "ResultSetMetaData object can't be null")
             .getColumnCount();
         resultSetCounter++) {
      final String name = resultSetMetaData.getColumnName(resultSetCounter);

      final int jdbcDataType = resultSetMetaData.getColumnType(resultSetCounter);

      final int jdbcIsNullable = resultSetMetaData.isNullable(resultSetCounter);
      final boolean arrowIsNullable = jdbcIsNullable == ResultSetMetaData.columnNullable;

      final int precision = resultSetMetaData.getPrecision(resultSetCounter);
      final int scale = resultSetMetaData.getScale(resultSetCounter);

      final ArrowType arrowType = getArrowTypeFromJdbcType(jdbcDataType, precision, scale);

      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/null);
      resultSetFields.add(new Field(name, fieldType, null));
    }

    return new Schema(resultSetFields);
  }

  private Schema buildSchema(ParameterMetaData parameterMetaData) throws SQLException {
    final List<Field> parameterFields = new ArrayList<>();

    for (int parameterCounter = 1; parameterCounter <=
        checkNotNull(parameterMetaData, "ParameterMetaData object can't be null")
            .getParameterCount();
         parameterCounter++) {
      final int jdbcDataType = parameterMetaData.getParameterType(parameterCounter);

      final int jdbcIsNullable = parameterMetaData.isNullable(parameterCounter);
      final boolean arrowIsNullable = jdbcIsNullable == ParameterMetaData.parameterNullable;

      final int precision = parameterMetaData.getPrecision(parameterCounter);
      final int scale = parameterMetaData.getScale(parameterCounter);

      final ArrowType arrowType = getArrowTypeFromJdbcType(jdbcDataType, precision, scale);

      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/null);
      parameterFields.add(new Field(null, fieldType, null));
    }

    return new Schema(parameterFields);
  }

  @Override
  public void close() throws Exception {
    try {
      commandExecutePreparedStatementLoadingCache.cleanUp();
    } catch (Throwable t) {
      LOGGER.error(format("Failed to close resources: <%s>", t.getMessage()), t);
    }

    try {
      preparedStatementLoadingCache.cleanUp();
    } catch (Throwable t) {
      LOGGER.error(format("Failed to close resources: <%s>", t.getMessage()), t);
    }

    AutoCloseables.close(dataSource);
  }

  @Override
  public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void createPreparedStatement(final ActionCreatePreparedStatementRequest request, final CallContext context,
                                      final StreamListener<Result> listener) {
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate command,
                                     CallContext context, FlightStream flightStream,
                                     StreamListener<PutResult> ackStream) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command, CallContext context,
                                                   FlightStream flightStream, StreamListener<PutResult> ackStream) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command, CallContext context,
                                                  FlightStream flightStream, StreamListener<PutResult> ackStream) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(final CommandGetSqlInfo request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(final CommandGetSqlInfo command, final CallContext context, final Ticket ticket,
                               final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(final CommandGetCatalogs request, final CallContext context,
                                          final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamCatalogs(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSchemas(final CommandGetSchemas request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamSchemas(final CommandGetSchemas command, final CallContext context, final Ticket ticket,
                               final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTables(final CommandGetTables request, final CallContext context,
                                        final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamTables(final CommandGetTables command, final CallContext context, final Ticket ticket,
                              final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(final CallContext context, final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamTableTypes(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(final CommandGetPrimaryKeys request, final CallContext context,
                                             final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamPrimaryKeys(final CommandGetPrimaryKeys command, final CallContext context, final Ticket ticket,
                                   final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoForeignKeys(final CommandGetForeignKeys request, final CallContext context,
                                             final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamForeignKeys(final CommandGetForeignKeys command, final CallContext context, final Ticket ticket,
                                   final ServerStreamListener listener) {
    // TODO - build example implementation
    throw UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamStatement(CommandStatementQuery command, CallContext context, Ticket ticket,
                                 ServerStreamListener listener) {
    throw UNIMPLEMENTED.asRuntimeException();
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
}
