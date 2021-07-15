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

import static com.google.common.base.Strings.emptyToNull;
import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator;
import static org.apache.arrow.flight.FlightStatusCode.INTERNAL;
import static org.slf4j.LoggerFactory.getLogger;

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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetForeignKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
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
import com.google.protobuf.ProtocolStringList;

import io.grpc.Status;

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
  private static final String DATABASE_URI = "jdbc:derby:target/derbyDB";
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
    Preconditions.checkState(
        removeDerbyDatabaseIfExists() && populateDerbyDatabase(),
        "Failed to reset Derby database!");

    final ConnectionFactory connectionFactory =
        new DriverManagerConnectionFactory(DATABASE_URI, new Properties());
    final PoolableConnectionFactory poolableConnectionFactory =
        new PoolableConnectionFactory(connectionFactory, null);
    final ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);

    poolableConnectionFactory.setPool(connectionPool);
    // PoolingDataSource takes ownership of `connectionPool`
    dataSource = new PoolingDataSource<>(connectionPool);

    preparedStatementLoadingCache =
        CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .removalListener(new PreparedStatementRemovalListener())
            .build(new PreparedStatementCacheLoader(dataSource));

    commandExecutePreparedStatementLoadingCache =
        CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .removalListener(new CommandExecutePreparedStatementRemovalListener())
            .build(new CommandExecutePreparedStatementCacheLoader(preparedStatementLoadingCache));

    this.location = location;
  }

  private static boolean removeDerbyDatabaseIfExists() {
    boolean wasSuccess;
    final Path path = Paths.get("target" + File.separator + "derbyDB");

    try (final Stream<Path> walk = Files.walk(path)) {
      /*
       * Iterate over all paths to delete, mapping each path to the outcome of its own
       * deletion as a boolean representing whether or not each individual operation was
       * successful; then reduce all booleans into a single answer, and store that into
       * `wasSuccess`, which will later be returned by this method.
       * If for whatever reason the resulting `Stream<Boolean>` is empty, throw an `IOException`;
       * this not expected.
       */
      wasSuccess = walk.sorted(Comparator.reverseOrder()).map(Path::toFile).map(File::delete)
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
   * @deprecated should replace.
   */
  @Deprecated
  static ArrowType getArrowTypeFromJdbcType(int jdbcDataType, int precision, int scale) {
    switch (jdbcDataType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return Bool.INSTANCE;
      case Types.TINYINT:
        // sint8
        return new Int(BIT_WIDTH_8, IS_SIGNED_TRUE);
      case Types.SMALLINT:
        // sint16
        return new Int(BIT_WIDTH_16, IS_SIGNED_TRUE);
      case Types.INTEGER:
        // sint32
        return new Int(BIT_WIDTH_32, IS_SIGNED_TRUE);
      case Types.BIGINT:
        // sint64
        return new Int(BIT_WIDTH_64, IS_SIGNED_TRUE);
      case Types.FLOAT:
      case Types.REAL:
        return new FloatingPoint(FloatingPointPrecision.SINGLE);
      case Types.DOUBLE:
        return new FloatingPoint(FloatingPointPrecision.DOUBLE);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return new Decimal(precision, scale);
      case Types.DATE:
        return new Date(DateUnit.DAY);
      case Types.TIME:
        // millis as int32
        return new Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, BIT_WIDTH_32);
      case Types.TIMESTAMP:
        return new Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return Binary.INSTANCE;
      case Types.NULL:
        return Null.INSTANCE;

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

  /**
   * Make the provided {@link ServerStreamListener} listen to the provided {@link ResultSet}.
   *
   * @param data     data to listen to.
   * @param listener the listener.
   * @throws SQLException an exception.
   * @throws IOException  an exception.
   */
  protected static void makeListen(final Iterable<VectorSchemaRoot> data, final ServerStreamListener listener)
      throws SQLException, IOException {
    data.forEach(root -> {
      listener.start(root);
      listener.putNext();
    });
  }

  protected Iterable<VectorSchemaRoot> getTablesRoot(final ResultSet data,
                                                            boolean includeSchema)
      throws SQLException, IOException {
    return stream(getVectorsFromData(data).spliterator(), false)
        .map(root ->
            new VectorSchemaRoot(
                root.getFieldVectors().stream().filter(vector -> {
                  switch (vector.getName()) {
                    case "TABLE_CAT":
                    case "TABLE_SCHEM":
                    case "TABLE_NAME":
                    case "TABLE_TYPE":
                      return true;
                    default:
                      return false;
                  }
                }).collect(toList())))
        .map(root -> {
          final VarCharVector vector =
              new VarCharVector("SCHEMA", new RootAllocator(Long.MAX_VALUE));
          final int valueCount = root.getRowCount();
          IntStream.range(0, valueCount)
              .forEachOrdered(
                  index ->
                      vector.setSafe(index, new Text(getSchemaTables().getSchema().toJson())));
          vector.setValueCount(valueCount);
          List<FieldVector> vectors = root.getFieldVectors();
          vectors.add(vector);
          return vectors;
        })
        .map(VectorSchemaRoot::new)
        .collect(toList());
  }

  protected static final Iterable<VectorSchemaRoot> getVectorsFromData(final ResultSet data)
      throws SQLException, IOException {
    Iterator<VectorSchemaRoot> iterator = sqlToArrowVectorIterator(data, new RootAllocator(Long.MAX_VALUE));
    return () -> iterator;
  }

  @Override
  public void getStreamPreparedStatement(CommandPreparedStatementQuery command, CallContext context, Ticket ticket,
                                         ServerStreamListener listener) {
    try (final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command)) {
      makeListen(getVectorsFromData(resultSet), listener);
    } catch (SQLException | IOException | ExecutionException e) {
      LOGGER.error(format("Failed to getStreamPreparedStatement: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      commandExecutePreparedStatementLoadingCache.invalidate(command);
    }
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
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
                                                   final CallContext context,
                                                   final FlightDescriptor descriptor) {
    try {
      /*
       * Do NOT prematurely close the `resultSet`!
       * Should be closed upon executing `ClosePreparedStatement`.
       */
      final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command);
      final Schema schema = buildSchema(resultSet.getMetaData());

      final List<FlightEndpoint> endpoints =
          singletonList(new FlightEndpoint(new Ticket(pack(command).toByteArray()), location));

      return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    } catch (ExecutionException | SQLException e) {
      LOGGER.error(
          format("There was a problem executing the prepared statement: <%s>.", e.getMessage()),
          e);
      throw new FlightRuntimeException(new CallStatus(INTERNAL, e, e.getMessage(), null));
    }
  }

  @Override
  public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
                                         final FlightDescriptor descriptor) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  // TODO Maybe replace with `FlightSqlProducer#getSchema`
  @Deprecated
  private Schema buildSchema(ResultSetMetaData resultSetMetaData) throws SQLException {
    final List<Field> resultSetFields = new ArrayList<>();

    for (int resultSetCounter = 1;
         resultSetCounter <= Preconditions.checkNotNull(resultSetMetaData, "ResultSetMetaData object can't be null")
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

  @Deprecated
  private Schema buildSchema(ParameterMetaData parameterMetaData) throws SQLException {
    final List<Field> parameterFields = new ArrayList<>();

    for (int parameterCounter = 1; parameterCounter <=
        Preconditions.checkNotNull(parameterMetaData, "ParameterMetaData object can't be null")
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
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void createPreparedStatement(final ActionCreatePreparedStatementRequest request, final CallContext context,
                                      final StreamListener<Result> listener) {
    final PreparedStatementCacheKey cacheKey =
        new PreparedStatementCacheKey(randomUUID().toString(), request.getQuery());
    try {
      final PreparedStatementContext statementContext =
          preparedStatementLoadingCache.get(cacheKey);
      /*
       * Do NOT prematurely close the `resultSet`!
       * Should be closed upon executing `ClosePreparedStatement`.
       */
      final PreparedStatement preparedStatement = statementContext.getPreparedStatement();
      final Schema parameterSchema = buildSchema(preparedStatement.getParameterMetaData());
      final Schema datasetSchema = buildSchema(preparedStatement.getMetaData());
      final ActionCreatePreparedStatementResult result = ActionCreatePreparedStatementResult.newBuilder()
          .setDatasetSchema(copyFrom(datasetSchema.toByteArray()))
          .setParameterSchema(copyFrom(parameterSchema.toByteArray()))
          .setPreparedStatementHandle(cacheKey.toProtocol())
          .build();
      listener.onNext(new Result(pack(result).toByteArray()));
    } catch (final Throwable t) {
      listener.onError(t);
    } finally {
      listener.onCompleted();
    }
  }

  @Override
  public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate command,
                                     CallContext context, FlightStream flightStream,
                                     StreamListener<PutResult> ackStream) {
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
  public FlightInfo getFlightInfoSqlInfo(final CommandGetSqlInfo request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(final CommandGetSqlInfo command, final CallContext context, final Ticket ticket,
                               final ServerStreamListener listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(final CommandGetCatalogs request, final CallContext context,
                                          final FlightDescriptor descriptor) {
    final Schema schema = getSchemaCatalogs().getSchema();
    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(new Ticket(pack(request).toByteArray()), location));
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }

  @Override
  public void getStreamCatalogs(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    try {
      final ResultSet catalogs = dataSource.getConnection().getMetaData().getCatalogs();
      makeListen(getVectorsFromData(catalogs), listener);
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoSchemas(final CommandGetSchemas request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    final Schema schema = getSchemaSchemas().getSchema();
    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(new Ticket(pack(request).toByteArray()), location));
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }

  @Override
  public void getStreamSchemas(final CommandGetSchemas command, final CallContext context, final Ticket ticket,
                               final ServerStreamListener listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTables(final CommandGetTables request, final CallContext context,
                                        final FlightDescriptor descriptor) {
    final Schema schema = getSchemaTables().getSchema();
    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(new Ticket(pack(request).toByteArray()), location));
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }

  @Override
  public void getStreamTables(final CommandGetTables command, final CallContext context,
                              final Ticket ticket, final ServerStreamListener listener) {
    final String catalog = emptyToNull(command.getCatalog());
    final String schemaFilterPattern = emptyToNull(command.getSchemaFilterPattern());
    final String tableFilterPattern = emptyToNull(command.getTableNameFilterPattern());

    final ProtocolStringList protocolStringList = command.getTableTypesList();
    final int protocolSize = protocolStringList.size();
    final String[] tableTypes =
        protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);

    try {
      final Connection connection = DriverManager.getConnection(DATABASE_URI);
      final ResultSet resultSet = connection.getMetaData()
          .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes);
      makeListen(getTablesRoot(resultSet, command.getIncludeSchema()), listener);
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamTables: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(final CallContext context, final FlightDescriptor descriptor) {
    try {
      final Schema schema = getSchemaTableTypes().getSchema();
      final List<FlightEndpoint> endpoints =
          singletonList(new FlightEndpoint(
              new Ticket(pack(CommandGetTableTypes.parseFrom(descriptor.getCommand())).toByteArray()), location));
      return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    } catch (InvalidProtocolBufferException e) {
      LOGGER.error(format("Failed to getFlightInfoTableTypes: <%s>.", e.getMessage()), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void getStreamTableTypes(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    try {
      final ResultSet tableTypes = dataSource.getConnection().getMetaData().getTableTypes();
      makeListen(getVectorsFromData(tableTypes), listener);
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamTableTypes: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(final CommandGetPrimaryKeys request, final CallContext context,
                                             final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamPrimaryKeys(final CommandGetPrimaryKeys command, final CallContext context, final Ticket ticket,
                                   final ServerStreamListener listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoForeignKeys(final CommandGetForeignKeys request, final CallContext context,
                                             final FlightDescriptor descriptor) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamForeignKeys(final CommandGetForeignKeys command, final CallContext context, final Ticket ticket,
                                   final ServerStreamListener listener) {
    // TODO - build example implementation
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamStatement(CommandStatementQuery command, CallContext context, Ticket ticket,
                                 ServerStreamListener listener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
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

      // Ownership of the connection will be passed to the context. Do NOT close!
      final Connection connection = dataSource.getConnection();
      try {
        /*
         * Do NOT prematurely close the `preparedStatement`!
         * Should be closed upon executing `ClosePreparedStatement`.
         */
        final PreparedStatement preparedStatement = connection.prepareStatement(key.getSql());
        return new PreparedStatementContext(connection, preparedStatement);
      } catch (SQLException e) {
        connection.close();
        throw e;
      }
    }
  }
}
