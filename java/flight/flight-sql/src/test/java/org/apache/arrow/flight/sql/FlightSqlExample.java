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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
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
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
import com.google.common.collect.ImmutableList;
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
  private final Location location;
  private final PoolingDataSource<PoolableConnection> dataSource;
  private final LoadingCache<CommandPreparedStatementQuery, ResultSet> commandExecutePreparedStatementLoadingCache;
  private final LoadingCache<PreparedStatementCacheKey, PreparedStatementContext> preparedStatementLoadingCache;

  public FlightSqlExample(final Location location) {
    // TODO Constructor should not be doing work.
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
    try (final Connection connection = DriverManager.getConnection("jdbc:derby:target/derbyDB;create=true");
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

  private static ArrowType getArrowTypeFromJdbcType(final int jdbcDataType, final int precision, final int scale) {
    final ArrowType type =
        JdbcToArrowConfig.getDefaultJdbcToArrowTypeConverter().apply(new JdbcFieldInfo(jdbcDataType, precision, scale),
            Calendar.getInstance());
    return isNull(type) ? ArrowType.Utf8.INSTANCE : type;
  }

  /**
   * Make the provided {@link ServerStreamListener} listen to the provided {@link VectorSchemaRoot}s.
   *
   * @param listener the listener.
   * @param data     data to listen to.
   */
  protected static void makeListen(final ServerStreamListener listener, final Iterable<VectorSchemaRoot> data) {
    makeListen(listener, stream(data.spliterator(), false).toArray(VectorSchemaRoot[]::new));
  }

  /**
   * Make the provided {@link ServerStreamListener} listen to the provided {@link VectorSchemaRoot}s.
   *
   * @param listener the listener.
   * @param data     data to listen to.
   */
  protected static void makeListen(final ServerStreamListener listener, final VectorSchemaRoot... data) {
    for (final VectorSchemaRoot datum : data) {
      listener.start(datum);
      listener.putNext();
    }
  }

  /**
   * Turns the provided {@link ResultSet} into an {@link Iterator} of {@link VectorSchemaRoot}s.
   *
   * @param data the data to convert
   * @return an {@code Iterator<VectorSchemaRoot>} representation of the provided data.
   * @throws SQLException if an error occurs while querying the {@code ResultSet}.
   * @throws IOException  if an I/O error occurs.
   */
  protected static Iterable<VectorSchemaRoot> getVectorsFromData(final ResultSet data)
      throws SQLException, IOException {
    Iterator<VectorSchemaRoot> iterator = sqlToArrowVectorIterator(data, new RootAllocator(Long.MAX_VALUE));
    return () -> iterator;
  }

  private static void saveToVector(final @Nullable String data, final VarCharVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
  }

  private static void saveToVector(final @Nullable byte[] data, final VarBinaryVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private static void preconditionCheckSaveToVector(final FieldVector vector, final int index) {
    checkNotNull(vector);
    checkState(index >= 0, "Index must be a positive number!");
  }

  private static <T, V extends FieldVector> void vectorConsumer(final T data, final V vector,
                                                                final Consumer<V> consumerIfNullable,
                                                                final BiConsumer<T, V> defaultConsumer) {
    if (isNull(data)) {
      consumerIfNullable.accept(vector);
      return;
    }
    defaultConsumer.accept(data, vector);
  }

  private VectorSchemaRoot getTablesRoot(final DatabaseMetaData databaseMetaData,
                                         final boolean includeSchema,
                                         final @Nullable String catalog,
                                         final @Nullable String schemaFilterPattern,
                                         final @Nullable String tableFilterPattern,
                                         final @Nullable String... tableTypes)
      throws SQLException, IOException {

    final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemaNameVector = new VarCharVector("schema_name", allocator);
    final VarCharVector tableNameVector = new VarCharVector("table_name", allocator);
    final VarCharVector tableTypeVector = new VarCharVector("table_type", allocator);

    final List<FieldVector> vectors =
        new ArrayList<>(
            ImmutableList.of(
                catalogNameVector, schemaNameVector, tableNameVector, tableTypeVector));
    vectors.forEach(FieldVector::allocateNew);

    int rows = 0;

    try (final ResultSet data =
             checkNotNull(
                 databaseMetaData,
                 format("%s cannot be null!", databaseMetaData.getClass().getName()))
                 .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {

      for (; data.next(); rows++) {
        saveToVector(emptyToNull(data.getString("TABLE_CAT")), catalogNameVector, rows);
        saveToVector(emptyToNull(data.getString("TABLE_SCHEM")), schemaNameVector, rows);
        saveToVector(emptyToNull(data.getString("TABLE_NAME")), tableNameVector, rows);
        saveToVector(emptyToNull(data.getString("TABLE_TYPE")), tableTypeVector, rows);
      }

      for (final FieldVector vector : vectors) {
        vector.setValueCount(rows);
      }
    }

    if (includeSchema) {
      final VarBinaryVector tableSchemaVector = new VarBinaryVector("table_schema", allocator);
      tableSchemaVector.allocateNew(rows);

      try (final ResultSet columnsData =
               databaseMetaData.getColumns(catalog, schemaFilterPattern, tableFilterPattern, null)) {
        final Map<String, List<Field>> tableToFields = new HashMap<>();

        while (columnsData.next()) {
          final String tableName = columnsData.getString("TABLE_NAME");
          final String fieldName = columnsData.getString("COLUMN_NAME");
          final int dataType = columnsData.getInt("DATA_TYPE");
          final boolean isNullable = columnsData.getInt("NULLABLE") == 1;
          final int precision = columnsData.getInt("NUM_PREC_RADIX");
          final int scale = columnsData.getInt("DECIMAL_DIGITS");
          final List<Field> fields = tableToFields.computeIfAbsent(tableName, tableName_ -> new ArrayList<>());
          final Field field =
              new Field(
                  fieldName,
                  new FieldType(
                      isNullable,
                      getArrowTypeFromJdbcType(dataType, precision, scale),
                      null),
                  null);
          fields.add(field);
        }

        for (int index = 0; index < rows; index++) {
          final String tableName = tableNameVector.getObject(index).toString();
          final Schema schema = new Schema(tableToFields.get(tableName));
          saveToVector(schema.toByteArray(), tableSchemaVector, index);
        }
      }

      tableSchemaVector.setValueCount(rows);
      vectors.add(tableSchemaVector);
    }

    return new VectorSchemaRoot(vectors);
  }

  @Override
  public void getStreamPreparedStatement(CommandPreparedStatementQuery command, CallContext context, Ticket ticket,
                                         ServerStreamListener listener) {
    try (final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command)) {
      makeListen(listener, getVectorsFromData(resultSet));
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
    /*
    final Schema schema = getSchemaCatalogs().getSchema();
    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(new Ticket(pack(request).toByteArray()), location));
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
     */
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamCatalogs(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    /* TODO
    try {
      final ResultSet catalogs = dataSource.getConnection().getMetaData().getCatalogs();
      makeListen(listener, getVectorsFromData(catalogs));
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
     */
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSchemas(final CommandGetSchemas request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    /* TODO
    final Schema schema = getSchemaSchemas().getSchema();
    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(new Ticket(pack(request).toByteArray()), location));
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    */
    throw Status.UNAVAILABLE.asRuntimeException();
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

    try (final Connection connection = DriverManager.getConnection(DATABASE_URI)) {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();
      makeListen(
          listener,
          getTablesRoot(
              databaseMetaData,
              command.getIncludeSchema(),
              catalog, schemaFilterPattern, tableFilterPattern, tableTypes));
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamTables: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(final CallContext context, final FlightDescriptor descriptor) {
    /* TODO
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
     */
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamTableTypes(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    /* TODO
    try {
      final ResultSet tableTypes = dataSource.getConnection().getMetaData().getTableTypes();
      makeListen(listener, getVectorsFromData(tableTypes));
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamTableTypes: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
     */
    throw Status.UNIMPLEMENTED.asRuntimeException();
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
        final PreparedStatement preparedStatement = connection.prepareStatement(key.getSql());
        return new PreparedStatementContext(connection, preparedStatement);
      } catch (SQLException e) {
        connection.close();
        throw e;
      }
    }
  }
}
