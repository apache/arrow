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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
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
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
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
import org.apache.arrow.vector.IntVector;
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
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

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
public class FlightSqlExample implements FlightSqlProducer, AutoCloseable {
  private static final String DATABASE_URI = "jdbc:derby:target/derbyDB";
  private static final Logger LOGGER = getLogger(FlightSqlExample.class);
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
    try (final Connection connection = DriverManager.getConnection("jdbc:derby:target/derbyDB;create=true");
         Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE foreignTable (id INT not null primary key GENERATED ALWAYS AS IDENTITY " +
          "(START WITH 1, INCREMENT BY 1), foreignName varchar(100), value int)");
      statement.execute("CREATE TABLE intTable (id INT not null primary key GENERATED ALWAYS AS IDENTITY " +
          "(START WITH 1, INCREMENT BY 1), keyName varchar(100), value int, foreignId int references foreignTable(id))");
      statement.execute("INSERT INTO foreignTable (foreignName, value) VALUES ('keyOne', 1)");
      statement.execute("INSERT INTO foreignTable (foreignName, value) VALUES ('keyTwo', 0)");
      statement.execute("INSERT INTO foreignTable (foreignName, value) VALUES ('keyThree', -1)");
      statement.execute("INSERT INTO intTable (keyName, value, foreignId) VALUES ('one', 1, 1)");
      statement.execute("INSERT INTO intTable (keyName, value, foreignId) VALUES ('zero', 0, 1)");
      statement.execute("INSERT INTO intTable (keyName, value, foreignId) VALUES ('negative one', -1, 1)");
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
            JdbcToArrowUtils.getUtcCalendar());
    return isNull(type) ? ArrowType.Utf8.INSTANCE : type;
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
          List<FieldVector> vectors = root.getFieldVectors();
          if (!includeSchema) {
            return vectors;
          }
          final VarCharVector vector =
              new VarCharVector("SCHEMA", new RootAllocator(Long.MAX_VALUE));
          final int valueCount = root.getRowCount();
          IntStream.range(0, valueCount)
              .forEachOrdered(
                  index ->
                      vector.setSafe(index, new Text(getSchemaTables().getSchema().toJson())));
          vector.setValueCount(valueCount);
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

  private static void saveToVector(final @Nullable String data, final VarCharVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
  }

  private static void saveToVector(final @Nullable Integer data, final IntVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
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

  private static VectorSchemaRoot getSchemasRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    final VarCharVector catalogs = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemas = new VarCharVector("schema_name", allocator);
    final List<FieldVector> vectors = ImmutableList.of(catalogs, schemas);
    vectors.forEach(FieldVector::allocateNew);
    final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
        catalogs, "TABLE_CATALOG",
        schemas, "TABLE_SCHEM");
    saveToVectors(vectorToColumnName, data);
    final int rows = vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
    vectors.forEach(vector -> vector.setValueCount(rows));
    return new VectorSchemaRoot(vectors);
  }

  private static <T extends FieldVector> void saveToVectors(final Map<T, String> vectorToColumnName,
                                                            final ResultSet data, boolean emptyToNull)
      throws SQLException {
    checkNotNull(vectorToColumnName);
    checkNotNull(data);
    final Set<Entry<T, String>> entrySet = vectorToColumnName.entrySet();
    int rows = 0;
    for (; data.next(); rows++) {
      for (final Entry<T, String> vectorToColumn : entrySet) {
        final T vector = vectorToColumn.getKey();
        final String columnName = vectorToColumn.getValue();
        if (vector instanceof VarCharVector) {
          String thisData = data.getString(columnName);
          saveToVector(emptyToNull ? emptyToNull(thisData) : thisData, (VarCharVector) vector, rows);
          continue;
        }
        throw Status.INVALID_ARGUMENT.asRuntimeException();
      }
    }
    for (final Entry<T, String> vectorToColumn : entrySet) {
      vectorToColumn.getKey().setValueCount(rows);
    }
  }

  private static <T extends FieldVector> void saveToVectors(final Map<T, String> vectorToColumnName,
                                                            final ResultSet data)
      throws SQLException {
    saveToVectors(vectorToColumnName, data, false);
  }

  private static VectorSchemaRoot getTableTypesRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    return getRoot(data, allocator, "table_type", "TABLE_TYPE");
  }

  private static VectorSchemaRoot getCatalogsRoot(final ResultSet data, final BufferAllocator allocator)
      throws SQLException {
    return getRoot(data, allocator, "catalog_name", "TABLE_CATALOG");
  }

  private static VectorSchemaRoot getRoot(final ResultSet data, final BufferAllocator allocator,
                                          final String fieldVectorName, final String columnName)
      throws SQLException {
    final VarCharVector dataVector = new VarCharVector(fieldVectorName, allocator);
    saveToVectors(ImmutableMap.of(dataVector, columnName), data);
    final int rows = dataVector.getValueCount();
    dataVector.setValueCount(rows);
    return new VectorSchemaRoot(singletonList(dataVector));
  }

  private static VectorSchemaRoot getTablesRoot(final DatabaseMetaData databaseMetaData,
                                                final BufferAllocator allocator,
                                                final boolean includeSchema,
                                                final @Nullable String catalog,
                                                final @Nullable String schemaFilterPattern,
                                                final @Nullable String tableFilterPattern,
                                                final @Nullable String... tableTypes)
      throws SQLException, IOException {
    /*
     * TODO Fix DerbyDB inconsistency if possible.
     * During the early development of this prototype, an inconsistency has been found in the database
     * used for this demonstration; as DerbyDB does not operate with the concept of catalogs, fetching
     * the catalog name for a given table from `DatabaseMetadata#getColumns` and `DatabaseMetadata#getSchemas`
     * returns null, as expected. However, the inconsistency lies in the fact that accessing the same
     * information -- that is, the catalog name for a given table -- from `DatabaseMetadata#getSchemas`
     * returns an empty String.The temporary workaround for this was making sure we convert the empty Strings
     * to null using `com.google.common.base.Strings#emptyToNull`.
     */
    final VarCharVector catalogNameVector = new VarCharVector("catalog_name", checkNotNull(allocator));
    final VarCharVector schemaNameVector = new VarCharVector("schema_name", allocator);
    final VarCharVector tableNameVector = new VarCharVector("table_name", allocator);
    final VarCharVector tableTypeVector = new VarCharVector("table_type", allocator);

    final List<FieldVector> vectors =
        new ArrayList<>(
            ImmutableList.of(
                catalogNameVector, schemaNameVector, tableNameVector, tableTypeVector));
    vectors.forEach(FieldVector::allocateNew);

    final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
        catalogNameVector, "TABLE_CAT",
        schemaNameVector, "TABLE_SCHEM",
        tableNameVector, "TABLE_NAME",
        tableTypeVector, "TABLE_TYPE");

    try (final ResultSet data =
             checkNotNull(
                 databaseMetaData,
                 format("%s cannot be null!", databaseMetaData.getClass().getName()))
                 .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {

      saveToVectors(vectorToColumnName, data, true);
      final int rows =
          vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
      vectors.forEach(vector -> vector.setValueCount(rows));

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
            final boolean isNullable = columnsData.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls;
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
    }

    return new VectorSchemaRoot(vectors);
  }

  private static Schema buildSchema(final ResultSetMetaData resultSetMetaData) throws SQLException {
    return JdbcToArrowUtils.jdbcToArrowSchema(resultSetMetaData, JdbcToArrowUtils.getUtcCalendar());
  }

  private static Schema buildSchema(final ParameterMetaData parameterMetaData) throws SQLException {
    checkNotNull(parameterMetaData);
    final List<Field> parameterFields = new ArrayList<>();
    for (int parameterCounter = 1; parameterCounter <= parameterMetaData.getParameterCount();
         parameterCounter++) {
      final int jdbcDataType = parameterMetaData.getParameterType(parameterCounter);
      final int jdbcIsNullable = parameterMetaData.isNullable(parameterCounter);
      final boolean arrowIsNullable = jdbcIsNullable != ParameterMetaData.parameterNoNulls;
      final int precision = parameterMetaData.getPrecision(parameterCounter);
      final int scale = parameterMetaData.getScale(parameterCounter);
      final ArrowType arrowType = getArrowTypeFromJdbcType(jdbcDataType, precision, scale);
      final FieldType fieldType = new FieldType(arrowIsNullable, arrowType, /*dictionary=*/null);
      parameterFields.add(new Field(null, fieldType, null));
    }

    return new Schema(parameterFields);
  }

  @Override
  public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
                                         final Ticket ticket, final ServerStreamListener listener) {
    try (final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command);
         final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      makeListen(listener, getVectorsFromData(resultSet, allocator));
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
       * Do NOT prematurely close this resource!
       * Should be closed upon executing `ClosePreparedStatement`.
       */
      final ResultSet resultSet = commandExecutePreparedStatementLoadingCache.get(command);
      return getFlightInfoForSchema(command, descriptor, buildSchema(resultSet.getMetaData()));
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
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
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
       * Do NOT prematurely close this resource!
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
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(final CommandGetSqlInfo command, final CallContext context, final Ticket ticket,
                               final ServerStreamListener listener) {
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
    try (final ResultSet catalogs = dataSource.getConnection().getMetaData().getCatalogs();
         final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      makeListen(listener, getCatalogsRoot(catalogs, allocator));
    } catch (SQLException e) {
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
    final String catalog = command.hasCatalog() ? command.getCatalog().getValue() : null;
    final String schemaFilterPattern =
        command.hasSchemaFilterPattern() ? command.getSchemaFilterPattern().getValue() : null;
    try (final Connection connection = dataSource.getConnection();
         final ResultSet schemas = connection.getMetaData().getSchemas(catalog, schemaFilterPattern);
         final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      makeListen(listener, getSchemasRoot(schemas, allocator));
    } catch (SQLException e) {
      LOGGER.error(format("Failed to getStreamSchemas: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  private static VectorSchemaRoot getRootSchemas(final ResultSet data) throws SQLException {
    final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    final VarCharVector catalogs = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemas = new VarCharVector("schema_name", allocator);
    final List<FieldVector> vectors = ImmutableList.of(catalogs, schemas);
    vectors.forEach(FieldVector::allocateNew);
    int rows = 0;

    for (; data.next(); rows++) {
      catalogs.setSafe(rows, new Text(data.getString("TABLE_CAT")));
      schemas.setSafe(rows, new Text(data.getString("TABLE_SCHEM")));
    }

    for (FieldVector vector : vectors) {
      vector.setValueCount(rows);
    }

    return null;
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
    final String catalog = command.hasCatalog() ? command.getCatalog().getValue() : null;
    final String schemaFilterPattern =
        command.hasSchemaFilterPattern() ? command.getSchemaFilterPattern().getValue() : null;
    final String tableFilterPattern =
        command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern().getValue() : null;

    final Schema schema = new Schema(singletonList(nullable("Sample", Null.INSTANCE)));
    return new FlightInfo(schema, descriptor, endpoints, Byte.MAX_VALUE, endpoints.size());
    */
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

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
  public FlightInfo getFlightInfoTableTypes(final CommandGetTableTypes request, final CallContext context,
                                            final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, getSchemaTableTypes().getSchema());
  }

  @Override
  public void getStreamTableTypes(final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
    try (final Connection connection = dataSource.getConnection();
         final ResultSet tableTypes = connection.getMetaData().getTableTypes();
         final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      makeListen(listener, getTableTypesRoot(tableTypes, allocator));
    } catch (SQLException e) {
      LOGGER.error(format("Failed to getStreamTableTypes: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(final CommandGetPrimaryKeys request, final CallContext context,
                                             final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, getSchemaPrimaryKeys().getSchema());
  }

  @Override
  public void getStreamPrimaryKeys(final CommandGetPrimaryKeys command, final CallContext context, final Ticket ticket,
                                   final ServerStreamListener listener) {

    final String catalog = command.hasCatalog() ? command.getCatalog().getValue() : null;
    final String schema = command.hasSchema() ? command.getSchema().getValue() : null;
    final String table = command.hasTable() ? command.getTable().getValue() : null;

    try (Connection connection = DriverManager.getConnection(DATABASE_URI)) {
      final ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(catalog, schema, table);

      final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      final VarCharVector catalogNameVector = new VarCharVector("catalog_nam", allocator);
      final VarCharVector schemaNameVector = new VarCharVector("schema_name", allocator);
      final VarCharVector tableNameVector = new VarCharVector("table_name", allocator);
      final VarCharVector columnNameVector = new VarCharVector("column_name", allocator);
      final IntVector keySequenceVector = new IntVector("key_sequence", allocator);
      final VarCharVector keyNameVector = new VarCharVector("key_name", allocator);

      final List<FieldVector> vectors =
          new ArrayList<>(
              ImmutableList.of(
                  catalogNameVector, schemaNameVector, tableNameVector, columnNameVector, keySequenceVector,
                  keyNameVector));
      vectors.forEach(FieldVector::allocateNew);

      int rows = 0;
      for (; primaryKeys.next(); rows++) {
        saveToVector(primaryKeys.getString("TABLE_CAT"), catalogNameVector, rows);
        saveToVector(primaryKeys.getString("TABLE_SCHEM"), schemaNameVector, rows);
        saveToVector(primaryKeys.getString("TABLE_NAME"), tableNameVector, rows);
        saveToVector(primaryKeys.getString("COLUMN_NAME"), columnNameVector, rows);
        final String key_seq = primaryKeys.getString("KEY_SEQ");
        saveToVector(key_seq != null ? Integer.parseInt(key_seq) : null, keySequenceVector, rows);
        saveToVector(primaryKeys.getString("PK_NAME"), keyNameVector, rows);
      }

      for (final FieldVector vector : vectors) {
        vector.setValueCount(rows);
      }

      makeListen(listener, singletonList(new VectorSchemaRoot(vectors)));
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(final FlightSql.CommandGetExportedKeys request, final CallContext context,
                                              final FlightDescriptor descriptor) {
    final Schema schema = getSchemaForeignKeys().getSchema();
    final List<FlightEndpoint> endpoints =
        singletonList(new FlightEndpoint(new Ticket(pack(request).toByteArray()), location));
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }

  @Override
  public void getStreamExportedKeys(final FlightSql.CommandGetExportedKeys command, final CallContext context, final Ticket ticket,
                                    final ServerStreamListener listener) {
    String foreignKeyCatalog = emptyToNull(command.getCatalog());
    String foreignKeySchema = emptyToNull(command.getSchema());
    String foreignKeyTable = emptyToNull(command.getTable());

    try(Connection connection = DriverManager.getConnection(DATABASE_URI)){

      final ResultSet keys = connection.getMetaData().getExportedKeys(foreignKeyCatalog,
          foreignKeySchema, foreignKeyTable);

      final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
      final VarCharVector pkCatalogNameVector = new VarCharVector("pk_catalog_name", allocator);
      final VarCharVector pkSchemaNameVector = new VarCharVector("pk_schema_name", allocator);
      final VarCharVector pkTableNameVector = new VarCharVector("pk_table_name", allocator);
      final VarCharVector pkColumnNameVector = new VarCharVector("pk_column_name", allocator);
      final VarCharVector fkCatalogNameVector = new VarCharVector("fk_catalog_name", allocator);
      final VarCharVector fkSchemaNameVector = new VarCharVector("fk_schema_name", allocator);
      final VarCharVector fkTableNameVector = new VarCharVector("fk_table_name", allocator);
      final VarCharVector fkColumnNameVector = new VarCharVector("fk_column_name", allocator);
      final IntVector keySequenceVector = new IntVector("key_sequence", allocator);
      final VarCharVector fkKeyNameVector = new VarCharVector("fk_key_name", allocator);
      final VarCharVector pkKeyNameVector = new VarCharVector("pk_key_name", allocator);
      final IntVector updateRuleVector = new IntVector("update_rule", allocator);
      final IntVector deleteRuleVector = new IntVector("delete_rule", allocator);

      final List<FieldVector> vectors =
          new ArrayList<>(
              ImmutableList.of(
                  pkCatalogNameVector, pkSchemaNameVector, pkTableNameVector, pkColumnNameVector, fkCatalogNameVector,
                  fkSchemaNameVector, fkTableNameVector, fkColumnNameVector, keySequenceVector, fkKeyNameVector,
                  pkKeyNameVector, updateRuleVector, deleteRuleVector));
      vectors.forEach(FieldVector::allocateNew);
      int rows = 0;

      for (; keys.next(); rows++) {
        saveToVector(emptyToNull(keys.getString("PKTABLE_CAT")), pkCatalogNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("PKTABLE_SCHEM")), pkSchemaNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("PKTABLE_NAME")), pkTableNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("PKCOLUMN_NAME")), pkColumnNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("FKTABLE_CAT")), fkCatalogNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("FKTABLE_SCHEM")), fkSchemaNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("FKTABLE_NAME")), fkTableNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("FKCOLUMN_NAME")), fkColumnNameVector ,rows);
        saveToVector(Integer.parseInt(keys.getString("KEY_SEQ")), keySequenceVector ,rows);
        saveToVector(Integer.parseInt(keys.getString("UPDATE_RULE")), updateRuleVector ,rows);
        saveToVector(Integer.parseInt(keys.getString("DELETE_RULE")), deleteRuleVector ,rows);
        saveToVector(emptyToNull(keys.getString("FK_NAME")), fkKeyNameVector ,rows);
        saveToVector(emptyToNull(keys.getString("PK_NAME")), pkKeyNameVector ,rows);
      }

      for (final FieldVector vector : vectors) {
        vector.setValueCount(rows);
      }

      makeListen(
          listener, singletonList(new VectorSchemaRoot(vectors)));
    } catch (SQLException e) {
      e.printStackTrace();
    }
    finally {
      listener.completed();
    }
  }

  @Override
  public void getStreamStatement(CommandStatementQuery command, CallContext context, Ticket ticket,
                                 ServerStreamListener listener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
                                                                final Schema schema) {
    final Ticket ticket = new Ticket(pack(request).toByteArray());
    // TODO Support multiple endpoints.
    final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, location));

    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
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
