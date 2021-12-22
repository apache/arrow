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

import static com.google.common.base.Strings.emptyToNull;
import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.UUID.randomUUID;
import static org.apache.arrow.adapter.jdbc.JdbcToArrow.sqlToArrowVectorIterator;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.jdbcToArrowSchema;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import static org.apache.arrow.util.Preconditions.checkState;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types.MinorType;
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;

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
  private static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
  private final Location location;
  private final PoolingDataSource<PoolableConnection> dataSource;
  private final BufferAllocator rootAllocator = new RootAllocator();
  private final Cache<ByteString, StatementContext<PreparedStatement>> preparedStatementLoadingCache;
  private final Cache<ByteString, StatementContext<Statement>> statementLoadingCache;
  private final SqlInfoBuilder sqlInfoBuilder;

  public FlightSqlExample(final Location location) {
    // TODO Constructor should not be doing work.
    checkState(
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
            .removalListener(new StatementRemovalListener<PreparedStatement>())
            .build();

    statementLoadingCache =
        CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .removalListener(new StatementRemovalListener<>())
            .build();

    this.location = location;

    sqlInfoBuilder = new SqlInfoBuilder();
    try (final Connection connection = dataSource.getConnection()) {
      final DatabaseMetaData metaData = connection.getMetaData();

      sqlInfoBuilder.withFlightSqlServerName(metaData.getDatabaseProductName())
          .withFlightSqlServerVersion(metaData.getDatabaseProductVersion())
          .withFlightSqlServerArrowVersion(metaData.getDriverVersion())
          .withFlightSqlServerReadOnly(metaData.isReadOnly())
          .withSqlIdentifierQuoteChar(metaData.getIdentifierQuoteString())
          .withSqlDdlCatalog(metaData.supportsCatalogsInDataManipulation())
          .withSqlDdlSchema( metaData.supportsSchemasInDataManipulation())
          .withSqlDdlTable( metaData.allTablesAreSelectable())
          .withSqlIdentifierCase(metaData.storesMixedCaseIdentifiers() ?
              SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE :
              metaData.storesUpperCaseIdentifiers() ?
                  SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE :
                  metaData.storesLowerCaseIdentifiers() ?
                      SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE :
                      SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
          .withSqlQuotedIdentifierCase(metaData.storesMixedCaseQuotedIdentifiers() ?
              SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE :
              metaData.storesUpperCaseQuotedIdentifiers() ?
                  SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE :
                  metaData.storesLowerCaseQuotedIdentifiers() ?
                      SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE :
                      SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

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
    try (final Connection connection = DriverManager.getConnection("jdbc:derby:target/derbyDB;create=true");
         Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE foreignTable (" +
          "id INT not null primary key GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
          "foreignName varchar(100), " +
          "value int)");
      statement.execute("CREATE TABLE intTable (" +
          "id INT not null primary key GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
          "keyName varchar(100), " +
          "value int, " +
          "foreignId int references foreignTable(id))");
      statement.execute("INSERT INTO foreignTable (foreignName, value) VALUES ('keyOne', 1)");
      statement.execute("INSERT INTO foreignTable (foreignName, value) VALUES ('keyTwo', 0)");
      statement.execute("INSERT INTO foreignTable (foreignName, value) VALUES ('keyThree', -1)");
      statement.execute("INSERT INTO intTable (keyName, value, foreignId) VALUES ('one', 1, 1)");
      statement.execute("INSERT INTO intTable (keyName, value, foreignId) VALUES ('zero', 0, 1)");
      statement.execute("INSERT INTO intTable (keyName, value, foreignId) VALUES ('negative one', -1, 1)");
    } catch (final SQLException e) {
      LOGGER.error(format("Failed attempt to populate DerbyDB: <%s>", e.getMessage()), e);
      return false;
    }
    return true;
  }

  private static ArrowType getArrowTypeFromJdbcType(final int jdbcDataType, final int precision, final int scale) {
    final ArrowType type =
        JdbcToArrowUtils.getArrowTypeFromJdbcType(new JdbcFieldInfo(jdbcDataType, precision, scale), DEFAULT_CALENDAR);
    return isNull(type) ? ArrowType.Utf8.INSTANCE : type;
  }

  private static void saveToVector(final Byte data, final UInt1Vector vector, final int index) {
    vectorConsumer(
        data,
        vector,
        fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private static void saveToVector(final String data, final VarCharVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, new Text(theData)));
  }

  private static void saveToVector(final Integer data, final IntVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private static void saveToVector(final byte[] data, final VarBinaryVector vector, final int index) {
    preconditionCheckSaveToVector(vector, index);
    vectorConsumer(data, vector, fieldVector -> fieldVector.setNull(index),
        (theData, fieldVector) -> fieldVector.setSafe(index, theData));
  }

  private static void preconditionCheckSaveToVector(final FieldVector vector, final int index) {
    Objects.requireNonNull(vector, "vector cannot be null.");
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
    final VarCharVector schemas =
        new VarCharVector("db_schema_name", FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);
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

  private static <T extends FieldVector> int saveToVectors(final Map<T, String> vectorToColumnName,
                                                           final ResultSet data, boolean emptyToNull)
      throws SQLException {
    Objects.requireNonNull(vectorToColumnName, "vectorToColumnName cannot be null.");
    Objects.requireNonNull(data, "data cannot be null.");
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
        } else if (vector instanceof IntVector) {
          final int intValue = data.getInt(columnName);
          saveToVector(data.wasNull() ? null : intValue, (IntVector) vector, rows);
          continue;
        } else if (vector instanceof UInt1Vector) {
          final byte byteValue = data.getByte(columnName);
          saveToVector(data.wasNull() ? null : byteValue, (UInt1Vector) vector, rows);
          continue;
        }
        throw CallStatus.INVALID_ARGUMENT.withDescription("Provided vector not supported").toRuntimeException();
      }
    }
    for (final Entry<T, String> vectorToColumn : entrySet) {
      vectorToColumn.getKey().setValueCount(rows);
    }

    return rows;
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
    final VarCharVector dataVector =
        new VarCharVector(fieldVectorName, FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);
    saveToVectors(ImmutableMap.of(dataVector, columnName), data);
    final int rows = dataVector.getValueCount();
    dataVector.setValueCount(rows);
    return new VectorSchemaRoot(singletonList(dataVector));
  }

  private static VectorSchemaRoot getTablesRoot(final DatabaseMetaData databaseMetaData,
                                                final BufferAllocator allocator,
                                                final boolean includeSchema,
                                                final String catalog,
                                                final String schemaFilterPattern,
                                                final String tableFilterPattern,
                                                final String... tableTypes)
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
    Objects.requireNonNull(allocator, "BufferAllocator cannot be null.");
    final VarCharVector catalogNameVector = new VarCharVector("catalog_name", allocator);
    final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", allocator);
    final VarCharVector tableNameVector =
        new VarCharVector("table_name", FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);
    final VarCharVector tableTypeVector =
        new VarCharVector("table_type", FieldType.notNullable(MinorType.VARCHAR.getType()), allocator);

    final List<FieldVector> vectors = new ArrayList<>(4);
    vectors.add(catalogNameVector);
    vectors.add(schemaNameVector);
    vectors.add(tableNameVector);
    vectors.add(tableTypeVector);

    vectors.forEach(FieldVector::allocateNew);

    final Map<FieldVector, String> vectorToColumnName = ImmutableMap.of(
        catalogNameVector, "TABLE_CAT",
        schemaNameVector, "TABLE_SCHEM",
        tableNameVector, "TABLE_NAME",
        tableTypeVector, "TABLE_TYPE");

    try (final ResultSet data =
             Objects.requireNonNull(
                     databaseMetaData,
                     format("%s cannot be null.", databaseMetaData.getClass().getName()))
                 .getTables(catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {

      saveToVectors(vectorToColumnName, data, true);
      final int rows =
          vectors.stream().map(FieldVector::getValueCount).findAny().orElseThrow(IllegalStateException::new);
      vectors.forEach(vector -> vector.setValueCount(rows));

      if (includeSchema) {
        final VarBinaryVector tableSchemaVector =
            new VarBinaryVector("table_schema", FieldType.notNullable(MinorType.VARBINARY.getType()), allocator);
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
            saveToVector(
                copyFrom(serializeMetadata(schema)).toByteArray(),
                tableSchemaVector, index);
          }
        }

        tableSchemaVector.setValueCount(rows);
        vectors.add(tableSchemaVector);
      }
    }

    return new VectorSchemaRoot(vectors);
  }

  private static ByteBuffer serializeMetadata(final Schema schema) {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);

      return ByteBuffer.wrap(outputStream.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }
  }

  @Override
  public void getStreamPreparedStatement(final CommandPreparedStatementQuery command, final CallContext context,
                                         final ServerStreamListener listener) {
    final ByteString handle = command.getPreparedStatementHandle();
    StatementContext<PreparedStatement> statementContext = preparedStatementLoadingCache.getIfPresent(handle);
    Objects.requireNonNull(statementContext);
    final PreparedStatement statement = statementContext.getStatement();
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
    } catch (final SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamPreparedStatement: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public void closePreparedStatement(final ActionClosePreparedStatementRequest request, final CallContext context,
                                     final StreamListener<Result> listener) {
    try {
      preparedStatementLoadingCache.invalidate(request.getPreparedStatementHandle());
    } catch (final Exception e) {
      listener.onError(e);
      return;
    }
    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(final CommandStatementQuery request, final CallContext context,
                                           final FlightDescriptor descriptor) {
    ByteString handle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));

    try {
      // Ownership of the connection will be passed to the context. Do NOT close!
      final Connection connection = dataSource.getConnection();
      final Statement statement = connection.createStatement(
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      final String query = request.getQuery();
      final StatementContext<Statement> statementContext = new StatementContext<>(statement, query);

      statementLoadingCache.put(handle, statementContext);
      final ResultSet resultSet = statement.executeQuery(query);

      TicketStatementQuery ticket = TicketStatementQuery.newBuilder()
          .setStatementHandle(handle)
          .build();
      return getFlightInfoForSchema(ticket, descriptor,
          jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR));
    } catch (final SQLException e) {
      LOGGER.error(
          format("There was a problem executing the prepared statement: <%s>.", e.getMessage()),
          e);
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    }
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(final CommandPreparedStatementQuery command,
                                                   final CallContext context,
                                                   final FlightDescriptor descriptor) {
    final ByteString preparedStatementHandle = command.getPreparedStatementHandle();
    StatementContext<PreparedStatement> statementContext =
        preparedStatementLoadingCache.getIfPresent(preparedStatementHandle);
    try {
      assert statementContext != null;
      PreparedStatement statement = statementContext.getStatement();

      ResultSetMetaData metaData = statement.getMetaData();
      return getFlightInfoForSchema(command, descriptor,
          jdbcToArrowSchema(metaData, DEFAULT_CALENDAR));
    } catch (final SQLException e) {
      LOGGER.error(
          format("There was a problem executing the prepared statement: <%s>.", e.getMessage()),
          e);
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    }
  }

  @Override
  public SchemaResult getSchemaStatement(final CommandStatementQuery command, final CallContext context,
                                         final FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void close() throws Exception {
    try {
      preparedStatementLoadingCache.cleanUp();
    } catch (Throwable t) {
      LOGGER.error(format("Failed to close resources: <%s>", t.getMessage()), t);
    }

    AutoCloseables.close(dataSource, rootAllocator);
  }

  @Override
  public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    // TODO - build example implementation
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void createPreparedStatement(final ActionCreatePreparedStatementRequest request, final CallContext context,
                                      final StreamListener<Result> listener) {
    try {
      final ByteString preparedStatementHandle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
      // Ownership of the connection will be passed to the context. Do NOT close!
      final Connection connection = dataSource.getConnection();
      final PreparedStatement preparedStatement = connection.prepareStatement(request.getQuery(),
          ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      final StatementContext<PreparedStatement> preparedStatementContext =
          new StatementContext<>(preparedStatement, request.getQuery());

      preparedStatementLoadingCache.put(preparedStatementHandle, preparedStatementContext);

      final Schema parameterSchema =
          jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);

      final ResultSetMetaData metaData = preparedStatement.getMetaData();
      final ByteString bytes = isNull(metaData) ?
          ByteString.EMPTY :
          ByteString.copyFrom(
              serializeMetadata(jdbcToArrowSchema(metaData, DEFAULT_CALENDAR)));
      final ActionCreatePreparedStatementResult result = ActionCreatePreparedStatementResult.newBuilder()
          .setDatasetSchema(bytes)
          .setParameterSchema(copyFrom(serializeMetadata(parameterSchema)))
          .setPreparedStatementHandle(preparedStatementHandle)
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
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate command,
                                     CallContext context, FlightStream flightStream,
                                     StreamListener<PutResult> ackStream) {
    final String query = command.getQuery();

    return () -> {
      try (final Connection connection = dataSource.getConnection();
           final Statement statement = connection.createStatement()) {
        final int result = statement.executeUpdate(query);

        final DoPutUpdateResult build =
            DoPutUpdateResult.newBuilder().setRecordCount(result).build();

        try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
          buffer.writeBytes(build.toByteArray());
          ackStream.onNext(PutResult.metadata(buffer));
          ackStream.onCompleted();
        }
      } catch (SQLException e) {
        ackStream.onError(e);
      }
    };
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command, CallContext context,
                                                   FlightStream flightStream, StreamListener<PutResult> ackStream) {
    final StatementContext<PreparedStatement> statement =
        preparedStatementLoadingCache.getIfPresent(command.getPreparedStatementHandle());

    return () -> {
      assert statement != null;
      try {
        final PreparedStatement preparedStatement = statement.getStatement();

        while (flightStream.next()) {
          final VectorSchemaRoot root = flightStream.getRoot();

          final int rowCount = root.getRowCount();
          final int recordCount;

          if (rowCount == 0) {
            preparedStatement.execute();
            recordCount = preparedStatement.getUpdateCount();
          } else {
            setDataPreparedStatement(preparedStatement, root, true);
            int[] recordCount1 = preparedStatement.executeBatch();
            recordCount = Arrays.stream(recordCount1).sum();
          }

          final DoPutUpdateResult build =
              DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();

          try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
            buffer.writeBytes(build.toByteArray());
            ackStream.onNext(PutResult.metadata(buffer));
          }
        }
      } catch (SQLException e) {
        ackStream.onError(e);
        return;
      }
      ackStream.onCompleted();
    };
  }

  /**
   * Method responsible to set the parameters, to the preparedStatement object, sent via doPut request.
   *
   * @param preparedStatement the preparedStatement object for the operation.
   * @param root              a {@link VectorSchemaRoot} object contain the values to be used in the
   *                          PreparedStatement setters.
   * @param isUpdate          a flag to indicate if is an update or query operation.
   * @throws SQLException in case of error.
   */
  private void setDataPreparedStatement(PreparedStatement preparedStatement, VectorSchemaRoot root,
                                        boolean isUpdate)
      throws SQLException {
    for (int i = 0; i < root.getRowCount(); i++) {
      for (FieldVector vector : root.getFieldVectors()) {
        final int vectorPosition = root.getFieldVectors().indexOf(vector);
        final int position = vectorPosition + 1;

        if (vector instanceof UInt1Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (UInt1Vector) vector);
        } else if (vector instanceof TimeStampNanoTZVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeStampNanoTZVector) vector);
        } else if (vector instanceof TimeStampMicroTZVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeStampMicroTZVector) vector);
        } else if (vector instanceof TimeStampMilliTZVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeStampMilliTZVector) vector);
        } else if (vector instanceof TimeStampSecTZVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeStampSecTZVector) vector);
        } else if (vector instanceof UInt2Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (UInt2Vector) vector);
        } else if (vector instanceof UInt4Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (UInt4Vector) vector);
        } else if (vector instanceof UInt8Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (UInt8Vector) vector);
        } else if (vector instanceof TinyIntVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (IntVector) vector);
        } else if (vector instanceof BigIntVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (BigIntVector) vector);
        } else if (vector instanceof Float4Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (Float8Vector) vector);
        } else if (vector instanceof BitVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (BitVector) vector);
        } else if (vector instanceof DecimalVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (DecimalVector) vector);
        } else if (vector instanceof Decimal256Vector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (Decimal256Vector) vector);
        } else if (vector instanceof TimeStampVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeStampVector) vector);
        } else if (vector instanceof TimeNanoVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeNanoVector) vector);
        } else if (vector instanceof TimeMicroVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeMicroVector) vector);
        } else if (vector instanceof TimeMilliVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeMilliVector) vector);
        } else if (vector instanceof TimeSecVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (TimeSecVector) vector);
        } else if (vector instanceof DateDayVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (DateDayVector) vector);
        } else if (vector instanceof DateMilliVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (DateMilliVector) vector);
        } else if (vector instanceof VarCharVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (VarCharVector) vector);
        } else if (vector instanceof LargeVarCharVector) {
          setOnPreparedStatement(preparedStatement, position, vectorPosition, (LargeVarCharVector) vector);
        }
      }
      if (isUpdate) {
        preparedStatement.addBatch();
      }
    }
  }

  protected TimeZone getTimeZoneForVector(TimeStampVector vector) {
    ArrowType.Timestamp arrowType = (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    String timezoneName = arrowType.getTimezone();
    if (timezoneName == null) {
      return TimeZone.getDefault();
    }

    return TimeZone.getTimeZone(timezoneName);
  }

  /**
   * Set a string parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, VarCharVector vector)
      throws SQLException {
    final Text object = vector.getObject(vectorIndex);
    statement.setObject(column, object.toString());
  }

  /**
   * Set a string parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex,
                                     LargeVarCharVector vector)
      throws SQLException {
    final Text object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a byte parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, TinyIntVector vector)
      throws SQLException {
    final Byte object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a short parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, SmallIntVector vector)
      throws SQLException {
    final Short object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set an integer parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, IntVector vector)
      throws SQLException {
    final Integer object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a long parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, BigIntVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a float parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, Float4Vector vector)
      throws SQLException {
    final Float object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a double parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, Float8Vector vector)
      throws SQLException {
    final Double object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a BigDecimal parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, DecimalVector vector)
      throws SQLException {
    final BigDecimal object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a BigDecimal parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, Decimal256Vector vector)
      throws SQLException {
    final BigDecimal object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a timestamp parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, TimeStampVector vector)
      throws SQLException {
    final Object object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a time parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, TimeNanoVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setTime(column, new Time(object * 1000L));
  }

  /**
   * Set a time parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, TimeMicroVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setTime(column, new Time(object / 1000L));
  }

  /**
   * Set a time parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, TimeMilliVector vector)
      throws SQLException {
    final LocalDateTime object = vector.getObject(vectorIndex);
    statement.setTime(column, Time.valueOf(object.toLocalTime()));
  }

  /**
   * Set a time parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, TimeSecVector vector)
      throws SQLException {
    final Integer object = vector.getObject(vectorIndex);
    statement.setTime(column, new Time(object));
  }

  /**
   * Set a date parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, DateDayVector vector)
      throws SQLException {
    final Integer object = vector.getObject(vectorIndex);
    statement.setDate(column, new Date(TimeUnit.DAYS.toMillis(object)));
  }

  /**
   * Set a date parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, DateMilliVector vector)
      throws SQLException {
    final LocalDateTime object = vector.getObject(vectorIndex);
    statement.setDate(column, Date.valueOf(object.toLocalDate()));

  }

  /**
   * Set an unsigned 1 byte number parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, UInt1Vector vector)
      throws SQLException {
    final Byte object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set an unsigned 2 bytes number parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, UInt2Vector vector)
      throws SQLException {
    final Character object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set an unsigned 4 bytes number parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, UInt4Vector vector)
      throws SQLException {
    final Integer object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set an unsigned 8 bytes number parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, UInt8Vector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a boolean parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex, BitVector vector)
      throws SQLException {
    final Boolean object = vector.getObject(vectorIndex);
    statement.setObject(column, object);
  }

  /**
   * Set a timestamp parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex,
                                     TimeStampNanoTZVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setTimestamp(column, new Timestamp(object / 1000000L),
        Calendar.getInstance(getTimeZoneForVector(vector)));
  }

  /**
   * Set a timestamp parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex,
                                     TimeStampMicroTZVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setTimestamp(column, new Timestamp(object / 1000L),
        Calendar.getInstance(getTimeZoneForVector(vector)));
  }

  /**
   * Set a timestamp parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex,
                                     TimeStampMilliTZVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setTimestamp(column, new Timestamp(object),
        Calendar.getInstance(getTimeZoneForVector(vector)));
  }

  /**
   * Set a timestamp parameter to the preparedStatement object.
   *
   * @param statement   an instance of the {@link PreparedStatement} class.
   * @param column      the index of the column in the  {@link PreparedStatement}.
   * @param vectorIndex the index from the vector which contain the value.
   * @param vector      an instance of the vector the will be accessed.
   * @throws SQLException in case of error.
   */
  public void setOnPreparedStatement(PreparedStatement statement, int column, int vectorIndex,
                                     TimeStampSecTZVector vector)
      throws SQLException {
    final Long object = vector.getObject(vectorIndex);
    statement.setTimestamp(column, new Timestamp(object * 1000L),
        Calendar.getInstance(getTimeZoneForVector(vector)));
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command, CallContext context,
                                                  FlightStream flightStream, StreamListener<PutResult> ackStream) {
    final StatementContext<PreparedStatement> statementContext =
        preparedStatementLoadingCache.getIfPresent(command.getPreparedStatementHandle());

    return () -> {
      assert statementContext != null;
      PreparedStatement preparedStatement = statementContext.getStatement();

      try {
        while (flightStream.next()) {
          final VectorSchemaRoot root = flightStream.getRoot();
          setDataPreparedStatement(preparedStatement, root, false);
        }

      } catch (SQLException e) {
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
  public FlightInfo getFlightInfoSqlInfo(final CommandGetSqlInfo request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
  }

  @Override
  public void getStreamSqlInfo(final CommandGetSqlInfo command, final CallContext context,
                               final ServerStreamListener listener) {
    this.sqlInfoBuilder.send(command.getInfoList(), listener);
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(final CommandGetCatalogs request, final CallContext context,
                                          final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
  }

  @Override
  public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
    try (final Connection connection = dataSource.getConnection();
         final ResultSet catalogs = connection.getMetaData().getCatalogs();
         final VectorSchemaRoot vectorSchemaRoot = getCatalogsRoot(catalogs, rootAllocator)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
    } catch (SQLException e) {
      LOGGER.error(format("Failed to getStreamCatalogs: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoSchemas(final CommandGetDbSchemas request, final CallContext context,
                                         final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
  }

  @Override
  public void getStreamSchemas(final CommandGetDbSchemas command, final CallContext context,
                               final ServerStreamListener listener) {
    final String catalog = command.hasCatalog() ? command.getCatalog() : null;
    final String schemaFilterPattern = command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
    try (final Connection connection = dataSource.getConnection();
         final ResultSet schemas = connection.getMetaData().getSchemas(catalog, schemaFilterPattern);
         final VectorSchemaRoot vectorSchemaRoot = getSchemasRoot(schemas, rootAllocator)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
    } catch (SQLException e) {
      LOGGER.error(format("Failed to getStreamSchemas: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoTables(final CommandGetTables request, final CallContext context,
                                        final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLES_SCHEMA);
  }

  @Override
  public void getStreamTables(final CommandGetTables command, final CallContext context,
                              final ServerStreamListener listener) {
    final String catalog = command.hasCatalog() ? command.getCatalog() : null;
    final String schemaFilterPattern =
        command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
    final String tableFilterPattern =
        command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;

    final ProtocolStringList protocolStringList = command.getTableTypesList();
    final int protocolSize = protocolStringList.size();
    final String[] tableTypes =
        protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);

    try (final Connection connection = DriverManager.getConnection(DATABASE_URI);
         final VectorSchemaRoot vectorSchemaRoot = getTablesRoot(
             connection.getMetaData(),
             rootAllocator,
             command.getIncludeSchema(),
             catalog, schemaFilterPattern, tableFilterPattern, tableTypes)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
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
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
  }

  @Override
  public void getStreamTableTypes(final CallContext context, final ServerStreamListener listener) {
    try (final Connection connection = dataSource.getConnection();
         final ResultSet tableTypes = connection.getMetaData().getTableTypes();
         final VectorSchemaRoot vectorSchemaRoot = getTableTypesRoot(tableTypes, rootAllocator)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
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
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
  }

  @Override
  public void getStreamPrimaryKeys(final CommandGetPrimaryKeys command, final CallContext context,
                                   final ServerStreamListener listener) {

    final String catalog = command.hasCatalog() ? command.getCatalog() : null;
    final String schema = command.hasDbSchema() ? command.getDbSchema() : null;
    final String table = command.getTable();

    try (Connection connection = DriverManager.getConnection(DATABASE_URI)) {
      final ResultSet primaryKeys = connection.getMetaData().getPrimaryKeys(catalog, schema, table);

      final VarCharVector catalogNameVector = new VarCharVector("catalog_name", rootAllocator);
      final VarCharVector schemaNameVector = new VarCharVector("db_schema_name", rootAllocator);
      final VarCharVector tableNameVector = new VarCharVector("table_name", rootAllocator);
      final VarCharVector columnNameVector = new VarCharVector("column_name", rootAllocator);
      final IntVector keySequenceVector = new IntVector("key_sequence", rootAllocator);
      final VarCharVector keyNameVector = new VarCharVector("key_name", rootAllocator);

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
        final int key_seq = primaryKeys.getInt("KEY_SEQ");
        saveToVector(primaryKeys.wasNull() ? null : key_seq, keySequenceVector, rows);
        saveToVector(primaryKeys.getString("PK_NAME"), keyNameVector, rows);
      }

      try (final VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors)) {
        vectorSchemaRoot.setRowCount(rows);

        listener.start(vectorSchemaRoot);
        listener.putNext();
      }
    } catch (SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(final CommandGetExportedKeys request, final CallContext context,
                                              final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
  }

  @Override
  public void getStreamExportedKeys(final CommandGetExportedKeys command, final CallContext context,
                                    final ServerStreamListener listener) {
    String catalog = command.hasCatalog() ? command.getCatalog() : null;
    String schema = command.hasDbSchema() ? command.getDbSchema() : null;
    String table = command.getTable();

    try (Connection connection = DriverManager.getConnection(DATABASE_URI);
         ResultSet keys = connection.getMetaData().getExportedKeys(catalog, schema, table);
         VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
    } catch (SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(final CommandGetImportedKeys request, final CallContext context,
                                              final FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
  }

  @Override
  public void getStreamImportedKeys(final CommandGetImportedKeys command, final CallContext context,
                                    final ServerStreamListener listener) {
    String catalog = command.hasCatalog() ? command.getCatalog() : null;
    String schema = command.hasDbSchema() ? command.getDbSchema() : null;
    String table = command.getTable();

    try (Connection connection = DriverManager.getConnection(DATABASE_URI);
         ResultSet keys = connection.getMetaData().getImportedKeys(catalog, schema, table);
         VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
    } catch (final SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(CommandGetCrossReference request, CallContext context,
                                                FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
  }

  @Override
  public void getStreamCrossReference(CommandGetCrossReference command, CallContext context,
                                      ServerStreamListener listener) {
    final String pkCatalog = command.hasPkCatalog() ? command.getPkCatalog() : null;
    final String pkSchema = command.hasPkDbSchema() ? command.getPkDbSchema() : null;
    final String fkCatalog = command.hasFkCatalog() ? command.getFkCatalog() : null;
    final String fkSchema = command.hasFkDbSchema() ? command.getFkDbSchema() : null;
    final String pkTable = command.getPkTable();
    final String fkTable = command.getFkTable();

    try (Connection connection = DriverManager.getConnection(DATABASE_URI);
         ResultSet keys = connection.getMetaData()
             .getCrossReference(pkCatalog, pkSchema, pkTable, fkCatalog, fkSchema, fkTable);
         VectorSchemaRoot vectorSchemaRoot = createVectors(keys)) {
      listener.start(vectorSchemaRoot);
      listener.putNext();
    } catch (final SQLException e) {
      listener.error(e);
    } finally {
      listener.completed();
    }
  }

  private VectorSchemaRoot createVectors(ResultSet keys) throws SQLException {
    final VarCharVector pkCatalogNameVector = new VarCharVector("pk_catalog_name", rootAllocator);
    final VarCharVector pkSchemaNameVector = new VarCharVector("pk_db_schema_name", rootAllocator);
    final VarCharVector pkTableNameVector = new VarCharVector("pk_table_name", rootAllocator);
    final VarCharVector pkColumnNameVector = new VarCharVector("pk_column_name", rootAllocator);
    final VarCharVector fkCatalogNameVector = new VarCharVector("fk_catalog_name", rootAllocator);
    final VarCharVector fkSchemaNameVector = new VarCharVector("fk_db_schema_name", rootAllocator);
    final VarCharVector fkTableNameVector = new VarCharVector("fk_table_name", rootAllocator);
    final VarCharVector fkColumnNameVector = new VarCharVector("fk_column_name", rootAllocator);
    final IntVector keySequenceVector = new IntVector("key_sequence", rootAllocator);
    final VarCharVector fkKeyNameVector = new VarCharVector("fk_key_name", rootAllocator);
    final VarCharVector pkKeyNameVector = new VarCharVector("pk_key_name", rootAllocator);
    final UInt1Vector updateRuleVector = new UInt1Vector("update_rule", rootAllocator);
    final UInt1Vector deleteRuleVector = new UInt1Vector("delete_rule", rootAllocator);

    Map<FieldVector, String> vectorToColumnName = new HashMap<>();
    vectorToColumnName.put(pkCatalogNameVector, "PKTABLE_CAT");
    vectorToColumnName.put(pkSchemaNameVector, "PKTABLE_SCHEM");
    vectorToColumnName.put(pkTableNameVector, "PKTABLE_NAME");
    vectorToColumnName.put(pkColumnNameVector, "PKCOLUMN_NAME");
    vectorToColumnName.put(fkCatalogNameVector, "FKTABLE_CAT");
    vectorToColumnName.put(fkSchemaNameVector, "FKTABLE_SCHEM");
    vectorToColumnName.put(fkTableNameVector, "FKTABLE_NAME");
    vectorToColumnName.put(fkColumnNameVector, "FKCOLUMN_NAME");
    vectorToColumnName.put(keySequenceVector, "KEY_SEQ");
    vectorToColumnName.put(updateRuleVector, "UPDATE_RULE");
    vectorToColumnName.put(deleteRuleVector, "DELETE_RULE");
    vectorToColumnName.put(fkKeyNameVector, "FK_NAME");
    vectorToColumnName.put(pkKeyNameVector, "PK_NAME");

    final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(
        pkCatalogNameVector, pkSchemaNameVector, pkTableNameVector, pkColumnNameVector, fkCatalogNameVector,
        fkSchemaNameVector, fkTableNameVector, fkColumnNameVector, keySequenceVector, fkKeyNameVector,
        pkKeyNameVector, updateRuleVector, deleteRuleVector);

    vectorSchemaRoot.allocateNew();
    final int rowCount = saveToVectors(vectorToColumnName, keys, true);

    vectorSchemaRoot.setRowCount(rowCount);

    return vectorSchemaRoot;
  }

  @Override
  public void getStreamStatement(final TicketStatementQuery ticketStatementQuery, final CallContext context,
                                 final ServerStreamListener listener) {
    final ByteString handle = ticketStatementQuery.getStatementHandle();
    final StatementContext<Statement> statementContext =
        Objects.requireNonNull(statementLoadingCache.getIfPresent(handle));
    try (final ResultSet resultSet = statementContext.getStatement().getResultSet()) {
      final Schema schema = jdbcToArrowSchema(resultSet.getMetaData(), DEFAULT_CALENDAR);
      try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator)) {
        final VectorLoader loader = new VectorLoader(vectorSchemaRoot);
        listener.start(vectorSchemaRoot);

        final ArrowVectorIterator iterator = sqlToArrowVectorIterator(resultSet, rootAllocator);
        while (iterator.hasNext()) {
          final VectorUnloader unloader = new VectorUnloader(iterator.next());
          loader.load(unloader.getRecordBatch());
          listener.putNext();
          vectorSchemaRoot.clear();
        }

        listener.putNext();
      }
    } catch (SQLException | IOException e) {
      LOGGER.error(format("Failed to getStreamPreparedStatement: <%s>.", e.getMessage()), e);
      listener.error(e);
    } finally {
      listener.completed();
      statementLoadingCache.invalidate(handle);
    }
  }

  private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
                                                                final Schema schema) {
    final Ticket ticket = new Ticket(pack(request).toByteArray());
    // TODO Support multiple endpoints.
    final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, location));

    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }

  private static class StatementRemovalListener<T extends Statement>
      implements RemovalListener<ByteString, StatementContext<T>> {
    @Override
    public void onRemoval(final RemovalNotification<ByteString, StatementContext<T>> notification) {
      try {
        AutoCloseables.close(notification.getValue());
      } catch (final Exception e) {
        // swallow
      }
    }
  }
}
