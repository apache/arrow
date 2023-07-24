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

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.doesBitmaskTranslateToEnum;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.arrow.driver.jdbc.utils.SqlTypes;
import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlOuterJoinsSupportLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedGroupBy;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedPositionedCommands;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedResultSetType;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedSubqueries;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedUnions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlTransactionIsolationLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

import com.google.protobuf.ProtocolMessageEnum;

/**
 * Arrow Flight JDBC's implementation of {@link DatabaseMetaData}.
 */
public class ArrowDatabaseMetadata extends AvaticaDatabaseMetaData {
  private static final String JAVA_REGEX_SPECIALS = "[]()|^-+*?{}$\\.";
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  static final int NO_DECIMAL_DIGITS = 0;
  private static final int BASE10_RADIX = 10;
  static final int COLUMN_SIZE_BYTE = (int) Math.ceil((Byte.SIZE - 1) * Math.log(2) / Math.log(10));
  static final int COLUMN_SIZE_SHORT =
      (int) Math.ceil((Short.SIZE - 1) * Math.log(2) / Math.log(10));
  static final int COLUMN_SIZE_INT =
      (int) Math.ceil((Integer.SIZE - 1) * Math.log(2) / Math.log(10));
  static final int COLUMN_SIZE_LONG = (int) Math.ceil((Long.SIZE - 1) * Math.log(2) / Math.log(10));
  static final int COLUMN_SIZE_VARCHAR_AND_BINARY = 65536;
  static final int COLUMN_SIZE_DATE = "YYYY-MM-DD".length();
  static final int COLUMN_SIZE_TIME = "HH:MM:ss".length();
  static final int COLUMN_SIZE_TIME_MILLISECONDS = "HH:MM:ss.SSS".length();
  static final int COLUMN_SIZE_TIME_MICROSECONDS = "HH:MM:ss.SSSSSS".length();
  static final int COLUMN_SIZE_TIME_NANOSECONDS = "HH:MM:ss.SSSSSSSSS".length();
  static final int COLUMN_SIZE_TIMESTAMP_SECONDS = COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME;
  static final int COLUMN_SIZE_TIMESTAMP_MILLISECONDS =
      COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME_MILLISECONDS;
  static final int COLUMN_SIZE_TIMESTAMP_MICROSECONDS =
      COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME_MICROSECONDS;
  static final int COLUMN_SIZE_TIMESTAMP_NANOSECONDS =
      COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME_NANOSECONDS;
  static final int DECIMAL_DIGITS_TIME_MILLISECONDS = 3;
  static final int DECIMAL_DIGITS_TIME_MICROSECONDS = 6;
  static final int DECIMAL_DIGITS_TIME_NANOSECONDS = 9;
  private static final Schema GET_COLUMNS_SCHEMA = new Schema(
      Arrays.asList(
          Field.nullable("TABLE_CAT", Types.MinorType.VARCHAR.getType()),
          Field.nullable("TABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("TABLE_NAME", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("COLUMN_NAME", Types.MinorType.VARCHAR.getType()),
          Field.nullable("DATA_TYPE", Types.MinorType.INT.getType()),
          Field.nullable("TYPE_NAME", Types.MinorType.VARCHAR.getType()),
          Field.nullable("COLUMN_SIZE", Types.MinorType.INT.getType()),
          Field.nullable("BUFFER_LENGTH", Types.MinorType.INT.getType()),
          Field.nullable("DECIMAL_DIGITS", Types.MinorType.INT.getType()),
          Field.nullable("NUM_PREC_RADIX", Types.MinorType.INT.getType()),
          Field.notNullable("NULLABLE", Types.MinorType.INT.getType()),
          Field.nullable("REMARKS", Types.MinorType.VARCHAR.getType()),
          Field.nullable("COLUMN_DEF", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SQL_DATA_TYPE", Types.MinorType.INT.getType()),
          Field.nullable("SQL_DATETIME_SUB", Types.MinorType.INT.getType()),
          Field.notNullable("CHAR_OCTET_LENGTH", Types.MinorType.INT.getType()),
          Field.notNullable("ORDINAL_POSITION", Types.MinorType.INT.getType()),
          Field.notNullable("IS_NULLABLE", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SCOPE_CATALOG", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SCOPE_SCHEMA", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SCOPE_TABLE", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SOURCE_DATA_TYPE", Types.MinorType.SMALLINT.getType()),
          Field.notNullable("IS_AUTOINCREMENT", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("IS_GENERATEDCOLUMN", Types.MinorType.VARCHAR.getType())
      ));
  private final Map<SqlInfo, Object> cachedSqlInfo =
      Collections.synchronizedMap(new EnumMap<>(SqlInfo.class));
  private static final Map<Integer, Integer> sqlTypesToFlightEnumConvertTypes = new HashMap<>();

  static {
    sqlTypesToFlightEnumConvertTypes.put(BIT, SqlSupportsConvert.SQL_CONVERT_BIT_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(INTEGER, SqlSupportsConvert.SQL_CONVERT_INTEGER_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(NUMERIC, SqlSupportsConvert.SQL_CONVERT_NUMERIC_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(SMALLINT, SqlSupportsConvert.SQL_CONVERT_SMALLINT_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(TINYINT, SqlSupportsConvert.SQL_CONVERT_TINYINT_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(FLOAT, SqlSupportsConvert.SQL_CONVERT_FLOAT_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(BIGINT, SqlSupportsConvert.SQL_CONVERT_BIGINT_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(REAL, SqlSupportsConvert.SQL_CONVERT_REAL_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(DECIMAL, SqlSupportsConvert.SQL_CONVERT_DECIMAL_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(BINARY, SqlSupportsConvert.SQL_CONVERT_BINARY_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(LONGVARBINARY,
        SqlSupportsConvert.SQL_CONVERT_LONGVARBINARY_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(CHAR, SqlSupportsConvert.SQL_CONVERT_CHAR_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(VARCHAR, SqlSupportsConvert.SQL_CONVERT_VARCHAR_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(LONGNVARCHAR,
        SqlSupportsConvert.SQL_CONVERT_LONGVARCHAR_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(DATE, SqlSupportsConvert.SQL_CONVERT_DATE_VALUE);
    sqlTypesToFlightEnumConvertTypes.put(TIMESTAMP, SqlSupportsConvert.SQL_CONVERT_TIMESTAMP_VALUE);
  }

  ArrowDatabaseMetadata(final AvaticaConnection connection) {
    super(connection);
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.FLIGHT_SQL_SERVER_NAME, String.class);
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.FLIGHT_SQL_SERVER_VERSION, String.class);
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR, String.class);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY, Boolean.class);
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return convertListSqlInfoToString(
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_KEYWORDS, List.class));
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return convertListSqlInfoToString(
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_NUMERIC_FUNCTIONS, List.class));
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return convertListSqlInfoToString(
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_STRING_FUNCTIONS, List.class));
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return convertListSqlInfoToString(
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SYSTEM_FUNCTIONS, List.class));
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return convertListSqlInfoToString(
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_DATETIME_FUNCTIONS, List.class));
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SEARCH_STRING_ESCAPE, String.class);
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_EXTRA_NAME_CHARACTERS, String.class);
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_COLUMN_ALIASING, Boolean.class);
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_NULL_PLUS_NULL_IS_NULL, Boolean.class);
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return !getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_CONVERT, Map.class).isEmpty();
  }

  @Override
  public boolean supportsConvert(final int fromType, final int toType) throws SQLException {
    final Map<Integer, List<Integer>> sqlSupportsConvert =
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_CONVERT, Map.class);

    if (!sqlTypesToFlightEnumConvertTypes.containsKey(fromType)) {
      return false;
    }

    final List<Integer> list =
        sqlSupportsConvert.get(sqlTypesToFlightEnumConvertTypes.get(fromType));

    return list != null && list.contains(sqlTypesToFlightEnumConvertTypes.get(toType));
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_TABLE_CORRELATION_NAMES,
        Boolean.class);
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES,
        Boolean.class);
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY,
        Boolean.class);
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_ORDER_BY_UNRELATED, Boolean.class);
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    final int bitmask =
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GROUP_BY, Integer.class);
    return bitmask != 0;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GROUP_BY,
        SqlSupportedGroupBy.SQL_GROUP_BY_UNRELATED);
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE, Boolean.class);
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_NON_NULLABLE_COLUMNS,
        Boolean.class);
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return checkEnumLevel(
        Arrays.asList(getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GRAMMAR,
                SupportedSqlGrammar.SQL_EXTENDED_GRAMMAR),
            getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GRAMMAR,
                SupportedSqlGrammar.SQL_CORE_GRAMMAR),
            getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GRAMMAR,
                SupportedSqlGrammar.SQL_MINIMUM_GRAMMAR)));
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return checkEnumLevel(
        Arrays.asList(getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GRAMMAR,
                SupportedSqlGrammar.SQL_EXTENDED_GRAMMAR),
            getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GRAMMAR,
                SupportedSqlGrammar.SQL_CORE_GRAMMAR)));
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_GRAMMAR,
        SupportedSqlGrammar.SQL_EXTENDED_GRAMMAR);
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return checkEnumLevel(
        Arrays.asList(getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
                SupportedAnsi92SqlGrammarLevel.ANSI92_ENTRY_SQL),
            getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
                SupportedAnsi92SqlGrammarLevel.ANSI92_INTERMEDIATE_SQL),
            getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
                SupportedAnsi92SqlGrammarLevel.ANSI92_FULL_SQL)));
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return checkEnumLevel(
        Arrays.asList(getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
                SupportedAnsi92SqlGrammarLevel.ANSI92_ENTRY_SQL),
            getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
                SupportedAnsi92SqlGrammarLevel.ANSI92_INTERMEDIATE_SQL)));
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
        SupportedAnsi92SqlGrammarLevel.ANSI92_FULL_SQL);
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY,
        Boolean.class);
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    final int bitmask =
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_OUTER_JOINS_SUPPORT_LEVEL, Integer.class);
    return bitmask != 0;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_OUTER_JOINS_SUPPORT_LEVEL,
        SqlOuterJoinsSupportLevel.SQL_FULL_OUTER_JOINS);
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_OUTER_JOINS_SUPPORT_LEVEL,
        SqlOuterJoinsSupportLevel.SQL_LIMITED_OUTER_JOINS);
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SCHEMA_TERM, String.class);
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_PROCEDURE_TERM, String.class);
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_CATALOG_TERM, String.class);
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_CATALOG_AT_START, Boolean.class);
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS,
        SqlSupportedElementActions.SQL_ELEMENT_IN_PROCEDURE_CALLS);
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS,
        SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS);
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS,
        SqlSupportedElementActions.SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS);
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS,
        SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS);
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS,
        SqlSupportedElementActions.SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS);
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS,
        SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE);
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS,
        SqlSupportedPositionedCommands.SQL_POSITIONED_UPDATE);
  }

  @Override
  public boolean supportsResultSetType(final int type) throws SQLException {
    final int bitmask =
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_RESULT_SET_TYPES, Integer.class);

    switch (type) {
      case ResultSet.TYPE_FORWARD_ONLY:
        return doesBitmaskTranslateToEnum(SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY,
            bitmask);
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return doesBitmaskTranslateToEnum(SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE,
            bitmask);
      case ResultSet.TYPE_SCROLL_SENSITIVE:
        return doesBitmaskTranslateToEnum(SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE,
            bitmask);
      default:
        throw new SQLException(
            "Invalid result set type argument. The informed type is not defined in java.sql.ResultSet.");
    }
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SELECT_FOR_UPDATE_SUPPORTED, Boolean.class);
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_STORED_PROCEDURES_SUPPORTED, Boolean.class);
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_SUBQUERIES,
        SqlSupportedSubqueries.SQL_SUBQUERIES_IN_COMPARISONS);
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_SUBQUERIES,
        SqlSupportedSubqueries.SQL_SUBQUERIES_IN_EXISTS);
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_SUBQUERIES,
        SqlSupportedSubqueries.SQL_SUBQUERIES_IN_INS);
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_SUBQUERIES,
        SqlSupportedSubqueries.SQL_SUBQUERIES_IN_QUANTIFIEDS);
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED,
        Boolean.class);
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    final int bitmask =
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_UNIONS, Integer.class);
    return bitmask != 0;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_UNIONS,
        SqlSupportedUnions.SQL_UNION_ALL);
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_BINARY_LITERAL_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_CHAR_LITERAL_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_COLUMN_NAME_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_COLUMNS_IN_GROUP_BY,
        Long.class).intValue();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_COLUMNS_IN_INDEX,
        Long.class).intValue();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_COLUMNS_IN_ORDER_BY,
        Long.class).intValue();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_COLUMNS_IN_SELECT,
        Long.class).intValue();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_COLUMNS_IN_TABLE,
        Long.class).intValue();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_CONNECTIONS, Long.class).intValue();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_CURSOR_NAME_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_INDEX_LENGTH, Long.class).intValue();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_DB_SCHEMA_NAME_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_PROCEDURE_NAME_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_CATALOG_NAME_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_ROW_SIZE, Long.class).intValue();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_ROW_SIZE_INCLUDES_BLOBS, Boolean.class);
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_STATEMENT_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_STATEMENTS, Long.class).intValue();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_TABLE_NAME_LENGTH,
        Long.class).intValue();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_TABLES_IN_SELECT,
        Long.class).intValue();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_MAX_USERNAME_LENGTH, Long.class).intValue();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_DEFAULT_TRANSACTION_ISOLATION,
        Long.class).intValue();
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_TRANSACTIONS_SUPPORTED, Boolean.class);
  }

  @Override
  public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
    final int bitmask =
        getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS,
            Integer.class);

    switch (level) {
      case Connection.TRANSACTION_NONE:
        return doesBitmaskTranslateToEnum(SqlTransactionIsolationLevel.SQL_TRANSACTION_NONE, bitmask);
      case Connection.TRANSACTION_READ_COMMITTED:
        return doesBitmaskTranslateToEnum(SqlTransactionIsolationLevel.SQL_TRANSACTION_READ_COMMITTED,
            bitmask);
      case Connection.TRANSACTION_READ_UNCOMMITTED:
        return doesBitmaskTranslateToEnum(SqlTransactionIsolationLevel.SQL_TRANSACTION_READ_UNCOMMITTED,
            bitmask);
      case Connection.TRANSACTION_REPEATABLE_READ:
        return doesBitmaskTranslateToEnum(SqlTransactionIsolationLevel.SQL_TRANSACTION_REPEATABLE_READ,
            bitmask);
      case Connection.TRANSACTION_SERIALIZABLE:
        return doesBitmaskTranslateToEnum(SqlTransactionIsolationLevel.SQL_TRANSACTION_SERIALIZABLE,
            bitmask);
      default:
        throw new SQLException(
            "Invalid transaction isolation level argument. The informed level is not defined in java.sql.Connection.");
    }
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT,
        Boolean.class);
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED,
        Boolean.class);
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_BATCH_UPDATES_SUPPORTED, Boolean.class);
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_SAVEPOINTS_SUPPORTED, Boolean.class);
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_NAMED_PARAMETERS_SUPPORTED, Boolean.class);
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_LOCATORS_UPDATE_COPY, Boolean.class);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(
        SqlInfo.SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED, Boolean.class);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  private <T> T getSqlInfoAndCacheIfCacheIsEmpty(final SqlInfo sqlInfoCommand,
                                                 final Class<T> desiredType)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    if (cachedSqlInfo.isEmpty()) {
      final FlightInfo sqlInfo = connection.getClientHandler().getSqlInfo();
      synchronized (cachedSqlInfo) {
        if (cachedSqlInfo.isEmpty()) {
          try (final ResultSet resultSet =
                   ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(
                       connection, sqlInfo, null)) {
            while (resultSet.next()) {
              cachedSqlInfo.put(SqlInfo.forNumber((Integer) resultSet.getObject("info_name")),
                  resultSet.getObject("value"));
            }
          }
        }
      }
    }
    return desiredType.cast(cachedSqlInfo.get(sqlInfoCommand));
  }

  private String convertListSqlInfoToString(final List<?> sqlInfoList) {
    return sqlInfoList.stream().map(Object::toString).collect(Collectors.joining(", "));
  }

  private boolean getSqlInfoEnumOptionAndCacheIfCacheIsEmpty(
      final SqlInfo sqlInfoCommand,
      final ProtocolMessageEnum enumInstance
  ) throws SQLException {
    final int bitmask = getSqlInfoAndCacheIfCacheIsEmpty(sqlInfoCommand, Integer.class);
    return doesBitmaskTranslateToEnum(enumInstance, bitmask);
  }

  private boolean checkEnumLevel(final List<Boolean> toCheck) {
    return toCheck.stream().anyMatch(e -> e);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoCatalogs = connection.getClientHandler().getCatalogs();

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_CATALOGS_SCHEMA, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoCatalogs,
        transformer);
  }

  @Override
  public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoImportedKeys =
        connection.getClientHandler().getImportedKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer = getForeignKeysTransformer(allocator);
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoImportedKeys,
        transformer);
  }

  @Override
  public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoExportedKeys =
        connection.getClientHandler().getExportedKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer = getForeignKeysTransformer(allocator);
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoExportedKeys,
        transformer);
  }

  @Override
  public ResultSet getCrossReference(final String parentCatalog, final String parentSchema,
                                     final String parentTable,
                                     final String foreignCatalog, final String foreignSchema,
                                     final String foreignTable)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoCrossReference = connection.getClientHandler().getCrossReference(
        parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer = getForeignKeysTransformer(allocator);
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoCrossReference,
        transformer);
  }

  /**
   * Transformer used on getImportedKeys, getExportedKeys and getCrossReference methods, since
   * all three share the same schema.
   */
  private VectorSchemaRootTransformer getForeignKeysTransformer(final BufferAllocator allocator) {
    return new VectorSchemaRootTransformer.Builder(Schemas.GET_IMPORTED_KEYS_SCHEMA,
        allocator)
        .renameFieldVector("pk_catalog_name", "PKTABLE_CAT")
        .renameFieldVector("pk_db_schema_name", "PKTABLE_SCHEM")
        .renameFieldVector("pk_table_name", "PKTABLE_NAME")
        .renameFieldVector("pk_column_name", "PKCOLUMN_NAME")
        .renameFieldVector("fk_catalog_name", "FKTABLE_CAT")
        .renameFieldVector("fk_db_schema_name", "FKTABLE_SCHEM")
        .renameFieldVector("fk_table_name", "FKTABLE_NAME")
        .renameFieldVector("fk_column_name", "FKCOLUMN_NAME")
        .renameFieldVector("key_sequence", "KEY_SEQ")
        .renameFieldVector("fk_key_name", "FK_NAME")
        .renameFieldVector("pk_key_name", "PK_NAME")
        .renameFieldVector("update_rule", "UPDATE_RULE")
        .renameFieldVector("delete_rule", "DELETE_RULE")
        .addEmptyField("DEFERRABILITY", new ArrowType.Int(Byte.SIZE, false))
        .build();
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoSchemas =
        connection.getClientHandler().getSchemas(catalog, schemaPattern);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_SCHEMAS_SCHEMA, allocator)
            .renameFieldVector("db_schema_name", "TABLE_SCHEM")
            .renameFieldVector("catalog_name", "TABLE_CATALOG")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoSchemas,
        transformer);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoTableTypes = connection.getClientHandler().getTableTypes();

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_TABLE_TYPES_SCHEMA, allocator)
            .renameFieldVector("table_type", "TABLE_TYPE")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTableTypes,
        transformer);
  }

  @Override
  public ResultSet getTables(final String catalog, final String schemaPattern,
                             final String tableNamePattern,
                             final String[] types)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final List<String> typesList = types == null ? null : Arrays.asList(types);
    final FlightInfo flightInfoTables =
        connection.getClientHandler()
            .getTables(catalog, schemaPattern, tableNamePattern, typesList, false);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .renameFieldVector("db_schema_name", "TABLE_SCHEM")
            .renameFieldVector("table_name", "TABLE_NAME")
            .renameFieldVector("table_type", "TABLE_TYPE")
            .addEmptyField("REMARKS", Types.MinorType.VARBINARY)
            .addEmptyField("TYPE_CAT", Types.MinorType.VARBINARY)
            .addEmptyField("TYPE_SCHEM", Types.MinorType.VARBINARY)
            .addEmptyField("TYPE_NAME", Types.MinorType.VARBINARY)
            .addEmptyField("SELF_REFERENCING_COL_NAME", Types.MinorType.VARBINARY)
            .addEmptyField("REF_GENERATION", Types.MinorType.VARBINARY)
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTables,
        transformer);
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoPrimaryKeys =
        connection.getClientHandler().getPrimaryKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_PRIMARY_KEYS_SCHEMA, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .renameFieldVector("db_schema_name", "TABLE_SCHEM")
            .renameFieldVector("table_name", "TABLE_NAME")
            .renameFieldVector("column_name", "COLUMN_NAME")
            .renameFieldVector("key_sequence", "KEY_SEQ")
            .renameFieldVector("key_name", "PK_NAME")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoPrimaryKeys,
        transformer);
  }

  @Override
  public ResultSet getColumns(final String catalog, final String schemaPattern,
                              final String tableNamePattern,
                              final String columnNamePattern)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoTables =
        connection.getClientHandler()
            .getTables(catalog, schemaPattern, tableNamePattern, null, true);

    final BufferAllocator allocator = connection.getBufferAllocator();

    final Pattern columnNamePat =
        columnNamePattern != null ? Pattern.compile(sqlToRegexLike(columnNamePattern)) : null;

    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTables,
        (originalRoot, transformedRoot) -> {
          int columnCounter = 0;
          if (transformedRoot == null) {
            transformedRoot = VectorSchemaRoot.create(GET_COLUMNS_SCHEMA, allocator);
          }

          final int originalRootRowCount = originalRoot.getRowCount();

          final VarCharVector catalogNameVector =
              (VarCharVector) originalRoot.getVector("catalog_name");
          final VarCharVector tableNameVector =
              (VarCharVector) originalRoot.getVector("table_name");
          final VarCharVector schemaNameVector =
              (VarCharVector) originalRoot.getVector("db_schema_name");

          final VarBinaryVector schemaVector =
              (VarBinaryVector) originalRoot.getVector("table_schema");

          for (int i = 0; i < originalRootRowCount; i++) {
            final Text catalogName = catalogNameVector.getObject(i);
            final Text tableName = tableNameVector.getObject(i);
            final Text schemaName = schemaNameVector.getObject(i);

            final Schema currentSchema;
            try {
              currentSchema = MessageSerializer.deserializeSchema(
                  new ReadChannel(Channels.newChannel(
                      new ByteArrayInputStream(schemaVector.get(i)))));
            } catch (final IOException e) {
              throw new IOException(
                  String.format("Failed to deserialize schema for table %s", tableName), e);
            }
            final List<Field> tableColumns = currentSchema.getFields();

            columnCounter = setGetColumnsVectorSchemaRootFromFields(transformedRoot, columnCounter,
                tableColumns,
                catalogName, tableName, schemaName, columnNamePat);
          }

          transformedRoot.setRowCount(columnCounter);

          originalRoot.clear();
          return transformedRoot;
        });
  }

  private int setGetColumnsVectorSchemaRootFromFields(final VectorSchemaRoot currentRoot,
                                                      int insertIndex,
                                                      final List<Field> tableColumns,
                                                      final Text catalogName,
                                                      final Text tableName, final Text schemaName,
                                                      final Pattern columnNamePattern) {
    int ordinalIndex = 1;
    final int tableColumnsSize = tableColumns.size();

    final VarCharVector tableCatVector = (VarCharVector) currentRoot.getVector("TABLE_CAT");
    final VarCharVector tableSchemVector = (VarCharVector) currentRoot.getVector("TABLE_SCHEM");
    final VarCharVector tableNameVector = (VarCharVector) currentRoot.getVector("TABLE_NAME");
    final VarCharVector columnNameVector = (VarCharVector) currentRoot.getVector("COLUMN_NAME");
    final IntVector dataTypeVector = (IntVector) currentRoot.getVector("DATA_TYPE");
    final VarCharVector typeNameVector = (VarCharVector) currentRoot.getVector("TYPE_NAME");
    final IntVector columnSizeVector = (IntVector) currentRoot.getVector("COLUMN_SIZE");
    final IntVector decimalDigitsVector = (IntVector) currentRoot.getVector("DECIMAL_DIGITS");
    final IntVector numPrecRadixVector = (IntVector) currentRoot.getVector("NUM_PREC_RADIX");
    final IntVector nullableVector = (IntVector) currentRoot.getVector("NULLABLE");
    final IntVector ordinalPositionVector = (IntVector) currentRoot.getVector("ORDINAL_POSITION");
    final VarCharVector isNullableVector = (VarCharVector) currentRoot.getVector("IS_NULLABLE");
    final VarCharVector isAutoincrementVector = (VarCharVector) currentRoot.getVector("IS_AUTOINCREMENT");
    final VarCharVector isGeneratedColumnVector = (VarCharVector) currentRoot.getVector("IS_GENERATEDCOLUMN");

    for (int i = 0; i < tableColumnsSize; i++, ordinalIndex++) {
      final Field field = tableColumns.get(i);
      final FlightSqlColumnMetadata columnMetadata = new FlightSqlColumnMetadata(field.getMetadata());
      final String columnName = field.getName();

      if (columnNamePattern != null && !columnNamePattern.matcher(columnName).matches()) {
        continue;
      }
      final ArrowType fieldType = field.getType();

      if (catalogName != null) {
        tableCatVector.setSafe(insertIndex, catalogName);
      }

      if (schemaName != null) {
        tableSchemVector.setSafe(insertIndex, schemaName);
      }

      if (tableName != null) {
        tableNameVector.setSafe(insertIndex, tableName);
      }

      if (columnName != null) {
        columnNameVector.setSafe(insertIndex, columnName.getBytes(CHARSET));
      }

      dataTypeVector.setSafe(insertIndex, SqlTypes.getSqlTypeIdFromArrowType(fieldType));
      byte[] typeName = columnMetadata.getTypeName() != null ?
          columnMetadata.getTypeName().getBytes(CHARSET) :
          SqlTypes.getSqlTypeNameFromArrowType(fieldType).getBytes(CHARSET);
      typeNameVector.setSafe(insertIndex, typeName);

      // We're not setting COLUMN_SIZE for ROWID SQL Types, as there's no such Arrow type.
      // We're not setting COLUMN_SIZE nor DECIMAL_DIGITS for Float/Double as their precision and scale are variable.
      if (fieldType instanceof ArrowType.Decimal) {
        numPrecRadixVector.setSafe(insertIndex, BASE10_RADIX);
      } else if (fieldType instanceof ArrowType.Int) {
        numPrecRadixVector.setSafe(insertIndex, BASE10_RADIX);
      } else if (fieldType instanceof ArrowType.FloatingPoint) {
        numPrecRadixVector.setSafe(insertIndex, BASE10_RADIX);
      }

      Integer decimalDigits = columnMetadata.getScale();
      if (decimalDigits == null) {
        decimalDigits = getDecimalDigits(fieldType);
      }
      if (decimalDigits != null) {
        decimalDigitsVector.setSafe(insertIndex, decimalDigits);
      }

      Integer columnSize = columnMetadata.getPrecision();
      if (columnSize == null) {
        columnSize = getColumnSize(fieldType);
      }
      if (columnSize != null) {
        columnSizeVector.setSafe(insertIndex, columnSize);
      }

      nullableVector.setSafe(insertIndex, field.isNullable() ? 1 : 0);

      isNullableVector.setSafe(insertIndex, booleanToYesOrNo(field.isNullable()));

      Boolean autoIncrement = columnMetadata.isAutoIncrement();
      if (autoIncrement != null) {
        isAutoincrementVector.setSafe(insertIndex, booleanToYesOrNo(autoIncrement));
      } else {
        isAutoincrementVector.setSafe(insertIndex, EMPTY_BYTE_ARRAY);
      }

      // Fields also don't hold information about IS_AUTOINCREMENT and IS_GENERATEDCOLUMN,
      // so we're setting an empty string (as bytes), which means it couldn't be determined.
      isGeneratedColumnVector.setSafe(insertIndex, EMPTY_BYTE_ARRAY);

      ordinalPositionVector.setSafe(insertIndex, ordinalIndex);

      insertIndex++;
    }
    return insertIndex;
  }

  private static byte[] booleanToYesOrNo(boolean autoIncrement) {
    return autoIncrement ? "YES".getBytes(CHARSET) : "NO".getBytes(CHARSET);
  }

  static Integer getDecimalDigits(final ArrowType fieldType) {
    // We're not setting  DECIMAL_DIGITS for Float/Double as their precision and scale are variable.
    if (fieldType instanceof ArrowType.Decimal) {
      final ArrowType.Decimal thisDecimal = (ArrowType.Decimal) fieldType;
      return thisDecimal.getScale();
    } else if (fieldType instanceof ArrowType.Int) {
      return NO_DECIMAL_DIGITS;
    } else if (fieldType instanceof ArrowType.Timestamp) {
      switch (((ArrowType.Timestamp) fieldType).getUnit()) {
        case SECOND:
          return NO_DECIMAL_DIGITS;
        case MILLISECOND:
          return DECIMAL_DIGITS_TIME_MILLISECONDS;
        case MICROSECOND:
          return DECIMAL_DIGITS_TIME_MICROSECONDS;
        case NANOSECOND:
          return DECIMAL_DIGITS_TIME_NANOSECONDS;
        default:
          break;
      }
    } else if (fieldType instanceof ArrowType.Time) {
      switch (((ArrowType.Time) fieldType).getUnit()) {
        case SECOND:
          return NO_DECIMAL_DIGITS;
        case MILLISECOND:
          return DECIMAL_DIGITS_TIME_MILLISECONDS;
        case MICROSECOND:
          return DECIMAL_DIGITS_TIME_MICROSECONDS;
        case NANOSECOND:
          return DECIMAL_DIGITS_TIME_NANOSECONDS;
        default:
          break;
      }
    } else if (fieldType instanceof ArrowType.Date) {
      return NO_DECIMAL_DIGITS;
    }

    return null;
  }

  static Integer getColumnSize(final ArrowType fieldType) {
    // We're not setting COLUMN_SIZE for ROWID SQL Types, as there's no such Arrow type.
    // We're not setting COLUMN_SIZE nor DECIMAL_DIGITS for Float/Double as their precision and scale are variable.
    if (fieldType instanceof ArrowType.Decimal) {
      final ArrowType.Decimal thisDecimal = (ArrowType.Decimal) fieldType;
      return thisDecimal.getPrecision();
    } else if (fieldType instanceof ArrowType.Int) {
      final ArrowType.Int thisInt = (ArrowType.Int) fieldType;
      switch (thisInt.getBitWidth()) {
        case Byte.SIZE:
          return COLUMN_SIZE_BYTE;
        case Short.SIZE:
          return COLUMN_SIZE_SHORT;
        case Integer.SIZE:
          return COLUMN_SIZE_INT;
        case Long.SIZE:
          return COLUMN_SIZE_LONG;
        default:
          break;
      }
    } else if (fieldType instanceof ArrowType.Utf8 || fieldType instanceof ArrowType.Binary) {
      return COLUMN_SIZE_VARCHAR_AND_BINARY;
    } else if (fieldType instanceof ArrowType.Timestamp) {
      switch (((ArrowType.Timestamp) fieldType).getUnit()) {
        case SECOND:
          return COLUMN_SIZE_TIMESTAMP_SECONDS;
        case MILLISECOND:
          return COLUMN_SIZE_TIMESTAMP_MILLISECONDS;
        case MICROSECOND:
          return COLUMN_SIZE_TIMESTAMP_MICROSECONDS;
        case NANOSECOND:
          return COLUMN_SIZE_TIMESTAMP_NANOSECONDS;
        default:
          break;
      }
    } else if (fieldType instanceof ArrowType.Time) {
      switch (((ArrowType.Time) fieldType).getUnit()) {
        case SECOND:
          return COLUMN_SIZE_TIME;
        case MILLISECOND:
          return COLUMN_SIZE_TIME_MILLISECONDS;
        case MICROSECOND:
          return COLUMN_SIZE_TIME_MICROSECONDS;
        case NANOSECOND:
          return COLUMN_SIZE_TIME_NANOSECONDS;
        default:
          break;
      }
    } else if (fieldType instanceof ArrowType.Date) {
      return COLUMN_SIZE_DATE;
    }

    return null;
  }

  static String sqlToRegexLike(final String sqlPattern) {
    final int len = sqlPattern.length();
    final StringBuilder javaPattern = new StringBuilder(len + len);

    for (int i = 0; i < len; i++) {
      final char currentChar = sqlPattern.charAt(i);

      if (JAVA_REGEX_SPECIALS.indexOf(currentChar) >= 0) {
        javaPattern.append('\\');
      }

      switch (currentChar) {
        case '_':
          javaPattern.append('.');
          break;
        case '%':
          javaPattern.append(".");
          javaPattern.append('*');
          break;
        default:
          javaPattern.append(currentChar);
          break;
      }
    }
    return javaPattern.toString();
  }
}
