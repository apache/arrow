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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedTransaction;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.createBitmaskFromEnums;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlOuterJoinsSupportLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedGroupBy;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedPositionedCommands;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedResultSetType;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedSubqueries;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedUnions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlTransactionIsolationLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.google.protobuf.ProtocolMessageEnum;

/**
 * Auxiliary class meant to facilitate the implementation of {@link FlightSqlProducer#getStreamSqlInfo}.
 * <p>
 * Usage requires the user to add the required SqlInfo values using the {@code with*} methods
 * like {@link SqlInfoBuilder#withFlightSqlServerName(String)}, and request it back
 * through the {@link SqlInfoBuilder#send(List, ServerStreamListener)} method.
 */
@SuppressWarnings({"unused"})
public class SqlInfoBuilder {
  private final Map<Integer, ObjIntConsumer<VectorSchemaRoot>> providers = new HashMap<>();

  /**
   * Gets a {@link NullableVarCharHolder} from the provided {@code string} using the provided {@code buf}.
   *
   * @param string the {@link StandardCharsets#UTF_8}-encoded text input to store onto the holder.
   * @param buf    the {@link ArrowBuf} from which to create the new holder.
   * @return a new {@link NullableVarCharHolder} with the provided input data {@code string}.
   */
  public static NullableVarCharHolder getHolderForUtf8(final String string, final ArrowBuf buf) {
    final byte[] bytes = string.getBytes(UTF_8);
    buf.setBytes(0, bytes);
    final NullableVarCharHolder holder = new NullableVarCharHolder();
    holder.buffer = buf;
    holder.end = bytes.length;
    holder.isSet = 1;
    return holder;
  }

  /**
   * Sets a value for {@link SqlInfo#FLIGHT_SQL_SERVER_NAME} in the builder.
   *
   * @param value the value for {@link SqlInfo#FLIGHT_SQL_SERVER_NAME} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withFlightSqlServerName(final String value) {
    return withStringProvider(SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#FLIGHT_SQL_SERVER_VERSION} in the builder.
   *
   * @param value the value for {@link SqlInfo#FLIGHT_SQL_SERVER_VERSION} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withFlightSqlServerVersion(final String value) {
    return withStringProvider(SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#FLIGHT_SQL_SERVER_ARROW_VERSION} in the builder.
   *
   * @param value the value for {@link SqlInfo#FLIGHT_SQL_SERVER_ARROW_VERSION} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withFlightSqlServerArrowVersion(final String value) {
    return withStringProvider(SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE, value);
  }

  /** Set a value for SQL support. */
  public SqlInfoBuilder withFlightSqlServerSql(boolean value) {
    return withBooleanProvider(SqlInfo.FLIGHT_SQL_SERVER_SQL_VALUE, value);
  }

  /** Set a value for Substrait support. */
  public SqlInfoBuilder withFlightSqlServerSubstrait(boolean value) {
    return withBooleanProvider(SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_VALUE, value);
  }

  /** Set a value for Substrait minimum version support. */
  public SqlInfoBuilder withFlightSqlServerSubstraitMinVersion(String value) {
    return withStringProvider(SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION_VALUE, value);
  }

  /** Set a value for Substrait maximum version support. */
  public SqlInfoBuilder withFlightSqlServerSubstraitMaxVersion(String value) {
    return withStringProvider(SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION_VALUE, value);
  }

  /** Set a value for transaction support. */
  public SqlInfoBuilder withFlightSqlServerTransaction(SqlSupportedTransaction value) {
    return withIntProvider(SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_VALUE, value.getNumber());
  }

  /** Set a value for query cancellation support. */
  public SqlInfoBuilder withFlightSqlServerCancel(boolean value) {
    return withBooleanProvider(SqlInfo.FLIGHT_SQL_SERVER_CANCEL_VALUE, value);
  }

  /** Set a value for statement timeouts. */
  public SqlInfoBuilder withFlightSqlServerStatementTimeout(int value) {
    return withIntProvider(SqlInfo.FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT_VALUE, value);
  }

  /** Set a value for transaction timeouts. */
  public SqlInfoBuilder withFlightSqlServerTransactionTimeout(int value) {
    return withIntProvider(SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_IDENTIFIER_QUOTE_CHAR} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_IDENTIFIER_QUOTE_CHAR} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlIdentifierQuoteChar(final String value) {
    return withStringProvider(SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SEARCH_STRING_ESCAPE} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SEARCH_STRING_ESCAPE} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSearchStringEscape(final String value) {
    return withStringProvider(SqlInfo.SQL_SEARCH_STRING_ESCAPE_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_EXTRA_NAME_CHARACTERS} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_EXTRA_NAME_CHARACTERS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlExtraNameCharacters(final String value) {
    return withStringProvider(SqlInfo.SQL_EXTRA_NAME_CHARACTERS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SCHEMA_TERM} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SCHEMA_TERM} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSchemaTerm(final String value) {
    return withStringProvider(SqlInfo.SQL_SCHEMA_TERM_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_CATALOG_TERM} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_CATALOG_TERM} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlCatalogTerm(final String value) {
    return withStringProvider(SqlInfo.SQL_CATALOG_TERM_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_PROCEDURE_TERM} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_PROCEDURE_TERM} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlProcedureTerm(final String value) {
    return withStringProvider(SqlInfo.SQL_PROCEDURE_TERM_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DDL_CATALOG} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DDL_CATALOG} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDdlCatalog(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_DDL_CATALOG_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DDL_SCHEMA} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DDL_SCHEMA} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDdlSchema(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_DDL_SCHEMA_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DDL_TABLE} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DDL_TABLE} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDdlTable(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_DDL_TABLE_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#FLIGHT_SQL_SERVER_READ_ONLY} in the builder.
   *
   * @param value the value for {@link SqlInfo#FLIGHT_SQL_SERVER_READ_ONLY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withFlightSqlServerReadOnly(final boolean value) {
    return withBooleanProvider(SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_COLUMN_ALIASING} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_COLUMN_ALIASING} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsColumnAliasing(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_COLUMN_ALIASING_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_NULL_PLUS_NULL_IS_NULL} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_NULL_PLUS_NULL_IS_NULL} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlNullPlusNullIsNull(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_NULL_PLUS_NULL_IS_NULL_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_TABLE_CORRELATION_NAMES} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_TABLE_CORRELATION_NAMES} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsTableCorrelationNames(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_TABLE_CORRELATION_NAMES_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsDifferentTableCorrelationNames(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsExpressionsInOrderBy(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_ORDER_BY_UNRELATED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_ORDER_BY_UNRELATED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsOrderByUnrelated(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_ORDER_BY_UNRELATED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsLikeEscapeClause(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_NON_NULLABLE_COLUMNS} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_NON_NULLABLE_COLUMNS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsNonNullableColumns(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_NON_NULLABLE_COLUMNS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsIntegrityEnhancementFacility(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_CATALOG_AT_START} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_CATALOG_AT_START} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlCatalogAtStart(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_CATALOG_AT_START_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SELECT_FOR_UPDATE_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SELECT_FOR_UPDATE_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSelectForUpdateSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SELECT_FOR_UPDATE_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_STORED_PROCEDURES_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_STORED_PROCEDURES_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlStoredProceduresSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_STORED_PROCEDURES_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_CORRELATED_SUBQUERIES_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_CORRELATED_SUBQUERIES_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlCorrelatedSubqueriesSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_ROW_SIZE_INCLUDES_BLOBS} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_ROW_SIZE_INCLUDES_BLOBS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxRowSizeIncludesBlobs(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_MAX_ROW_SIZE_INCLUDES_BLOBS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_TRANSACTIONS_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_TRANSACTIONS_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlTransactionsSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_TRANSACTIONS_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDataDefinitionCausesTransactionCommit(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT_VALUE,
        value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDataDefinitionsInTransactionsIgnored(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED_VALUE,
        value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_BATCH_UPDATES_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_BATCH_UPDATES_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlBatchUpdatesSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_BATCH_UPDATES_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for { @link SqlInfo#SQL_SAVEPOINTS_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_SAVEPOINTS_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSavepointsSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_SAVEPOINTS_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_NAMED_PARAMETERS_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_NAMED_PARAMETERS_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlNamedParametersSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_NAMED_PARAMETERS_SUPPORTED_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_LOCATORS_UPDATE_COPY} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_LOCATORS_UPDATE_COPY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlLocatorsUpdateCopy(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_LOCATORS_UPDATE_COPY_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlStoredFunctionsUsingCallSyntaxSupported(final boolean value) {
    return withBooleanProvider(SqlInfo.SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED_VALUE,
        value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_IDENTIFIER_CASE} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_IDENTIFIER_CASE} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlIdentifierCase(final SqlSupportedCaseSensitivity value) {
    return withBitIntProvider(SqlInfo.SQL_IDENTIFIER_CASE_VALUE, value.getNumber());
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_QUOTED_IDENTIFIER_CASE} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_QUOTED_IDENTIFIER_CASE} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlQuotedIdentifierCase(final SqlSupportedCaseSensitivity value) {
    return withBitIntProvider(SqlInfo.SQL_QUOTED_IDENTIFIER_CASE_VALUE, value.getNumber());
  }

  /**
   * Sets a value SqlInf @link SqlInfo#SQL_MAX_BINARY_LITERAL_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_BINARY_LITERAL_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxBinaryLiteralLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_BINARY_LITERAL_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_CHAR_LITERAL_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_CHAR_LITERAL_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxCharLiteralLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_CHAR_LITERAL_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_COLUMN_NAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_COLUMN_NAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxColumnNameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_COLUMN_NAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_GROUP_BY} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_GROUP_BY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxColumnsInGroupBy(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_GROUP_BY_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_INDEX} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_INDEX} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxColumnsInIndex(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_INDEX_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_ORDER_BY} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_ORDER_BY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxColumnsInOrderBy(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_ORDER_BY_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_SELECT} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_COLUMNS_IN_SELECT} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxColumnsInSelect(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_SELECT_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_CONNECTIONS} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_CONNECTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxConnections(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_CONNECTIONS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_CURSOR_NAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_CURSOR_NAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxCursorNameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_CURSOR_NAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_INDEX_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_INDEX_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxIndexLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_INDEX_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DB_SCHEMA_NAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DB_SCHEMA_NAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDbSchemaNameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_DB_SCHEMA_NAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_PROCEDURE_NAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_PROCEDURE_NAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxProcedureNameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_PROCEDURE_NAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_CATALOG_NAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_CATALOG_NAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxCatalogNameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_CATALOG_NAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_ROW_SIZE} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_ROW_SIZE} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxRowSize(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_ROW_SIZE_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_STATEMENT_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_STATEMENT_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxStatementLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_STATEMENT_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_STATEMENTS} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_STATEMENTS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxStatements(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_STATEMENTS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_TABLE_NAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_TABLE_NAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxTableNameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_TABLE_NAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_TABLES_IN_SELECT} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_TABLES_IN_SELECT} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxTablesInSelect(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_TABLES_IN_SELECT_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_MAX_USERNAME_LENGTH} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_MAX_USERNAME_LENGTH} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlMaxUsernameLength(final long value) {
    return withBitIntProvider(SqlInfo.SQL_MAX_USERNAME_LENGTH_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DEFAULT_TRANSACTION_ISOLATION} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_DEFAULT_TRANSACTION_ISOLATION} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDefaultTransactionIsolation(final long value) {
    return withBitIntProvider(SqlInfo.SQL_DEFAULT_TRANSACTION_ISOLATION_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_GROUP_BY} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_SUPPORTED_GROUP_BY} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportedGroupBy(final SqlSupportedGroupBy... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_GROUP_BY_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_GRAMMAR} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_SUPPORTED_GRAMMAR} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportedGrammar(final SupportedSqlGrammar... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_GRAMMAR_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_ANSI92_SUPPORTED_LEVEL} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_ANSI92_SUPPORTED_LEVEL} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlAnsi92SupportedLevel(final SupportedAnsi92SqlGrammarLevel... values) {
    return withEnumProvider(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SCHEMAS_SUPPORTED_ACTIONS} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_SCHEMAS_SUPPORTED_ACTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSchemasSupportedActions(final SqlSupportedElementActions... values) {
    return withEnumProvider(SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_CATALOGS_SUPPORTED_ACTIONS} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_CATALOGS_SUPPORTED_ACTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlCatalogsSupportedActions(final SqlSupportedElementActions... values) {
    return withEnumProvider(SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_POSITIONED_COMMANDS} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_SUPPORTED_POSITIONED_COMMANDS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportedPositionedCommands(final SqlSupportedPositionedCommands... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_SUBQUERIES} in the builder.
   *
   * @param values the value for {@link SqlInfo#SQL_SUPPORTED_SUBQUERIES} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSubQueriesSupported(final SqlSupportedSubqueries... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_SUBQUERIES_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_UNIONS} in the builder.
   *
   * @param values the values for {@link SqlInfo#SQL_SUPPORTED_UNIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportedUnions(final SqlSupportedUnions... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_UNIONS_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_OUTER_JOINS_SUPPORT_LEVEL} in the builder.
   *
   * @param value the value for {@link SqlInfo#SQL_OUTER_JOINS_SUPPORT_LEVEL} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlOuterJoinSupportLevel(final SqlOuterJoinsSupportLevel... value) {
    return withEnumProvider(SqlInfo.SQL_OUTER_JOINS_SUPPORT_LEVEL_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS} in the builder.
   *
   * @param values the values for {@link SqlInfo#SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportedTransactionsIsolationLevels(final SqlTransactionIsolationLevel... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS_VALUE, values);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTED_RESULT_SET_TYPES} in the builder.
   *
   * @param values the values for {@link SqlInfo#SQL_SUPPORTED_RESULT_SET_TYPES} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportedResultSetTypes(final SqlSupportedResultSetType... values) {
    return withEnumProvider(SqlInfo.SQL_SUPPORTED_RESULT_SET_TYPES_VALUE, values
    );
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_KEYWORDS} in the builder.
   *
   * @param value the values for {@link SqlInfo#SQL_KEYWORDS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlKeywords(final String[] value) {
    return withStringArrayProvider(SqlInfo.SQL_KEYWORDS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_NUMERIC_FUNCTIONS} in the builder.
   *
   * @param value the values for {@link SqlInfo#SQL_NUMERIC_FUNCTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlNumericFunctions(final String[] value) {
    return withStringArrayProvider(SqlInfo.SQL_NUMERIC_FUNCTIONS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_STRING_FUNCTIONS} in the builder.
   *
   * @param value the values for {@link SqlInfo#SQL_STRING_FUNCTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlStringFunctions(final String[] value) {
    return withStringArrayProvider(SqlInfo.SQL_STRING_FUNCTIONS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SYSTEM_FUNCTIONS} in the builder.
   *
   * @param value the values for {@link SqlInfo#SQL_SYSTEM_FUNCTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSystemFunctions(final String[] value) {
    return withStringArrayProvider(SqlInfo.SQL_SYSTEM_FUNCTIONS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_DATETIME_FUNCTIONS} in the builder.
   *
   * @param value the values for {@link SqlInfo#SQL_DATETIME_FUNCTIONS} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlDatetimeFunctions(final String[] value) {
    return withStringArrayProvider(SqlInfo.SQL_DATETIME_FUNCTIONS_VALUE, value);
  }

  /**
   * Sets a value for {@link SqlInfo#SQL_SUPPORTS_CONVERT} in the builder.
   *
   * @param value the values for {@link SqlInfo#SQL_SUPPORTS_CONVERT} to be set.
   * @return the SqlInfoBuilder itself.
   */
  public SqlInfoBuilder withSqlSupportsConvert(final Map<Integer, List<Integer>> value) {
    return withIntToIntListMapProvider(SqlInfo.SQL_SUPPORTS_CONVERT_VALUE, value);
  }

  private void addProvider(final int sqlInfo, final ObjIntConsumer<VectorSchemaRoot> provider) {
    providers.put(sqlInfo, provider);
  }

  private SqlInfoBuilder withEnumProvider(final int sqlInfo, final ProtocolMessageEnum[] values) {
    return withIntProvider(sqlInfo, (int) createBitmaskFromEnums(values));
  }

  private SqlInfoBuilder withIntProvider(final int sqlInfo, final int value) {
    addProvider(sqlInfo, (root, index) -> setDataForIntField(root, index, sqlInfo, value));
    return this;
  }

  private SqlInfoBuilder withBitIntProvider(final int sqlInfo, final long value) {
    addProvider(sqlInfo, (root, index) -> setDataForBigIntField(root, index, sqlInfo, value));
    return this;
  }

  private SqlInfoBuilder withBooleanProvider(final int sqlInfo,
                                             final boolean value) {
    addProvider(sqlInfo, (root, index) -> setDataForBooleanField(root, index, sqlInfo, value));
    return this;
  }

  private SqlInfoBuilder withStringProvider(final int sqlInfo, final String value) {
    addProvider(sqlInfo, (root, index) -> setDataForUtf8Field(root, index, sqlInfo, value));
    return this;
  }

  private SqlInfoBuilder withStringArrayProvider(final int sqlInfo,
                                                 final String[] value) {
    addProvider(sqlInfo, (root, index) -> setDataVarCharListField(root, index, sqlInfo, value));
    return this;
  }

  private SqlInfoBuilder withIntToIntListMapProvider(final int sqlInfo,
                                                     final Map<Integer, List<Integer>> value) {
    addProvider(sqlInfo, (root, index) -> setIntToIntListMapField(root, index, sqlInfo, value));
    return this;
  }

  /**
   * Send the requested information to given ServerStreamListener.
   *
   * @param infos    List of SqlInfo to be sent.
   * @param listener ServerStreamListener to send data to.
   */
  public void send(List<Integer> infos, final ServerStreamListener listener) {
    if (infos == null || infos.isEmpty()) {
      infos = new ArrayList<>(providers.keySet());
    }
    try (final BufferAllocator allocator = new RootAllocator();
         final VectorSchemaRoot root = VectorSchemaRoot.create(
             FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA,
             allocator)) {
      final int rows = infos.size();
      for (int i = 0; i < rows; i++) {
        providers.get(infos.get(i)).accept(root, i);
      }
      root.setRowCount(rows);
      listener.start(root);
      listener.putNext();
    } catch (final Throwable throwable) {
      listener.error(throwable);
    } finally {
      listener.completed();
    }
  }

  private void setInfoName(final VectorSchemaRoot root, final int index, final int info) {
    final UInt4Vector infoName = (UInt4Vector) root.getVector("info_name");
    infoName.setSafe(index, info);
  }

  private void setValues(final VectorSchemaRoot root, final int index, final byte typeId,
                         final Consumer<DenseUnionVector> dataSetter) {
    final DenseUnionVector values = (DenseUnionVector) root.getVector("value");
    values.setTypeId(index, typeId);
    dataSetter.accept(values);
  }

  /**
   * Executes the given action on an ad-hoc, newly created instance of {@link ArrowBuf}.
   *
   * @param executor the action to take.
   */
  private void onCreateArrowBuf(final Consumer<ArrowBuf> executor) {
    try (final BufferAllocator allocator = new RootAllocator();
         final ArrowBuf buf = allocator.buffer(1024)) {
      executor.accept(buf);
    }
  }

  private void setDataForUtf8Field(final VectorSchemaRoot root, final int index,
                                   final int sqlInfo, final String value) {
    setInfoName(root, index, sqlInfo);
    onCreateArrowBuf(buf -> {
      final Consumer<DenseUnionVector> producer =
          values -> values.setSafe(index, getHolderForUtf8(value, buf));
      setValues(root, index, (byte) 0, producer);
    });
  }

  private void setDataForIntField(final VectorSchemaRoot root, final int index,
                                  final int sqlInfo, final int value) {
    setInfoName(root, index, sqlInfo);
    final NullableIntHolder dataHolder = new NullableIntHolder();
    dataHolder.isSet = 1;
    dataHolder.value = value;
    setValues(root, index, (byte) 3, values -> values.setSafe(index, dataHolder));
  }

  private void setDataForBigIntField(final VectorSchemaRoot root, final int index,
                                     final int sqlInfo, final long value) {
    setInfoName(root, index, sqlInfo);
    final NullableBigIntHolder dataHolder = new NullableBigIntHolder();
    dataHolder.isSet = 1;
    dataHolder.value = value;
    setValues(root, index, (byte) 2, values -> values.setSafe(index, dataHolder));
  }

  private void setDataForBooleanField(final VectorSchemaRoot root, final int index,
                                      final int sqlInfo, final boolean value) {
    setInfoName(root, index, sqlInfo);
    final NullableBitHolder dataHolder = new NullableBitHolder();
    dataHolder.isSet = 1;
    dataHolder.value = value ? 1 : 0;
    setValues(root, index, (byte) 1, values -> values.setSafe(index, dataHolder));
  }

  private void setDataVarCharListField(final VectorSchemaRoot root, final int index,
                                       final int sqlInfo,
                                       final String[] values) {
    final DenseUnionVector denseUnion = (DenseUnionVector) root.getVector("value");
    final ListVector listVector = denseUnion.getList((byte) 4);
    final int listIndex = listVector.getValueCount();
    final int denseUnionValueCount = index + 1;
    final int listVectorValueCount = listIndex + 1;
    denseUnion.setValueCount(denseUnionValueCount);
    listVector.setValueCount(listVectorValueCount);

    final UnionListWriter writer = listVector.getWriter();
    writer.setPosition(listIndex);
    writer.startList();
    final int length = values.length;
    range(0, length)
        .forEach(i -> onCreateArrowBuf(buf -> {
          final byte[] bytes = values[i].getBytes(UTF_8);
          buf.setBytes(0, bytes);
          writer.writeVarChar(0, bytes.length, buf);
        }));
    writer.endList();
    writer.setValueCount(listVectorValueCount);

    denseUnion.setTypeId(index, (byte) 4);
    denseUnion.getOffsetBuffer().setInt(index * 4L, listIndex);
    setInfoName(root, index, sqlInfo);
  }

  private void setIntToIntListMapField(final VectorSchemaRoot root, final int index,
                                       final int sqlInfo,
                                       final Map<Integer, List<Integer>> values) {
    final DenseUnionVector denseUnion = (DenseUnionVector) root.getVector("value");
    final MapVector mapVector = denseUnion.getMap((byte) 5);
    final int mapIndex = mapVector.getValueCount();
    denseUnion.setValueCount(index + 1);
    mapVector.setValueCount(mapIndex + 1);

    final UnionMapWriter mapWriter = mapVector.getWriter();
    mapWriter.setPosition(mapIndex);
    mapWriter.startMap();
    values.forEach((key, value) -> {
      mapWriter.startEntry();
      mapWriter.key().integer().writeInt(key);
      final BaseWriter.ListWriter listWriter = mapWriter.value().list();
      listWriter.startList();
      for (final int v : value) {
        listWriter.integer().writeInt(v);
      }
      listWriter.endList();
      mapWriter.endEntry();
    });
    mapWriter.endMap();
    mapWriter.setValueCount(mapIndex + 1);

    denseUnion.setTypeId(index, (byte) 5);
    denseUnion.getOffsetBuffer().setInt(index * 4L, mapIndex);
    setInfoName(root, index, sqlInfo);
  }
}
