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

package org.apache.arrow.driver.jdbc.utils;

import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;

import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.AvaticaConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ConnectionWrapperTest {

  private static final String SCHEMA_NAME = "SCHEMA";
  private static final String PLACEHOLDER_QUERY = "SELECT * FROM DOES_NOT_MATTER";
  private static final int[] COLUMN_INDICES = range(0, 10).toArray();
  private static final String[] COLUMN_NAMES =
      Arrays.stream(COLUMN_INDICES).mapToObj(i -> format("col%d", i)).toArray(String[]::new);
  private static final String TYPE_NAME = "TYPE_NAME";
  private static final String SAVEPOINT_NAME = "SAVEPOINT";
  private static final String CLIENT_INFO = "CLIENT_INFO";
  private static final int RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
  private static final int RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
  private static final int RESULT_SET_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;
  private static final int GENERATED_KEYS = Statement.NO_GENERATED_KEYS;
  private static final Random RANDOM = new Random(Long.MAX_VALUE);
  private static final int TIMEOUT = RANDOM.nextInt(Integer.MAX_VALUE);

  @Mock
  public AvaticaConnection underlyingConnection;
  private ConnectionWrapper connectionWrapper;
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Before
  public void setUp() {
    connectionWrapper = new ConnectionWrapper(underlyingConnection);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(connectionWrapper, underlyingConnection);
  }

  @Test
  public void testUnwrappingUnderlyingConnectionShouldReturnUnderlyingConnection() {
    collector.checkThat(
        collector.checkSucceeds(() -> connectionWrapper.unwrap(Object.class)),
        is(sameInstance(underlyingConnection)));
    collector.checkThat(
        collector.checkSucceeds(() -> connectionWrapper.unwrap(Connection.class)),
        is(sameInstance(underlyingConnection)));
    collector.checkThat(
        collector.checkSucceeds(() -> connectionWrapper.unwrap(AvaticaConnection.class)),
        is(sameInstance(underlyingConnection)));
    ThrowableAssertionUtils.simpleAssertThrowableClass(ClassCastException.class,
        () -> connectionWrapper.unwrap(ArrowFlightConnection.class));
    ThrowableAssertionUtils.simpleAssertThrowableClass(ClassCastException.class,
        () -> connectionWrapper.unwrap(ConnectionWrapper.class));
  }

  @Test
  public void testCreateStatementShouldCreateStatementFromUnderlyingConnection()
      throws SQLException {
    collector.checkThat(
        connectionWrapper.createStatement(),
        is(sameInstance(verify(underlyingConnection, times(1)).createStatement())));
    collector.checkThat(
        connectionWrapper.createStatement(RESULT_SET_TYPE, RESULT_SET_CONCURRENCY,
            RESULT_SET_HOLDABILITY),
        is(verify(underlyingConnection, times(1))
            .createStatement(RESULT_SET_TYPE, RESULT_SET_CONCURRENCY, RESULT_SET_HOLDABILITY)));
    collector.checkThat(
        connectionWrapper.createStatement(RESULT_SET_TYPE, RESULT_SET_CONCURRENCY),
        is(verify(underlyingConnection, times(1))
            .createStatement(RESULT_SET_TYPE, RESULT_SET_CONCURRENCY)));
  }

  @Test
  public void testPrepareStatementShouldPrepareStatementFromUnderlyingConnection()
      throws SQLException {
    collector.checkThat(
        connectionWrapper.prepareStatement(PLACEHOLDER_QUERY),
        is(sameInstance(
            verify(underlyingConnection, times(1)).prepareStatement(PLACEHOLDER_QUERY))));
    collector.checkThat(
        connectionWrapper.prepareStatement(PLACEHOLDER_QUERY, COLUMN_INDICES),
        is(allOf(sameInstance(verify(underlyingConnection, times(1))
                .prepareStatement(PLACEHOLDER_QUERY, COLUMN_INDICES)),
            nullValue())));
    collector.checkThat(
        connectionWrapper.prepareStatement(PLACEHOLDER_QUERY, COLUMN_NAMES),
        is(allOf(sameInstance(verify(underlyingConnection, times(1))
                .prepareStatement(PLACEHOLDER_QUERY, COLUMN_NAMES)),
            nullValue())));
    collector.checkThat(
        connectionWrapper.prepareStatement(PLACEHOLDER_QUERY, RESULT_SET_TYPE,
            RESULT_SET_CONCURRENCY),
        is(allOf(sameInstance(verify(underlyingConnection, times(1))
                .prepareStatement(PLACEHOLDER_QUERY, RESULT_SET_TYPE, RESULT_SET_CONCURRENCY)),
            nullValue())));
    collector.checkThat(
        connectionWrapper.prepareStatement(PLACEHOLDER_QUERY, GENERATED_KEYS),
        is(allOf(sameInstance(verify(underlyingConnection, times(1))
                .prepareStatement(PLACEHOLDER_QUERY, GENERATED_KEYS)),
            nullValue())));
  }

  @Test
  public void testPrepareCallShouldPrepareCallFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.prepareCall(PLACEHOLDER_QUERY),
        is(sameInstance(
            verify(underlyingConnection, times(1)).prepareCall(PLACEHOLDER_QUERY))));
    collector.checkThat(
        connectionWrapper.prepareCall(PLACEHOLDER_QUERY, RESULT_SET_TYPE, RESULT_SET_CONCURRENCY),
        is(verify(underlyingConnection, times(1))
            .prepareCall(PLACEHOLDER_QUERY, RESULT_SET_TYPE, RESULT_SET_CONCURRENCY)));
  }

  @Test
  public void testNativeSqlShouldGetNativeSqlFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.nativeSQL(PLACEHOLDER_QUERY),
        is(sameInstance(
            verify(underlyingConnection, times(1)).nativeSQL(PLACEHOLDER_QUERY))));
  }

  @Test
  public void testSetAutoCommitShouldSetAutoCommitInUnderlyingConnection() throws SQLException {
    connectionWrapper.setAutoCommit(true);
    verify(underlyingConnection, times(1)).setAutoCommit(true);
    connectionWrapper.setAutoCommit(false);
    verify(underlyingConnection, times(1)).setAutoCommit(false);
  }

  @Test
  public void testGetAutoCommitShouldGetAutoCommitFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getAutoCommit(),
        is(verify(underlyingConnection, times(1)).getAutoCommit()));
  }

  @Test
  public void testCommitShouldCommitToUnderlyingConnection() throws SQLException {
    connectionWrapper.commit();
    verify(underlyingConnection, times(1)).commit();
  }

  @Test
  public void testRollbackShouldRollbackFromUnderlyingConnection() throws SQLException {
    connectionWrapper.rollback();
    verify(underlyingConnection, times(1)).rollback();
  }

  @Test
  public void testCloseShouldCloseUnderlyingConnection() throws SQLException {
    connectionWrapper.close();
    verify(underlyingConnection, times(1)).close();
  }

  @Test
  public void testIsClosedShouldGetStatusFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.isClosed(), is(verify(underlyingConnection, times(1)).isClosed()));
  }

  @Test
  public void testGetMetadataShouldGetMetadataFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getMetaData(), is(verify(underlyingConnection, times(1)).getMetaData()));
  }

  @Test
  public void testSetReadOnlyShouldSetUnderlyingConnectionAsReadOnly() throws SQLException {
    connectionWrapper.setReadOnly(false);
    verify(underlyingConnection, times(1)).setReadOnly(false);
    connectionWrapper.setReadOnly(true);
    verify(underlyingConnection, times(1)).setReadOnly(true);
  }

  @Test
  public void testSetIsReadOnlyShouldGetStatusFromUnderlyingConnection() throws SQLException {
    collector.checkThat(connectionWrapper.isReadOnly(),
        is(verify(underlyingConnection).isReadOnly()));
  }

  @Test
  public void testSetCatalogShouldSetCatalogInUnderlyingConnection() throws SQLException {
    final String catalog = "CATALOG";
    connectionWrapper.setCatalog(catalog);
    verify(underlyingConnection, times(1)).setCatalog(catalog);
  }

  @Test
  public void testGetCatalogShouldGetCatalogFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getCatalog(),
        is(allOf(sameInstance(verify(underlyingConnection, times(1)).getCatalog()), nullValue())));
  }

  @Test
  public void setTransactionIsolationShouldSetUnderlyingTransactionIsolation() throws SQLException {
    final int transactionIsolation = Connection.TRANSACTION_NONE;
    connectionWrapper.setTransactionIsolation(Connection.TRANSACTION_NONE);
    verify(underlyingConnection, times(1)).setTransactionIsolation(transactionIsolation);
  }

  @Test
  public void getTransactionIsolationShouldGetUnderlyingConnectionIsolation() throws SQLException {
    collector.checkThat(
        connectionWrapper.getTransactionIsolation(),
        is(equalTo(verify(underlyingConnection, times(1)).getTransactionIsolation())));
  }

  @Test
  public void getWarningShouldGetWarningsFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getWarnings(),
        is(allOf(
            sameInstance(verify(underlyingConnection, times(1)).getWarnings()),
            nullValue())));
  }

  @Test
  public void testClearWarningShouldClearWarningsFromUnderlyingConnection() throws SQLException {
    connectionWrapper.clearWarnings();
    verify(underlyingConnection, times(1)).clearWarnings();
  }

  @Test
  public void getTypeMapShouldGetTypeMapFromUnderlyingConnection() throws SQLException {
    when(underlyingConnection.getTypeMap()).thenReturn(null);
    collector.checkThat(
        connectionWrapper.getTypeMap(),
        is(verify(underlyingConnection, times(1)).getTypeMap()));
  }

  @Test
  public void testSetTypeMapShouldSetTypeMapFromUnderlyingConnection() throws SQLException {
    connectionWrapper.setTypeMap(null);
    verify(underlyingConnection, times(1)).setTypeMap(null);
  }

  @Test
  public void testSetHoldabilityShouldSetUnderlyingConnection() throws SQLException {
    connectionWrapper.setHoldability(RESULT_SET_HOLDABILITY);
    verify(underlyingConnection, times(1)).setHoldability(RESULT_SET_HOLDABILITY);
  }

  @Test
  public void testGetHoldabilityShouldGetHoldabilityFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getHoldability(),
        is(equalTo(verify(underlyingConnection, times(1)).getHoldability())));
  }

  @Test
  public void testSetSavepointShouldSetSavepointInUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.setSavepoint(),
        is(allOf(
            sameInstance(verify(underlyingConnection, times(1)).setSavepoint()),
            nullValue())));
    collector.checkThat(
        connectionWrapper.setSavepoint(SAVEPOINT_NAME),
        is(sameInstance(
            verify(underlyingConnection, times(1)).setSavepoint(SAVEPOINT_NAME))));
  }

  @Test
  public void testRollbackShouldRollbackInUnderlyingConnection() throws SQLException {
    connectionWrapper.rollback(null);
    verify(underlyingConnection, times(1)).rollback(null);
  }

  @Test
  public void testReleaseSavepointShouldReleaseSavepointFromUnderlyingConnection()
      throws SQLException {
    connectionWrapper.releaseSavepoint(null);
    verify(underlyingConnection, times(1)).releaseSavepoint(null);
  }

  @Test
  public void testCreateClobShouldCreateClobFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.createClob(),
        is(allOf(sameInstance(
            verify(underlyingConnection, times(1)).createClob()), nullValue())));
  }

  @Test
  public void testCreateBlobShouldCreateBlobFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.createBlob(),
        is(allOf(sameInstance(
            verify(underlyingConnection, times(1)).createBlob()), nullValue())));
  }

  @Test
  public void testCreateNClobShouldCreateNClobFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.createNClob(),
        is(allOf(sameInstance(
            verify(underlyingConnection, times(1)).createNClob()), nullValue())));
  }

  @Test
  public void testCreateSQLXMLShouldCreateSQLXMLFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.createSQLXML(),
        is(allOf(sameInstance(
            verify(underlyingConnection, times(1)).createSQLXML()), nullValue())));
  }

  @Test
  public void testIsValidShouldReturnWhetherUnderlyingConnectionIsValid() throws SQLException {
    collector.checkThat(
        connectionWrapper.isValid(TIMEOUT),
        is(verify(underlyingConnection, times(1)).isValid(TIMEOUT)));
  }

  @Test
  public void testSetClientInfoShouldSetClientInfoInUnderlyingConnection()
      throws SQLClientInfoException {
    connectionWrapper.setClientInfo(null);
    verify(underlyingConnection, times(1)).setClientInfo(null);
  }

  @Test
  public void testGetClientInfoShouldGetClientInfoFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getClientInfo(CLIENT_INFO),
        is(allOf(
            sameInstance(
                verify(underlyingConnection, times(1)).getClientInfo(CLIENT_INFO)),
            nullValue())));
    collector.checkThat(
        connectionWrapper.getClientInfo(),
        is(allOf(
            sameInstance(
                verify(underlyingConnection, times(1)).getClientInfo()),
            nullValue())));
  }

  @Test
  public void testCreateArrayOfShouldCreateArrayFromUnderlyingConnection() throws SQLException {
    final Object[] elements = range(0, 100).boxed().toArray();
    collector.checkThat(
        connectionWrapper.createArrayOf(TYPE_NAME, elements),
        is(allOf(
            sameInstance(
                verify(underlyingConnection, times(1)).createArrayOf(TYPE_NAME, elements)),
            nullValue())));
  }

  @Test
  public void testCreateStructShouldCreateStructFromUnderlyingConnection() throws SQLException {
    final Object[] attributes = range(0, 120).boxed().toArray();
    collector.checkThat(
        connectionWrapper.createStruct(TYPE_NAME, attributes),
        is(allOf(
            sameInstance(
                verify(underlyingConnection, times(1)).createStruct(TYPE_NAME, attributes)),
            nullValue())));
  }

  @Test
  public void testSetSchemaShouldSetSchemaInUnderlyingConnection() throws SQLException {
    connectionWrapper.setSchema(SCHEMA_NAME);
    verify(underlyingConnection, times(1)).setSchema(SCHEMA_NAME);
  }

  @Test
  public void testGetSchemaShouldGetSchemaFromUnderlyingConnection() throws SQLException {
    collector.checkThat(
        connectionWrapper.getSchema(),
        is(allOf(
            sameInstance(verify(underlyingConnection, times(1)).getSchema()),
            nullValue())));
  }

  @Test
  public void testAbortShouldAbortUnderlyingConnection() throws SQLException {
    connectionWrapper.abort(null);
    verify(underlyingConnection, times(1)).abort(null);
  }

  @Test
  public void testSetNetworkTimeoutShouldSetNetworkTimeoutInUnderlyingConnection()
      throws SQLException {
    connectionWrapper.setNetworkTimeout(null, TIMEOUT);
    verify(underlyingConnection, times(1)).setNetworkTimeout(null, TIMEOUT);
  }

  @Test
  public void testGetNetworkTimeoutShouldGetNetworkTimeoutFromUnderlyingConnection()
      throws SQLException {
    collector.checkThat(
        connectionWrapper.getNetworkTimeout(),
        is(equalTo(verify(underlyingConnection, times(1)).getNetworkTimeout())));
  }
}
