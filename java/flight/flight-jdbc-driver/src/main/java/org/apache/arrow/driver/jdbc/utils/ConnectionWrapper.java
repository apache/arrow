package org.apache.arrow.driver.jdbc.utils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ConnectionWrapper implements Connection {
  private final Connection wrappedConnection;

  public ConnectionWrapper(Connection connection) {
    wrappedConnection = connection;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    return wrappedConnection.unwrap(aClass);
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return wrappedConnection.isWrapperFor(aClass);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return wrappedConnection.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String s) throws SQLException {
    return wrappedConnection.prepareStatement(s);
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    return wrappedConnection.prepareCall(s);
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    return wrappedConnection.nativeSQL(s);
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {
    wrappedConnection.setAutoCommit(b);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return wrappedConnection.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    wrappedConnection.commit();
  }

  @Override
  public void rollback() throws SQLException {
    wrappedConnection.rollback();
  }

  @Override
  public void close() throws SQLException {
    wrappedConnection.close();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return wrappedConnection.isClosed();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return wrappedConnection.getMetaData();
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {
    wrappedConnection.setReadOnly(b);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return wrappedConnection.isReadOnly();
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    wrappedConnection.setCatalog(s);
  }

  @Override
  public String getCatalog() throws SQLException {
    return wrappedConnection.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {
    wrappedConnection.setTransactionIsolation(i);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return wrappedConnection.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return wrappedConnection.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    wrappedConnection.clearWarnings();
  }

  @Override
  public Statement createStatement(int i, int i1) throws SQLException {
    return wrappedConnection.createStatement(i, i1);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
    return wrappedConnection.prepareStatement(s, i, i1);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
    return wrappedConnection.prepareCall(s, i, i1);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return wrappedConnection.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    wrappedConnection.setTypeMap(map);
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    wrappedConnection.setHoldability(i);
  }

  @Override
  public int getHoldability() throws SQLException {
    return wrappedConnection.getHoldability();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return wrappedConnection.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    return wrappedConnection.setSavepoint(s);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    wrappedConnection.rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    wrappedConnection.releaseSavepoint(savepoint);
  }

  @Override
  public Statement createStatement(int i, int i1, int i2) throws SQLException {
    return wrappedConnection.createStatement(i, i1, i2);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
    return wrappedConnection.prepareStatement(s, i, i1, i2);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
    return wrappedConnection.prepareCall(s, i, i1, i2);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {
    return wrappedConnection.prepareStatement(s, i);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    return wrappedConnection.prepareStatement(s, ints);
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    return wrappedConnection.prepareStatement(s, strings);
  }

  @Override
  public Clob createClob() throws SQLException {
    return wrappedConnection.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return wrappedConnection.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return wrappedConnection.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return wrappedConnection.createSQLXML();
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    return wrappedConnection.isValid(i);
  }

  @Override
  public void setClientInfo(String s, String s1) throws SQLClientInfoException {
    wrappedConnection.setClientInfo(s, s1);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    wrappedConnection.setClientInfo(properties);
  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    return wrappedConnection.getClientInfo(s);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return wrappedConnection.getClientInfo();
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    return wrappedConnection.createArrayOf(s, objects);
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    return wrappedConnection.createStruct(s, objects);
  }

  @Override
  public void setSchema(String s) throws SQLException {
    wrappedConnection.setSchema(s);
  }

  @Override
  public String getSchema() throws SQLException {
    return wrappedConnection.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    wrappedConnection.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int i) throws SQLException {
    wrappedConnection.setNetworkTimeout(executor, i);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return wrappedConnection.getNetworkTimeout();
  }
}
