package org.apache.arrow.driver.jdbc;

import org.apache.calcite.avatica.*;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class ArrowJdbc41Factory extends AbstractFactory {
  public ArrowJdbc41Factory() {
    this(4, 1);
  }

  protected ArrowJdbc41Factory(int major, int minor) {
    super(major, minor);
  }

  @Override
  ArrowFlightConnection newConnection(ArrowFlightJdbcDriver driver,
                                      AbstractFactory factory,
                                      String url,
                                      Properties info) throws SQLException {
    return new ArrowFlightConnection(driver, factory, url, info);
  }

  @Override
  public AvaticaStatement newStatement(AvaticaConnection avaticaConnection, Meta.StatementHandle statementHandle, int i, int i1, int i2) throws SQLException {
    return null;
  }

  @Override
  public ArrowFlightJdbc41PreparedStatement newPreparedStatement(AvaticaConnection connection,
                                                       Meta.StatementHandle statementHandle,
                                                       Meta.Signature signature,
                                                       int resultType,
                                                       int resultSetConcurrency,
                                                       int resultSetHoldability) throws SQLException {

    ArrowFlightConnection arrowFlightConnection = (ArrowFlightConnection) connection;

    return new ArrowFlightJdbc41PreparedStatement(arrowFlightConnection, statementHandle,
            signature, resultType, resultSetConcurrency, resultSetHoldability, null);
  }

  @Override
  public ArrowFlightResultSet newResultSet(AvaticaStatement statement,
                                       QueryState state,
                                       Meta.Signature signature,
                                       TimeZone timeZone,
                                       Meta.Frame frame) throws SQLException {
    final ResultSetMetaData metadata = newResultSetMetaData(statement, signature);
    return new ArrowFlightResultSet(statement, state, signature, metadata, timeZone, frame);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
    return new ArrowDatabaseMetadata(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement avaticaStatement, Meta.Signature signature) throws SQLException {
    return null;
  }

  @Override
  public int getJdbcMajorVersion() {
    return 0;
  }

  @Override
  public int getJdbcMinorVersion() {
    return 0;
  }

  private static class ArrowFlightJdbc41PreparedStatement extends ArrowFlightPreparedStatement {

    public ArrowFlightJdbc41PreparedStatement(AvaticaConnection connection,
                                              Meta.StatementHandle h,
                                              Meta.Signature signature,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability,
                                              PreparedStatement preparedStatement) throws SQLException {
      super(connection, h, signature, resultSetType,
              resultSetConcurrency, resultSetHoldability, preparedStatement);
    }
  }
}

