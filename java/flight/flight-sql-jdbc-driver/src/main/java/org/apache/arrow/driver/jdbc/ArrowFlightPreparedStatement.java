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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.flatbuffers.LongVector;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 * Arrow Flight JBCS's implementation {@link PreparedStatement}.
 */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement
    implements ArrowFlightInfoStatement {

  private final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement;

  private ArrowFlightPreparedStatement(final ArrowFlightConnection connection,
                                       final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement,
                                       final StatementHandle handle,
                                       final Signature signature, final int resultSetType,
                                       final int resultSetConcurrency,
                                       final int resultSetHoldability)
      throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.preparedStatement = Preconditions.checkNotNull(preparedStatement);
  }

  /**
   * Creates a new {@link ArrowFlightPreparedStatement} from the provided information.
   *
   * @param connection           the {@link Connection} to use.
   * @param statementHandle      the {@link StatementHandle} to use.
   * @param signature            the {@link Signature} to use.
   * @param resultSetType        the ResultSet type.
   * @param resultSetConcurrency the ResultSet concurrency.
   * @param resultSetHoldability the ResultSet holdability.
   * @return a new {@link PreparedStatement}.
   * @throws SQLException on error.
   */
  static ArrowFlightPreparedStatement createNewPreparedStatement(
      final ArrowFlightConnection connection,
      final StatementHandle statementHandle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability) throws SQLException {

    final ArrowFlightSqlClientHandler.PreparedStatement prepare = connection.getClientHandler().prepare(signature.sql);
    final Schema resultSetSchema = prepare.getDataSetSchema();
    final Schema parameterSchema = prepare.getParameterSchema();

    signature.columns.addAll(ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
    signature.parameters.addAll(ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields()));

    return new ArrowFlightPreparedStatement(
        connection, prepare, statementHandle,
        signature, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public synchronized void close() throws SQLException {
    this.preparedStatement.close();
    super.close();
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    copyParameters();
    return preparedStatement.executeUpdate();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    copyParameters();
    return preparedStatement.executeQuery();
  }

  private void copyParameters() throws SQLException {
    BufferAllocator allocator = new RootAllocator(1024 * 1024);
    List<FieldVector> fields = new ArrayList<>();
    List<TypedValue> values = this.getParameterValues();
    for(int i = 0; i < this.getParameterCount(); i++) {
      AvaticaParameter param = this.getParameter(i + 1);
      switch (param.parameterType) {
        case java.sql.Types.TINYINT:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.INTEGER:
          IntVector intVec = new IntVector(param.name, allocator);
          intVec.setSafe(0, (int)values.get(i).value);
          intVec.setValueCount(1);
          fields.add(intVec);
          break;
        case java.sql.Types.BIGINT:
          BigIntVector longVec = new BigIntVector(param.name, allocator);
          Object lv = values.get(i).value;
          if(lv instanceof Long){
            longVec.setSafe(0, (long)lv);
          } else {
            longVec.setSafe(0, (int)lv);
          }
          longVec.setValueCount(1);
          fields.add(longVec);
          break;
        case java.sql.Types.BIT:
        case java.sql.Types.BOOLEAN:
          BitVector bitVec = new BitVector(param.name, allocator);
          bitVec.setSafe(0, (int)values.get(i).value);
          bitVec.setValueCount(1);
          fields.add(bitVec);
          break;
        case java.sql.Types.FLOAT:
          Float4Vector floatVec = new Float4Vector(param.name, allocator);
          TypedValue tfVal = values.get(i);
          float floatVal = (float)tfVal.value;
          floatVec.setSafe(0, floatVal);
          floatVec.setValueCount(1);
          fields.add(floatVec);
          break;
        case java.sql.Types.DOUBLE:
          Float8Vector doubleVec = new Float8Vector(param.name, allocator);
          doubleVec.setSafe(0, (double)values.get(i).value);
          doubleVec.setValueCount(1);
          fields.add(doubleVec);
          break;
        case java.sql.Types.REAL:
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DECIMAL:
          DecimalVector decVec = new DecimalVector(param.name, allocator, param.precision, param.scale);
          decVec.setSafe(0, (BigDecimal) values.get(i).value);
          decVec.setValueCount(1);
          fields.add(decVec);
          break;
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.NCHAR:
        case java.sql.Types.NVARCHAR:
          Text txt = new Text((String) values.get(i).value);
          VarCharVector strVec = new VarCharVector(param.name, allocator);
          strVec.setSafe(0, txt);
          strVec.setValueCount(1);
          fields.add(strVec);
          break;
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.LONGNVARCHAR:
          LargeVarCharVector textVec = new LargeVarCharVector(param.name, allocator);
          textVec.setSafe(0, new Text((String) values.get(i).value));
          textVec.setValueCount(1);
          fields.add(textVec);
          break;
        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
        case java.sql.Types.TIME_WITH_TIMEZONE:
        case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
          DateMilliVector timeVec = new DateMilliVector(param.name, allocator);
          TypedValue tmVal = values.get(i);
          String dtStr = (String)tmVal.value;

          String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
          DateTime dateTime = DateTime.parse(dtStr, DateTimeFormat.forPattern(pattern));

          timeVec.setSafe(0, dateTime.getMillis());
          timeVec.setValueCount(1);
          fields.add(timeVec);
          break;
        case java.sql.Types.BINARY:
        case java.sql.Types.VARBINARY:
          VarBinaryVector binVec = new VarBinaryVector(param.name, allocator);
          binVec.setSafe(0, (byte[])values.get(i).value);
          binVec.setValueCount(1);
          fields.add(binVec);
          break;
        case java.sql.Types.BLOB:
        case java.sql.Types.LONGVARBINARY:
          LargeVarBinaryVector blobVec = new LargeVarBinaryVector(param.name, allocator);
          blobVec.setSafe(0, (byte[])values.get(i).value);
          blobVec.setValueCount(1);
          fields.add(blobVec);
          break;
        case java.sql.Types.NULL:
          NullVector nullVec = new NullVector(param.name);
          nullVec.setValueCount(1);
          fields.add(nullVec);
          break;
        default:
          throw new SQLException("Unknown type: " + param.typeName);
      }
    }
    VectorSchemaRoot parameters = new VectorSchemaRoot(fields);
    this.preparedStatement.setParameters(parameters);
  }
}
