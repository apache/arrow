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

import static com.google.protobuf.Any.pack;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

public class PartitionedFlightSqlProducer extends BasicFlightSqlProducer {

  /**
   * A minimal FlightProducer intended to just serve data when given the correct Ticket.
   */
  public static class DataOnlyFlightSqlProducer extends NoOpFlightProducer {
    private final Ticket ticket;
    private final VectorSchemaRoot data;

    public DataOnlyFlightSqlProducer(Ticket ticket, VectorSchemaRoot data) {
      this.ticket = ticket;
      this.data = data;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      if (!Arrays.equals(ticket.getBytes(), this.ticket.getBytes())) {
        listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Illegal ticket.").toRuntimeException());
        return;
      }

      listener.start(data);
      listener.putNext();
      listener.completed();
    }
  }

  private final List<FlightEndpoint> endpoints;

  private final Schema schema;

  public PartitionedFlightSqlProducer(Schema schema, FlightEndpoint... endpoints) {
    this.schema = schema;
    this.endpoints = Arrays.asList(endpoints);
  }

  @Override
  protected <T extends Message> List<FlightEndpoint> determineEndpoints(
      T request, FlightDescriptor flightDescriptor, Schema schema) {
    return endpoints;
  }

  @Override
  public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
                                      CallContext context, StreamListener<Result> listener) {
    final FlightSql.ActionCreatePreparedStatementResult.Builder resultBuilder =
        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
            .setPreparedStatementHandle(ByteString.EMPTY);

    final ByteString datasetSchemaBytes = ByteString.copyFrom(schema.serializeAsMessage());

    resultBuilder.setDatasetSchema(datasetSchemaBytes);
    listener.onNext(new Result(pack(resultBuilder.build()).toByteArray()));
    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(
      FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    return FlightInfo.builder(schema, descriptor, endpoints).build();
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                                   CallContext context, FlightDescriptor descriptor) {
    return FlightInfo.builder(schema, descriptor, endpoints).build();
  }

  @Override
  public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
                                     CallContext context, StreamListener<Result> listener) {
    listener.onCompleted();
  }

  // Note -- getStream() is intentionally not implemented.
}
