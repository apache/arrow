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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

public class FallbackFlightSqlProducer extends BasicFlightSqlProducer {
  private final VectorSchemaRoot data;

  public FallbackFlightSqlProducer(VectorSchemaRoot resultData) {
    this.data = resultData;
  }

  @Override
  protected <T extends Message> List<FlightEndpoint> determineEndpoints(
      T request, FlightDescriptor flightDescriptor, Schema schema) {
    return Collections.emptyList();
  }

  @Override
  public void createPreparedStatement(
      FlightSql.ActionCreatePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    final FlightSql.ActionCreatePreparedStatementResult.Builder resultBuilder =
        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
            .setPreparedStatementHandle(request.getQueryBytes());

    final ByteString datasetSchemaBytes =
        ByteString.copyFrom(data.getSchema().serializeAsMessage());

    resultBuilder.setDatasetSchema(datasetSchemaBytes);
    listener.onNext(new Result(Any.pack(resultBuilder.build()).toByteArray()));
    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(
      FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
    return getFlightInfo(descriptor, command.getQuery());
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
      FlightSql.CommandPreparedStatementQuery command,
      CallContext context,
      FlightDescriptor descriptor) {
    return getFlightInfo(descriptor, command.getPreparedStatementHandle().toStringUtf8());
  }

  @Override
  public void getStreamStatement(
      FlightSql.TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
    listener.start(data);
    listener.putNext();
    listener.completed();
  }

  @Override
  public void closePreparedStatement(
      FlightSql.ActionClosePreparedStatementRequest request,
      CallContext context,
      StreamListener<Result> listener) {
    listener.onCompleted();
  }

  private FlightInfo getFlightInfo(FlightDescriptor descriptor, String query) {
    final List<FlightEndpoint> endpoints;
    final Ticket ticket =
        new Ticket(Any.pack(FlightSql.TicketStatementQuery.getDefaultInstance()).toByteArray());
    if (query.equals("fallback")) {
      endpoints =
          Collections.singletonList(
              FlightEndpoint.builder(ticket, Location.reuseConnection()).build());
    } else if (query.equals("fallback with error")) {
      endpoints =
          Collections.singletonList(
              FlightEndpoint.builder(
                      ticket,
                      Location.forGrpcInsecure("localhost", 9999),
                      Location.reuseConnection())
                  .build());
    } else {
      throw CallStatus.UNIMPLEMENTED.withDescription(query).toRuntimeException();
    }
    return FlightInfo.builder(data.getSchema(), descriptor, endpoints).build();
  }
}
