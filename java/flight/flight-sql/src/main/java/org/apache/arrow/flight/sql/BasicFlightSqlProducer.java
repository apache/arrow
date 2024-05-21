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

import java.util.List;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Message;

/**
 * A {@link FlightSqlProducer} that implements getting FlightInfo for each metadata request.
 */
public abstract class BasicFlightSqlProducer extends NoOpFlightSqlProducer {

  @Override
  public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context,
                                         FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                          FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, CallContext context,
                                          FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context,
                                         FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context,
                                        FlightDescriptor descriptor) {
    if (request.getIncludeSchema()) {
      return generateFlightInfo(request, descriptor, Schemas.GET_TABLES_SCHEMA);
    }
    return generateFlightInfo(request, descriptor, Schemas.GET_TABLES_SCHEMA_NO_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context,
                                            FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context,
                                             FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context,
                                              FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context,
                                              FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context,
                                                FlightDescriptor descriptor) {
    return generateFlightInfo(request, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
  }

  /**
   * Return a list of FlightEndpoints for the given request and FlightDescriptor. This method should validate that
   * the request is supported by this FlightSqlProducer.
   */
  protected abstract <T extends Message>
        List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor flightDescriptor, Schema schema);

  protected <T extends Message> FlightInfo generateFlightInfo(T request, FlightDescriptor descriptor, Schema schema) {
    final List<FlightEndpoint> endpoints = determineEndpoints(request, descriptor, schema);
    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }
}
