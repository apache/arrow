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

import java.util.Objects;

import org.apache.arrow.flight.sql.impl.FlightSqlExample.PreparedStatementHandle;
import org.apache.arrow.util.Preconditions;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

class PreparedStatementCacheKey {

  private final String uuid;
  private final String sql;

  PreparedStatementCacheKey(final String uuid, final String sql) {
    this.uuid = uuid;
    this.sql = sql;
  }

  String getUuid() {
    return uuid;
  }

  String getSql() {
    return sql;
  }

  ByteString toProtocol() {
    return Any.pack(org.apache.arrow.flight.sql.impl.FlightSqlExample.PreparedStatementHandle
            .newBuilder()
            .setSql(getSql())
            .setUuid(getUuid())
            .build())
            .toByteString();
  }

  static PreparedStatementCacheKey fromProtocol(ByteString byteString) throws InvalidProtocolBufferException {
    final Any parsed = Any.parseFrom(byteString);
    Preconditions.checkArgument(parsed.is(PreparedStatementHandle.class));

    final PreparedStatementHandle preparedStatementHandle = parsed.unpack(PreparedStatementHandle.class);
    return new PreparedStatementCacheKey(preparedStatementHandle.getUuid(), preparedStatementHandle.getSql());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PreparedStatementCacheKey)) {
      return false;
    }

    PreparedStatementCacheKey that = (PreparedStatementCacheKey) o;

    return Objects.equals(uuid, that.uuid) &&
            Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid, sql);
  }
}
