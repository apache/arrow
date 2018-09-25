/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.ByteString;

public class Result {

  private final byte[] body;

  public Result(byte[] body) {
    this.body = body;
  }

  public Result(Flight.Result result) {
    this.body = result.getBody().toByteArray();
  }

  public byte[] getBody() {
    return body;
  }

  Flight.Result toProtocol() {
    return Flight.Result.newBuilder()
        .setBody(ByteString.copyFrom(body))
        .build();
  }
}
