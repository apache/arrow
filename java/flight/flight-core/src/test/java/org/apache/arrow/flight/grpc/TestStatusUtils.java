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

package org.apache.arrow.flight.grpc;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.Assert;
import org.junit.Test;

import io.grpc.Metadata;
import io.grpc.Status;

public class TestStatusUtils {

  @Test
  public void testParseTrailers() {
    Status status = Status.CANCELLED;
    Metadata trailers = new Metadata();

    // gRPC can have trailers with certain metadata keys beginning with ":", such as ":status".
    // See https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
    trailers.put(StatusUtils.keyOfAscii(":status"), "502");
    trailers.put(StatusUtils.keyOfAscii("date"), "Fri, 13 Sep 2015 11:23:58 GMT");
    trailers.put(StatusUtils.keyOfAscii("content-type"), "text/html");

    CallStatus callStatus = StatusUtils.fromGrpcStatusAndTrailers(status, trailers);

    Assert.assertEquals(FlightStatusCode.CANCELLED, callStatus.code());
    Assert.assertTrue(callStatus.metadata().containsKey(":status"));
    Assert.assertEquals("502", callStatus.metadata().get(":status"));
    Assert.assertTrue(callStatus.metadata().containsKey("date"));
    Assert.assertEquals("Fri, 13 Sep 2015 11:23:58 GMT", callStatus.metadata().get("date"));
    Assert.assertTrue(callStatus.metadata().containsKey("content-type"));
    Assert.assertEquals("text/html", callStatus.metadata().get("content-type"));
  }
}
