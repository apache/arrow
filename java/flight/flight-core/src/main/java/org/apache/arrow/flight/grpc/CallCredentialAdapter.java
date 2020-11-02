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

import java.util.concurrent.Executor;
import java.util.function.Consumer;

import org.apache.arrow.flight.CallHeaders;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

/**
 * Adapter class to utilize a CredentialWriter to implement Grpc CallCredentials.
 */
public class CallCredentialAdapter extends CallCredentials {

  private final Consumer<CallHeaders> credentialWriter;

  public CallCredentialAdapter(Consumer<CallHeaders> credentialWriter) {
    this.credentialWriter = credentialWriter;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
    executor.execute(() ->
    {
      final Metadata headers = new Metadata();
      credentialWriter.accept(new MetadataAdapter(headers));
      metadataApplier.apply(headers);
    });
  }

  @Override
  public void thisUsesUnstableApi() {
    // Mandatory to override this to acknowledge that CallCredentials is Experimental.
  }
}
