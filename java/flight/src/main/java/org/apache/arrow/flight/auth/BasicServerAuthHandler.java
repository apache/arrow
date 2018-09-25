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

package org.apache.arrow.flight.auth;

import java.util.Iterator;

import org.apache.arrow.flight.impl.Flight.BasicAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A ServerAuthHandler for username/password authentication.
 */
public class BasicServerAuthHandler implements ServerAuthHandler {

  private static final Logger logger = LoggerFactory.getLogger(BasicServerAuthHandler.class);
  private final BasicAuthValidator authValidator;

  public BasicServerAuthHandler(BasicAuthValidator authValidator) {
    super();
    this.authValidator = authValidator;
  }

  public interface BasicAuthValidator {

    public byte[] getToken(String username, String password) throws Exception;

    public boolean isValid(byte[] token);

  }

  @Override
  public boolean authenticate(ServerAuthSender outgoing, Iterator<byte[]> incoming) {
    byte[] bytes = incoming.next();
    try {
      BasicAuth auth = BasicAuth.parseFrom(bytes);
      byte[] token = authValidator.getToken(auth.getUsername(), auth.getPassword());
      outgoing.send(token);
      return true;
    } catch (InvalidProtocolBufferException e) {
      logger.debug("Failure parsing auth message.", e);
    } catch (Exception e) {
      logger.debug("Unknown error during authorization.", e);
    }

    return false;
  }

  @Override
  public boolean isValid(byte[] token) {
    return authValidator.isValid(token);
  }

}
