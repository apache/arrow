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

package org.apache.arrow.vector.validate;

/**
 * Status used for vector and visitor.
 */
public class Status {

  private boolean success;
  private String msg;

  private Status(boolean success) {
    this.success = success;
  }

  private Status(boolean success, String msg) {
    this.success = success;
    this.msg = msg;
  }

  public static Status success() {
    return new Status(true);
  }

  public static Status invalid(String msg) {
    return new Status(false, msg);
  }

  public boolean isSuccess() {
    return this.success;
  }

  public String getMessage() {
    return this.msg;
  }

  @Override
  public String toString() {
    return msg;
  }

}
