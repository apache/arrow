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

package org.apache.arrow.flight;

import java.util.Arrays;

import org.apache.arrow.flight.impl.Flight;

/**
 * A union-like container interface for supported session option value types.
 */
public abstract class SessionOptionValue {
  /**
   * Value access via a caller-provided visitor/functor.
   */
  public abstract <T> T acceptVisitor(SessionOptionValueVisitor<T> v);

  Flight.SessionOptionValue toProtocol() {
    Flight.SessionOptionValue.Builder b = Flight.SessionOptionValue.newBuilder();
    SessionOptionValueToProtocolVisitor visitor = new SessionOptionValueToProtocolVisitor(b);
    this.acceptVisitor(visitor);
    return b.build();
  }

  private class SessionOptionValueToProtocolVisitor implements SessionOptionValueVisitor<Void> {
    final Flight.SessionOptionValue.Builder b;

    SessionOptionValueToProtocolVisitor(Flight.SessionOptionValue.Builder b) {
      this.b = b;
    }

    public Void visit(String value) {
      b.setStringValue(value);
      return null;
    }

    public Void visit(boolean value) {
      b.setBoolValue(value);
      return null;
    }

    public Void visit(int value) {
      b.setInt32Value(value);
      return null;
    }

    public Void visit(long value) {
      b.setInt64Value(value);
      return null;
    }

    public Void visit(float value) {
      b.setFloatValue(value);
      return null;
    }

    public Void visit(double value) {
      b.setDoubleValue(value);
      return null;
    }

    public Void visit(String[] value) {
      Flight.SessionOptionValue.StringListValue.Builder pbSLVBuilder =
          Flight.SessionOptionValue.StringListValue.newBuilder();
      pbSLVBuilder.addAllValues(Arrays.asList(value));
      b.setStringListValue(pbSLVBuilder.build());
      return null;
    }

    public Void visit(Void ignored) {
      // Proto SessionOptionValue builder is assumed to have its value unset.
      return null;
    }
  }
}
