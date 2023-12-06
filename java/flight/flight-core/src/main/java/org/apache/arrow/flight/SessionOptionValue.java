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

import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;

/**
 * A union-like container interface for supported session option value types.
 */
public abstract class SessionOptionValue {
    /**
     * Value access via a caller-provided visitor/functor.
     */
    abstract void acceptVisitor(SessionOptionValueVisitor);

    Flight.SessionOptionValue toProtocol() {
        Flight.SessionOptionValue.Builder b = Flight.SessionOptionValue.newBuilder();
        SessionOptionValueToProtocolVisitor visitor = new SessionOptionValueToProtocolVisitor(b);
        this.acceptVisitor(visitor);
        return b.build();
    }
}

class SessionOptionValueToProtocolVisitor implements SessionOptionValueVisitor {
    final Flight.SessionOptionValue.Builder b;

    SessionOptionValueVisitor(Flight.SessionOptionValue.Builder b) { this.b = b; }

    void visit(String value) {
        b.setStringValue(value);
    }

    void visit(boolean value) { b.setBoolValue(value); }

    void visit(int value) {
        b.setIn32Value(value);
    }

    void visit(long value) {
        b.setInt64Value(value);
    }

    void visit(float value) {
        b.setFloatValue(value);
    }

    void visit(double value) {
        b.setDoubleValue(value);
    }

    void visit(String[] value) {
        Flight.SessionOptionValue.StringListValue pbValue;
        pbValue.addAllStringValues(value);
        b.setValue(pbValue);
    }
}
