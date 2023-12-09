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

/** Abstract factory for concrete SessionOptionValue instances. */
public class SessionOptionValueFactory {
  public static SessionOptionValue makeSessionOptionValue(String value) {
    return new SessionOptionValueString(value);
  }

  public static SessionOptionValue makeSessionOptionValue(bool value) {
    return new SessionOptionValueBoolean(value);
  }

  public static SessionOptionValue makeSessionOptionValue(int value) {
    return new SessionOptionValueInt(value);
  }

  public static SessionOptionValue makeSessionOptionValue(long value) {
    return new SessionOptionValueLong(value);
  }

  public static SessionOptionValue makeSessionOptionValue(float value) {
    return new SessionOptionValueFloat(value);
  }

  public static SessionOptionValue makeSessionOptionValue(double value) {
    return new SessionOptionValueDouble(value);
  }

  public static SessionOptionValue makeSessionOptionValue(String[] value) {
    return new SessionOptionValueStringList(value);
  }

  public static SessionOptionValue makeSessionOptionValue(Flight.SessionOptionValue proto) {
    switch (proto.getOptionValueCase()) {
      case STRING_VALUE:
        return new SessionOptionValueString(proto.getStringValue());
      case BOOL_VALUE:
        return new SessionOptionValueBoolean(proto.getValue());
      case INT32_VALUE:
        return new SessionOptionValueInteger(proto.getInt32Value());
      case INT64_VALUE:
        return new SessionOptionValueLong(proto.getInt64Value());
      case FLOAT_VALUE:
        return new SessionOptionValueFloat(proto.getFloatValue());
      case DOUBLE_VALUE:
        return new SessionOptionValueDouble(proto.getDoubleValue());
      case STRING_LIST_VALUE:
        // FIXME PHOXME is this what's in the ProtocolStringList?
        return new SessionOptionValueStringList(proto.getValueStringList().stream().collect(
        Collectors.toList(e -> google.protocol.StringValue.parseFrom(e).getValue())));
      default:
        // Unreachable
        throw new IllegalArgumentException("");
    }
  }

  class SessionOptionValueString extends SessionOptionvalue {
    private final String value;

    SessionOptionValue(String value) {
      this.value = value;
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }

  class SessionOptionValueBoolean extends SessionOptionvalue {
    private final boolean value;

    SessionOptionValueBoolean(boolean value) {
      this.value = value;
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }

  class SessionOptionValueInt extends SessionOptionvalue {
    private final int value;

    SessionOptionValueInt(int value) {
      this.value = value;
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }

  class SessionOptionValueLong extends SessionOptionvalue {
    private final long value;

    SessionOptionValueLong(long value) {
      this.value = value;
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }

  class SessionOptionValueFloat extends SessionOptionvalue {
    private final float value;

    SessionOptionValueFloat(Float value) {
      this.value = value;
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }

  class SessionOptionValueDouble extends SessionOptionvalue {
    private final double value;

    SessionOptionValueDouble(double value) {
      this.value = value;
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }

  class SessionOptionValueStringList extends SessionOptionvalue {
    private final String[] value;

    SessionOptionValueStringList(String[] value) {
      this.value = value.clone();
    }

    void acceptVisitor(SessionOptionValueVisitor v) {
      v.visit(value);
    }
  }
}
