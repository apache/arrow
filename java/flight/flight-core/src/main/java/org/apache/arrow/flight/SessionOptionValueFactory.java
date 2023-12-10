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

import java.io.IOException;

import org.apache.arrow.flight.impl.Flight;

//import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;

/** Abstract factory for concrete SessionOptionValue instances. */
public class SessionOptionValueFactory {
  public static SessionOptionValue makeSessionOptionValue(String value) {
    return new SessionOptionValueString(value);
  }

  public static SessionOptionValue makeSessionOptionValue(boolean value) {
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

  /** Construct a SessionOptionValue from its Protobuf object representation. */
  public static SessionOptionValue makeSessionOptionValue(Flight.SessionOptionValue proto) {
    switch (proto.getOptionValueCase()) {
      case STRING_VALUE:
        return new SessionOptionValueString(proto.getStringValue());
      case BOOL_VALUE:
        return new SessionOptionValueBoolean(proto.getBoolValue());
      case INT32_VALUE:
        return new SessionOptionValueInt(proto.getInt32Value());
      case INT64_VALUE:
        return new SessionOptionValueLong(proto.getInt64Value());
      case FLOAT_VALUE:
        return new SessionOptionValueFloat(proto.getFloatValue());
      case DOUBLE_VALUE:
        return new SessionOptionValueDouble(proto.getDoubleValue());
      case STRING_LIST_VALUE:
        return new SessionOptionValueStringList(proto.getStringListValue().getValuesList().asByteStringList().stream()
          .map((e) -> {
            try {
              return StringValue.parseFrom(e).getValue();
            } catch (IOException ignore) {
              // Unreachable, notwithstanding Proto bugs
              return null;
            }
          }).toArray(String[]::new));
      default:
        // Unreachable
        throw new IllegalArgumentException("");
    }
  }

  private static class SessionOptionValueString extends SessionOptionValue {
    private final String value;

    SessionOptionValueString(String value) {
      this.value = value;
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }

  private static class SessionOptionValueBoolean extends SessionOptionValue {
    private final boolean value;

    SessionOptionValueBoolean(boolean value) {
      this.value = value;
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }

  private static class SessionOptionValueInt extends SessionOptionValue {
    private final int value;

    SessionOptionValueInt(int value) {
      this.value = value;
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }

  private static class SessionOptionValueLong extends SessionOptionValue {
    private final long value;

    SessionOptionValueLong(long value) {
      this.value = value;
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }

  private static class SessionOptionValueFloat extends SessionOptionValue {
    private final float value;

    SessionOptionValueFloat(Float value) {
      this.value = value;
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }

  private static class SessionOptionValueDouble extends SessionOptionValue {
    private final double value;

    SessionOptionValueDouble(double value) {
      this.value = value;
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }

  private static class SessionOptionValueStringList extends SessionOptionValue {
    private final String[] value;

    SessionOptionValueStringList(String[] value) {
      this.value = value.clone();
    }

    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }
  }
}
