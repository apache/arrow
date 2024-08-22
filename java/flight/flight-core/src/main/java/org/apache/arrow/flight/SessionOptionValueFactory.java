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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.arrow.flight.impl.Flight;

/** Abstract factory for concrete SessionOptionValue instances. */
public class SessionOptionValueFactory {
  public static SessionOptionValue makeSessionOptionValue(String value) {
    return new SessionOptionValueString(value);
  }

  public static SessionOptionValue makeSessionOptionValue(boolean value) {
    return new SessionOptionValueBoolean(value);
  }

  public static SessionOptionValue makeSessionOptionValue(long value) {
    return new SessionOptionValueLong(value);
  }

  public static SessionOptionValue makeSessionOptionValue(double value) {
    return new SessionOptionValueDouble(value);
  }

  public static SessionOptionValue makeSessionOptionValue(String[] value) {
    return new SessionOptionValueStringList(value);
  }

  public static SessionOptionValue makeEmptySessionOptionValue() {
    return new SessionOptionValueEmpty();
  }

  /** Construct a SessionOptionValue from its Protobuf object representation. */
  public static SessionOptionValue makeSessionOptionValue(Flight.SessionOptionValue proto) {
    switch (proto.getOptionValueCase()) {
      case STRING_VALUE:
        return new SessionOptionValueString(proto.getStringValue());
      case BOOL_VALUE:
        return new SessionOptionValueBoolean(proto.getBoolValue());
      case INT64_VALUE:
        return new SessionOptionValueLong(proto.getInt64Value());
      case DOUBLE_VALUE:
        return new SessionOptionValueDouble(proto.getDoubleValue());
      case STRING_LIST_VALUE:
        // Using ByteString::toByteArray() here otherwise we still somehow get `ByteArray`s with
        // broken .equals(String)
        return new SessionOptionValueStringList(
            proto.getStringListValue().getValuesList().asByteStringList().stream()
                .map((e) -> new String(e.toByteArray(), StandardCharsets.UTF_8))
                .toArray(String[]::new));
      case OPTIONVALUE_NOT_SET:
        return new SessionOptionValueEmpty();
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

    @Override
    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionOptionValueString that = (SessionOptionValueString) o;
      return value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public String toString() {
      return '"' + value + '"';
    }
  }

  private static class SessionOptionValueBoolean extends SessionOptionValue {
    private final boolean value;

    SessionOptionValueBoolean(boolean value) {
      this.value = value;
    }

    @Override
    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionOptionValueBoolean that = (SessionOptionValueBoolean) o;
      return value == that.value;
    }

    @Override
    public int hashCode() {
      return Boolean.hashCode(value);
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private static class SessionOptionValueLong extends SessionOptionValue {
    private final long value;

    SessionOptionValueLong(long value) {
      this.value = value;
    }

    @Override
    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionOptionValueLong that = (SessionOptionValueLong) o;
      return value == that.value;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private static class SessionOptionValueDouble extends SessionOptionValue {
    private final double value;

    SessionOptionValueDouble(double value) {
      this.value = value;
    }

    @Override
    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionOptionValueDouble that = (SessionOptionValueDouble) o;
      return value == that.value;
    }

    @Override
    public int hashCode() {
      return Double.hashCode(value);
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private static class SessionOptionValueStringList extends SessionOptionValue {
    private final String[] value;

    SessionOptionValueStringList(String[] value) {
      this.value = value.clone();
    }

    @Override
    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SessionOptionValueStringList that = (SessionOptionValueStringList) o;
      return Arrays.deepEquals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(value);
    }

    @Override
    public String toString() {
      if (value.length == 0) {
        return "[]";
      }
      return "[\"" + String.join("\", \"", value) + "\"]";
    }
  }

  private static class SessionOptionValueEmpty extends SessionOptionValue {
    @Override
    public <T> T acceptVisitor(SessionOptionValueVisitor<T> v) {
      return v.visit((Void) null);
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      return SessionOptionValueEmpty.class.hashCode();
    }

    @Override
    public String toString() {
      return "<empty>";
    }
  }
}
