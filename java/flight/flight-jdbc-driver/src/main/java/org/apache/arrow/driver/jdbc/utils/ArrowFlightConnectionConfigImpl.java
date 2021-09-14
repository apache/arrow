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

package org.apache.arrow.driver.jdbc.utils;

import static java.lang.String.format;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.ArrowFlightConnection;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.ConnectionProperty;

/**
 * A {@link ConnectionConfig} for the {@link ArrowFlightConnection}.
 */
public final class ArrowFlightConnectionConfigImpl extends ConnectionConfigImpl {
  public ArrowFlightConnectionConfigImpl(final Properties properties) {
    super(properties);
  }

  /**
   * Gets the host.
   *
   * @return the host.
   */
  public String getHost() {
    return (String) ArrowFlightConnectionProperty.HOST.get(properties);
  }

  /**
   * Gets the port.
   *
   * @return the port.
   */
  public int getPort() {
    return (int) ArrowFlightConnectionProperty.PORT.get(properties);
  }

  /**
   * Gets the host.
   *
   * @return the host.
   */
  public String getUser() {
    return (String) ArrowFlightConnectionProperty.USER.get(properties);
  }

  /**
   * Gets the host.
   *
   * @return the host.
   */
  public String getPassword() {
    return (String) ArrowFlightConnectionProperty.PASSWORD.get(properties);
  }

  /**
   * Gets the KeyStore path.
   *
   * @return the path.
   */
  public String getKeyStorePath() {
    final File keyStore = keystore();
    return keyStore == null ? null : keyStore.getPath();
  }

  /**
   * Whether to use TLS encryption.
   *
   * @return whether to use TLS encryption.
   */
  public boolean useTls() {
    return (boolean) ArrowFlightConnectionProperty.USE_TLS.get(properties);
  }

  /**
   * Gets the thread pool size.
   *
   * @return the thread pool size.
   */
  public int threadPoolSize() {
    return (int) ArrowFlightConnectionProperty.THREAD_POOL_SIZE.get(properties);
  }

  /**
   * Gets the {@link CallOption}s from this {@link ConnectionConfig}.
   *
   * @return the call options.
   */
  public CallOption toCallOption() {
    final CallHeaders headers = new FlightCallHeaders();
    properties.forEach(
        (key, val) -> headers.insert(key == null ? null : key.toString(), val == null ? null : val.toString()));
    return new HeaderCallOption(headers);
  }

  /**
   * Gets a copy of this {@link ArrowFlightConnectionConfigImpl} with replaced properties.
   *
   * @param replacements the replacements.
   * @return a copy of this instance with replacements applied.
   */
  public ArrowFlightConnectionConfigImpl copyReplace(final Map<ConnectionProperty, Object> replacements) {
    Preconditions.checkNotNull(replacements);
    final Properties newProperties = new Properties();
    newProperties.putAll(properties);
    replacements.forEach((key, value) -> newProperties.replace(key.camelName(), value));
    return new ArrowFlightConnectionConfigImpl(newProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties);
  }

  @Override
  public boolean equals(final Object o) {
    return o != null &&
        o.getClass().isInstance(o) &&
        properties.equals(((ArrowFlightConnectionConfigImpl) o).properties);
  }

  /**
   * Custom {@link ConnectionProperty} for the {@link ArrowFlightConnectionConfigImpl}.
   */
  public enum ArrowFlightConnectionProperty implements ConnectionProperty {
    HOST("host", null, Type.STRING, true),
    PORT("port", null, Type.NUMBER, true),
    USER("user", null, Type.STRING, false),
    PASSWORD("password", null, Type.STRING, false),
    USE_TLS("useTls", false, Type.BOOLEAN, false),
    THREAD_POOL_SIZE("threadPoolSize", 1, Type.NUMBER, false);

    private final String camelName;
    private final Object defaultValue;
    private final Type type;
    private final boolean required;

    ArrowFlightConnectionProperty(final String camelName, final Object defaultValue,
                                  final Type type, final boolean required) {
      this.camelName = Preconditions.checkNotNull(camelName);
      this.defaultValue = defaultValue;
      this.type = Preconditions.checkNotNull(type);
      this.required = required;
    }

    /**
     * Gets the property.
     *
     * @param properties the properties from which to fetch this property.
     * @return the property.
     */
    public Object get(final Properties properties) {
      Preconditions.checkNotNull(properties, "Properties cannot be null.");
      Preconditions.checkState(
          properties.containsKey(camelName) || !required,
          format("Required property not provided: <%s>.", this));
      return properties.getOrDefault(camelName, defaultValue);
    }

    @Override
    public String camelName() {
      return camelName;
    }

    @Override
    public Object defaultValue() {
      return defaultValue;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public PropEnv wrap(final Properties properties) {
      throw new UnsupportedOperationException("Operation unsupported.");
    }

    @Override
    public boolean required() {
      return required;
    }

    @Override
    public Class<?> valueClass() {
      return type.defaultValueClass();
    }
  }

  /**
   * Utility class for {@link Properties} instances.
   */
  public static final class PropertiesUtils {
    private PropertiesUtils() {
      // Prevent instantiation.
    }

    /**
     * Creates a copy of the provided {@code target} properties with the provided {@code replacements}.
     *
     * @param replacements the replacements to make.
     * @return a copy of the provided {@link Properties}, with the provided {@code replacements}.
     */
    public static Properties copyReplace(final Properties target, final Map<ConnectionProperty, Object> replacements) {
      final Properties properties = new Properties();
      properties.putAll(target);
      replacements.forEach(
          (property, value) ->
              properties.replace(property.camelName(), value == null ? property.defaultValue() : value));
      return properties;
    }
  }
}
