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

import java.util.Arrays;
import java.util.HashMap;
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
    return ArrowFlightConnectionProperty.HOST.getString(properties);
  }

  /**
   * Gets the port.
   *
   * @return the port.
   */
  public int getPort() {
    return ArrowFlightConnectionProperty.PORT.getInteger(properties);
  }

  /**
   * Gets the host.
   *
   * @return the host.
   */
  public String getUser() {
    return ArrowFlightConnectionProperty.USER.getString(properties);
  }

  /**
   * Gets the host.
   *
   * @return the host.
   */
  public String getPassword() {
    return ArrowFlightConnectionProperty.PASSWORD.getString(properties);
  }


  public String getToken() {
    return ArrowFlightConnectionProperty.TOKEN.getString(properties);
  }

  /**
   * Gets the KeyStore path.
   *
   * @return the path.
   */
  public String getTrustStorePath() {
    return ArrowFlightConnectionProperty.TRUST_STORE.getString(properties);
  }

  /**
   * Gets the KeyStore password.
   *
   * @return the password.
   */
  public String getTrustStorePassword() {
    return ArrowFlightConnectionProperty.TRUST_STORE_PASSWORD.getString(properties);
  }

  /**
   * Check if the JDBC should use the trusted store files from the operating system.
   *
   * @return whether to use system trusted store certificates.
   */
  public boolean useSystemTrustStore() {
    return ArrowFlightConnectionProperty.USE_SYSTEM_TRUST_STORE.getBoolean(properties);
  }

  /**
   * Whether to use TLS encryption.
   *
   * @return whether to use TLS encryption.
   */
  public boolean useEncryption() {
    return ArrowFlightConnectionProperty.USE_ENCRYPTION.getBoolean(properties);
  }

  public boolean getDisableCertificateVerification() {
    return ArrowFlightConnectionProperty.CERTIFICATE_VERIFICATION.getBoolean(properties);
  }

  /**
   * Gets the thread pool size.
   *
   * @return the thread pool size.
   */
  public int threadPoolSize() {
    return ArrowFlightConnectionProperty.THREAD_POOL_SIZE.getInteger(properties);
  }

  /**
   * Gets the {@link CallOption}s from this {@link ConnectionConfig}.
   *
   * @return the call options.
   */
  public CallOption toCallOption() {
    final CallHeaders headers = new FlightCallHeaders();
    Map<String, String> headerAttributes = getHeaderAttributes();
    headerAttributes.forEach(headers::insert);
    return new HeaderCallOption(headers);
  }

  /**
   * Gets which properties should be added as headers.
   *
   * @return {@link Map}
   */
  public Map<String, String> getHeaderAttributes() {
    Map<String, String> headers = new HashMap<>();
    ArrowFlightConnectionProperty[] builtInProperties = ArrowFlightConnectionProperty.values();
    properties.forEach(
        (key, val) -> {
          // For built-in properties before adding new headers
          if (Arrays.stream(builtInProperties)
              .noneMatch(builtInProperty -> builtInProperty.camelName.equalsIgnoreCase(key.toString()))) {
            headers.put(key.toString(), val.toString());
          }
        });
    return headers;
  }

  /**
   * Custom {@link ConnectionProperty} for the {@link ArrowFlightConnectionConfigImpl}.
   */
  public enum ArrowFlightConnectionProperty implements ConnectionProperty {
    HOST("host", null, Type.STRING, true),
    PORT("port", null, Type.NUMBER, true),
    USER("user", null, Type.STRING, false),
    PASSWORD("password", null, Type.STRING, false),
    USE_ENCRYPTION("useEncryption", true, Type.BOOLEAN, false),
    CERTIFICATE_VERIFICATION("disableCertificateVerification", false, Type.BOOLEAN, false),
    TRUST_STORE("trustStore", null, Type.STRING, false),
    TRUST_STORE_PASSWORD("trustStorePassword", null, Type.STRING, false),
    USE_SYSTEM_TRUST_STORE("useSystemTrustStore", true, Type.BOOLEAN, false),
    THREAD_POOL_SIZE("threadPoolSize", 1, Type.NUMBER, false),
    TOKEN("token", null, Type.STRING, false);

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
      Object value = properties.get(camelName);
      if (value == null) {
        value = properties.get(camelName.toLowerCase());
      }
      if (required) {
        if (value == null) {
          throw new IllegalStateException(String.format("Required property not provided: <%s>.", this));
        }
        return value;
      } else {
        return value != null ? value : defaultValue;
      }
    }

    /**
     * Gets the property as Boolean.
     *
     * @param properties the properties from which to fetch this property.
     * @return the property.
     */
    public Boolean getBoolean(final Properties properties) {
      final String valueFromProperties = String.valueOf(get(properties));
      return valueFromProperties.equals("1") || valueFromProperties.equals("true");
    }

    /**
     * Gets the property as Integer.
     *
     * @param properties the properties from which to fetch this property.
     * @return the property.
     */
    public Integer getInteger(final Properties properties) {
      final String valueFromProperties = String.valueOf(get(properties));
      return valueFromProperties.equals("null") ? null : Integer.parseInt(valueFromProperties);
    }

    /**
     * Gets the property as String.
     *
     * @param properties the properties from which to fetch this property.
     * @return the property.
     */
    public String getString(final Properties properties) {
      return Objects.toString(get(properties), null);
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

    /**
     * Replaces the semicolons in the URL to the proper format.
     *
     * @param url the current connection string
     * @return the formatted url
     */
    public static String replaceSemiColons(String url) {
      if (url != null) {
        url = url.replaceFirst(";", "?");
        url = url.replaceAll(";", "&");
      }
      return url;
    }
  }
}
