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

package org.apache.arrow.driver.jdbc.client.utils;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientAuthenticationUtilsTest {
  @Mock
  KeyStore keyStoreMock;

  @Test
  public void testGetCertificatesInputStream() throws IOException, KeyStoreException {
    JcaPEMWriter pemWriterMock = mock(JcaPEMWriter.class);
    Certificate certificateMock = mock(Certificate.class);
    Enumeration<String> alias = Collections.enumeration(Arrays.asList("test1", "test2"));

    Mockito.when(keyStoreMock.aliases()).thenReturn(alias);
    Mockito.when(keyStoreMock.isCertificateEntry("test1")).thenReturn(true);
    Mockito.when(keyStoreMock.getCertificate("test1")).thenReturn(certificateMock);

    ClientAuthenticationUtils.getCertificatesInputStream(keyStoreMock, pemWriterMock);
    Mockito.verify(pemWriterMock).writeObject(certificateMock);
    Mockito.verify(pemWriterMock).flush();
  }

  @Test
  public void testGetKeyStoreInstance() throws IOException,
      KeyStoreException, CertificateException, NoSuchAlgorithmException {
    try (MockedStatic<KeyStore> keyStoreMockedStatic = Mockito.mockStatic(KeyStore.class)) {
      keyStoreMockedStatic
          .when(() -> ClientAuthenticationUtils.getKeyStoreInstance(Mockito.any()))
          .thenReturn(keyStoreMock);

      KeyStore receiveKeyStore = ClientAuthenticationUtils.getKeyStoreInstance("test1");
      Mockito
          .verify(keyStoreMock)
          .load(null, null);

      Assert.assertEquals(receiveKeyStore, keyStoreMock);
    }
  }

  @Test
  public void testGetCertificateInputStreamFromMacSystem() throws IOException,
      KeyStoreException, CertificateException, NoSuchAlgorithmException {
    InputStream mock = mock(InputStream.class);

    try (MockedStatic<KeyStore> keyStoreMockedStatic = createKeyStoreStaticMock();
         MockedStatic<ClientAuthenticationUtils>
             clientAuthenticationUtilsMockedStatic = createClientAuthenticationUtilsStaticMock()) {

      setOperatingSystemMock(clientAuthenticationUtilsMockedStatic, false, true);
      keyStoreMockedStatic.when(() -> ClientAuthenticationUtils
          .getKeyStoreInstance("KeychainStore"))
          .thenReturn(keyStoreMock);
      keyStoreMockedStatic.when(() -> ClientAuthenticationUtils
          .getCertificatesInputStream(Mockito.any()))
          .thenReturn(mock);

      InputStream inputStream = ClientAuthenticationUtils.getCertificateInputStreamFromSystem("test");
      Assert.assertEquals(inputStream, mock);
    }
  }

  @Test
  public void testGetCertificateInputStreamFromWindowsSystem() throws IOException,
      KeyStoreException, CertificateException, NoSuchAlgorithmException {
    InputStream mock = mock(InputStream.class);

    try (MockedStatic<KeyStore> keyStoreMockedStatic = createKeyStoreStaticMock();
        MockedStatic<ClientAuthenticationUtils>
            clientAuthenticationUtilsMockedStatic = createClientAuthenticationUtilsStaticMock()) {

      setOperatingSystemMock(clientAuthenticationUtilsMockedStatic, true, false);
      keyStoreMockedStatic
          .when(() -> ClientAuthenticationUtils.getKeyStoreInstance("Windows-ROOT"))
          .thenReturn(keyStoreMock);
      keyStoreMockedStatic
          .when(() -> ClientAuthenticationUtils.getKeyStoreInstance("Windows-MY"))
          .thenReturn(keyStoreMock);
      keyStoreMockedStatic
          .when(() -> ClientAuthenticationUtils.getCertificatesInputStream(Mockito.any()))
          .thenReturn(mock);

      InputStream inputStream = ClientAuthenticationUtils.getCertificateInputStreamFromSystem("test");
      Assert.assertEquals(inputStream, mock);
    }
  }

  @Test
  public void testGetCertificateInputStreamFromLinuxSystem() throws IOException,
      KeyStoreException, CertificateException, NoSuchAlgorithmException {
    InputStream mock = mock(InputStream.class);

    try (
        MockedStatic<KeyStore> keyStoreMockedStatic = createKeyStoreStaticMock();
        MockedStatic<ClientAuthenticationUtils>
            clientAuthenticationUtilsMockedStatic = createClientAuthenticationUtilsStaticMock()) {

      setOperatingSystemMock(clientAuthenticationUtilsMockedStatic, false, false);
      keyStoreMockedStatic.when(() -> ClientAuthenticationUtils
              .getCertificatesInputStream(Mockito.any()))
          .thenReturn(mock);

      clientAuthenticationUtilsMockedStatic
          .when(ClientAuthenticationUtils::getKeystoreInputStream)
          .thenCallRealMethod();
      keyStoreMockedStatic.when(KeyStore::getDefaultType).thenCallRealMethod();

      InputStream inputStream = ClientAuthenticationUtils.getCertificateInputStreamFromSystem("changeit");
      Assert.assertEquals(inputStream, mock);
      inputStream = ClientAuthenticationUtils.getCertificateInputStreamFromSystem(null);
      Assert.assertEquals(inputStream, mock);
    }
  }


  private MockedStatic<KeyStore> createKeyStoreStaticMock() {
    return Mockito.mockStatic(KeyStore.class, invocationOnMock -> {
          Method method = invocationOnMock.getMethod();
          if (method.getName().equals("getInstance")) {
            return invocationOnMock.callRealMethod();
          }
          return invocationOnMock.getMock();
        }
    );
  }

  private MockedStatic<ClientAuthenticationUtils> createClientAuthenticationUtilsStaticMock() {
    return Mockito.mockStatic(ClientAuthenticationUtils.class , invocationOnMock -> {
      Method method = invocationOnMock.getMethod();
      if (method.getName().equals("getCertificateInputStreamFromSystem")) {
        return invocationOnMock.callRealMethod();
      }
      return invocationOnMock.getMock();
    });
  }

  private void setOperatingSystemMock(MockedStatic<ClientAuthenticationUtils> clientAuthenticationUtilsMockedStatic,
                                      boolean isWindows, boolean isMac) {
    clientAuthenticationUtilsMockedStatic.when(ClientAuthenticationUtils::isMac).thenReturn(isMac);
    clientAuthenticationUtilsMockedStatic.when(ClientAuthenticationUtils::isWindows).thenReturn(isWindows);
  }
}
