<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Connection Properties on Apache Arrow Flight SQL ODBC Driver

## Setting ODBC Connection Properties

ODBC connection parameters can be set in a connection string or defined in a DSN inside your system's odbc.ini.

The following sample connection string and sample DSN are two equivalent ways to connect to Arrow.

### Sample Connection String
```
"driver={Apache Arrow Flight SQL ODBC Driver};HOST=1234.56.789;port=12345;uid=sample_user;pwd=sample_password;useEncryption=false;useWideChar=true;"
```

### Sample DSN
```
[Apache Arrow Flight SQL]
Driver        = Apache Arrow Flight SQL ODBC Driver
Host          = 1234.56.789
Port          = 12345
UID           = sample_user
PWD           = sample_password
useEncryption = false
useWideChar   = true
```

### Driver Connection Options
| Option | Description | Default |
|--------|-------------|---------------|
| `driver` | Required: the driver for this ODBC driver. | Apache Arrow Flight SQL 
| `dsn` | Data Source Name used for configuring the connection. | `NONE`
| `host` | The IP address or hostname for the server. | `NONE`
| `port` | The TCP port number the server uses for ODBC connections. | `NONE`
| `user` | The username for authentication to the server. | `NONE`
| `user id` | The username for authentication to the server. | `NONE`
| `uid` | The username for authentication to the server. | `NONE`
| `password` | The password for authentication to the server. | `NONE`
| `pwd` | The password for authentication to the server. | `NONE`
| `token` | The personal access token for authentication to the server. | `NONE`
| `useEncryption` | Setting to determine if an SSL-encrypted connection should be used. | `true` on Windows & Linux, `false` on MacOS
| `disableCertificateVerification` | Setting to determine if the driver should verify the host certificate against the trust store. | `false`
| `trustedCerts` | The full path of the .pem file containing certificates for the purpose of verifying the server. | `NONE`
| `useSystemTrustStore` | Setting to determine whether to use a CA certificate from the system's trust store or from a specified .pem file. | `true`
| `stringColumnLength` | Maximum length of a string column. Some apps may require a lower length. | `NONE`
| `useWideChar` | Setting to determine if wide characters should be used. Important for Unicode applications. | `true` on Windows, `false` on MacOS & Linux
| `chunkBufferCapacity` | Capacity of a chunk buffer. Larger values will improve throughput at the cost of increased memory usage. | 5
