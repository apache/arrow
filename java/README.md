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

# Arrow Java

## Setup Build Environment

install:
 - java 7 or later
 - maven 3.3 or later

## Building running tests

```
cd java
mvn install
```

## Test Logging Configuration

When running tests, Arrow Java uses the Logback logger with SLF4J. By default,
Logback has a log level set to DEBUG. Besides setting this level
programmatically, it can also be configured with a file named either
"logback.xml" or "logback-test.xml" residing in the classpath. The file
location can also be specified in the Maven command line with the following
option `-Dlogback.configurationFile=file:<absolute-file-path>`. A sample
logback.xml file is available in `java/dev` with a log level of ERROR. Arrow
Java can be built with this file using the following command run in the project
root directory:

```bash
mvn -Dlogback.configurationFile=file:`pwd`/dev/logback.xml
```

See [Logback Configuration][1] for more details.

[1]: https://logback.qos.ch/manual/configuration.html
