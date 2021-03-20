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
# Setting up a Rust development environment

You will need a standard Rust development environment. The easiest way to achieve this is by using rustup: https://rustup.rs/

## Install OpenSSL

Follow instructions for [setting up OpenSSL](https://docs.rs/openssl/0.10.28/openssl/). For Ubuntu users, the following 
command works.

```bash
sudo apt-get install pkg-config libssl-dev
```

## Install CMake

You'll need cmake in order to compile some of ballista's dependencies. Ubuntu users can use the following command:

```bash
sudo apt-get install cmake
```