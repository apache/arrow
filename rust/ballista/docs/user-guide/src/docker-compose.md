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

# Installing Ballista with Docker Compose

Docker Compose is a convenient way to launch a cluister when testing locally. The following Docker Compose example 
demonstrates how to start a cluster using a single process that acts as both a scheduler and an executor, with a data 
volume mounted into the container so that Ballista can access the host file system.

```yaml
version: '2.0'
services:
  etcd:
    image: quay.io/coreos/etcd:v3.4.9
    command: "etcd -advertise-client-urls http://etcd:2379 -listen-client-urls http://0.0.0.0:2379"
    ports:
      - "2379:2379"
  ballista-executor:
    image: ballistacompute/ballista-rust:0.4.2-SNAPSHOT
    command: "/executor --bind-host 0.0.0.0 --port 50051 --local"
    environment:
      - RUST_LOG=info
    ports:
      - "50050:50050"
      - "50051:50051"
    volumes:
      - ./data:/data


```

With the above content saved to a `docker-compose.yaml` file, the following command can be used to start the single 
node cluster.

```bash
docker-compose up
```

The scheduler listens on port 50050 and this is the port that clients will need to connect to.
