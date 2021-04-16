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
## Deploying a standalone Ballista cluster

### Start a Scheduler

Start a scheduler using the following syntax:

```bash
docker run --network=host \
  -d ballistacompute/ballista-rust:0.4.2-SNAPSHOT \
  /scheduler --port 50050
```

Run `docker ps` to check that the process is running:

```
$ docker ps
CONTAINER ID   IMAGE                                         COMMAND                  CREATED         STATUS         PORTS     NAMES
59452ce72138   ballistacompute/ballista-rust:0.4.2-SNAPSHOT   "/scheduler --port 5…"   6 seconds ago   Up 5 seconds             affectionate_hofstadter
```

Run `docker logs CONTAINER_ID` to check the output from the process:

```
$ docker logs 59452ce72138
[2021-02-14T18:32:20Z INFO  scheduler] Ballista v0.4.2-SNAPSHOT Scheduler listening on 0.0.0.0:50050
```

### Start executors

Start one or more executor processes. Each executor process will need to listen on a different port.

```bash
docker run --network=host \
  -d ballistacompute/ballista-rust:0.4.2-SNAPSHOT \
  /executor --external-host localhost --port 50051 
```

Use `docker ps` to check that both the scheduer and executor(s) are now running:

```
$ docker ps
CONTAINER ID   IMAGE                                         COMMAND                  CREATED         STATUS         PORTS     NAMES
0746ce262a19   ballistacompute/ballista-rust:0.4.2-SNAPSHOT   "/executor --externa…"   2 seconds ago   Up 1 second              naughty_mclean
59452ce72138   ballistacompute/ballista-rust:0.4.2-SNAPSHOT   "/scheduler --port 5…"   4 minutes ago   Up 4 minutes             affectionate_hofstadter
```

Use `docker logs CONTAINER_ID` to check the output from the executor(s):

```
$ docker logs 0746ce262a19
[2021-02-14T18:36:25Z INFO  executor] Running with config: ExecutorConfig { host: "localhost", port: 50051, work_dir: "/tmp/.tmpVRFSvn", concurrent_tasks: 4 }
[2021-02-14T18:36:25Z INFO  executor] Ballista v0.4.2-SNAPSHOT Rust Executor listening on 0.0.0.0:50051
[2021-02-14T18:36:25Z INFO  executor] Starting registration with scheduler
```

The external host and port will be registered with the scheduler. The executors will discover other executors by 
requesting a list of executors from the scheduler.

### Using etcd as backing store

_NOTE: This functionality is currently experimental_

Ballista can optionally use [etcd](https://etcd.io/) as a backing store for the scheduler. 

```bash
docker run --network=host \
  -d ballistacompute/ballista-rust:0.4.2-SNAPSHOT \
  /scheduler --port 50050 \
  --config-backend etcd \
  --etcd-urls etcd:2379
```

Please refer to the [etcd](https://etcd.io/) web site for installation instructions. Etcd version 3.4.9 or later is 
recommended.
