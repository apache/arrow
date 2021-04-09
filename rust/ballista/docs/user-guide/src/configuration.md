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
# Configuration 
The rust executor and scheduler can be configured using toml files, environment variables and command line arguments. The specification for config options can be found in `rust/ballista/src/bin/[executor|scheduler]_config_spec.toml`. 

Those files fully define Ballista's configuration. If there is a discrepancy between this documentation and the files, assume those files are correct.

To get a list of command line arguments, run the binary with `--help`

There is an example config file at `ballista/rust/ballista/examples/example_executor_config.toml`

The order of precedence for arguments is: default config file < environment variables < specified config file < command line arguments. 

The executor and scheduler will look for the default config file at `/etc/ballista/[executor|scheduler].toml` To specify a config file use the `--config-file` argument. 

Environment variables are prefixed by `BALLISTA_EXECUTOR` or `BALLISTA_SCHEDULER` for the executor and scheduler respectively. Hyphens in command line arguments become underscores. For example, the `--scheduler-host` argument for the executor becomes `BALLISTA_EXECUTOR_SCHEDULER_HOST`