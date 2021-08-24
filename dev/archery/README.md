<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Developing with Archery

Archery is documented on the Arrow website:

* [Daily development using Archery](https://arrow.apache.org/docs/developers/archery.html)
* [Using Archery and Crossbow](https://arrow.apache.org/docs/developers/crossbow.html)
* [Using Archer and Docker](https://arrow.apache.org/docs/developers/docker.html)

# Installing Archery

See the pages linked aboved for more details. As a general overview, Archery
comes in a number of subpackages, each needing to be installed if you want
to use the functionality of it:

* lint – lint (and in some cases auto-format) code in the Arrow repo
  To install: `pip install -e "arrow/dev/archery[lint]"`
* benchmark – to run Arrow benchmarks using Archery
  To install: `pip install -e "arrow/dev/archery[benchmark]"`
* docker – to run docker-compose based tasks more easily
  To install: `pip install -e "arrow/dev/archery[docker]"`
* release – release related helpers
  To install: `pip install -e "arrow/dev/archery[release]"`
* crossbow – to trigger + interact with the crossbow build system
  To install: `pip install -e "arrow/dev/archery[crossbow]"`
* crossbow-upload
  To install: `pip install -e "arrow/dev/archery[crossbow-upload]"`

Additionally, if you would prefer to install everything at once,
`pip install -e "arrow/dev/archery[all]"` is an alias for all of
the above subpackages.