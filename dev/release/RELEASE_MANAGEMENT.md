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

# Guide for Arrow Release Managers

## Main source release and vote

## Post-release tasks

Upload release artifacts to Apache SVN (PMC karma required)

Update conda-forge recipes and shepherd pull requests through the process

Generate PyPI wheels for pip on all 3 platforms, then upload to PyPI. This is about 50% automated now luckily, it requires pull requests into github.com/apache/arrow-dist

Update API documentation for C, C++, Python, Java (4 different manual builds, PMC karma required to upload only)

Update website to add changelog, links, etc. for 0.7.0

Write a short blog post summarizing the 0.7.0 (like http://arrow.apache.org/blog/2017/08/16/0.6.0-release/)

Write a release announcement to be send to announce@apache.org

Publish Java Maven artifacts to Maven central