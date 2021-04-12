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
# Release Process

These instructions are for project maintainers wishing to create public releases of Ballista.

- Create a `release-0.4` branch or merge latest from `main` into an existing `release-0.4` branch.
- Update version numbers using `./dev/bump-version.sh`
- Run integration tests with `./dev/integration-tests.sh`
- Push changes
- Create `v0.4.x` release tag from the `release-0.4` branch
- Publish Docker images
- Publish crate if possible (if we're using a published version of Arrow)

## Publishing Java artifacts to Maven Central

The JVM artifacts are published to Maven central by uploading to sonatype. You will need to set the environment 
variables `SONATYPE_USERNAME` and `SONATYPE_PASSWORD` to the correct values for your account and you will also need 
verified GPG keys available for signing the artifacts (instructions tbd).

Run the follow commands to publish the artifacts to a sonatype staging repository.

```bash
./dev/publish-jvm.sh
```

## Publishing Rust Artifacts

Run the following script to publish the Rust crate to crates.io.

```
./dev/publish-rust.sh
```

## Publishing Docker Images

Run the following script to publish the executor Docker images to Docker Hub.

```
./dev/publish-docker-images.sh
```

## GPG Notes

Refer to [this article](https://help.github.com/en/github/authenticating-to-github/generating-a-new-gpg-key) for 
instructions on setting up GPG keys. Some useful commands are:

```bash
gpg --full-generate-key
gpg --export-secret-keys > ~/.gnupg/secring.gpg
gpg --key-server keys.openpgp.org --send-keys KEYID
```