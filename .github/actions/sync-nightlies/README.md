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
--->

# Sync Nightlies
This action can be used to sync directories from/to [nightlies.apache.org] with
rsync. It requires the correct secrets to be in place as described 
[below](#usage).
Currently this action is intended to sync the *contents* of `local_path` to
`remote_path` (or vice versa), so a slash will be appended to the source path.
Uploading single files or dirs is not possible directly but only by wrapping
them in an additional directory.

## Inputs 
  - `upload` Set to `true` to upload from `local_path` to `remote_path`  
  - `switches` See rsync --help for available switches.
  - `local_path` The relative local path within $GITHUB_WORKSPACE
  - `remote_path` The remote path incl. sub dirs e.g. {{secrets.path}}/arrow/r.
  - `remote_host` The remote host.
  - `remote_port` The remote port.
  - `remote_user` The remote user.
  - `remote_key` The remote ssh key.
  - `remote_host_key` The host key fot StrictHostKeyChecking.

## Usage
The secrets have to be set by INFRA, except `secrets.NIGHTLIES_RSYNC_HOST_KEY`
which should contain the result of `ssh-keyscan -H nightlies.apache.org 2>
/dev/null`. This example requires apache/arrow to be checked out in `arrow`.

```yaml
      - name: Sync from Remote
        uses: ./arrow/.github/actions/sync-nightlies
        with:
          switches: -avzh --update --delete --progress
          local_path: repo
          remote_path: ${{ secrets.NIGHTLIES_RSYNC_PATH }}/arrow/r
          remote_host: ${{ secrets.NIGHTLIES_RSYNC_HOST }}
          remote_port: ${{ secrets.NIGHTLIES_RSYNC_PORT }}
          remote_user: ${{ secrets.NIGHTLIES_RSYNC_USER }}
          remote_key: ${{ secrets.NIGHTLIES_RSYNC_KEY }}
          remote_host_key: ${{ secrets.NIGHTLIES_RSYNC_HOST_KEY }}

      - name: Sync to Remote
        uses: ./arrow/.github/actions/sync-nightlies
        with:
          upload: true
          switches: -avzh --update --delete --progress
          local_path: repo
          remote_path: ${{ secrets.NIGHTLIES_RSYNC_PATH }}/arrow/r
          remote_host: ${{ secrets.NIGHTLIES_RSYNC_HOST }}
          remote_port: ${{ secrets.NIGHTLIES_RSYNC_PORT }}
          remote_user: ${{ secrets.NIGHTLIES_RSYNC_USER }}
          remote_key: ${{ secrets.NIGHTLIES_RSYNC_KEY }}
          remote_host_key: ${{ secrets.NIGHTLIES_RSYNC_HOST_KEY }}
```