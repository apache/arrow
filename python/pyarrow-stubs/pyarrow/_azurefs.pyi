# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Literal

from ._fs import FileSystem


class AzureFileSystem(FileSystem):
    def __init__(
        self,
        account_name: str | None = None,
        account_key: str | None = None,
        blob_storage_authority: str | None = None,
        dfs_storage_authority: str | None = None,
        blob_storage_scheme: Literal["http", "https"] = "https",
        dfs_storage_scheme: Literal["http", "https"] = "https",
        sas_token: str | None = None,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None: ...
