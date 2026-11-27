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

import datetime as dt

from ._fs import FileSystem
from .lib import KeyValueMetadata


class GcsFileSystem(FileSystem):
    def __init__(
        self,
        *,
        anonymous: bool = False,
        access_token: str | None = None,
        target_service_account: str | None = None,
        credential_token_expiration: dt.datetime | None = None,
        default_bucket_location: str = "US",
        scheme: str = "https",
        endpoint_override: str | None = None,
        default_metadata: dict | KeyValueMetadata | None = None,
        retry_time_limit: dt.timedelta | None = None,
        project_id: str | None = None,
    ): ...
    @property
    def default_bucket_location(self) -> str: ...

    @property
    def project_id(self) -> str: ...
