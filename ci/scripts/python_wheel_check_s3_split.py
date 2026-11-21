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

# Run with pyarrow installed but not pyarrow-s3: S3 use must fail with a
# message pointing at the pyarrow-s3 package.

import pyarrow.fs as fs

try:
    fs.S3FileSystem
except ImportError as e:
    assert "pyarrow-s3" in str(e), e
else:
    raise AssertionError("S3FileSystem available without pyarrow-s3")

try:
    fs.FileSystem.from_uri("s3://bucket/key?region=us-east-1")
except ValueError as e:
    assert "pyarrow-s3" in str(e), e
else:
    raise AssertionError("from_uri resolved s3:// without pyarrow-s3")
