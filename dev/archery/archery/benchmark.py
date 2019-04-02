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

import os

from .utils.git import GitClone, GitCheckout


class BenchmarkSuite:
    def __init__(self, git_repo, root, revision):
        self.root = root
        self.clone_dir = os.path.join(root, "arrow-src")
        self.revision = revision
        self.git_repo = git_repo
        self.setup()

    def setup(self):
    # Prepare local clone and checkout the proper revision.
        GitClone("--local", self.git_repo, self.clone_dir)
        GitCheckout("-b", self.revision, git_dir=self.clone_dir)
