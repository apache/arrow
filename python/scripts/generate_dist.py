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
import pathlib
import shutil


def main():
    src_dir = pathlib.Path(os.environ["MESON_SOURCE_ROOT"])
    parent_dir = src_dir.parent.resolve()
    dest_dir = pathlib.Path(os.environ["MESON_DIST_ROOT"]).resolve()

    license_file = parent_dir / 'LICENSE.txt'
    dest_license = dest_dir / 'LICENSE.txt'
    dest_license.unlink(missing_ok=True)
    shutil.copy(license_file, dest_license)

    notice_file = parent_dir / 'NOTICE.txt'
    dest_notice = dest_dir / 'NOTICE.txt'
    dest_notice.unlink(missing_ok=True)
    shutil.copy(notice_file, dest_notice)


if __name__ == "__main__":
    main()
