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

import argparse
import re
import zipfile


def validate_wheel(wheel):
    f = zipfile.ZipFile(wheel)
    # An empty "pyarrow." folder is currently present on the wheel
    # but we haven't been able to locate the origin.
    outliers = [
        info.filename for info in f.filelist if not re.match(
            r'(pyarrow\.?/|pyarrow-[-.\w\d]+\.dist-info/)', info.filename
        )
    ]
    assert not outliers, f"Unexpected contents in wheel: {sorted(outliers)}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", type=str, required=True,
                        help="wheel filename to validate")
    args = parser.parse_args()
    validate_wheel(args.wheel)


if __name__ == '__main__':
    main()
