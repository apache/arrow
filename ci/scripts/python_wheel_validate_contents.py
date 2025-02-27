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
from pathlib import Path
import re
import zipfile


def validate_wheel(path):
    p = Path(path)
    wheels = list(p.glob('*.whl'))
    error_msg = f"{len(wheels)} wheels found but only 1 expected ({wheels})"
    assert len(wheels) == 1, error_msg
    f = zipfile.ZipFile(wheels[0])
    outliers = [
        info.filename for info in f.filelist if not re.match(
            r'(pyarrow/|pyarrow-[-.\w\d]+\.dist-info/|pyarrow\.libs/)', info.filename
        )
    ]
    assert not outliers, f"Unexpected contents in wheel: {sorted(outliers)}"
    print(f"The wheel: {wheels[0]} seems valid.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, required=True,
                        help="Directory where wheel is located")
    args = parser.parse_args()
    validate_wheel(args.path)


if __name__ == '__main__':
    main()
