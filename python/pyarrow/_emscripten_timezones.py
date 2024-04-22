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

import js

from importlib import resources
from pathlib import Path
from shutil import copytree


def setup_emscripten_timezone_database():
    """Setup the timezone database for use on browser based
    emscripten environments.
    """
    if not Path("/usr/share/zoneinfo").exists():
        Path("/usr/share/").mkdir(parents=True, exist_ok=True)
        try:
            tzpath = resources.files("tzdata").joinpath("zoneinfo")
            copytree(tzpath, "/usr/share/zoneinfo", dirs_exist_ok=True)
            localtime_path = Path("/etc/localtime")
            if not localtime_path.exists():
                # get local timezone from browser js object
                timezone = js.Intl.DateTimeFormat().resolvedOptions().timeZone
                if timezone and str(timezone) != "":
                    timezone = str(timezone)
                    # make symbolic link to local time
                    Path("/etc/").mkdir(parents=True, exist_ok=True)
                    localtime_path.symlink_to(tzpath / timezone)
        except (ImportError, IOError):
            print("Arrow couldn't install timezone db to /usr/share/zoneinfo")
