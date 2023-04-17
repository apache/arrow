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

import json
import sys

dir_path = sys.argv[1]
# X.Y.Z
version = sys.argv[2]
# {X+1}.Y.Z, X.{Y+1}.Z or X.Y.{Z+1}
next_version = sys.argv[3]

main_versions_path = dir_path + "/docs/source/_static/versions.json"
r_versions_path = dir_path + "/r/pkgdown/assets/versions.json"

split_version = version.split(".")
split_next_version = next_version.split(".")

if split_next_version[1:] == ["0", "0"]:
    release_type = "major"
elif split_next_version[2:] == ["0"]:
    release_type = "minor"
else:
    release_type = "patch"

# Update main docs version script
if release_type != "patch":
    with open(main_versions_path) as json_file:
        old_versions = json.load(json_file)

    dev_compatible_version = ".".join(split_next_version[:2])
    stable_compatible_version = ".".join(split_version[:2])
    previous_compatible_version = old_versions[1]["name"].split(" ")[0]

    # Create new versions
    new_versions = [
        {"name": f"{dev_compatible_version} (dev)",
         "version": "dev/"},
        {"name": f"{stable_compatible_version} (stable)",
         "version": ""},
        {"name": previous_compatible_version,
         "version": f"{previous_compatible_version}/"},
        *old_versions[2:],
    ]
    with open(main_versions_path, 'w') as json_file:
        json.dump(new_versions, json_file, indent=4)
        json_file.write("\n")


# Update R package version script

with open(r_versions_path) as json_file:
    old_r_versions = json.load(json_file)

dev_r_version = f"{version}.9000"
release_r_version = version
previous_r_name = old_r_versions[1]["name"].split(" ")[0]
previous_r_version = ".".join(previous_r_name.split(".")[:2])

if release_type == "major":
    new_r_versions = [
        {"name": f"{dev_r_version} (dev)", "version": "dev/"},
        {"name": f"{release_r_version} (release)", "version": ""},
        {"name": previous_r_name, "version": f"{previous_r_version}/"},
        *old_r_versions[2:],
    ]
else:
    new_r_versions = [
        {"name": f"{dev_r_version} (dev)", "version": "dev/"},
        {"name": f"{release_r_version} (release)", "version": ""},
        *old_r_versions[2:],
    ]
with open(r_versions_path, 'w') as json_file:
    json.dump(new_r_versions, json_file, indent=4)
    json_file.write("\n")
