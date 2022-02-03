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
version = sys.argv[2]
r_version = sys.argv[3]

main_versions_path = dir_path + "/docs/source/_static/versions.json"
r_versions_path = dir_path + "/r/pkgdown/assets/versions.json"

# Update main docs version script

with open(main_versions_path) as json_file:
    old_versions = json.load(json_file)

split_version = version.split(".")
major_minor = split_version[0] + "." + split_version[1]
dev_version = str(int(split_version[0]) + 1) + ".0"
previous_major_minor = old_versions[1]["name"].split(" ")[0]

# Create new versions
new_versions = [
    {'name': dev_version + " (dev)", 'version': 'dev/'},
    {'name': major_minor + " (stable)", 'version': ''},
    {'name': previous_major_minor, 'version': f'{previous_major_minor}/'},
    *old_versions[2:],
]
with open(main_versions_path, 'w') as json_file:
    json.dump(new_versions, json_file, indent=4)
    json_file.write("\n")

# Update R package version script

with open(r_versions_path) as json_file:
    old_r_versions = json.load(json_file)

# update release to oldrel
old_r_versions[1]["name"] = old_r_versions[1]["name"].split(" ")[0]
old_rel_split = old_r_versions[1]["name"].split(".")
old_r_versions[1]["version"] = old_rel_split[0] + "." + old_rel_split[1] + "/"

# update dev to release
old_r_versions[0]["name"] = r_version + " (release)"
old_r_versions[0]["version"] = ""

# add new dev version
new_r_version = [{'name': r_version + ".9000" + " (dev)", 'version': 'dev/'}]
new_r_version.extend(old_r_versions)
with open(r_versions_path, 'w') as json_file:
    json.dump(new_r_version, json_file, indent=4)
    json_file.write("\n")
