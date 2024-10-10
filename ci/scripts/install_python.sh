#!/usr/bin/env bash
#
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

set -eu

declare -A platforms
platforms=([windows]=Windows
           [macos]=macOS
           [linux]=Linux)

declare -A versions
versions=([3.9]=3.9.13
          [3.10]=3.10.11
          [3.11]=3.11.9
          [3.12]=3.12.9
          [3.13]=3.13.2
          [3.13t]=3.13.2)

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <platform> <version>"
  exit 1
elif [[ -z ${platforms[$1]} ]]; then
  echo "Unexpected platform: ${1}"
  exit 1
fi

platform=${platforms[$1]}
version=$2
full_version=${versions[$2]}

if [ $platform = "macOS" ]; then
    echo "Downloading Python installer..."

    if [ "$(uname -m)" = "x86_64" ] && [ "$version" = "3.9" ];
    then
        fname="python-${full_version}-macosx10.9.pkg"
    else
        fname="python-${full_version}-macos11.pkg"
    fi
    wget "https://www.python.org/ftp/python/${full_version}/${fname}"

    echo "Installing Python..."
    if [[ $2 == "3.13t" ]]; then
        # See https://github.com/python/cpython/issues/120098#issuecomment-2151122033 for more info on this.
        cat > ./choicechanges.plist <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<array>
        <dict>
                <key>attributeSetting</key>
                <integer>1</integer>
                <key>choiceAttribute</key>
                <string>selected</string>
                <key>choiceIdentifier</key>
                <string>org.python.Python.PythonTFramework-3.13</string>
        </dict>
</array>
</plist>
EOF
        installer -pkg $fname -applyChoiceChangesXML ./choicechanges.plist -target /
        rm ./choicechanges.plist
    else
        installer -pkg $fname -target /
    fi
    rm $fname

    python="/Library/Frameworks/Python.framework/Versions/${version}/bin/python${version}"
    if [[ $2 == "3.13t" ]]; then
        python="/Library/Frameworks/PythonT.framework/Versions/3.13/bin/python3.13t"
    fi

    echo "Installing Pip..."
    $python -m ensurepip
    $python -m pip install -U pip setuptools
else
    echo "Unsupported platform: $platform"
    exit 1
fi
