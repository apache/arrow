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

# Use this script to prevent errors in the pkgdown site being rendered due to missing YAML entries

# all .Rd files in the repo
all_rd_files=`find ./r/man -maxdepth 1 -name "*.Rd" | sed -e 's/.\/r\/man\///g' | sed -e 's/.Rd//g' | sort`

# .Rd files to exclude from search (i.e. are internal)
exclusions=`grep "\keyword{internal}" -rl ./r/man --include=*.Rd | sed -e 's/.\/r\/man\///g' | sed -e 's/.Rd//g' | sort`

# .Rd files to check against pkgdown.yml
rd_files=`echo ${exclusions[@]} ${all_rd_files[@]} | tr ' ' '\n' | sort | uniq -u`

# pkgdown sections
pkgdown_sections=`awk '/^[^ ]/{ f=/reference:/; next } f{ if (sub(/:$/,"")) pkg=$2; else print pkg, $2 }' ./r/_pkgdown.yml | grep -v "title:" | sort`

# get things that appear in man files that don't appear in pkgdown sections
pkgdown_missing=`echo ${pkgdown_sections[@]} ${pkgdown_sections[@]} ${rd_files[@]} | tr ' ' '\n' | sort | uniq -u`

# if any sections are missing raise an error
if ([ ${#pkgdown_missing} -ge 1 ]); then
	echo "Error! $pkgdown_missing missing from ./r/_pkgdown.yml"
  	exit 1 
fi
