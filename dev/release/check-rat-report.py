#!/usr/bin/python
##############################################################################
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
##############################################################################
import fnmatch
import re
import sys
import xml.etree.ElementTree as ET

if len(sys.argv) != 3:
    sys.stderr.write("Usage: %s exclude_globs.lst rat_report.xml\n" %
                     sys.argv[0])
    sys.exit(1)

exclude_globs_filename = sys.argv[1]
xml_filename = sys.argv[2]

globs = [line.strip() for line in open(exclude_globs_filename, "r")]

tree = ET.parse(xml_filename)
root = tree.getroot()
resources = root.findall('resource')

all_ok = True
for r in resources:
    approvals = r.findall('license-approval')
    if not approvals or approvals[0].attrib['name'] == 'true':
        continue
    clean_name = re.sub('^[^/]+/', '', r.attrib['name'])
    excluded = False
    for g in globs:
        if fnmatch.fnmatch(clean_name, g):
            excluded = True
            break
    if not excluded:
        sys.stdout.write("NOT APPROVED: %s (%s): %s\n" % (
            clean_name, r.attrib['name'], approvals[0].attrib['name']))
        all_ok = False

if not all_ok:
    sys.exit(1)

print('OK')
sys.exit(0)
