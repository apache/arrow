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

import fnmatch
import re
from xml.etree import ElementTree

from ..lang.java import Jar
from .cache import Cache
from .command import capture_stdout

RAT_VERSION = 0.13
RAT_JAR_FILENAME = "apache-rat-{}.jar".format(RAT_VERSION)
RAT_URL_ = "https://repo1.maven.org/maven2/org/apache/rat/apache-rat"
RAT_URL = "/".join([RAT_URL_, str(RAT_VERSION), RAT_JAR_FILENAME])


class Rat(Jar):
    def __init__(self):
        jar = Cache().get_or_insert_from_url(RAT_JAR_FILENAME, RAT_URL)
        Jar.__init__(self, jar)

    @capture_stdout(strip=False)
    def run_report(self, archive_path, **kwargs):
        return self.run("--xml", archive_path, **kwargs)

    def report(self, archive_path, **kwargs):
        return RatReport(self.run_report(archive_path, **kwargs))


def exclusion_from_globs(exclusions_path):
    with open(exclusions_path, 'r') as exclusions_fd:
        exclusions = [e.strip() for e in exclusions_fd]
        return lambda path: any([fnmatch.fnmatch(path, e) for e in exclusions])


class RatReport:
    def __init__(self, xml):
        self.xml = xml
        self.tree = ElementTree.fromstring(xml)

    def __repr__(self):
        return "RatReport({})".format(self.xml)

    def validate(self, exclusion=None):
        for r in self.tree.findall('resource'):
            approvals = r.findall('license-approval')
            if not approvals or approvals[0].attrib['name'] == 'true':
                continue

            clean_name = re.sub('^[^/]+/', '', r.attrib['name'])

            if exclusion and exclusion(clean_name):
                continue

            yield clean_name
