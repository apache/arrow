#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re

import argparse
import concurrent.futures as cf
import functools
import hashlib
import json
import os
import subprocess
import urllib.request


BINTRAY_API_ROOT = "https://bintray.com/api/v1"
BINTRAY_DL_ROOT = "https://dl.bintray.com"
BINTRAY_REPO = os.getenv('BINTRAY_REPOSITORY', 'apache/arrow')
DEFAULT_PARALLEL_DOWNLOADS = 8


class Bintray:

    def __init__(self, repo=BINTRAY_REPO):
        self.repo = repo

    def get_file_list(self, package, version):
        url = os.path.join(BINTRAY_API_ROOT, 'packages', self.repo, package,
                           'versions', version, 'files')
        request = urllib.request.urlopen(url).read()
        return json.loads(request)

    def download_files(self, files, dest=None, num_parallel=None,
                       re_match=None):
        """
        Download files from Bintray in parallel. If file already exists, will
        overwrite if the checksum does not match what Bintray says it should be

        Parameters
        ----------
        files : List[Dict]
            File listing from Bintray
        dest : str, default None
            Defaults to current working directory
        num_parallel : int, default 8
            Number of files to download in parallel. If set to None, uses
            default
        """
        if dest is None:
            dest = os.getcwd()
        if num_parallel is None:
            num_parallel = DEFAULT_PARALLEL_DOWNLOADS

        if re_match is not None:
            regex = re.compile(re_match)
            files = [x for x in files if regex.match(x['path'])]

        if num_parallel == 1:
            for path in files:
                self._download_file(dest, path)
        else:
            parallel_map_terminate_early(
                functools.partial(self._download_file, dest),
                files,
                num_parallel
            )

    def _download_file(self, dest, info):
        relpath = info['path']

        base, filename = os.path.split(relpath)

        dest_dir = os.path.join(dest, base)
        os.makedirs(dest_dir, exist_ok=True)

        dest_path = os.path.join(dest_dir, filename)

        if os.path.exists(dest_path):
            with open(dest_path, 'rb') as f:
                sha256sum = hashlib.sha256(f.read()).hexdigest()
            if sha256sum == info['sha256']:
                print('Local file {} sha256 matches, skipping'
                      .format(dest_path))
                return
            else:
                print('Local file sha256 does not match, overwriting')

        print("Downloading {} to {}".format(relpath, dest_path))

        bintray_abspath = os.path.join(BINTRAY_DL_ROOT, self.repo, relpath)

        cmd = [
            'curl', '--fail', '--location', '--retry', '5',
            '--output', dest_path, bintray_abspath
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise Exception("Downloading {} failed\nstdout: {}\nstderr: {}"
                            .format(relpath, stdout, stderr))


def parallel_map_terminate_early(f, iterable, num_parallel):
    tasks = []
    with cf.ProcessPoolExecutor(num_parallel) as pool:
        for v in iterable:
            tasks.append(pool.submit(functools.partial(f, v)))

        for task in cf.as_completed(tasks):
            if task.exception() is not None:
                e = task.exception()
                for task in tasks:
                    task.cancel()
                raise e


ARROW_PACKAGE_TYPES = ['centos', 'debian', 'nuget', 'python', 'ubuntu']


def download_rc_binaries(version, rc_number, re_match=None, dest=None,
                         num_parallel=None):
    bintray = Bintray()

    version_string = '{}-rc{}'.format(version, rc_number)
    for package_type in ARROW_PACKAGE_TYPES:
        files = bintray.get_file_list('{}-rc'.format(package_type),
                                      version_string)
        bintray.download_files(files, re_match=re_match, dest=dest,
                               num_parallel=num_parallel)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download release candidate binaries'
    )
    parser.add_argument('version', type=str, help='The version number')
    parser.add_argument('rc_number', type=int,
                        help='The release candidate number, e.g. 0, 1, etc')
    parser.add_argument('-e', '--regexp', type=str, default=None,
                        help=('Regular expression to match on file names '
                              'to only download certain files'))
    parser.add_argument('--dest', type=str, default=os.getcwd(),
                        help='The output folder for the downloaded files')
    parser.add_argument('--num_parallel', type=int, default=8,
                        help='The number of concurrent downloads to do')
    args = parser.parse_args()

    download_rc_binaries(args.version, args.rc_number, dest=args.dest,
                         re_match=args.regexp, num_parallel=args.num_parallel)
