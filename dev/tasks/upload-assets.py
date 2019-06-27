#!/usr/bin/env python

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

import os
import mimetypes
from glob import glob

import click
import github3


# TODO(kszucs): move it to crossbow.py


@click.command()
@click.option('--sha', help='Target committish')
@click.option('--tag', help='Target tag')
@click.option('--pattern', help='File pattern')
def upload_assets(tag, sha, pattern):
    token = os.environ['CROSSBOW_GITHUB_TOKEN']
    owner, repository = os.environ['CROSSBOW_GITHUB_REPO'].split('/')

    gh = github3.login(token=token)
    repo = gh.repository(owner, repository)
    click.echo('Selected repository: {}/{}'.format(owner, repository))

    try:
        release = repo.release_from_tag(tag)
    except github3.exceptions.NotFoundError:
        pass
    else:
        click.echo('Removing release `{}`'.format(release.tag_name))
        release.delete()

    click.echo('Creating release `{}`'.format(tag))
    release = repo.create_release(tag, sha)

    for path in glob(pattern):
        name = os.path.basename(path)
        mime = mimetypes.guess_type(name)[0] or 'application/octet-stream'

        click.echo('Uploading asset `{}`...'.format(name))
        with open(path, 'rb') as fp:
            release.upload_asset(name=name, asset=fp, content_type=mime)


if __name__ == '__main__':
    upload_assets()
