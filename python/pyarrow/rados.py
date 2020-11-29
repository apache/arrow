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


import urllib
import ast
import rados
try:
    from pyarrow._rados import RadosDatasetFactoryOptions
except ImportError:
    raise ImportError(
        "The pyarrow installation is not built with support for rados."
    )


_RADOS_URI_SCHEME = 'rados'


def generate_uri(
        ceph_config_path='/etc/ceph/ceph.conf',
        cluster='ceph',
        pool='test-pool',
        objects=[],
        username=None,
        flags=None):
    params = {}
    params['ids'] = urllib.parse.quote(str(objects), safe='')
    if username:
        params['username'] = username
    if flags:
        params['flags'] = flags
    params['pool'] = pool
    params['cluster'] = cluster
    query = urllib.parse.urlencode(params)
    return "{}://{}?{}".format(_RADOS_URI_SCHEME, ceph_config_path, query)


def parse_uri(uri):
    if not is_valid_rados_uri(uri):
        return None
    url_object = urllib.parse.urlparse(uri)
    params = urllib.parse.parse_qs(url_object.query)
    args = {}
    args['ceph_config_path'] = url_object.path
    if 'cluster' in params:
        args['cluster_name'] = params['cluster'][0]
    if 'pool' in params:
        args['pool_name'] = params['pool'][0]
    if 'username' in params:
        args['user_name'] = params['username'][0]
    if 'ids' in params:
        ids = ast.literal_eval(urllib.parse.unquote(params['ids'][0]))
        args['objects'] = ids
    if 'flags' in params:
        args['flags'] = int(params['flags'][0])
    options = RadosDatasetFactoryOptions(**args)
    return options


def is_valid_rados_uri(uri):
    url_object = urllib.parse.urlparse(uri)
    if url_object.scheme == _RADOS_URI_SCHEME:
        params = urllib.parse.parse_qs(url_object.query)
        if params['cluster'] and params['pool']:
            if _is_valid_ceph_conf(url_object.netloc):
                return True
    return False


def _is_valid_ceph_conf(path):
    try:
        cluster = rados.Rados(conffile=path)
        cluster.version()
    except ImportError:
        return False
    return True
