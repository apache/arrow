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

import warnings

# Miscellaneous utility code


def implements(f):
    def decorator(g):
        g.__doc__ = f.__doc__
        return g
    return decorator


def _deprecate_class(old_name, new_name, klass, next_version='0.5.0'):
    msg = ('pyarrow.{0} is deprecated as of {1}, please use {2} instead'
           .format(old_name, next_version, new_name))

    def deprecated_factory(*args, **kwargs):
        warnings.warn(msg, FutureWarning)
        return klass(*args)
    return deprecated_factory
