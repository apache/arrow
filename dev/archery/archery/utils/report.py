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

from abc import ABCMeta, abstractmethod
import datetime

import jinja2


def markdown_escape(s):
    for char in ('*', '#', '_', '~', '`', '>'):
        s = s.replace(char, '\\' + char)
    return s


class Report(metaclass=ABCMeta):

    def __init__(self, **kwargs):
        for field in self.fields:
            if field not in kwargs:
                raise ValueError('Missing keyword argument {}'.format(field))
        self._data = kwargs

    def __getattr__(self, key):
        return self._data[key]

    @abstractmethod
    def fields(self):
        pass

    @property
    @abstractmethod
    def templates(self):
        pass


class JinjaReport(Report):

    def __init__(self, **kwargs):
        self.env = jinja2.Environment(
            loader=jinja2.PackageLoader('archery', 'templates')
        )
        self.env.filters['md'] = markdown_escape
        self.env.globals['today'] = datetime.date.today
        super().__init__(**kwargs)

    def render(self, template_name):
        template_path = self.templates[template_name]
        template = self.env.get_template(template_path)
        return template.render(**self._data)
