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

import importlib

import click

from .source import ArrowSources, InvalidArrowSource


class ArrowBool(click.types.BoolParamType):
    """
    ArrowBool supports the 'ON' and 'OFF' values on top of the values
    supported by BoolParamType. This is convenient to port script which exports
    CMake options variables.
    """
    name = "boolean"

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            lowered = value.lower()
            if lowered == "on":
                return True
            elif lowered == "off":
                return False

        return super().convert(value, param, ctx)


def validate_arrow_sources(ctx, param, src):
    """
    Ensure a directory contains Arrow cpp sources.
    """
    try:
        return ArrowSources.find(src)
    except InvalidArrowSource as e:
        raise click.BadParameter(str(e))


def add_optional_command(name, module, function, parent):
    try:
        module = importlib.import_module(module, package="archery")
        command = getattr(module, function)
    except ImportError as exc:
        error_message = exc.name

        @parent.command(
            name,
            context_settings={
                "allow_extra_args": True,
                "ignore_unknown_options": True,
            }
        )
        def command():
            raise click.ClickException(
                f"Couldn't import command `{name}` due to {error_message}"
            )
    else:
        parent.add_command(command)
