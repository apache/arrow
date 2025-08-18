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

import click

from ..compat import _get_module
from .logger import logger
from ..lang.python import NumpyDoc


class LintValidationException(Exception):
    pass


class LintResult:
    def __init__(self, success, reason=None):
        self.success = success

    def ok(self):
        if not self.success:
            raise LintValidationException

    @staticmethod
    def from_cmd(command_result):
        return LintResult(command_result.returncode == 0)


def python_numpydoc(symbols=None, allow_rules=None, disallow_rules=None):
    """Run numpydoc linter on python.

    Pyarrow must be available for import.
    """
    logger.info("Running Python docstring linters")
    # by default try to run on all pyarrow package
    symbols = symbols or {
        'pyarrow',
        'pyarrow.compute',
        'pyarrow.csv',
        'pyarrow.dataset',
        'pyarrow.feather',
        # 'pyarrow.flight',
        'pyarrow.fs',
        'pyarrow.gandiva',
        'pyarrow.ipc',
        'pyarrow.json',
        'pyarrow.orc',
        'pyarrow.parquet',
        'pyarrow.types',
    }
    try:
        numpydoc = NumpyDoc(symbols)
    except RuntimeError as e:
        logger.error(str(e))
        yield LintResult(success=False)
        return

    results = numpydoc.validate(
        # limit the validation scope to the pyarrow package
        from_package='pyarrow',
        allow_rules=allow_rules,
        disallow_rules=disallow_rules
    )

    if len(results) == 0:
        yield LintResult(success=True)
        return

    number_of_violations = 0
    for obj, result in results:
        errors = result['errors']

        # inspect doesn't play nice with cython generated source code,
        # to use a hacky way to represent a proper __qualname__
        doc = getattr(obj, '__doc__', '')
        name = getattr(obj, '__name__', '')
        qualname = getattr(obj, '__qualname__', '')
        module = _get_module(obj, default='')
        instance = getattr(obj, '__self__', '')
        if instance:
            klass = instance.__class__.__name__
        else:
            klass = ''

        try:
            cython_signature = doc.splitlines()[0]
        except Exception:
            cython_signature = ''

        desc = '.'.join(filter(None, [module, klass, qualname or name]))

        click.echo()
        click.echo(click.style(desc, bold=True, fg='yellow'))
        if cython_signature:
            qualname_with_signature = '.'.join([module, cython_signature])
            click.echo(
                click.style(
                    f'-> {qualname_with_signature}',
                    fg='yellow'
                )
            )

        for error in errors:
            number_of_violations += 1
            click.echo(f'{error[0]}: {error[1]}')

    msg = f'Total number of docstring violations: {number_of_violations}'
    click.echo()
    click.echo(click.style(msg, fg='red'))

    yield LintResult(success=False)
