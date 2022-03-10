# -*- coding: utf-8 -*-
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

import os
import sys
import warnings
from unittest import mock
from docutils.parsers.rst import Directive, directives


sys.path.extend([
    os.path.join(os.path.dirname(__file__), '..')
])

from conf_common import *


# Suppresses all warnings printed when sphinx is traversing the code (e.g.
# deprecation warnings)
warnings.filterwarnings("ignore", category=FutureWarning, message=".*pyarrow.*")


# Paths that need to be adjusted
templates_path = ['../_templates']
html_logo = "../_static/arrow.png"
html_favicon = "../_static/favicon.ico"
html_static_path = ['../_static']


# Additional extensions
extensions = extensions + [
    'IPython.sphinxext.ipython_directive',
    'IPython.sphinxext.ipython_console_highlighting',
    'numpydoc',
]

# ipython directive options
ipython_mplbackend = ''

# numpydoc configuration
numpydoc_xref_param_type = True
numpydoc_show_class_members = False
numpydoc_xref_ignore = {
    "or", "and", "of", "if", "default", "optional", "object",
    "dicts", "rows", "Python", "source", "filesystem",
    "dataset", "datasets",
    # TODO those one could be linked to a glossary or python docs?
    "file", "path", "paths", "mapping", "Mapping", "URI", "function",
    "iterator", "Iterator",
    # TODO this term is used regularly, but isn't actually exposed (base class)
    "RecordBatchReader",
    # additional ignores that could be fixed by rewriting the docstrings
    "other", "supporting", "buffer", "protocol",  # from Codec / pa.compress
    "depends", "on", "inputs",  # pyarrow.compute
    "values", "coercible", "to", "arrays",  # pa.chunked_array, Table methods
    "depending",  # to_pandas
}
numpydoc_xref_aliases = {
    "array-like": ":func:`array-like <pyarrow.array>`",
    "Array": "pyarrow.Array",
    "Schema": "pyarrow.Schema",
    "RecordBatch": "pyarrow.RecordBatch",
    "Table": "pyarrow.Table",
    "MemoryPool": "pyarrow.MemoryPool",
    "NativeFile": "pyarrow.NativeFile",
    "FileSystem": "pyarrow.fs.FileSystem",
    "FileType": "pyarrow.fs.FileType",
}

# -- Options for HTML output ----------------------------------------------

html_context["doc_path"] = "docs/source/python"
html_context["subproject"] = "Python"

# The base URL which points to the root of the HTML documentation,
# used for canonical url
html_baseurl = "https://arrow.apache.org/docs/python"


intersphinx_mapping["arrow"] = ("https://arrow.apache.org/docs/", ('../../_build/html/objects.inv', None))
intersphinx_mapping["cpp"] = ("https://arrow.apache.org/docs/cpp/", ('../../_build/html/cpp/objects.inv', None))


# Conditional API doc generation

# Sphinx has two features for conditional inclusion:
# - The "only" directive
#   https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#including-content-based-on-tags
# - The "ifconfig" extension
#   https://www.sphinx-doc.org/en/master/usage/extensions/ifconfig.html
#
# Both have issues, but "ifconfig" seems to work in this setting.

import pyarrow


try:
    import pyarrow.cuda
    cuda_enabled = True
except ImportError:
    cuda_enabled = False
    # Mock pyarrow.cuda to avoid autodoc warnings.
    # XXX I can't get autodoc_mock_imports to work, so mock manually instead
    # (https://github.com/sphinx-doc/sphinx/issues/2174#issuecomment-453177550)
    pyarrow.cuda = sys.modules['pyarrow.cuda'] = mock.Mock()

try:
    import pyarrow.flight
    flight_enabled = True
except ImportError:
    flight_enabled = False
    pyarrow.flight = sys.modules['pyarrow.flight'] = mock.Mock()


def setup(app):
    # Use a config value to indicate whether CUDA API docs can be generated.
    # This will also rebuild appropriately when the value changes.
    app.add_config_value('cuda_enabled', cuda_enabled, 'env')
    app.add_config_value('flight_enabled', flight_enabled, 'env')
    app.add_directive('arrow-computefuncs', ComputeFunctionsTableDirective)
    app.connect("html-page-context", setup_dropdown_href)


class ComputeFunctionsTableDirective(Directive):
    """Generate a table of Arrow compute functions.

    .. arrow-computefuncs::
        :kind: hash_aggregate

    The generated table will include function name,
    description and option class reference.

    The functions listed in the table can be restricted
    with the :kind: option.
    """
    has_content = True
    option_spec = {
        "kind": directives.unchanged
    }

    def run(self):
        from docutils.statemachine import ViewList
        from docutils import nodes
        import pyarrow.compute as pc

        result = ViewList()
        function_kind = self.options.get('kind', None)

        result.append(".. csv-table::", "<computefuncs>")
        result.append("   :widths: 20, 60, 20", "<computefuncs>")
        result.append("   ", "<computefuncs>")
        for fname in pc.list_functions():
            func = pc.get_function(fname)
            option_class = ""
            if func._doc.options_class:
                option_class = f":class:`{func._doc.options_class}`"
            if not function_kind or func.kind == function_kind:
                result.append(
                    f'   "{fname}", "{func._doc.summary}", "{option_class}"',
                    "<computefuncs>"
                )

        node = nodes.section()
        node.document = self.state.document
        self.state.nested_parse(result, 0, node)
        return node.children
