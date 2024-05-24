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
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

import datetime
import os
import sys
import warnings
from unittest import mock
from docutils.parsers.rst import Directive, directives

sys.path.extend([
    os.path.join(os.path.dirname(__file__),
                 '..', '../..')

])

# -- Customization --------------------------------------------------------

try:
    import pyarrow
    exclude_patterns = []
    pyarrow_version =  pyarrow.__version__

    # Conditional API doc generation

    # Sphinx has two features for conditional inclusion:
    # - The "only" directive
    #   https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#including-content-based-on-tags
    # - The "ifconfig" extension
    #   https://www.sphinx-doc.org/en/master/usage/extensions/ifconfig.html
    #
    # Both have issues, but "ifconfig" seems to work in this setting.

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

    try:
        import pyarrow.orc
        orc_enabled = True
    except ImportError:
        orc_enabled = False
        pyarrow.orc = sys.modules['pyarrow.orc'] = mock.Mock()

    try:
        import pyarrow.parquet.encryption
        parquet_encryption_enabled = True
    except ImportError:
        parquet_encryption_enabled = False
        pyarrow.parquet.encryption = sys.modules['pyarrow.parquet.encryption'] = mock.Mock()
except (ImportError, LookupError):
    exclude_patterns = ['python']
    pyarrow_version =  ""
    cuda_enabled = False
    flight_enabled = False

# Suppresses all warnings printed when sphinx is traversing the code (e.g.
# deprecation warnings)
warnings.filterwarnings("ignore", category=FutureWarning, message=".*pyarrow.*")

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'breathe',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive',
    'myst_parser',
    'numpydoc',
    'sphinx_design',
    'sphinx_copybutton',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.ifconfig',
    'sphinx.ext.intersphinx',
    'sphinx.ext.mathjax',
    'sphinx.ext.viewcode',
    'sphinxcontrib.mermaid',
]

# Show members for classes in .. autosummary
autodoc_default_options = {
    'members': None,
    'special-members': '__dataframe__',
    'undoc-members': None,
    'show-inheritance': None,
    'inherited-members': None
}

# Breathe configuration
breathe_projects = {
    "arrow_cpp": os.environ.get("ARROW_CPP_DOXYGEN_XML", "../../cpp/apidoc/xml"),
}
breathe_default_project = "arrow_cpp"

# Overridden conditionally below
autodoc_mock_imports = []

# copybutton configuration
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: "
copybutton_prompt_is_regexp = True
copybutton_line_continuation_character = "\\"

# ipython directive options
ipython_mplbackend = ''

# MyST-Parser configuration
myst_enable_extensions = [
    'amsmath',
    'attrs_inline',
    # 'colon_fence',
    'deflist',
    'dollarmath',
    'fieldlist',
    'html_admonition',
    'html_image',
    'linkify',
    # 'replacements',
    # 'smartquotes',
    'strikethrough',
    'substitution',
    'tasklist',
]

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


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#

source_suffix = {
    # We need to keep "'.rst': 'restructuredtext'" as the first item.
    # This is a workaround of
    # https://github.com/sphinx-doc/sphinx/issues/12147 .
    #
    # We can sort these items in alphabetical order with Sphinx 7.3.0
    # or later that will include the fix of this problem.
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

autosummary_generate = True

# The encoding of source files.
#
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Apache Arrow'
copyright = (
    f"2016-{datetime.datetime.now().year} Apache Software Foundation.\n"
    "Apache Arrow, Arrow, Apache, the Apache feather logo, and the Apache Arrow "
    "project logo are either registered trademarks or trademarks of The Apache "
    "Software Foundation in the United States and other countries"
)
author = u'Apache Software Foundation'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = os.environ.get('ARROW_DOCS_VERSION', pyarrow_version)

# The full version, including alpha/beta/rc tags.
release = os.environ.get('ARROW_DOCS_VERSION', pyarrow_version)

if "+" in release:
    release = release.split(".dev")[0] + " (dev)"

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = "en"

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
#
# today = ''
#
# Else, today_fmt is used as the format for a strftime call.
#
# today_fmt = '%B %d, %Y'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = exclude_patterns + ['_build', 'Thumbs.db', '.DS_Store']

# The reST default role (used for this markup: `text`) to use for all
# documents.
#
# default_role = None

# If true, '()' will be appended to :func: etc. cross-reference text.
#
# add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
#
# add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
#
# show_authors = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

# If true, keep warnings as "system message" paragraphs in the built documents.
# keep_warnings = False

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'pandas': ('https://pandas.pydata.org/docs/', None)
}


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'pydata_sphinx_theme'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#

switcher_version = version
if ".dev" in version:
    switcher_version = "dev/"
else:
    # If we are not building dev version of the docs, we are building
    # docs for the stable version
    switcher_version = ""

html_theme_options = {
    "show_toc_level": 2,
    "use_edit_page_button": True,
    "logo": {
      "image_light": "_static/arrow.png",
      "image_dark": "_static/arrow-dark.png",
    },
    "header_links_before_dropdown": 2,
    "header_dropdown_text": "Implementations",
    "navbar_end": ["version-switcher", "theme-switcher", "navbar-icon-links"],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/apache/arrow",
            "icon": "fa-brands fa-square-github",
        },
        {
            "name": "X",
            "url": "https://twitter.com/ApacheArrow",
            "icon": "fa-brands fa-square-x-twitter",
        },
    ],
    "show_version_warning_banner": True,
    "switcher": {
        "json_url": "/docs/_static/versions.json",
        "version_match": switcher_version,
    },
}

html_context = {
    "github_user": "apache",
    "github_repo": "arrow",
    "github_version": "main",
    "doc_path": "docs/source",
}

# Add any paths that contain custom themes here, relative to this directory.
# html_theme_path = []

# The name for this set of Sphinx documents.
# "<project> v<release> documentation" by default.
#
html_title = u'Apache Arrow v{}'.format(version)

# A shorter title for the navigation bar.  Default is the same as html_title.
#
# html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
#
# html_logo = "_static/arrow.png"

# The name of an image file (relative to this directory) to use as a favicon of
# the docs.  This file should be a Windows icon file (.ico) being 16x16 or
# 32x32 pixels large.
#
html_favicon = "_static/favicon.ico"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Custom fixes to the RTD theme
html_css_files = ['theme_overrides.css']

# Add any extra paths that contain custom files (such as robots.txt or
# .htaccess) here, relative to this directory. These files are copied
# directly to the root of the documentation.
#
# html_extra_path = []

# If not None, a 'Last updated on:' timestamp is inserted at every page
# bottom, using the given strftime format.
# The empty string is equivalent to '%b %d, %Y'.
#
# html_last_updated_fmt = None

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
#
# html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
#
# html_sidebars = {
#    '**': ['sidebar-logo.html', 'sidebar-search-bs.html', 'sidebar-nav-bs.html'],
# }

# The base URL which points to the root of the HTML documentation,
# used for canonical url
html_baseurl = "https://arrow.apache.org/docs/"

# Additional templates that should be rendered to pages, maps page names to
# template names.
#
# html_additional_pages = {}

# If false, no module index is generated.
#
# html_domain_indices = True

# If false, no index is generated.
#
# html_use_index = True

# If true, the index is split into individual pages for each letter.
#
# html_split_index = False

# If true, links to the reST sources are added to the pages.
#
html_show_sourcelink = False

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
#
# html_show_sphinx = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
#
# html_show_copyright = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
#
# html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
# html_file_suffix = None

# Language to be used for generating the HTML full-text search index.
# Sphinx supports the following languages:
#   'da', 'de', 'en', 'es', 'fi', 'fr', 'hu', 'it', 'ja'
#   'nl', 'no', 'pt', 'ro', 'ru', 'sv', 'tr', 'zh'
#
# html_search_language = 'en'

# A dictionary with options for the search language support, empty by default.
# 'ja' uses this config value.
# 'zh' user can custom change `jieba` dictionary path.
#
# html_search_options = {'type': 'default'}

# The name of a javascript file (relative to the configuration directory) that
# implements a search results scorer. If empty, the default will be used.
#
# html_search_scorer = 'scorer.js'

# Output file base name for HTML help builder.
htmlhelp_basename = 'arrowdoc'

# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
     # The paper size ('letterpaper' or 'a4paper').
     #
     # 'papersize': 'letterpaper',

     # The font size ('10pt', '11pt' or '12pt').
     #
     # 'pointsize': '10pt',

     # Additional stuff for the LaTeX preamble.
     #
     # 'preamble': '',

     # Latex figure (float) alignment
     #
     # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'arrow.tex', u'Apache Arrow Documentation',
     u'Apache Arrow Team', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#
# latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#
# latex_use_parts = False

# If true, show page references after internal links.
#
# latex_show_pagerefs = False

# If true, show URL addresses after external links.
#
# latex_show_urls = False

# Documents to append as an appendix to all manuals.
#
# latex_appendices = []

# It false, will not define \strong, \code, \titleref, \crossref ... but only
# \sphinxstrong, ..., \sphinxtitleref, ... To help avoid clash with user added
# packages.
#
# latex_keep_old_macro_names = True

# If false, no module index is generated.
#
# latex_domain_indices = True


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'arrow', u'Apache Arrow Documentation',
     [author], 1)
]

# If true, show URL addresses after external links.
#
# man_show_urls = False


# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'arrow', u'Apache Arrow Documentation',
     author, 'Apache Arrow', 'One line description of project.',
     'Miscellaneous'),
]

# Documents to append as an appendix to all manuals.
#
# texinfo_appendices = []

# If false, no module index is generated.
#
# texinfo_domain_indices = True

# How to display URL addresses: 'footnote', 'no', or 'inline'.
#
# texinfo_show_urls = 'footnote'

# If true, do not generate a @detailmenu in the "Top" node's menu.
#
# texinfo_no_detailmenu = False

# -- Options for mermaid output -------------------------------------------

mermaid_output_format = 'svg'

def setup(app):
    # Use a config value to indicate whether CUDA API docs can be generated.
    # This will also rebuild appropriately when the value changes.
    app.add_config_value('cuda_enabled', cuda_enabled, 'env')
    app.add_config_value('flight_enabled', flight_enabled, 'env')
    app.add_directive('arrow-computefuncs', ComputeFunctionsTableDirective)


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
