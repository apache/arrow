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
# Makefile for Sphinx documentation
#

# You can set these variables from the command line.

# Do not fail the build if there are warnings
# SPHINXOPTS    = -j8 -W
SPHINXOPTS    = -j8

SPHINXBUILD   = sphinx-build
SPHINXAUTOBUILD = sphinx-autobuild
PAPER         =
BUILDDIR      = _build
SOURCEDIR     = source

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR)
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) source
# sphinx-autobuild needs an full absolute path for the ignored files
CURRENT_DIR     = $(shell pwd)
IGNORE_DIR      = $(CURRENT_DIR)/source/python/generated/*.rst

.PHONY: help
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html           to make standalone HTML files"
	@echo "  dev            to make only the developers documentation"
	@echo "  format         to make only the format documentation"
	@echo "  cpp            to make only the C++ documentation"
	@echo "  python         to make only the Python documentation"
	@echo "  dirhtml        to make HTML files named index.html in directories"
	@echo "  singlehtml     to make a single large HTML file"
	@echo "  pickle         to make pickle files"
	@echo "  json           to make JSON files"
	@echo "  htmlhelp       to make HTML files and a HTML help project"
	@echo "  qthelp         to make HTML files and a qthelp project"
	@echo "  applehelp      to make an Apple Help Book"
	@echo "  devhelp        to make HTML files and a Devhelp project"
	@echo "  epub           to make an epub"
	@echo "  epub3          to make an epub3"
	@echo "  latex          to make LaTeX files, you can set PAPER=a4 or PAPER=letter"
	@echo "  latexpdf       to make LaTeX files and run them through pdflatex"
	@echo "  latexpdfja     to make LaTeX files and run them through platex/dvipdfmx"
	@echo "  text           to make text files"
	@echo "  man            to make manual pages"
	@echo "  texinfo        to make Texinfo files"
	@echo "  info           to make Texinfo files and run them through makeinfo"
	@echo "  gettext        to make PO message catalogs"
	@echo "  changes        to make an overview of all changed/added/deprecated items"
	@echo "  xml            to make Docutils-native XML files"
	@echo "  pseudoxml      to make pseudoxml-XML files for display purposes"
	@echo "  linkcheck      to check all external links for integrity"
	@echo "  doctest        to run all doctests embedded in the documentation (if enabled)"
	@echo "  coverage       to run coverage check of the documentation (if enabled)"
	@echo "  dummy          to check syntax errors of document sources"
	@echo "  java_tutorial  to make only the Java Tutorial documentation"
	@echo "  java_dev       to make only the Java Development documentation"


.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
	rm -rf source/python/generated/*

.PHONY: clean-dev
clean-dev:
	rm -rf $(BUILDDIR)/html/developers

.PHONY: clean-format
clean-format:
	rm -rf $(BUILDDIR)/html/format

.PHONY: clean-cpp
clean-cpp:
	rm -rf $(BUILDDIR)/html/cpp

.PHONY: clean-python
clean-python:
	rm -rf $(BUILDDIR)/html/python
	rm -rf source/python/generated/*

.PHONY: html
html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) source $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: html-live
html-live:
	$(SPHINXAUTOBUILD) -b html $(SPHINXOPTS) source $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: dev
dev:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/developers $(BUILDDIR)/html/developers
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/developers."

.PHONY: dev-live
dev-live:
	$(SPHINXAUTOBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/developers $(BUILDDIR)/html/developers
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/developers."

.PHONY: format
format:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/format $(BUILDDIR)/html/format
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/format."

.PHONY: format-live
format-live:
	$(SPHINXAUTOBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/format $(BUILDDIR)/html/format
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/format."

.PHONY: cpp
cpp:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/cpp $(BUILDDIR)/html/cpp
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/cpp."

.PHONY: cpp-live
cpp-live:
	$(SPHINXAUTOBUILD) -c $(SOURCEDIR) $(SOURCEDIR)/cpp $(BUILDDIR)/html/cpp
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/cpp."

.PHONY: python
python:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/python $(BUILDDIR)/html/python
	@echo
	@echo "Build finished. The HTML files are in $(BUILDDIR)/html/python"

.PHONY: python-live
python-live:
	$(SPHINXAUTOBUILD) -b html $(SPHINXOPTS) --ignore $(IGNORE_DIR) -c $(SOURCEDIR) $(SOURCEDIR)/python $(BUILDDIR)/html/python
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html/python."

.PHONY: dirhtml
dirhtml:
	$(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: singlehtml
singlehtml:
	$(SPHINXBUILD) -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."

.PHONY: pickle
pickle:
	$(SPHINXBUILD) -b pickle $(ALLSPHINXOPTS) $(BUILDDIR)/pickle
	@echo
	@echo "Build finished; now you can process the pickle files."

.PHONY: json
json:
	$(SPHINXBUILD) -b json $(ALLSPHINXOPTS) $(BUILDDIR)/json
	@echo
	@echo "Build finished; now you can process the JSON files."

.PHONY: htmlhelp
htmlhelp:
	$(SPHINXBUILD) -b htmlhelp $(ALLSPHINXOPTS) $(BUILDDIR)/htmlhelp
	@echo
	@echo "Build finished; now you can run HTML Help Workshop with the" \
	      ".hhp project file in $(BUILDDIR)/htmlhelp."

.PHONY: qthelp
qthelp:
	$(SPHINXBUILD) -b qthelp $(ALLSPHINXOPTS) $(BUILDDIR)/qthelp
	@echo
	@echo "Build finished; now you can run "qcollectiongenerator" with the" \
	      ".qhcp project file in $(BUILDDIR)/qthelp, like this:"
	@echo "# qcollectiongenerator $(BUILDDIR)/qthelp/pyarrow.qhcp"
	@echo "To view the help file:"
	@echo "# assistant -collectionFile $(BUILDDIR)/qthelp/pyarrow.qhc"

.PHONY: applehelp
applehelp:
	$(SPHINXBUILD) -b applehelp $(ALLSPHINXOPTS) $(BUILDDIR)/applehelp
	@echo
	@echo "Build finished. The help book is in $(BUILDDIR)/applehelp."
	@echo "N.B. You won't be able to view it unless you put it in" \
	      "~/Library/Documentation/Help or install it in your application" \
	      "bundle."

.PHONY: devhelp
devhelp:
	$(SPHINXBUILD) -b devhelp $(ALLSPHINXOPTS) $(BUILDDIR)/devhelp
	@echo
	@echo "Build finished."
	@echo "To view the help file:"
	@echo "# mkdir -p $$HOME/.local/share/devhelp/pyarrow"
	@echo "# ln -s $(BUILDDIR)/devhelp $$HOME/.local/share/devhelp/pyarrow"
	@echo "# devhelp"

.PHONY: epub
epub:
	$(SPHINXBUILD) -b epub $(ALLSPHINXOPTS) $(BUILDDIR)/epub
	@echo
	@echo "Build finished. The epub file is in $(BUILDDIR)/epub."

.PHONY: epub3
epub3:
	$(SPHINXBUILD) -b epub3 $(ALLSPHINXOPTS) $(BUILDDIR)/epub3
	@echo
	@echo "Build finished. The epub3 file is in $(BUILDDIR)/epub3."

.PHONY: latex
latex:
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) $(BUILDDIR)/latex
	@echo
	@echo "Build finished; the LaTeX files are in $(BUILDDIR)/latex."
	@echo "Run \`make' in that directory to run these through (pdf)latex" \
	      "(use \`make latexpdf' here to do that automatically)."

.PHONY: latexpdf
latexpdf:
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) $(BUILDDIR)/latex
	@echo "Running LaTeX files through pdflatex..."
	$(MAKE) -C $(BUILDDIR)/latex all-pdf
	@echo "pdflatex finished; the PDF files are in $(BUILDDIR)/latex."

.PHONY: latexpdfja
latexpdfja:
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) $(BUILDDIR)/latex
	@echo "Running LaTeX files through platex and dvipdfmx..."
	$(MAKE) -C $(BUILDDIR)/latex all-pdf-ja
	@echo "pdflatex finished; the PDF files are in $(BUILDDIR)/latex."

.PHONY: text
text:
	$(SPHINXBUILD) -b text $(ALLSPHINXOPTS) $(BUILDDIR)/text
	@echo
	@echo "Build finished. The text files are in $(BUILDDIR)/text."

.PHONY: man
man:
	$(SPHINXBUILD) -b man $(ALLSPHINXOPTS) $(BUILDDIR)/man
	@echo
	@echo "Build finished. The manual pages are in $(BUILDDIR)/man."

.PHONY: texinfo
texinfo:
	$(SPHINXBUILD) -b texinfo $(ALLSPHINXOPTS) $(BUILDDIR)/texinfo
	@echo
	@echo "Build finished. The Texinfo files are in $(BUILDDIR)/texinfo."
	@echo "Run \`make' in that directory to run these through makeinfo" \
	      "(use \`make info' here to do that automatically)."

.PHONY: info
info:
	$(SPHINXBUILD) -b texinfo $(ALLSPHINXOPTS) $(BUILDDIR)/texinfo
	@echo "Running Texinfo files through makeinfo..."
	make -C $(BUILDDIR)/texinfo info
	@echo "makeinfo finished; the Info files are in $(BUILDDIR)/texinfo."

.PHONY: gettext
gettext:
	$(SPHINXBUILD) -b gettext $(I18NSPHINXOPTS) $(BUILDDIR)/locale
	@echo
	@echo "Build finished. The message catalogs are in $(BUILDDIR)/locale."

.PHONY: changes
changes:
	$(SPHINXBUILD) -b changes $(ALLSPHINXOPTS) $(BUILDDIR)/changes
	@echo
	@echo "The overview file is in $(BUILDDIR)/changes."

.PHONY: linkcheck
linkcheck:
	$(SPHINXBUILD) -b linkcheck $(ALLSPHINXOPTS) $(BUILDDIR)/linkcheck
	@echo
	@echo "Link check complete; look for any errors in the above output " \
	      "or in $(BUILDDIR)/linkcheck/output.txt."

.PHONY: doctest
doctest:
	$(SPHINXBUILD) -b doctest $(ALLSPHINXOPTS) $(BUILDDIR)/doctest
	@echo "Testing of doctests in the sources finished, look at the " \
	      "results in $(BUILDDIR)/doctest/output.txt."

.PHONY: coverage
coverage:
	$(SPHINXBUILD) -b coverage $(ALLSPHINXOPTS) $(BUILDDIR)/coverage
	@echo "Testing of coverage in the sources finished, look at the " \
	      "results in $(BUILDDIR)/coverage/python.txt."

.PHONY: xml
xml:
	$(SPHINXBUILD) -b xml $(ALLSPHINXOPTS) $(BUILDDIR)/xml
	@echo
	@echo "Build finished. The XML files are in $(BUILDDIR)/xml."

.PHONY: pseudoxml
pseudoxml:
	$(SPHINXBUILD) -b pseudoxml $(ALLSPHINXOPTS) $(BUILDDIR)/pseudoxml
	@echo
	@echo "Build finished. The pseudo-XML files are in $(BUILDDIR)/pseudoxml."

.PHONY: dummy
dummy:
	$(SPHINXBUILD) -b dummy $(ALLSPHINXOPTS) $(BUILDDIR)/dummy
	@echo
	@echo "Build finished. Dummy builder generates no files."

java_tutorial:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/java $(BUILDDIR)/html/tutorial/java
	@echo
	@echo "Build finished. The HTML files are in $(BUILDDIR)/html/tutorial/java"

java_dev:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/developers/java $(BUILDDIR)/html/developers/java
	@echo
	@echo "Build finished. The HTML files are in $(BUILDDIR)/html/developers/java"

java:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) -c $(SOURCEDIR) $(SOURCEDIR)/java $(BUILDDIR)/html/java
	@echo
	@echo "Build finished. The HTML files are in $(BUILDDIR)/html/java"
