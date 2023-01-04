.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.


.. SCOPE OF THIS SECTION
.. To expose that help with existing documentation is a
.. very good way to start and also a very important part of
.. the project! If possible add summary of the structure of
.. the existing documentation, including different Cookbooks.


.. _documentation:

**************************
Helping with documentation
**************************

**A great way to contribute to the project is to improve
documentation.**

If you are an Arrow user and you found some docs to be
incomplete or inaccurate, share your hard-earned knowledge
with the rest of the community.

If you didn't come across something to improve in the
documentation itself, you can search for an issue in GitHub.

.. note::
   When searching for an issue that deals with documentation,
   navigate to `GitHub labels <https://github.com/apache/arrow/labels>`_
   and select **Component: Documentation** or search for **Documentation**
   in the "Search all labels" window.

   See `Example search. <https://github.com/apache/arrow/issues?q=is%3Aopen+is%3Aissue+label%3A%22Component%3A+Documentation%22+>`_

Documentation improvements are also a great way to gain some
experience with our submission and review process without
requiring a lot of local development environment setup. 

.. note::
   Many documentation-only changes can be made directly in the
   GitHub web interface by clicking the **Edit this page**
   on the right top corner of the documentations page. This
   will handle making a fork and a pull request for you.

   .. figure:: /developers/images/edit_page.jpeg
      :scale: 20 %
      :alt: click on edit this page

      On the right corner of the file in GitHub click on pen icon.

   .. figure:: /developers/images/github_edit_page.jpeg
      :scale: 30 %
      :alt: edit file in GitHub.

      Now you can edit the file in GitHub.

You could also build the entire project, make the change locally on
your branch and make the PR this way. But it is by no means superior
to simply editing via GitHub.

If you wish to build the documentation locally, follow detailed instructions
on :ref:`building-docs`.

Where to find the correct file to change?
-----------------------------------------

Most of the documentation is located in the ``docs/source`` of the Arrow
library. Source folder includes:

- **C++ documentation** section: ``docs/source/cpp``.
- **Development** section: ``docs/source/developers``.
- **Specificatons and protocols** section: ``docs/source/format``.
- **Language documentation**

  **C (GLib), Java, JavaScript** and **Python** documentation is located
  in folders named by the language, for example: ``docs/source/c_glib``.

  **Reference documentation**

  To edit `API reference documentation for Python <https://arrow.apache.org/docs/python/api.html>`_,
  you will need to edit the docstrings of the functions and methods located in
  the ``pyarrow`` package itself. For example, to edit
  `pyarrow.Array <https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array>`_
  you will need to edit ``docstrings`` in `arrow/pyarrow/array.pxi <https://github.com/apache/arrow/blob/bc223c688add2f4f06be0c3569192178f1ca1091/python/pyarrow/array.pxi#L790-L796>`_.

  It is similar for C++.

- The documentation for the **arrow R package** (which is published on
  the pkgdown site at ``arrow.apache.org/docs/r/)`` is located within
  the ``r/`` subdirectory.

  .. seealso::

     To read more about documentation in R please visit:

     - `Object documentation from the R packages book <https://r-pkgs.org/man.html>`_
     - `roxygen2 <https://roxygen2.r-lib.org/>`_
     - `pkgdown <https://pkgdown.r-lib.org/>`_

**Cookbooks** have their own repository `<https://github.com/apache/arrow-cookbook>`_
and can be separately cloned and built.

