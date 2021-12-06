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
documentation itself, you can search for an issue on JIRA.

.. note::
   When searching for JIRA issue that deals with documentation
   try selecting **Components** from **More** tab in JIRA search
   and there find **Documentation** from the list.

   See `Example search. <https://issues.apache.org/jira/issues/?jql=project%20%3D%20ARROW%20AND%20status%20%3D%20Open%20AND%20resolution%20%3D%20Unresolved%20AND%20component%20%3D%20Documentation%20AND%20assignee%20in%20(EMPTY)%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC>`_

   .. figure:: jira_search_component.jpeg
      :scale: 40 %
      :alt: selecting Components in JIRA search

      First select Components tab in JIRA.

   .. figure:: jira_search_documentation.jpeg
      :scale: 40 %
      :alt: selecting Documentation in JIRA search

      Then choose Documentation from the Components list.

Documentation improvements are also a great way to gain some
experience with our submission and review process without
requiring a lot of local development environment setup. 

.. note::
   Many documentation-only changes can be made directly in the
   GitHub web interface by clicking the **Edit this page**
   on the right top corner of the documentations page. This
   will handle making a fork and a pull request for you.

   .. figure:: edit_page.jpeg
      :scale: 20 %
      :alt: click on edit this page

      On the right corner of the file in GitHub click on pen icon.

   .. figure:: github_edit_page.jpeg
      :scale: 30 %
      :alt: edit file in GitHub.

      Now you can edit the file in GitHub.

You could also build the entire project, make the change locally on
your branch and make the PR this way. But it is by no means superior
to simply editing via GitHub.

If you wish to build the documentation also, follow detailed instructions
on :ref:`building-docs`.

Where to find the correct file to change?
-----------------------------------------

Most of the documentation is located in the ``docs/source`` of the Arrow
library. Source folder includes:

- **C++ documentation** section: ``docs/source/cpp``.
- **Development** section: ``docs/source/developers``.
- **Specificatons and protocols** section: ``docs/source/format``.
- Other language documentation: **C (GLib), Java, JavaScript** and **Python**
  in folders named by the language, for example: ``docs/source/c_glib``.
- The documentation for the **R language** is located in the ``r/sub-directory``.

**Cookbooks** have their own repository `<https://github.com/apache/arrow-cookbook>`_
and can be separately cloned and built.

