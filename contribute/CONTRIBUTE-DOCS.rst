.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. contents:: :local:

Overview
========

Contributing to documentation is one of the easiest and most welcome ways to improve Apache Airflow. Airflow documentation consists of the following components:

- `Apache Airflow documentation <https://airflow.apache.org/docs/apache-airflow/stable/index.html>`__, which explains how to install and use Airflow.
- `Provider documentation <https://airflow.apache.org/docs/apache-airflow-providers/index.html>`__, which explains how to connect Airflow to third-party provider packages.
- `Helm chart documentation <https://github.com/apache/airflow/tree/master/docs/helm-chart`__, which explains how to run Airflow on Kubernetes using Helm
- READMEs and contribution guidelines (including this page).
- Inline docstrings, which describe functions within the source code itself.

This guide provides guidance on working with docs, as well as best practices for making changes and submitting your work for review.

What Should I Contribute?
----------------------
Do you want to contribute to Airflow docs, but you're not sure exactly where? Consider any of the following:

- Typo fixes
- Additional information about existing functionality
- Documentation about a new feature
- Images or screenshots that help provide users context
- New tutorials

Anything that makes Airflow easier to use and understand is welcome.

Before you Begin
-----------------
To contribute to Airflow documentation, you need the following:

- Git and a GitHub account
- A `cloned fork <https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/working-with-fork`__ of the `Airflow repository (apache/airflow) <https://github.com/apache/airflow>`__
- `Docker <https://www.docker.com/get-started>`__
- A text editor that can render ``.rst`` formatting, such as Atom or Vim

Airflow documentation is written in `reStructured text syntax <https://thomas-cokelaer.info/tutorials/sphinx/rest_syntax.html>`__. It's easy to pick up and is *not* a prerequisite for contributing to Airflow docs, but you may find it helpful to read best practices before you start.

If you plan to contribute changes to Airflow that go beyond documentation, we recommend following the `Contributors Quickstart <https://github.com/apache/airflow/blob/master/CONTRIBUTORS_QUICK_START.rst>`__ to install a more robust Airflow testing environment on your local machine.

Contribute to Apache Airflow Documentation
==========================================
All `Apache Airflow Documentation <https://airflow.apache.org/docs/apache-airflow/stable/index.html>`__ is versioned according to a corresponding major/minor version of Airflow. The same day that an Airflow release is published and made available to the community, a corresponding version of Airflow docs are published.
To access documentation changes in between releases, a `developer version <http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/>`__ with the most recent commits is available.

The website is built directly from ``airflow/docs/apache-airflow``. This is where the majority of documentation changes are made.

Update an Existing Document
---------------------------
The general workflow for improving documentation is as follows:

1. In your local Airflow project folder, make sure you're working from the latest commit of Airflow by running the following commands:

.. code-block:: bash

   git remote add apache https://github.com/apache/airflow.git
   git fetch apache
   git rebase apache/master

2. Create a new branch of your forked repo. Generally speaking, you should create a new branch for each new contribution you plan to make:

.. code-block:: bash

    git checkout -b doc-update

3. Make the documentation change in your text editor and save your work.
4. In your local Airflow project folder, run the following command to build your documentation:

.. code-block:: bash

    ./breeze build-docs

This command builds all Airflow documentation as HTML files in the ``airflow/docs/_build`` folder. Running this command without any flags can take an extremely long time, so we recommend building only the documents you're editing using the following command instead:

.. code-block:: bash

    ./breeze build-docs -- --package-filter apache-airflow
    airflow/docs/apache-airflow/<updated-file>.rst

5. Open the airflow/docs/_build folder and open your rendered document in the web browser of your choice. This is your opportunity to review your changes and make any necessary formatting or spelling changes.
Alternatively, you can host the entire website by running the following command from the airflow directory:

.. code-block:: bash

    docs/start_doc_server.sh

After running this command, you can access the updated documentation directly in your web browser by visiting ``localhost:8000``. 

6. Once you've finished making your changes, create a `pull request <https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__ from your forked repository.
7. Repeat steps 3-5 for any suggestions you get from your PR reviewers.


Create a new Document
---------------------
The process for creating a new document is similar to editing an existing document. The main difference is that you have to edit some additional files to make your document appear across the documentation site.

Before creating a new document:

- Look through the `developer version of Airflow docs <http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/>`__ to make sure there isn't an existing document where your content might fit.
- Check `existing pull requests <https://github.com/apache/airflow/pulls?q=is%3Aopen+is%3Apr+label%3Akind%3Adocumentation>`__ with the ``kind:documentation`` label to see if someone else has a similar document in review.

When you're sure that a new document is the best place for the information you're writing, follow the steps below.

1. In your local Airflow project folder, make sure you're working from the latest commit of Airflow by running the following commands:

.. code-block:: bash

    git remote add apache https://github.com/apache/airflow.git
    git fetch apache
    git rebase apache/master

2. Create a new branch of your forked repo. Generally speaking, you should create a new branch for each new contribution you plan to make:

.. code-block:: bash

    git checkout -b doc-update

3. Create a new .rst file in the appropriate folder within ``airflow/docs/apache-airflow``.

4. At the top of your file, add the following copyright notice:

.. code-block:: yaml

.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

5. Add your content to your new ``.rst`` file.
6. Open the ``index.rst`` file of the folder you're working in. Every folder in ``docs`` has its own ``index.rst`` file to generate the Table of Contents side menu.
7. Add the file name of your new document alongside the other file names in the ``toctree`` element.
If you want the Table of Contents to show a different title than the one specified at the beginning of your document, you can specify an alternative title and enter your file name in pointed brackets. For example:

.. code-block:: yaml

    Using the CLI <usage-cli> # Full title is "Using the Command Line Interface"

Note: If the ``toctree`` has no files and a single ``*`` , you don't need to specify your file name there.

8. In your local Airflow project folder, run the following command to build your documentation:

.. code-block:: bash

    ./breeze build-docs

This command builds all Airflow documentation as HTML files in the ``airflow/docs/_build`` folder. Running this command without any flags can take a long time, so we recommend building only the documents you're editing using the following command instead:

.. code-block:: bash

    ./breeze build-docs -- --package-filter apache-airflow
    airflow/docs/apache-airflow/<updates-file>.rst

This command builds only your ``<updated-file>`` in the apache-airflow repository. Additionally, you can specify flags to skip spelling checks, which can take additional time and should only be run when you've finished your contribution:

.. code-block:: bash

    ./breeze build-docs -- --docs-only --package-filter apache-airflow
    airflow/docs/apache-airflow/<updated-file>.rst

9. Open the ``airflow/docs/_build`` folder and open your rendered document in the web browser of your choice. This is your opportunity to review your changes and make any necessary formatting or spelling changes.
10. Once you've finished making your changes, create a `pull request <https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__ from your forked repository.
11. Repeat steps 3-5 for any suggestions you get from your PR reviewers.

Additional Documentation Workflows
==================================

As you're contributing to Airflow documentation, you may need to add a screenshot/image, or reference other documentation.

Add an Image to Airflow Docs
----------------------------

1. Make sure that your image is a high quality ``.png`` file. If you're taking a screenshot, try limiting the image to only what's necessary. Avoid too much white space or otherwise irrelevant elements.
2. Add your image to the ``img`` folder in ``docs/apache-airflow``.
3. Reference the image in your file using the following syntax:

.. code-block:: bash

    .. image:: /img/add-permissions.png


Cross-reference Airflow Code or Files
-------------------------------------
When writing docs, you might want to reference something else within the Airflow repository, such as another document or a snippet of code.
You can do this using the cross referencing syntax in ``ReST``.

All cross references share the same general formatting:

.. code-block:: bash

    ... :role:`target`

Here, ``role`` is the type of content you're referencing, while ``target`` is where to look for that content. 

For example:

.. code-block:: bash

    :class:`airflow.models.dag.DAG` - link to Pyton API reference documentation
    :doc:`/docs/operators` - link to other document

.. exampleinclude:: /../../airflow/providers/microsoft/azure/example_dags/example_adls_delete.py - code snippet
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_adls_delete]
    :end-before: [END howto_operator_adls_delete]

If you want to link to a specific section of another document, use a ``ref`` role. First, specify a target hook above the doc section you want to reference:

.. code-block:: yaml

    .. _jinja-templating: - The target for our reference

    Jinja Templating
    ================

You can then use this target in any other Airflow document without needing to reference a filepath:

. code-block::

    You can use :ref:`Jinja templating <jinja-templating>` with
    :template-fields:`airflow.providers.google.cloud.transfers.gdrive_to_gcs.GoogleDriveToGCSOperator`
    parameters which allows you to dynamically determine values.

This should only be used when you need to reference a section within a long document. It should not be used to reference an entire document. For that, you can use a standard ``doc`` role.

For more information on using roles, read `Sphinx documentation <https://www.sphinx-doc.org/en/master/usage/restructuredtext/roles.html>`__.

Troubleshooting
===============

Breeze Build Failure
--------------------
The build timeout for  ``./breeze build-docs`` is fixed at 4 minutes, as seen in `docs_builder.py <https://github.com/apache/airflow/blob/2.0.0/docs/exts/docs_build/docs_builder.py#L40>`__.
If your build is timing out or otherwise failing, re-run the build. 

If the timeout persists, try building one document or directory at a time using the ``--package-filter`` flag.

If the build errors persist even after multiple attempts, reach out to someone in the ``#documentation`` channel on Slack.
There might be something going on behind the scenes that a community member can help you identify.

Spellcheck Flags a Proper Noun
------------------------------
If Breeze gets caught on words or proper nouns specific to Apache Airflow that are spelled correctly, you might need to add those words to `docs/spelling_wordlist.txt <https://github.com/apache/airflow/blob/master/docs/spelling_wordlist.txt>`__ and rebuild your document.

Contribute to Other Documentation
=================================

If you're interested in contributing to documentation outside of Airflow's core documentation suite, read the guidelines below.

Contribute to Provider Documentation
------------------------------------

Most provider package documentation is generated from inline documentation within the provider packages themselves.
For more information contributing to provider packages, read `Contributing to Provider Packages <https://github.com/apache/airflow/blob/master/contribute/CONTRIBUTE-PROVIDERS.rst>`__.

Contribute to Airflow READMEs
-----------------------------

If you want to contribute to GitHub-hosted documentation, such as a README file or other GitHub-hosted ``.rst file``, you can do so in the same fork you make other documentation changes in.
The best part is: You don't have to build them using Breeze! Simply make your changes and create a new PR. Once merged, your file will be immediately available to view on GitHub.

Contribute to Helm Chart Documentation
--------------------------------------

Currently, Airflow's Helm Chart documentation exists only on GitHub. To contribute, access the `Helm chart documentation repo <https://github.com/apache/airflow/tree/master/docs/helm-chart>`__ and follow the contribution guidelines.


















