
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

Provider packages
-----------------

.. contents:: :local:

Community maintained providers
''''''''''''''''''''''''''''''

Unlike Apache Airflow 1.10, the Airflow 2.0 is delivered in multiple, separate but connected packages.
The core of Airflow scheduling system is delivered as ``apache-airflow`` package and there are around
60 providers packages which can be installed separately as so called "Airflow Provider packages".
Those provider packages are separated per-provider (for example ``amazon``, ``google``, ``salesforce``
etc.). Those packages are available as ``apache-airflow-providers`` packages - separately per each provider
(for example there is an ``apache-airflow-providers-amazon`` or ``apache-airflow-providers-google`` package).

You can install those provider packages separately in order to interface with a given service. For those
providers that have corresponding extras, the provider packages (latest version from PyPI) are installed
automatically when Airflow is installed with the extra.

Community maintained providers are released and versioned separately from the Airflow releases. We are
following the `Semver <https://semver.org/>`_ versioning scheme for the packages. Some versions of the
provider packages might depend on particular versions of Airflow, but the general approach we have is that
unless there is a good reason, new version of providers should work with recent versions of Airflow 2.x.
Details will vary per-provider and if there is a limitation for particular version of particular provider,
constraining the Airflow version used, it will be included as limitation of dependencies in the provider
package.

Some of the providers have cross-provider dependencies as well. Those are not required dependencies, they
might simply enable certain features (for example transfer operators often create dependency between
different providers. Again, the general approach here is that the providers are backwards compatible,
including cross-dependencies. Any kind of breaking changes and requirements on particular versions of other
provider packages are automatically documented in the release notes of every provider.

.. note::
    We also provide ``apache-airflow-backport-providers`` packages that can be installed for Airflow 1.10.
    Those are the same providers as for 2.0 but automatically back-ported to work for Airflow 1.10. The
    last release of backport providers was done on March 17, 2021.


Provider packages functionality
'''''''''''''''''''''''''''''''

Separate provider packages give the possibilities that were not available in 1.10:

1. You can upgrade to latest version of particular providers without the need of Apache Airflow core upgrade.

2. You can downgrade to previous version of particular provider in case the new version introduces
   some problems, without impacting the main Apache Airflow core package.

3. You can release and upgrade/downgrade provider packages incrementally, independent from each other. This
   means that you can incrementally validate each of the provider package update in your environment,
   following the usual tests you have in your environment.


Extending Airflow Connections and Extra links via Providers
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Providers can contain operators, hooks, sensor, and transfer operators to communicate with a
multitude of external systems, but they can also extend Airflow core. Airflow has several extension
capabilities that can be used by providers. Airflow automatically discovers which providers add those
additional capabilities and, once you install provider package and re-start Airflow, those become
automatically available to Airflow Users.

The capabilities are:

* Adding Extra Links to operators delivered by the provider. See :doc:`apache-airflow:howto/define_extra_link`
  for a description of what extra links are and examples of provider registering an operator with extra links

* Adding custom connection types, extending connection form and handling custom form field behaviour for the
  connections defined by the provider. See :doc:`apache-airflow:howto/connection` for a description of
  connection and what capabilities of custom connection you can define.

Custom provider packages
''''''''''''''''''''''''

However, there is more. You can develop your own providers. This is a bit involved, but your custom operators,
hooks, sensors, transfer operators can be packaged together in a standard airflow package and installed
using the same mechanisms. Moreover they can also use the same mechanisms to extend the Airflow Core with
custom connections and extra operator links as described in the previous chapter.

How to create your own provider
'''''''''''''''''''''''''''''''

As mentioned in the `Providers <http://airflow.apache.org/docs/apache-airflow-providers/index.html>`_
documentation, custom providers can extend Airflow core - they can add extra links to operators as well
as custom connections. You can use build your own providers and install them as packages if you would like
to use the mechanism for your own, custom providers.

Adding a provider to Airflow is just a matter of building a Python package and adding the right meta-data to
the package. We are using standard mechanism of python to define
`entry points <https://docs.python.org/3/library/importlib.metadata.html#entry-points>`_ . Your package
needs to define appropriate entry-point ``apache_airflow_provider`` which has to point to a callable
implemented by your package and return a dictionary containing the list of discoverable capabilities
of your package. The dictionary has to follow the
`json-schema specification <https://github.com/apache/airflow/blob/master/airflow/provider_info.schema.json>`_.

Most of the schema provides extension point for the documentation (which you might want to also use for
your own purpose) but the important fields from the extensibility point of view are those:

Displaying package information in CLI/API:

* ``package-name`` - Name of the package for the provider.

* ``name`` - Human-friendly name of the provider.

* ``description`` - Additional description of the provider.

* ``version`` - List of versions of the package (in reverse-chronological order). The first version in the
  list is the current package version. It is taken from the version of package installed, not from the
  provider_info information.

Exposing customized functionality to the Airflow's core:

* ``extra-links`` - this field should contain the list of all operator class names that are adding extra links
  capability. See :doc:`apache-airflow:howto/define_extra_link` for description of how to add extra link
  capability to the operators of yours.

* ``hook-class-names`` - this field should contain the list of all hook class names that provide
  custom connection types with custom extra fields and field behaviour. See
  :doc:`apache-airflow:howto/connection` for more details.


When your providers are installed you can query the installed providers and their capabilities with the
``airflow providers`` command. This way you can verify if your providers are properly recognized and whether
they define the extensions properly. See :doc:`cli-and-env-variables-ref` for details of available CLI
sub-commands.

When you write your own provider, consider following the
`Naming conventions for provider packages <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages>`_


FAQ for Airflow and Providers
'''''''''''''''''''''''''''''

Upgrading Airflow 2.0 and Providers
"""""""""""""""""""""""""""""""""""

**When upgrading to a new Airflow version such as 2.0, but possibly 2.0.1 and beyond, is the best practice
to also upgrade provider packages at the same time?**

It depends on your use case. If you have automated or semi-automated verification of your installation,
that you can run a new version of Airflow including all provider packages, then definitely go for it.
If you rely more on manual testing, it is advised that you upgrade in stages. Depending on your choice
you can either upgrade all used provider packages first, and then upgrade Airflow Core or the other way
round. The first approach - when you first upgrade all providers is probably safer, as you can do it
incrementally, step-by-step replacing provider by provider in your environment.

Using Backport Providers in Airflow 1.10
""""""""""""""""""""""""""""""""""""""""

**I have an Airflow version (1.10.12) running and it is stable. However, because of a Cloud provider change,
I would like to upgrade the provider package. If I don't need to upgrade the Airflow version anymore,
how do I know that this provider version is compatible with my Airflow version?**

We have Backport Providers are compatible with 1.10 but they stopped being released on
March 17, 2021. Since then, no new changes to providers for Airflow 2.0 are going to be
released as backport packages. It's the highest time to upgrade to Airflow 2.0.

When it comes to compatibility of providers with different Airflow 2 versions, each
provider package will keep its own dependencies, and while we expect those providers to be generally
backwards-compatible, particular versions of particular providers might introduce dependencies on
specific Airflow versions.

Customizing Provider Packages
"""""""""""""""""""""""""""""

**I have an older version of my provider package which we have lightly customized and is working
fine with my MSSQL installation. I am upgrading my Airflow version. Do I need to upgrade my provider,
or can I keep it as it is?**

It depends on the scope of customization. There is no need to upgrade the provider packages to later
versions unless you want to upgrade to Airflow version that introduces backwards-incompatible changes.
Generally speaking, with Airflow 2 we are following the `Semver <https://semver.org/>`_  approach where
we will introduce backwards-incompatible changes in Major releases, so all your modifications (as long
as you have not used internal Airflow classes) should work for All Airflow 2.* versions.


Creating your own providers
"""""""""""""""""""""""""""

**When I write my own provider, do I need to do anything special to make it available to others?**

You do not need to do anything special besides creating the ``apache_airflow_provider`` entry point
returning properly formatted meta-data (dictionary with ``extra-links`` and ``hook-class-names`` fields).

Anyone who runs airflow in an environment that has your Python package installed will be able to use the
package as a provider package.

**What do I need to do to turn a package into a provider?**

You need to do the following to turn an existing Python package into a provider (see below for examples):

* Add the ``apache_airflow_provider`` entry point in the ``setup.cfg`` - this tells airflow where to get
  the required provider metadata
* Create the function that you refer to in the first step as part of your package: this functions returns a
  dictionary that contains all meta-data about your provider package
* note that the dictionary should be compliant with ``airflow/provider_info.schema.json`` JSON-schema
  specification. The community-managed providers have more fields there that are used to build
  documentation, but the requirement for runtime information only contains several fields which are defined
  in the schema:

.. exampleinclude:: /../../airflow/provider_info.schema.json
    :language: json

Example ``setup.cfg``:

.. code-block:: cfg

  [options.entry_points]
  # the function get_provider_info is defined in myproviderpackage.somemodule
  apache_airflow_provider=
    provider_info=myproviderpackage.somemodule:get_provider_info

Example ``myproviderpackage/somemodule.py``:

.. code-block:: Python

  def get_provider_info():
      return {
          "package-name": "my-package-name",
          "name": "name",
          "description": "a description",
          "hook-class-names": [
              "myproviderpackage.hooks.source.SourceHook",
          ],
      }

**How do provider packages work under the hood?**

When running airflow with your provider package, there will be (at least) three components to your airflow installation:

* The installation itself (for example, a ``venv`` where you installed airflow with ``pip install apache-airflow``)
  together with the related files (e.g. ``dags`` folder)
* The ``apache-airflow`` package
* Your own ``myproviderpackage`` package that is independent of ``apache-airflow`` or your airflow installation, which
  can be a local Python package (that you install via ``pip pip install -e /path/to/my-package``), a normal pip package
  (``pip install myproviderpackage``), or any other type of Python package

In the ``myproviderpackage`` package you need to add the entry point and provide the appropriate metadata as described above.
If you have done that, airflow does the following at runtime:

* Loop through ALL packages installed in your environment / ``venv``
* For each package, if the package's ``setup.cfg`` has a section ``[options.entry_points]``, and if that section has a value
  for ``apache_airflow_provider``, then get the value for ``provider_info``, e.g. ``myproviderpackage.somemodule:get_provider_info``
* That value works like an import statement: ``myproviderpackage.somemodule:get_provider_info`` translates to something like
  ``from myproviderpackage.somemodule import get_provider_info``, and the ``get_provider_info`` that is being imported should be a
  callable, i.e. a function
* This function should return a dictionary with metadata
* If you have custom connection types as part of your package, that metadata will including a field called ``hook-class-names``
  which should be a list of strings of your custom hooks - those strings should also be in an import-like format, e.g.
  ``myproviderpackage.hooks.source.SourceHook`` means that there is a class ``SourceHook`` in ``myproviderpackage/hooks/source.py``
  - airflow then imports these hooks and looks for the functions ``get_ui_field_behaviour`` and ``get_connection_form_widgets``
  (both optional) as well as the attributes ``conn_type`` and ``hook_name`` to create the custom connection type in the airflow UI

**Should I name my provider specifically or should it be created in ``airflow.providers`` package?**

We have quite a number (>60) of providers managed by the community and we are going to maintain them
together with Apache Airflow. All those providers have well-defined structured and follow the
naming conventions we defined and they are all in ``airflow.providers`` package. If your intention is
to contribute your provider, then you should follow those conventions and make a PR to Apache Airflow
to contribute to it. But you are free to use any package name as long as there are no conflicts with other
names, so preferably choose package that is in your "domain".

**Is there a convention for a connection id and type?**

Very good question. Glad that you asked. We usually follow the convention ``<NAME>_default`` for connection
id and just ``<NAME>`` for connection type. Few examples:

* ``google_cloud_default`` id and ``google_cloud_platform`` type
* ``aws_default`` id and ``aws`` type

You should follow this convention. It is important, to use unique names for connection type,
so it should be unique for your provider. If two providers try to add connection with the same type
only one of them will succeed.

**Can I contribute my own provider to Apache Airflow?**

Of course, but it's better to check at developer's mailing list whether such contribution will be accepted by
the Community, before investing time to make the provider compliant with community requirements.
The Community only accepts providers that are generic enough, are well documented, fully covered by tests
and with capabilities of being tested by people in the community. So we might not always be in the
position to accept such contributions.

However, in case you have your own, specific provider, which you can maintain on your own or by your
team, you are free to publish the providers in whatever form you find appropriate. The custom and
community-managed providers have exactly the same capabilities.

**Can I advertise my own provider to Apache Airflow users and share it with others as package in PyPI?**

Absolutely! We have an `Ecosystem <https://airflow.apache.org/ecosystem/>`_ area on our website where
we share non-community managed extensions and work for Airflow. Feel free to make a PR to the page and
add we will evaluate and merge it when we see that such provider can be useful for the community of
Airflow users.

**Can I charge for the use of my provider?**

This is something that is outside of our control and domain. As an Apache project, we are
commercial-friendly and there are many businesses built around Apache Airflow and many other
Apache projects. As a community, we provide all the software for free and this will never
change. What 3rd-party developers are doing is not under control of Apache Airflow community.


Content
-------

.. toctree::
    :maxdepth: 1

    Packages <packages-ref>
    Operators and hooks <operators-and-hooks-ref/index>
