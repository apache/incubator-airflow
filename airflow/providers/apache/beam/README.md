<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->


# Package apache-airflow-providers-apache-beam

Release: 1.0.0

**Table of contents**

- [Provider package](#provider-package)
- [Installation](#installation)
- [PIP requirements](#pip-requirements)
- [Cross provider package dependencies](#cross-provider-package-dependencies)
- [Provider class summary](#provider-classes-summary)
    - [Operators](#operators)
    - [Transfer operators](#transfer-operators)
    - [Hooks](#hooks)
- [Releases](#releases)

## Provider package

This is a provider package for `apache.beam` provider. All classes for this provider package
are in `airflow.providers.apache.beam` python package.

## Installation

NOTE!

On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
`pip install --upgrade pip==20.2.4` or, in case you use Pip 20.3, you need to add option
`--use-deprecated legacy-resolver` to your pip install command.

You can install this package on top of an existing airflow 2.* installation via
`pip install apache-airflow-providers-apache-beam`

## Cross provider package dependencies

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider packages in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

```bash
pip install apache-airflow-providers-apache-beam[google]
```

| Dependent package                                                                           | Extra       |
|:--------------------------------------------------------------------------------------------|:------------|
| [apache-airflow-providers-google](https://pypi.org/project/apache-airflow-providers-google) | google      |


# Provider classes summary

In Airflow 2.0, all operators, transfers, hooks, sensors, secrets for the `apache.beam` provider
are in the `airflow.providers.apache.beam` package. You can read more about the naming conventions used
in [Naming conventions for provider packages](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#naming-conventions-for-provider-packages)


## Operators

### New operators

| New Airflow 2.0 operators: `airflow.providers.apache.beam` package                                                                                                                 |
|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| [operators.beam.BeamRunJavaPipelineOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/operators/beam.py)    |
| [operators.beam.BeamRunPythonPipelineOperator](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/operators/beam.py)  |


## Hooks

### New hooks

| New Airflow 2.0 hooks: `airflow.providers.apache.beam` package                                                   |
|:-----------------------------------------------------------------------------------------------------------------|
| [hooks.beam.BeamHook](https://github.com/apache/airflow/blob/master/airflow/providers/apache/beam/hooks/beam.py) |


## Releases
