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

# Updating Airflow

This file documents any backwards-incompatible changes in Airflow and
assists users migrating to a new version.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Master](#master)
- [Airflow 2.0.1](#airflow-201)
- [Airflow 2.0.0](#airflow-200)
- [Airflow 1.10.15](#airflow-11015)
- [Airflow 1.10.14](#airflow-11014)
- [Airflow 1.10.13](#airflow-11013)
- [Airflow 1.10.12](#airflow-11012)
- [Airflow 1.10.11](#airflow-11011)
- [Airflow 1.10.10](#airflow-11010)
- [Airflow 1.10.9](#airflow-1109)
- [Airflow 1.10.8](#airflow-1108)
- [Airflow 1.10.7](#airflow-1107)
- [Airflow 1.10.6](#airflow-1106)
- [Airflow 1.10.5](#airflow-1105)
- [Airflow 1.10.4](#airflow-1104)
- [Airflow 1.10.3](#airflow-1103)
- [Airflow 1.10.2](#airflow-1102)
- [Airflow 1.10.1](#airflow-1101)
- [Airflow 1.10](#airflow-110)
- [Airflow 1.9](#airflow-19)
- [Airflow 1.8.1](#airflow-181)
- [Airflow 1.8](#airflow-18)
- [Airflow 1.7.1.2](#airflow-1712)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Master

<!--

I'm glad you want to write a new note. Remember that this note is intended for users.
Make sure it contains the following information:

- [ ] Previous behaviors
- [ ] New behaviors
- [ ] If possible, a simple example of how to migrate. This may include a simple code example.
- [ ] If possible, the benefit for the user after migration e.g. "we want to make these changes to unify class names."
- [ ] If possible, the reason for the change, which adds more context to that interested, e.g. reference for Airflow Improvement Proposal.

More tips can be found in the guide:
https://developers.google.com/style/inclusive-documentation

-->

### Default `[webserver] worker_refresh_interval` is changed to `6000` seconds

The default value for `[webserver] worker_refresh_interval` was `30` seconds for
Airflow <=2.0.1. However, since Airflow 2.0 DAG Serialization is a hard requirement
and the Webserver used the serialized DAGs, there is no need to kill an existing
worker and create a new one as frequently as `30` seconds.

This setting can be raised to an even higher value, currently it is
set to `6000` seconds (10 minutes) to
serve as a DagBag cache burst time.

### `default_queue` configuration has been moved to the `operators` section.

The `default_queue` configuration option has been moved from `[celery]` section to `[operators]` section to allow for re-use between different executors.

## Airflow 2.0.1

### Permission to view Airflow Configurations has been removed from `User` and `Viewer` role

Previously, Users with `User` or `Viewer` role were able to get/view configurations using
the REST API or in the Webserver. From Airflow 2.0.1, only users with `Admin` or `Op` role would be able
to get/view Configurations.

To allow users with other roles to view configuration, add `can read on Configurations` permissions to that role.

Note that if `[webserver] expose_config` is set to `False`, the API will throw a `403` response even if
the user has role with `can read on Configurations` permission.

### Default `[celery] worker_concurrency` is changed to `16`

The default value for `[celery] worker_concurrency` was `16` for Airflow <2.0.0.
However, it was unintentionally changed to `8` in 2.0.0.

From Airflow 2.0.1, we revert to the old default of `16`.

### Default `[scheduler] min_file_process_interval` is changed to `30`

The default value for `[scheduler] min_file_process_interval` was `0`,
due to which the CPU Usage mostly stayed around 100% as the DAG files are parsed
constantly.

From Airflow 2.0.0, the scheduling decisions have been moved from
DagFileProcessor to Scheduler, so we can keep the default a bit higher: `30`.

## Airflow 2.0.0

The 2.0 release of the Airflow is a significant upgrade, and includes substantial major changes,
and some of them may be breaking. Existing code written for earlier versions of this project will may require updates
to use this version. Sometimes necessary configuration changes are also required.
This document describes the changes that have been made, and what you need to do to update your usage.

If you experience issues or have questions, please file [an issue](https://github.com/apache/airflow/issues/new/choose).

### Major changes

This section describes the major changes that have been made in this release.

### The experimental REST API is disabled by default

The experimental REST API is disabled by default. To restore these APIs while migrating to
the stable REST API, set `enable_experimental_api` option in `[api]` section to `True`.

Please note that the experimental REST API do not have access control.
The authenticated user has full access.

### SparkJDBCHook default connection

For SparkJDBCHook default connection was `spark-default`, and for SparkSubmitHook it was
`spark_default`. Both hooks now use the `spark_default` which is a common pattern for the connection
names used across all providers.

### Changes to output argument in commands

From Airflow 2.0, We are replacing [tabulate](https://pypi.org/project/tabulate/) with [rich](https://github.com/willmcgugan/rich) to render commands output. Due to this change, the `--output` argument
will no longer accept formats of tabulate tables. Instead, it now accepts:

- `table` - will render the output in predefined table
- `json` - will render the output as a json
- `yaml` - will render the output as yaml

By doing this we increased consistency and gave users possibility to manipulate the
output programmatically (when using json or yaml).

Affected commands:

- `airflow dags list`
- `airflow dags report`
- `airflow dags list-runs`
- `airflow dags list-jobs`
- `airflow connections list`
- `airflow connections get`
- `airflow pools list`
- `airflow pools get`
- `airflow pools set`
- `airflow pools delete`
- `airflow pools import`
- `airflow pools export`
- `airflow role list`
- `airflow providers list`
- `airflow providers get`
- `airflow providers hooks`
- `airflow tasks states-for-dag-run`
- `airflow users list`
- `airflow variables list`

### Azure Wasb Hook does not work together with Snowflake hook

The WasbHook in Apache Airflow use a legacy version of Azure library. While the conflict is not
significant for most of the Azure hooks, it is a problem for Wasb Hook because the `blob` folders
for both libraries overlap. Installing both Snowflake and Azure extra will result in non-importable
WasbHook.

### Rename `all` to `devel_all` extra

The `all` extras were reduced to include only user-facing dependencies. This means
that this extra does not contain development dependencies. If you were relying on
`all` extra then you should use now `devel_all` or figure out if you need development
extras at all.

### Context variables `prev_execution_date_success` and `prev_execution_date_success` are now `pendulum.DateTime`

### Rename policy to task_policy

Because Airflow introduced DAG level policy (`dag_policy`) we decided to rename existing `policy`
function to `task_policy` to make the distinction more profound and avoid any confusion.

Users using cluster policy need to rename their `policy` functions in `airflow_local_settings.py`
to `task_policy`.

### Default value for `[celery] operation_timeout` has changed to `1.0`

From Airflow 2, by default Airflow will retry 3 times to publish task to Celery broker. This is controlled by
`[celery] task_publish_max_retries`. Because of this we can now have a lower Operation timeout that raises
`AirflowTaskTimeout`. This generally occurs during network blips or intermittent DNS issues.

### Adding Operators and Sensors via plugins is no longer supported

Operators and Sensors should no longer be registered or imported via Airflow's plugin mechanism -- these types of classes are just treated as plain python classes by Airflow, so there is no need to register them with Airflow.

If you previously had a `plugins/my_plugin.py` and you used it like this in a DAG:

```
from airflow.operators.my_plugin import MyOperator
```

You should instead import it as:

```
from my_plugin import MyOperator
```

The name under `airflow.operators.` was the plugin name, where as in the second example it is the python module name where the operator is defined.

See https://airflow.apache.org/docs/stable/howto/custom-operator.html for more info.

### Importing Hooks via plugins is no longer supported

Importing hooks added in plugins via `airflow.hooks.<plugin_name>` is no longer supported, and hooks should just be imported as regular python modules.

```
from airflow.hooks.my_plugin import MyHook
```

You should instead import it as:

```
from my_plugin import MyHook
```

It is still possible (but not required) to "register" hooks in plugins. This is to allow future support for dynamically populating the Connections form in the UI.

See https://airflow.apache.org/docs/stable/howto/custom-operator.html for more info.

### Adding Operators and Sensors via plugins is no longer supported

### The default value for `[core] enable_xcom_pickling` has been changed to `False`

The pickle type for XCom messages has been replaced to JSON by default to prevent RCE attacks.
Note that JSON serialization is stricter than pickling, so for example if you want to pass
raw bytes through XCom you must encode them using an encoding like ``base64``.
If you understand the risk and still want to use [pickling](https://docs.python.org/3/library/pickle.html),
set `enable_xcom_pickling = True` in your Airflow config's `core` section.

### Airflowignore of base path

There was a bug fixed in https://github.com/apache/airflow/pull/11993 that the "airflowignore" checked
the base path of the dag folder for forbidden dags, not only the relative part. This had the effect
that if the base path contained the excluded word the whole dag folder could have been excluded. For
example if the airflowignore file contained x, and the dags folder was '/var/x/dags', then all dags in
the folder would be excluded. The fix only matches the relative path only now which means that if you
previously used full path as ignored, you should change it to relative one. For example if your dag
folder was '/var/dags/' and your airflowignore contained '/var/dag/excluded/', you should change it
to 'excluded/'.

### `ExternalTaskSensor` provides all task context variables to `execution_date_fn` as keyword arguments

The old syntax of passing `context` as a dictionary will continue to work with the caveat that the argument must be named `context`. The following will break. To fix it, change `ctx` to `context`.

```python
def execution_date_fn(execution_date, ctx):
```

`execution_date_fn` can take in any number of keyword arguments available in the task context dictionary. The following forms of `execution_date_fn` are all supported:

```python
def execution_date_fn(dt):

def execution_date_fn(execution_date):

def execution_date_fn(execution_date, ds_nodash):

def execution_date_fn(execution_date, ds_nodash, dag):
```

### The default value for `[webserver] cookie_samesite` has been changed to `Lax`

As [recommended](https://flask.palletsprojects.com/en/1.1.x/config/#SESSION_COOKIE_SAMESITE) by Flask, the
`[webserver] cookie_samesite` has been changed to `Lax` from `''` (empty string) .

#### Changes to import paths

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons. In Airflow 2.0, we want to organize packages and move integrations
with third party services to the ``airflow.providers`` package.

All changes made are backward compatible, but if you use the old import paths you will
see a deprecation warning. The old import paths can be abandoned in the future.

According to [AIP-21](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths)
`_operator` suffix has been removed from operators. A deprecation warning has also been raised for paths
importing with the suffix.


The following table shows changes in import paths.


| Old path                            | New path                   |
|-------------------------------------|----------------------------|
| airflow.hooks.base_hook.BaseHook | airflow.hooks.base.BaseHook |
| airflow.hooks.dbapi_hook.DbApiHook | airflow.hooks.dbapi.DbApiHook |
| airflow.operators.dummy_operator.DummyOperator | airflow.operators.dummy.DummyOperator |
| airflow.operators.dagrun_operator.TriggerDagRunOperator | airflow.operators.trigger_dagrun.TriggerDagRunOperator |
| airflow.operators.branch_operator.BaseBranchOperator | airflow.operators.branch.BaseBranchOperator |
| airflow.operators.subdag_operator.SubDagOperator | airflow.operators.subdag.SubDagOperator |
| airflow.sensors.base_sensor_operator.BaseSensorOperator | airflow.sensors.base.BaseSensorOperator |
| airflow.sensors.date_time_sensor.DateTimeSensor | airflow.sensors.date_time.DateTimeSensor |
| airflow.sensors.external_task_sensor.ExternalTaskMarker | airflow.sensors.external_task.ExternalTaskMarker |
| airflow.sensors.external_task_sensor.ExternalTaskSensor | airflow.sensors.external_task.ExternalTaskSensor |
| airflow.sensors.sql_sensor.SqlSensor | airflow.sensors.sql.SqlSensor |
| airflow.sensors.time_delta_sensor.TimeDeltaSensor | airflow.sensors.time_delta.TimeDeltaSensor |
| airflow.contrib.sensors.weekday_sensor.DayOfWeekSensor | airflow.sensors.weekday.DayOfWeekSensor |


### Database schema changes

In order to migrate the database, you should use the command `airflow db upgrade`, but in
some cases manual steps are required.

#### Unique conn_id in connection table

Previously, Airflow allowed users to add more than one connection with the same `conn_id` and on access it would choose one connection randomly. This acted as a basic load balancing and fault tolerance technique, when used in conjunction with retries.

This behavior caused some confusion for users, and there was no clear evidence if it actually worked well or not.

Now the `conn_id` will be unique. If you already have duplicates in your metadata database, you will have to manage those duplicate connections before upgrading the database.

#### Not-nullable conn_type column in connection table

The `conn_type` column in the `connection` table must contain content. Previously, this rule was enforced
by application logic, but was not enforced by the database schema.

If you made any modifications to the table directly, make sure you don't have
null in the `conn_type` column.

### Configuration changes

This release contains many changes that require a change in the configuration of this application or
other application that integrate with it.

This section describes the changes that have been made, and what you need to do to.

#### airflow.contrib.utils.log has been moved

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons. In Airflow 2.0, we want to organize packages and move integrations
with third party services to the ``airflow.providers`` package.

To clean up, the following packages were moved:
| Old package | New package |
|-|-|
| ``airflow.contrib.utils.log`` | ``airflow.utils.log`` |
| ``airflow.utils.log.gcs_task_handler`` | ``airflow.providers.google.cloud.log.gcs_task_handler`` |
| ``airflow.utils.log.wasb_task_handler`` | ``airflow.providers.microsoft.azure.log.wasb_task_handler`` |
| ``airflow.utils.log.stackdriver_task_handler`` | ``airflow.providers.google.cloud.log.stackdriver_task_handler`` |
| ``airflow.utils.log.s3_task_handler`` | ``airflow.providers.amazon.aws.log.s3_task_handler`` |
| ``airflow.utils.log.es_task_handler`` | ``airflow.providers.elasticsearch.log.es_task_handler`` |
| ``airflow.utils.log.cloudwatch_task_handler`` | ``airflow.providers.amazon.aws.log.cloudwatch_task_handler`` |

You should update the import paths if you are setting log configurations with the ``logging_config_class`` option.
The old import paths still works but can be abandoned.

#### SendGrid emailer has been moved

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons.

To clean up, the `send_mail` function from the `airflow.contrib.utils.sendgrid` module has been moved.

If your configuration file looks like this:

```ini
[email]
email_backend = airflow.contrib.utils.sendgrid.send_email
```

It should look like this now:

```ini
[email]
email_backend = airflow.providers.sendgrid.utils.emailer.send_email
```

The old configuration still works but can be abandoned.

#### Unify `hostname_callable` option in `core` section

The previous option used a colon(`:`) to split the module from function. Now the dot(`.`) is used.

The change aims to unify the format of all options that refer to objects in the `airflow.cfg` file.


#### Custom executors is loaded using full import path

In previous versions of Airflow it was possible to use plugins to load custom executors. It is still
possible, but the configuration has changed. Now you don't have to create a plugin to configure a
custom executor, but you need to provide the full path to the module in the `executor` option
in the `core` section. The purpose of this change is to simplify the plugin mechanism and make
it easier to configure executor.

If your module was in the path `my_acme_company.executors.MyCustomExecutor`  and the plugin was
called `my_plugin` then your configuration looks like this

```ini
[core]
executor = my_plugin.MyCustomExecutor
```

And now it should look like this:

```ini
[core]
executor = my_acme_company.executors.MyCustomExecutor
```

The old configuration is still works but can be abandoned at any time.

#### Use `CustomSQLAInterface` instead of `SQLAInterface` for custom data models.

From Airflow 2.0, if you want to define your own Flask App Builder data models you need to use CustomSQLAInterface
instead of SQLAInterface.

For Non-RBAC replace:

```python
from flask_appbuilder.models.sqla.interface import SQLAInterface

datamodel = SQLAInterface(your_data_model)
```

with RBAC (in 1.10):

```python
from airflow.www_rbac.utils import CustomSQLAInterface

datamodel = CustomSQLAInterface(your_data_model)
```

and in 2.0:

```python
from airflow.www.utils import CustomSQLAInterface

datamodel = CustomSQLAInterface(your_data_model)
```

#### Drop plugin support for stat_name_handler

In previous version, you could use plugins mechanism to configure ``stat_name_handler``. You should now use the `stat_name_handler`
option in `[scheduler]` section to achieve the same effect.

If your plugin looked like this and was available through the `test_plugin` path:

```python
def my_stat_name_handler(stat):
    return stat

class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    stat_name_handler = my_stat_name_handler
```

then your `airflow.cfg` file should look like this:

```ini
[scheduler]
stat_name_handler=test_plugin.my_stat_name_handler
```

This change is intended to simplify the statsd configuration.

#### Logging configuration has been moved to new section

The following configurations have been moved from `[core]` to the new `[logging]` section.

* `base_log_folder`
* `remote_logging`
* `remote_log_conn_id`
* `remote_base_log_folder`
* `encrypt_s3_logs`
* `logging_level`
* `fab_logging_level`
* `logging_config_class`
* `colored_console_log`
* `colored_log_format`
* `colored_formatter_class`
* `log_format`
* `simple_log_format`
* `task_log_prefix_template`
* `log_filename_template`
* `log_processor_filename_template`
* `dag_processor_manager_log_location`
* `task_log_reader`

#### Metrics configuration has been moved to new section

The following configurations have been moved from `[scheduler]` to the new `[metrics]` section.

- `statsd_on`
- `statsd_host`
- `statsd_port`
- `statsd_prefix`
- `statsd_allow_list`
- `stat_name_handler`
- `statsd_datadog_enabled`
- `statsd_datadog_tags`
- `statsd_custom_client_path`

#### Changes to Elasticsearch logging provider

When JSON output to stdout is enabled, log lines will now contain the `log_id` & `offset` fields, this should make reading task logs from elasticsearch on the webserver work out of the box. Example configuration:

```ini
[logging]
remote_logging = True
[elasticsearch]
host = http://es-host:9200
write_stdout = True
json_format = True
```

Note that the webserver expects the log line data itself to be present in the `message` field of the document.

#### Remove gcp_service_account_keys option in airflow.cfg file

This option has been removed because it is no longer supported by the Google Kubernetes Engine. The new
recommended service account keys for the Google Cloud management method is
[Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).

#### Fernet is enabled by default

The fernet mechanism is enabled by default to increase the security of the default installation.  In order to
restore the previous behavior, the user must consciously set an empty key in the ``fernet_key`` option of
section ``[core]`` in the ``airflow.cfg`` file.

At the same time, this means that the `apache-airflow[crypto]` extra-packages are always installed.
However, this requires that your operating system has ``libffi-dev`` installed.

#### Changes to propagating Kubernetes worker annotations

`kubernetes_annotations` configuration section has been removed.
A new key `worker_annotations` has been added to existing `kubernetes` section instead.
That is to remove restriction on the character set for k8s annotation keys.
All key/value pairs from `kubernetes_annotations` should now go to `worker_annotations` as a json. I.e. instead of e.g.

```
[kubernetes_annotations]
annotation_key = annotation_value
annotation_key2 = annotation_value2
```

it should be rewritten to

```
[kubernetes]
worker_annotations = { "annotation_key" : "annotation_value", "annotation_key2" : "annotation_value2" }
```

#### Remove run_duration

We should not use the `run_duration` option anymore. This used to be for restarting the scheduler from time to time, but right now the scheduler is getting more stable and therefore using this setting is considered bad and might cause an inconsistent state.

#### Rename pool statsd metrics

Used slot has been renamed to running slot to make the name self-explanatory
and the code more maintainable.

This means `pool.used_slots.<pool_name>` metric has been renamed to
`pool.running_slots.<pool_name>`. The `Used Slots` column in Pools Web UI view
has also been changed to `Running Slots`.

#### Removal of Mesos Executor

The Mesos Executor is removed from the code base as it was not widely used and not maintained. [Mailing List Discussion on deleting it](https://lists.apache.org/thread.html/daa9500026b820c6aaadeffd66166eae558282778091ebbc68819fb7@%3Cdev.airflow.apache.org%3E).

#### Change dag loading duration metric name

Change DAG file loading duration metric from
`dag.loading-duration.<dag_id>` to `dag.loading-duration.<dag_file>`. This is to
better handle the case when a DAG file has multiple DAGs.

#### Sentry is disabled by default

Sentry is disabled by default. To enable these integrations, you need set ``sentry_on`` option
in ``[sentry]`` section to ``"True"``.

#### Simplified GCSTaskHandler configuration

In previous versions, in order to configure the service account key file, you had to create a connection entry.
In the current version, you can configure ``google_key_path`` option in ``[logging]`` section to set
the key file path.

Users using Application Default Credentials (ADC) need not take any action.

The change aims to simplify the configuration of logging, to prevent corruption of
the instance configuration by changing the value controlled by the user - connection entry. If you
configure a backend secret, it also means the webserver doesn't need to connect to it. This
simplifies setups with multiple GCP projects, because only one project will require the Secret Manager API
to be enabled.

### Changes to the core operators/hooks

We strive to ensure that there are no changes that may affect the end user and your files, but this
release may contain changes that will require changes to your DAG files.

This section describes the changes that have been made, and what you need to do to update your DAG File,
if you use core operators or any other.

#### BaseSensorOperator now respects the trigger_rule of downstream tasks

Previously, BaseSensorOperator with setting `soft_fail=True` skips itself
and skips all its downstream tasks unconditionally, when it fails i.e the trigger_rule of downstream tasks is not
respected.

In the new behavior, the trigger_rule of downstream tasks is respected.
User can preserve/achieve the original behaviour by setting the trigger_rule of each downstream task to `all_success`.

#### BaseOperator uses metaclass

`BaseOperator` class uses a `BaseOperatorMeta` as a metaclass. This meta class is based on
`abc.ABCMeta`. If your custom operator uses different metaclass then you will have to adjust it.

#### Remove SQL support in BaseHook

Remove ``get_records`` and ``get_pandas_df`` and ``run`` from BaseHook, which only apply for SQL-like hook,
If want to use them, or your custom hook inherit them, please use ``airflow.hooks.dbapi.DbApiHook``

#### Assigning task to a DAG using bitwise shift (bit-shift) operators are no longer supported

Previously, you could assign a task to a DAG as follows:

```python
dag = DAG('my_dag')
dummy = DummyOperator(task_id='dummy')

dag >> dummy
```

This is no longer supported. Instead, we recommend using the DAG as context manager:

```python
with DAG('my_dag') as dag:
    dummy = DummyOperator(task_id='dummy')
```

#### Removed deprecated import mechanism

The deprecated import mechanism has been removed so the import of modules becomes more consistent and explicit.

For example: `from airflow.operators import BashOperator`
becomes `from airflow.operators.bash_operator import BashOperator`

#### Changes to sensor imports

Sensors are now accessible via `airflow.sensors` and no longer via `airflow.operators.sensors`.

For example: `from airflow.operators.sensors import BaseSensorOperator`
becomes `from airflow.sensors.base import BaseSensorOperator`

#### Skipped tasks can satisfy wait_for_downstream

Previously, a task instance with `wait_for_downstream=True` will only run if the downstream task of
the previous task instance is successful. Meanwhile, a task instance with `depends_on_past=True`
will run if the previous task instance is either successful or skipped. These two flags are close siblings
yet they have different behavior. This inconsistency in behavior made the API less intuitive to users.
To maintain consistent behavior, both successful or skipped downstream task can now satisfy the
`wait_for_downstream=True` flag.

#### `airflow.utils.helpers.cross_downstream`

#### `airflow.utils.helpers.chain`

The `chain` and `cross_downstream` methods are now moved to airflow.models.baseoperator module from
`airflow.utils.helpers` module.

The baseoperator module seems to be a better choice to keep
closely coupled methods together. Helpers module is supposed to contain standalone helper methods
that can be imported by all classes.

The `chain` method and `cross_downstream` method both use BaseOperator. If any other package imports
any classes or functions from helpers module, then it automatically has an
implicit dependency to BaseOperator. That can often lead to cyclic dependencies.

More information in [AIRFLOW-6392](https://issues.apache.org/jira/browse/AIRFLOW-6392)

In Airflow < 2.0 you imported those two methods like this:

```python
from airflow.utils.helpers import chain
from airflow.utils.helpers import cross_downstream
```

In Airflow 2.0 it should be changed to:

```python
from airflow.models.baseoperator import chain
from airflow.models.baseoperator import cross_downstream
```

#### `airflow.operators.python.BranchPythonOperator`

`BranchPythonOperator` will now return a value equal to the `task_id` of the chosen branch,
where previously it returned None. Since it inherits from BaseOperator it will do an
`xcom_push` of this value if `do_xcom_push=True`. This is useful for downstream decision-making.

#### `airflow.sensors.sql_sensor.SqlSensor`

SQLSensor now consistent with python `bool()` function and the `allow_null` parameter has been removed.

It will resolve after receiving any value  that is casted to `True` with python `bool(value)`. That
changes the previous response receiving `NULL` or `'0'`. Earlier `'0'` has been treated as success
criteria. `NULL` has been treated depending on value of `allow_null`parameter.  But all the previous
behaviour is still achievable setting param `success` to `lambda x: x is None or str(x) not in ('0', '')`.

#### `airflow.operators.trigger_dagrun.TriggerDagRunOperator`

The TriggerDagRunOperator now takes a `conf` argument to which a dict can be provided as conf for the DagRun.
As a result, the `python_callable` argument was removed. PR: https://github.com/apache/airflow/pull/6317.

#### `airflow.operators.python.PythonOperator`

`provide_context` argument on the PythonOperator was removed. The signature of the callable passed to the PythonOperator is now inferred and argument values are always automatically provided. There is no need to explicitly provide or not provide the context anymore. For example:

```python
def myfunc(execution_date):
    print(execution_date)

python_operator = PythonOperator(task_id='mytask', python_callable=myfunc, dag=dag)
```

Notice you don't have to set provide_context=True, variables from the task context are now automatically detected and provided.

All context variables can still be provided with a double-asterisk argument:

```python
def myfunc(**context):
    print(context)  # all variables will be provided to context

python_operator = PythonOperator(task_id='mytask', python_callable=myfunc)
```

The task context variable names are reserved names in the callable function, hence a clash with `op_args` and `op_kwargs` results in an exception:

```python
def myfunc(dag):
    # raises a ValueError because "dag" is a reserved name
    # valid signature example: myfunc(mydag)

python_operator = PythonOperator(
    task_id='mytask',
    op_args=[1],
    python_callable=myfunc,
)
```

The change is backwards compatible, setting `provide_context` will add the `provide_context` variable to the `kwargs` (but won't do anything).

PR: [#5990](https://github.com/apache/airflow/pull/5990)

#### `airflow.sensors.filesystem.FileSensor`

FileSensor is now takes a glob pattern, not just a filename. If the filename you are looking for has `*`, `?`, or `[` in it then you should replace these with `[*]`, `[?]`, and `[[]`.

#### `airflow.operators.subdag_operator.SubDagOperator`

`SubDagOperator` is changed to use Airflow scheduler instead of backfill
to schedule tasks in the subdag. User no longer need to specify the executor
in `SubDagOperator`.


#### `airflow.providers.google.cloud.operators.datastore.CloudDatastoreExportEntitiesOperator`

#### `airflow.providers.google.cloud.operators.datastore.CloudDatastoreImportEntitiesOperator`

#### `airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`

#### `airflow.providers.ssh.operators.ssh.SSHOperator`

#### `airflow.providers.microsoft.winrm.operators.winrm.WinRMOperator`

#### `airflow.operators.bash.BashOperator`

#### `airflow.providers.docker.operators.docker.DockerOperator`

#### `airflow.providers.http.operators.http.SimpleHttpOperator`

#### `airflow.providers.http.operators.http.SimpleHttpOperator`

The `do_xcom_push` flag (a switch to push the result of an operator to xcom or not) was appearing in different incarnations in different operators. It's function has been unified under a common name (`do_xcom_push`) on `BaseOperator`. This way it is also easy to globally disable pushing results to xcom.

The following operators were affected:

* DatastoreExportOperator (Backwards compatible)
* DatastoreImportOperator (Backwards compatible)
* KubernetesPodOperator (Not backwards compatible)
* SSHOperator (Not backwards compatible)
* WinRMOperator (Not backwards compatible)
* BashOperator (Not backwards compatible)
* DockerOperator (Not backwards compatible)
* SimpleHttpOperator (Not backwards compatible)

See [AIRFLOW-3249](https://jira.apache.org/jira/browse/AIRFLOW-3249) for details

#### `airflow.operators.latest_only_operator.LatestOnlyOperator`

In previous versions, the `LatestOnlyOperator` forcefully skipped all (direct and undirect) downstream tasks on its own. From this version on the operator will **only skip direct downstream** tasks and the scheduler will handle skipping any further downstream dependencies.

No change is needed if only the default trigger rule `all_success` is being used.

If the DAG relies on tasks with other trigger rules (i.e. `all_done`) being skipped by the `LatestOnlyOperator`, adjustments to the DAG need to be made to accommodate the change in behaviour, i.e. with additional edges from the `LatestOnlyOperator`.

The goal of this change is to achieve a more consistent and configurale cascading behaviour based on the `BaseBranchOperator` (see [AIRFLOW-2923](https://jira.apache.org/jira/browse/AIRFLOW-2923) and [AIRFLOW-1784](https://jira.apache.org/jira/browse/AIRFLOW-1784)).

### Changes to the core Python API

We strive to ensure that there are no changes that may affect the end user, and your Python files, but this
release may contain changes that will require changes to your plugins, DAG File or other integration.

Only changes unique to this provider are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your Python files.

#### Removed sub-package imports from `airflow/__init__.py`

The imports `LoggingMixin`, `conf`, and `AirflowException` have been removed from `airflow/__init__.py`.
All implicit references of these objects will no longer be valid. To migrate, all usages of each old path must be
replaced with its corresponding new path.

| Old Path (Implicit Import)   | New Path (Explicit Import)                       |
|------------------------------|--------------------------------------------------|
| ``airflow.LoggingMixin``     | ``airflow.utils.log.logging_mixin.LoggingMixin`` |
| ``airflow.conf``             | ``airflow.configuration.conf``                   |
| ``airflow.AirflowException`` | ``airflow.exceptions.AirflowException``          |

#### Variables removed from the task instance context

The following variables were removed from the task instance context:

- end_date
- latest_date
- tables


#### `airflow.contrib.utils.Weekday`

Formerly the core code was maintained by the original creators - Airbnb. The code that was in the contrib
package was supported by the community. The project was passed to the Apache community and currently the
entire code is maintained by the community, so now the division has no justification, and it is only due
to historical reasons.

To clean up, `Weekday` enum has been moved from `airflow.contrib.utils` into `airflow.utils` module.

#### `airflow.models.connection.Connection`

The connection module has new deprecated methods:

- `Connection.parse_from_uri`
- `Connection.log_info`
- `Connection.debug_info`

and one deprecated function:

- `parse_netloc_to_hostname`

Previously, users could create a connection object in two ways

```
conn_1 = Connection(conn_id="conn_a", uri="mysql://AAA/")
# or
conn_2 = Connection(conn_id="conn_a")
conn_2.parse_uri(uri="mysql://AAA/")
```

Now the second way is not supported.

`Connection.log_info` and `Connection.debug_info` method have been deprecated. Read each Connection field individually or use the
default representation (`__repr__`).

The old method is still works but can be abandoned at any time. The changes are intended to delete method
that are rarely used.

#### `airflow.models.dag.DAG.create_dagrun`

DAG.create_dagrun accepts run_type and does not require run_id
This change is caused by adding `run_type` column to `DagRun`.

Previous signature:

```python
def create_dagrun(self,
                  run_id,
                  state,
                  execution_date=None,
                  start_date=None,
                  external_trigger=False,
                  conf=None,
                  session=None):
```

current:

```python
def create_dagrun(self,
                  state,
                  execution_date=None,
                  run_id=None,
                  start_date=None,
                  external_trigger=False,
                  conf=None,
                  run_type=None,
                  session=None):
```

If user provides `run_id` then the `run_type` will be derived from it by checking prefix, allowed types
: `manual`, `scheduled`, `backfill` (defined by `airflow.utils.types.DagRunType`).

If user provides `run_type` and `execution_date` then `run_id` is constructed as
`{run_type}__{execution_data.isoformat()}`.

Airflow should construct dagruns using `run_type` and `execution_date`, creation using
`run_id` is preserved for user actions.


#### `airflow.models.dagrun.DagRun`

Use DagRunType.SCHEDULED.value instead of DagRun.ID_PREFIX

All the run_id prefixes for different kind of DagRuns have been grouped into a single
enum in `airflow.utils.types.DagRunType`.

Previously, there were defined in various places, example as `ID_PREFIX` class variables for
`DagRun`, `BackfillJob` and in `_trigger_dag` function.

Was:

```python
>> from airflow.models.dagrun import DagRun
>> DagRun.ID_PREFIX
scheduled__
```

Replaced by:

```python
>> from airflow.utils.types import DagRunType
>> DagRunType.SCHEDULED.value
scheduled
```


#### `airflow.utils.file.TemporaryDirectory`

We remove airflow.utils.file.TemporaryDirectory
Since Airflow dropped support for Python < 3.5 there's no need to have this custom
implementation of `TemporaryDirectory` because the same functionality is provided by
`tempfile.TemporaryDirectory`.

Now users instead of `import from airflow.utils.files import TemporaryDirectory` should
do `from tempfile import TemporaryDirectory`. Both context managers provide the same
interface, thus no additional changes should be required.

#### `airflow.AirflowMacroPlugin`

We removed `airflow.AirflowMacroPlugin` class. The class was there in airflow package but it has not been used (apparently since 2015).
It has been removed.

#### `airflow.settings.CONTEXT_MANAGER_DAG`

CONTEXT_MANAGER_DAG was removed from settings. It's role has been taken by `DagContext` in
'airflow.models.dag'. One of the reasons was that settings should be rather static than store
dynamic context from the DAG, but the main one is that moving the context out of settings allowed to
untangle cyclic imports between DAG, BaseOperator, SerializedDAG, SerializedBaseOperator which was
part of AIRFLOW-6010.

#### `airflow.utils.log.logging_mixin.redirect_stderr`

#### `airflow.utils.log.logging_mixin.redirect_stdout`

Function `redirect_stderr` and `redirect_stdout` from `airflow.utils.log.logging_mixin` module has
been deleted because it can be easily replaced by the standard library.
The functions of the standard library are more flexible and can be used in larger cases.

The code below

```python
import logging

from airflow.utils.log.logging_mixin import redirect_stderr, redirect_stdout

logger = logging.getLogger("custom-logger")
with redirect_stdout(logger, logging.INFO), redirect_stderr(logger, logging.WARN):
    print("I love Airflow")
```

can be replaced by the following code:

```python
from contextlib import redirect_stdout, redirect_stderr
import logging

from airflow.utils.log.logging_mixin import StreamLogWriter

logger = logging.getLogger("custom-logger")

with redirect_stdout(StreamLogWriter(logger, logging.INFO)), \
        redirect_stderr(StreamLogWriter(logger, logging.WARN)):
    print("I Love Airflow")
```

#### `airflow.models.baseoperator.BaseOperator`

Now, additional arguments passed to BaseOperator cause an exception. Previous versions of Airflow took additional arguments and displayed a message on the console. When the
message was not noticed by users, it caused very difficult to detect errors.

In order to restore the previous behavior, you must set an ``True`` in  the ``allow_illegal_arguments``
option of section ``[operators]`` in the ``airflow.cfg`` file. In the future it is possible to completely
delete this option.

#### `airflow.models.dagbag.DagBag`

Passing `store_serialized_dags` argument to DagBag.__init__ and accessing `DagBag.store_serialized_dags` property
are deprecated and will be removed in future versions.


**Previous signature**:

```python
DagBag(
    dag_folder=None,
    include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
    safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'),
    store_serialized_dags=False
):
```

**current**:

```python
DagBag(
    dag_folder=None,
    include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
    safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE'),
    read_dags_from_db=False
):
```

If you were using positional arguments, it requires no change but if you were using keyword
arguments, please change `store_serialized_dags` to `read_dags_from_db`.

Similarly, if you were using `DagBag().store_serialized_dags` property, change it to
`DagBag().read_dags_from_db`.

### Changes in `google` provider package

We strive to ensure that there are no changes that may affect the end user and your Python files, but this
release may contain changes that will require changes to your configuration, DAG Files or other integration
e.g. custom operators.

Only changes unique to this provider are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your if
you use operators or hooks which integrate with Google services (including Google Cloud - GCP).

#### Direct impersonation added to operators communicating with Google services

[Directly impersonating a service account](https://cloud.google.com/iam/docs/understanding-service-accounts#directly_impersonating_a_service_account)
has been made possible for operators communicating with Google services via new argument called `impersonation_chain`
(`google_impersonation_chain` in case of operators that also communicate with services of other cloud providers).
As a result, GCSToS3Operator no longer derivatives from GCSListObjectsOperator.

#### Normalize gcp_conn_id for Google Cloud

Previously not all hooks and operators related to Google Cloud use
`gcp_conn_id` as parameter for GCP connection. There is currently one parameter
which apply to most services. Parameters like ``datastore_conn_id``, ``bigquery_conn_id``,
``google_cloud_storage_conn_id`` and similar have been deprecated. Operators that require two connections are not changed.

Following components were affected by normalization:

  * airflow.providers.google.cloud.hooks.datastore.DatastoreHook
  * airflow.providers.google.cloud.hooks.bigquery.BigQueryHook
  * airflow.providers.google.cloud.hooks.gcs.GoogleCloudStorageHook
  * airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator
  * airflow.providers.google.cloud.operators.bigquery.BigQueryTableDeleteOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageCreateBucketOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageListOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageDownloadOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageDeleteOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageBucketCreateAclEntryOperator
  * airflow.providers.google.cloud.operators.gcs.GoogleCloudStorageObjectCreateAclEntryOperator
  * airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator
  * airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator
  * airflow.operators.gcs_to_s3.GoogleCloudStorageToS3Operator
  * airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator
  * airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator
  * airflow.operators.local_to_gcs.FileToGoogleCloudStorageOperator
  * airflow.operators.cassandra_to_gcs.CassandraToGoogleCloudStorageOperator
  * airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator

#### Changes to import paths and names of GCP operators and hooks

According to [AIP-21](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths)
operators related to Google Cloud has been moved from contrib to core.
The following table shows changes in import paths.

|                                                     Old path                                                     |                                                 New path                                                                     |
|------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
|airflow.contrib.hooks.bigquery_hook.BigQueryHook                                                                  |airflow.providers.google.cloud.hooks.bigquery.BigQueryHook                                                                    |
|airflow.contrib.hooks.datastore_hook.DatastoreHook                                                                |airflow.providers.google.cloud.hooks.datastore.DatastoreHook                                                                  |
|airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook                                                              |airflow.providers.google.cloud.hooks.bigtable.BigtableHook                                                                    |
|airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook                                                         |airflow.providers.google.cloud.hooks.cloud_build.CloudBuildHook                                                               |
|airflow.contrib.hooks.gcp_container_hook.GKEClusterHook                                                           |airflow.providers.google.cloud.hooks.kubernetes_engine.GKEHook                                                                |
|airflow.contrib.hooks.gcp_compute_hook.GceHook                                                                    |airflow.providers.google.cloud.hooks.compute.ComputeEngineHook                                                                |
|airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook                                                              |airflow.providers.google.cloud.hooks.dataflow.DataflowHook                                                                    |
|airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook                                                              |airflow.providers.google.cloud.hooks.dataproc.DataprocHook                                                                    |
|airflow.contrib.hooks.gcp_dlp_hook.CloudDLPHook                                                                   |airflow.providers.google.cloud.hooks.dlp.CloudDLPHook                                                                         |
|airflow.contrib.hooks.gcp_function_hook.GcfHook                                                                   |airflow.providers.google.cloud.hooks.functions.CloudFunctionsHook                                                             |
|airflow.contrib.hooks.gcp_kms_hook.GoogleCloudKMSHook                                                             |airflow.providers.google.cloud.hooks.kms.CloudKMSHook                                                                         |
|airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook                                                              |airflow.providers.google.cloud.hooks.mlengine.MLEngineHook                                                                    |
|airflow.contrib.hooks.gcp_natural_language_hook.CloudNaturalLanguageHook                                          |airflow.providers.google.cloud.hooks.natural_language.CloudNaturalLanguageHook                                                |
|airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook                                                                  |airflow.providers.google.cloud.hooks.pubsub.PubSubHook                                                                        |
|airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook                                                 |airflow.providers.google.cloud.hooks.speech_to_text.CloudSpeechToTextHook                                                     |
|airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook                                                           |airflow.providers.google.cloud.hooks.spanner.SpannerHook                                                                      |
|airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook                                                           |airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook                                                           |
|airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook                                                                   |airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLHook                                                                   |
|airflow.contrib.hooks.gcp_tasks_hook.CloudTasksHook                                                               |airflow.providers.google.cloud.hooks.tasks.CloudTasksHook                                                                     |
|airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook                                                 |airflow.providers.google.cloud.hooks.text_to_speech.CloudTextToSpeechHook                                                     |
|airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook                                                    |airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.CloudDataTransferServiceHook                              |
|airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook                                                       |airflow.providers.google.cloud.hooks.translate.CloudTranslateHook                                                             |
|airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook                                      |airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook                                            |
|airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook                                                             |airflow.providers.google.cloud.hooks.vision.CloudVisionHook                                                                   |
|airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook                                                             |airflow.providers.google.cloud.hooks.gcs.GCSHook                                                                              |
|airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator                                            |airflow.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator                                                                |
|airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator                                           |airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator                                                       |
|airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator                                   |airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator                                               |
|airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator                                      |airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator                                                  |
|airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator                                               |airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator                                                     |
|airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyDatasetOperator                                    |airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator                                          |
|airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyTableOperator                                      |airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator                                            |
|airflow.contrib.operators.bigquery_operator.BigQueryCreateExternalTableOperator                                   |airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator                                         |
|airflow.contrib.operators.bigquery_operator.BigQueryDeleteDatasetOperator                                         |airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator                                               |
|airflow.contrib.operators.bigquery_operator.BigQueryOperator                                                      |airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator                                                |
|airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator                              |airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator                                                 |
|airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator                                         |airflow.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator                                                             |
|airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator                                          |airflow.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator                                                              |
|airflow.contrib.operators.bigquery_to_mysql_operator.BigQueryToMySqlOperator                                      |airflow.operators.bigquery_to_mysql.BigQueryToMySqlOperator                                                                   |
|airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator                                                  |airflow.providers.google.cloud.operators.dataflow.DataFlowJavaOperator                                                        |
|airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator                                                |airflow.providers.google.cloud.operators.dataflow.DataFlowPythonOperator                                                      |
|airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator                                              |airflow.providers.google.cloud.operators.dataflow.DataflowTemplateOperator                                                    |
|airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator                                                |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator                                             |
|airflow.contrib.operators.dataproc_operator.DataProcHiveOperator                                                  |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataProcJobBaseOperator                                               |airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator                                                     |
|airflow.contrib.operators.dataproc_operator.DataProcPigOperator                                                   |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator                                                |
|airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator                                               |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator                                            |
|airflow.contrib.operators.dataproc_operator.DataProcSparkOperator                                                 |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator                                              |
|airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator                                              |airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator                                           |
|airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator                                         |airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator                                         |airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator                                          |airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator                                                |
|airflow.contrib.operators.dataproc_operator.DataprocOperationBaseOperator                                         |airflow.providers.google.cloud.operators.dataproc.DataprocOperationBaseOperator                                               |
|airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator                     |airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateInlineWorkflowTemplateOperator                           |
|airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator                           |airflow.providers.google.cloud.operators.dataproc.DataprocInstantiateWorkflowTemplateOperator                                 |
|airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator                                       |airflow.providers.google.cloud.operators.datastore.DatastoreExportOperator                                                    |
|airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator                                       |airflow.providers.google.cloud.operators.datastore.DatastoreImportOperator                                                    |
|airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator                                            |airflow.providers.google.cloud.transfers.local_to_gcs.FileToGoogleCloudStorageOperator                                        |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableClusterUpdateOperator                                     |airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator                                               |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceCreateOperator                                    |airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator                                              |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableInstanceDeleteOperator                                    |airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator                                              |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableTableCreateOperator                                       |airflow.providers.google.cloud.operators.bigtable.BigtableCreateTableOperator                                                 |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableTableDeleteOperator                                       |airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator                                                 |
|airflow.contrib.operators.gcp_bigtable_operator.BigtableTableWaitForReplicationSensor                             |airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor                                       |
|airflow.contrib.operators.gcp_cloud_build_operator.CloudBuildCreateBuildOperator                                  |airflow.providers.google.cloud.operators.cloud_build.CloudBuildCreateBuildOperator                                            |
|airflow.contrib.operators.gcp_compute_operator.GceBaseOperator                                                    |airflow.providers.google.cloud.operators.compute.GceBaseOperator                                                              |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceGroupManagerUpdateTemplateOperator                      |airflow.providers.google.cloud.operators.compute.GceInstanceGroupManagerUpdateTemplateOperator                                |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceStartOperator                                           |airflow.providers.google.cloud.operators.compute.GceInstanceStartOperator                                                     |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceStopOperator                                            |airflow.providers.google.cloud.operators.compute.GceInstanceStopOperator                                                      |
|airflow.contrib.operators.gcp_compute_operator.GceInstanceTemplateCopyOperator                                    |airflow.providers.google.cloud.operators.compute.GceInstanceTemplateCopyOperator                                              |
|airflow.contrib.operators.gcp_compute_operator.GceSetMachineTypeOperator                                          |airflow.providers.google.cloud.operators.compute.GceSetMachineTypeOperator                                                    |
|airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator                                         |airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator                                           |
|airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator                                         |airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator                                           |
|airflow.contrib.operators.gcp_container_operator.GKEPodOperator                                                   |airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCancelDLPJobOperator                                           |airflow.providers.google.cloud.operators.dlp.CloudDLPCancelDLPJobOperator                                                     |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDLPJobOperator                                           |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDLPJobOperator                                                     |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateDeidentifyTemplateOperator                               |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator                                         |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateInspectTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateInspectTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateJobTriggerOperator                                       |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateJobTriggerOperator                                                 |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPCreateStoredInfoTypeOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPCreateStoredInfoTypeOperator                                             |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeidentifyContentOperator                                      |airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDeidentifyTemplateOperator                               |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator                                         |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteDlpJobOperator                                           |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator                                                     |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteInspectTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteJobTriggerOperator                                       |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator                                                 |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPDeleteStoredInfoTypeOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator                                             |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDeidentifyTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPGetDeidentifyTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetDlpJobOperator                                              |airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator                                                        |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetInspectTemplateOperator                                     |airflow.providers.google.cloud.operators.dlp.CloudDLPGetInspectTemplateOperator                                               |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetJobTripperOperator                                          |airflow.providers.google.cloud.operators.dlp.CloudDLPGetJobTriggerOperator                                                    |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPGetStoredInfoTypeOperator                                      |airflow.providers.google.cloud.operators.dlp.CloudDLPGetStoredInfoTypeOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPInspectContentOperator                                         |airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator                                                   |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDeidentifyTemplatesOperator                                |airflow.providers.google.cloud.operators.dlp.CloudDLPListDeidentifyTemplatesOperator                                          |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListDlpJobsOperator                                            |airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator                                                      |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInfoTypesOperator                                          |airflow.providers.google.cloud.operators.dlp.CloudDLPListInfoTypesOperator                                                    |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListInspectTemplatesOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPListInspectTemplatesOperator                                             |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListJobTriggersOperator                                        |airflow.providers.google.cloud.operators.dlp.CloudDLPListJobTriggersOperator                                                  |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPListStoredInfoTypesOperator                                    |airflow.providers.google.cloud.operators.dlp.CloudDLPListStoredInfoTypesOperator                                              |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPRedactImageOperator                                            |airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator                                                      |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPReidentifyContentOperator                                      |airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator                                                |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateDeidentifyTemplateOperator                               |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator                                         |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateInspectTemplateOperator                                  |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateInspectTemplateOperator                                            |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateJobTriggerOperator                                       |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateJobTriggerOperator                                                 |
|airflow.contrib.operators.gcp_dlp_operator.CloudDLPUpdateStoredInfoTypeOperator                                   |airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateStoredInfoTypeOperator                                             |
|airflow.contrib.operators.gcp_function_operator.GcfFunctionDeleteOperator                                         |airflow.providers.google.cloud.operators.functions.GcfFunctionDeleteOperator                                                  |
|airflow.contrib.operators.gcp_function_operator.GcfFunctionDeployOperator                                         |airflow.providers.google.cloud.operators.functions.GcfFunctionDeployOperator                                                  |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeEntitiesOperator               |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitiesOperator                         |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeEntitySentimentOperator        |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeEntitySentimentOperator                  |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageAnalyzeSentimentOperator              |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageAnalyzeSentimentOperator                        |
|airflow.contrib.operators.gcp_natural_language_operator.CloudNaturalLanguageClassifyTextOperator                  |airflow.providers.google.cloud.operators.natural_language.CloudNaturalLanguageClassifyTextOperator                            |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator                         |airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator                                        |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator                         |airflow.providers.google.cloud.operators.spanner.SpannerDeployDatabaseInstanceOperator                                        |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator                          |airflow.providers.google.cloud.operators.spanner.SpannerQueryDatabaseInstanceOperator                                         |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator                         |airflow.providers.google.cloud.operators.spanner.SpannerUpdateDatabaseInstanceOperator                                        |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator                                 |airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator                                                |
|airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeployOperator                                 |airflow.providers.google.cloud.operators.spanner.SpannerDeployInstanceOperator                                                |
|airflow.contrib.operators.gcp_speech_to_text_operator.GcpSpeechToTextRecognizeSpeechOperator                      |airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextRecognizeSpeechOperator                              |
|airflow.contrib.operators.gcp_text_to_speech_operator.GcpTextToSpeechSynthesizeOperator                           |airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechSynthesizeOperator                                   |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobCreateOperator                               |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator             |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobDeleteOperator                               |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceDeleteJobOperator             |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceJobUpdateOperator                               |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceUpdateJobOperator             |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationCancelOperator                         |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCancelOperationOperator       |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationGetOperator                            |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGetOperationOperator          |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationPauseOperator                          |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServicePauseOperationOperator        |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationResumeOperator                         |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceResumeOperationOperator       |
|airflow.contrib.operators.gcp_transfer_operator.GcpTransferServiceOperationsListOperator                          |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceListOperationsOperator        |
|airflow.contrib.operators.gcp_transfer_operator.GoogleCloudStorageToGoogleCloudStorageTransferOperator            |airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceGCSToGCSOperator              |
|airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator                                       |airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator                                                 |
|airflow.contrib.operators.gcp_translate_speech_operator.GcpTranslateSpeechOperator                                |airflow.providers.google.cloud.operators.translate_speech.GcpTranslateSpeechOperator                                          |
|airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoExplicitContentOperator|airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoExplicitContentOperator          |
|airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoLabelsOperator         |airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoLabelsOperator                   |
|airflow.contrib.operators.gcp_video_intelligence_operator.CloudVideoIntelligenceDetectVideoShotsOperator          |airflow.providers.google.cloud.operators.video_intelligence.CloudVideoIntelligenceDetectVideoShotsOperator                    |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionAddProductToProductSetOperator                           |airflow.providers.google.cloud.operators.vision.CloudVisionAddProductToProductSetOperator                                     |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionAnnotateImageOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionImageAnnotateOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectDocumentTextOperator                               |airflow.providers.google.cloud.operators.vision.CloudVisionTextDetectOperator                                                 |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageLabelsOperator                                |airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageLabelsOperator                                          |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectImageSafeSearchOperator                            |airflow.providers.google.cloud.operators.vision.CloudVisionDetectImageSafeSearchOperator                                      |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionDetectTextOperator                                       |airflow.providers.google.cloud.operators.vision.CloudVisionDetectTextOperator                                                 |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator                                       |airflow.providers.google.cloud.operators.vision.CloudVisionGetProductOperator                                                 |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator                                 |airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator                                           |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator                                 |airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator                                           |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionGetProductSetOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator                                 |airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductSetOperator                                           |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator                                    |airflow.providers.google.cloud.operators.vision.CloudVisionUpdateProductOperator                                              |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionReferenceImageCreateOperator                             |airflow.providers.google.cloud.operators.vision.CloudVisionCreateReferenceImageOperator                                       |
|airflow.contrib.operators.gcp_vision_operator.CloudVisionRemoveProductFromProductSetOperator                      |airflow.providers.google.cloud.operators.vision.CloudVisionRemoveProductFromProductSetOperator                                |
|airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageBucketCreateAclEntryOperator                         |airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator                                                  |
|airflow.contrib.operators.gcs_acl_operator.GoogleCloudStorageObjectCreateAclEntryOperator                         |airflow.providers.google.cloud.operators.gcs.GCSObjectCreateAclEntryOperator                                                  |
|airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageDeleteOperator                                    |airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator                                                         |
|airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator                                |airflow.providers.google.cloud.operators.gcs.GCSToLocalFilesystemOperator                                                               |
|airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator                                        |airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator                                                           |
|airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator                                     |airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator                                                          |
|airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator                                          |airflow.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator                                                              |
|airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator                               |airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator                                                   |
|airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageToS3Operator                                                |airflow.operators.gcs_to_s3.GCSToS3Operator                                                                                   |
|airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator                                       |airflow.providers.google.cloud.operators.mlengine.MLEngineStartBatchPredictionJobOperator                                     |
|airflow.contrib.operators.mlengine_operator.MLEngineModelOperator                                                 |airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator                                                 |
|airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator                                              |airflow.providers.google.cloud.operators.mlengine.MLEngineStartTrainingJobOperator                                            |
|airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator                                               |airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator                                               |
|airflow.contrib.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator                                          |airflow.operators.mssql_to_gcs.MsSqlToGoogleCloudStorageOperator                                                              |
|airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator                                          |airflow.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator                                                              |
|airflow.contrib.operators.postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator                           |airflow.operators.postgres_to_gcs.PostgresToGoogleCloudStorageOperator                                                        |
|airflow.contrib.operators.pubsub_operator.PubSubPublishOperator                                                   |airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator                                                  |
|airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator                                        |airflow.providers.google.cloud.operators.pubsub.PubSubCreateSubscriptionOperator                                              |
|airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator                                        |airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator                                              |
|airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator                                               |airflow.providers.google.cloud.operators.pubsub.PubSubCreateTopicOperator                                                     |
|airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator                                               |airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator                                                     |
|airflow.contrib.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator                                          |airflow.operators.sql_to_gcs.BaseSQLToGoogleCloudStorageOperator                                                              |
|airflow.contrib.sensors.bigquery_sensor.BigQueryTableSensor                                                       |airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor                                                  |
|airflow.contrib.sensors.gcp_transfer_sensor.GCPTransferServiceWaitForJobStatusSensor                              |airflow.providers.google.cloud.sensors.cloud_storage_transfer_service.DataTransferServiceJobStatusSensor                      |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectSensor                                                 |airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor                                                           |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectUpdatedSensor                                          |airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor                                                              |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStoragePrefixSensor                                                 |airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor                                                |
|airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageUploadSessionCompleteSensor                                  |airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor                                                     |
|airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor                                                            |airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor                                                                |

#### Unify default conn_id for Google Cloud

Previously not all hooks and operators related to Google Cloud use
``google_cloud_default`` as a default conn_id. There is currently one default
variant. Values like ``google_cloud_storage_default``, ``bigquery_default``,
``google_cloud_datastore_default`` have been deprecated. The configuration of
existing relevant connections in the database have been preserved. To use those
deprecated GCP conn_id, you need to explicitly pass their conn_id into
operators/hooks. Otherwise, ``google_cloud_default`` will be used as GCP's conn_id
by default.

#### `airflow.providers.google.cloud.hooks.dataflow.DataflowHook`

#### `airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator`

#### `airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator`

#### `airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`

To use project_id argument consistently across GCP hooks and operators, we did the following changes:

- Changed order of arguments in DataflowHook.start_python_dataflow. Uses
    with positional arguments may break.
- Changed order of arguments in DataflowHook.is_job_dataflow_running. Uses
    with positional arguments may break.
- Changed order of arguments in DataflowHook.cancel_job. Uses
    with positional arguments may break.
- Added optional project_id argument to DataflowCreateJavaJobOperator
    constructor.
- Added optional project_id argument to DataflowTemplatedJobStartOperator
    constructor.
- Added optional project_id argument to DataflowCreatePythonJobOperator
    constructor.

#### `airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`

To provide more precise control in handling of changes to objects in
underlying GCS Bucket the constructor of this sensor now has changed.

- Old Behavior: This constructor used to optionally take ``previous_num_objects: int``.
- New replacement constructor kwarg: ``previous_objects: Optional[Set[str]]``.

Most users would not specify this argument because the bucket begins empty
and the user wants to treat any files as new.

Example of Updating usage of this sensor:
Users who used to call:

``GCSUploadSessionCompleteSensor(bucket='my_bucket', prefix='my_prefix', previous_num_objects=1)``

Will now call:

``GCSUploadSessionCompleteSensor(bucket='my_bucket', prefix='my_prefix', previous_num_objects={'.keep'})``

Where '.keep' is a single file at your prefix that the sensor should not consider new.

#### `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor`

#### `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook`

To simplify BigQuery operators (no need of `Cursor`) and standardize usage of hooks within all GCP integration methods from `BiqQueryBaseCursor`
were moved to `BigQueryHook`. Using them by from `Cursor` object is still possible due to preserved backward compatibility but they will raise `DeprecationWarning`.
The following methods were moved:

| Old path                                                                                       | New path                                                                                 |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.cancel_query                  | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_query                  |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_dataset          | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_dataset          |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_table            | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table            |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_external_table         | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_external_table         |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.delete_dataset                | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_dataset                |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset                   | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset                   |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset_tables            | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables            |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset_tables_list       | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables_list       |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_datasets_list             | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_datasets_list             |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_schema                    | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema                    |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_tabledata                 | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_tabledata                 |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.insert_all                    | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_all                    |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.patch_dataset                 | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_dataset                 |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.patch_table                   | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_table                   |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.poll_job_complete             | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete             |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_copy                      | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_copy                      |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_extract                   | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_extract                   |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_grant_dataset_view_access | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_grant_dataset_view_access |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_load                      | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_load                      |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_query                     | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_query                     |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_delete              | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_delete              |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_upsert              | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_upsert              |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_with_configuration        | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration        |
| airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.update_dataset                | airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset                |

#### `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor`

Since BigQuery is the part of the GCP it was possible to simplify the code by handling the exceptions
by usage of the `airflow.providers.google.common.hooks.base.GoogleBaseHook.catch_http_exception` decorator however it changes
exceptions raised by the following methods:

* `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.run_table_delete` raises `AirflowException` instead of `Exception`.
* `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.create_empty_dataset` raises `AirflowException` instead of `ValueError`.
* `airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor.get_dataset` raises `AirflowException` instead of `ValueError`.

#### `airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator`

#### `airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator`

Idempotency was added to `BigQueryCreateEmptyTableOperator` and `BigQueryCreateEmptyDatasetOperator`.
But to achieve that try / except clause was removed from `create_empty_dataset` and `create_empty_table`
methods of `BigQueryHook`.

#### `airflow.providers.google.cloud.hooks.dataflow.DataflowHook`

#### `airflow.providers.google.cloud.hooks.mlengine.MLEngineHook`

#### `airflow.providers.google.cloud.hooks.pubsub.PubSubHook`

The change in GCP operators implies that GCP Hooks for those operators require now keyword parameters rather
than positional ones in all methods where `project_id` is used. The methods throw an explanatory exception
in case they are called using positional parameters.

Other GCP hooks are unaffected.

#### `airflow.providers.google.cloud.hooks.pubsub.PubSubHook`

#### `airflow.providers.google.cloud.operators.pubsub.PubSubTopicCreateOperator`

#### `airflow.providers.google.cloud.operators.pubsub.PubSubSubscriptionCreateOperator`

#### `airflow.providers.google.cloud.operators.pubsub.PubSubTopicDeleteOperator`

#### `airflow.providers.google.cloud.operators.pubsub.PubSubSubscriptionDeleteOperator`

#### `airflow.providers.google.cloud.operators.pubsub.PubSubPublishOperator`

#### `airflow.providers.google.cloud.sensors.pubsub.PubSubPullSensor`

In the `PubSubPublishOperator` and `PubSubHook.publsh` method the data field in a message should be bytestring (utf-8 encoded) rather than base64 encoded string.

Due to the normalization of the parameters within GCP operators and hooks a parameters like `project` or `topic_project`
are deprecated and will be substituted by parameter `project_id`.
In `PubSubHook.create_subscription` hook method in the parameter `subscription_project` is replaced by `subscription_project_id`.
Template fields are updated accordingly and old ones may not work.

It is required now to pass key-word only arguments to `PubSub` hook.

These changes are not backward compatible.

#### `airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`

The gcp_conn_id parameter in GKEPodOperator is required. In previous versions, it was possible to pass
the `None` value to the `gcp_conn_id` in the GKEStartPodOperator
operator, which resulted in credentials being determined according to the
[Application Default Credentials](https://cloud.google.com/docs/authentication/production) strategy.

Now this parameter requires a value. To restore the previous behavior, configure the connection without
specifying the service account.

Detailed information about connection management is available:
[Google Cloud Connection](https://airflow.apache.org/howto/connection/gcp.html).


#### `airflow.providers.google.cloud.hooks.gcs.GCSHook`

* The following parameters have been replaced in all the methods in GCSHook:
  * `bucket` is changed to `bucket_name`
  * `object` is changed to `object_name`

* The `maxResults` parameter in `GoogleCloudStorageHook.list` has been renamed to `max_results` for consistency.

#### `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator`

#### `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator`

#### `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator`

#### `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator`

#### `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator`

#### `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator`

The 'properties' and 'jars' properties for the Dataproc related operators (`DataprocXXXOperator`) have been renamed from
`dataproc_xxxx_properties` and `dataproc_xxx_jars`  to `dataproc_properties`
and `dataproc_jars`respectively.
Arguments for dataproc_properties dataproc_jars

#### `airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator`

To obtain pylint compatibility the `filter` argument in `CloudDataTransferServiceCreateJobOperator`
has been renamed to `request_filter`.

#### `airflow.providers.google.cloud.hooks.cloud_storage_transfer_service.CloudDataTransferServiceHook`

 To obtain pylint compatibility the `filter` argument in `CloudDataTransferServiceHook.list_transfer_job` and
 `CloudDataTransferServiceHook.list_transfer_operations` has been renamed to `request_filter`.

#### `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook`

In general all hook methods are decorated with `@GoogleBaseHook.fallback_to_default_project_id` thus
parameters to hook can only be passed via keyword arguments.

- `create_empty_table` method accepts now `table_resource` parameter. If provided all
other parameters are ignored.
- `create_empty_dataset` will now use values from `dataset_reference` instead of raising error
if parameters were passed in `dataset_reference` and as arguments to method. Additionally validation
of `dataset_reference` is done using `Dataset.from_api_repr`. Exception and log messages has been
changed.
- `update_dataset` requires now new `fields` argument (breaking change)
- `delete_dataset` has new signature (dataset_id, project_id, ...)
previous one was (project_id, dataset_id, ...) (breaking change)
- `get_tabledata` returns list of rows instead of API response in dict format. This method is deprecated in
 favor of `list_rows`. (breaking change)

#### `airflow.providers.google.cloud.hooks.dataflow.DataflowHook.start_python_dataflow`

#### `airflow.providers.google.cloud.hooks.dataflow.DataflowHook.start_python_dataflow`

#### `airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator`

Change python3 as Dataflow Hooks/Operators default interpreter

Now the `py_interpreter` argument for DataFlow Hooks/Operators has been changed from python2 to python3.

#### `airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

To simplify the code, the decorator provide_gcp_credential_file has been moved from the inner-class.

Instead of `@GoogleBaseHook._Decorators.provide_gcp_credential_file`,
you should write `@GoogleBaseHook.provide_gcp_credential_file`

#### `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`

It is highly recommended to have 1TB+ disk size for Dataproc to have sufficient throughput:
https://cloud.google.com/compute/docs/disks/performance

Hence, the default value for `master_disk_size` in `DataprocCreateClusterOperator` has been changed from 500GB to 1TB.

#### `<airflow class="providers google c"></airflow>loud.operators.bigquery.BigQueryGetDatasetTablesOperator`

We changed signature of BigQueryGetDatasetTablesOperator.

Before:

```python
BigQueryGetDatasetTablesOperator(dataset_id: str, dataset_resource: dict, ...)
```

After:

```python
BigQueryGetDatasetTablesOperator(dataset_resource: dict, dataset_id: Optional[str] = None, ...)
```

### Changes in `amazon` provider package

We strive to ensure that there are no changes that may affect the end user, and your Python files, but this
release may contain changes that will require changes to your configuration, DAG Files or other integration
e.g. custom operators.

Only changes unique to this provider are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your if
you use operators or hooks which integrate with Amazon services (including Amazon Web Service - AWS).

#### Migration of AWS components

All AWS components (hooks, operators, sensors, example DAGs) will be grouped together as decided in
[AIP-21](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths). Migrated
components remain backwards compatible but raise a `DeprecationWarning` when imported from the old module.
Migrated are:

| Old path                                                     | New path                                                 |
| ------------------------------------------------------------ | -------------------------------------------------------- |
| airflow.hooks.S3_hook.S3Hook                                 | airflow.providers.amazon.aws.hooks.s3.S3Hook                    |
| airflow.contrib.hooks.aws_athena_hook.AWSAthenaHook          | airflow.providers.amazon.aws.hooks.athena.AWSAthenaHook         |
| airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook          | airflow.providers.amazon.aws.hooks.lambda_function.AwsLambdaHook         |
| airflow.contrib.hooks.aws_sqs_hook.SQSHook                   | airflow.providers.amazon.aws.hooks.sqs.SQSHook        |
| airflow.contrib.hooks.aws_sns_hook.AwsSnsHook                   | airflow.providers.amazon.aws.hooks.sns.AwsSnsHook        |
| airflow.contrib.operators.aws_athena_operator.AWSAthenaOperator | airflow.providers.amazon.aws.operators.athena.AWSAthenaOperator |
| airflow.contrib.operators.awsbatch.AWSBatchOperator | airflow.providers.amazon.aws.operators.batch.AwsBatchOperator |
| airflow.contrib.operators.awsbatch.BatchProtocol | airflow.providers.amazon.aws.hooks.batch_client.AwsBatchProtocol |
| private attrs and methods on AWSBatchOperator | airflow.providers.amazon.aws.hooks.batch_client.AwsBatchClient |
| n/a | airflow.providers.amazon.aws.hooks.batch_waiters.AwsBatchWaiters |
| airflow.contrib.operators.aws_sqs_publish_operator.SQSPublishOperator | airflow.providers.amazon.aws.operators.sqs.SQSPublishOperator |
| airflow.contrib.operators.aws_sns_publish_operator.SnsPublishOperator | airflow.providers.amazon.aws.operators.sns.SnsPublishOperator |
| airflow.contrib.sensors.aws_athena_sensor.AthenaSensor       | airflow.providers.amazon.aws.sensors.athena.AthenaSensor        |
| airflow.contrib.sensors.aws_sqs_sensor.SQSSensor             | airflow.providers.amazon.aws.sensors.sqs.SQSSensor        |

#### `airflow.providers.amazon.aws.hooks.emr.EmrHook`

#### `airflow.providers.amazon.aws.operators.emr_add_steps.EmrAddStepsOperator`

#### `airflow.providers.amazon.aws.operators.emr_create_job_flow.EmrCreateJobFlowOperator`

#### `airflow.providers.amazon.aws.operators.emr_terminate_job_flow.EmrTerminateJobFlowOperator`

The default value for the [aws_conn_id](https://airflow.apache.org/howto/manage-connections.html#amazon-web-services) was accidentally set to 's3_default' instead of 'aws_default' in some of the emr operators in previous
versions. This was leading to EmrStepSensor not being able to find their corresponding emr cluster. With the new
changes in the EmrAddStepsOperator, EmrTerminateJobFlowOperator and EmrCreateJobFlowOperator this issue is
solved.

#### `airflow.providers.amazon.aws.operators.batch.AwsBatchOperator`

The `AwsBatchOperator` was refactored to extract an `AwsBatchClient` (and inherit from it).  The
changes are mostly backwards compatible and clarify the public API for these classes; some
private methods on `AwsBatchOperator` for polling a job status were relocated and renamed
to surface new public methods on `AwsBatchClient` (and via inheritance on `AwsBatchOperator`).  A
couple of job attributes are renamed on an instance of `AwsBatchOperator`; these were mostly
used like private attributes but they were surfaced in the public API, so any use of them needs
to be updated as follows:

- `AwsBatchOperator().jobId` -> `AwsBatchOperator().job_id`
- `AwsBatchOperator().jobName` -> `AwsBatchOperator().job_name`

The `AwsBatchOperator` gets a new option to define a custom model for waiting on job status changes.
The `AwsBatchOperator` can use a new `waiters` parameter, an instance of `AwsBatchWaiters`, to
specify that custom job waiters will be used to monitor a batch job.  See the latest API
documentation for details.

#### `airflow.providers.amazon.aws.sensors.athena.AthenaSensor`

Replace parameter `max_retires` with `max_retries` to fix typo.

#### `airflow.providers.amazon.aws.hooks.s3.S3Hook`

Note: The order of arguments has changed for `check_for_prefix`.
The `bucket_name` is now optional. It falls back to the `connection schema` attribute.
The `delete_objects` now returns `None` instead of a response, since the method now makes multiple api requests when the keys list length is > 1000.

### Changes in other provider packages

We strive to ensure that there are no changes that may affect the end user and your Python files, but this
release may contain changes that will require changes to your configuration, DAG Files or other integration
e.g. custom operators.

Only changes unique to providers are described here. You should still pay attention to the changes that
have been made to the core (including core operators) as they can affect the integration behavior
of this provider.

This section describes the changes that have been made, and what you need to do to update your if
you use any code located in `airflow.providers` package.

#### Changed return type of `list_prefixes` and `list_keys` methods in `S3Hook`

Previously, the `list_prefixes` and `list_keys` methods returned `None` when there were no
results. The behavior has been changed to return an empty list instead of `None` in this
case.

#### Removed Hipchat integration

Hipchat has reached end of life and is no longer available.

For more information please see
https://community.atlassian.com/t5/Stride-articles/Stride-and-Hipchat-Cloud-have-reached-End-of-Life-updated/ba-p/940248

#### `airflow.providers.salesforce.hooks.salesforce.SalesforceHook`

Replace parameter ``sandbox`` with ``domain``. According to change in simple-salesforce package.

Rename `sign_in` function to `get_conn`.

#### `airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook.create_segment`

Rename parameter name from ``format`` to ``segment_format`` in PinotAdminHook function create_segment fro pylint compatible

#### `airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_partitions`

Rename parameter name from ``filter`` to ``partition_filter`` in HiveMetastoreHook function get_partitions for pylint compatible

#### `airflow.providers.ftp.hooks.ftp.FTPHook.list_directory`

Remove unnecessary parameter ``nlst`` in FTPHook function ``list_directory`` for pylint compatible

#### `airflow.providers.postgres.hooks.postgres.PostgresHook.copy_expert`

Remove unnecessary parameter ``open`` in PostgresHook function ``copy_expert`` for pylint compatible

#### `airflow.providers.opsgenie.operators.opsgenie_alert.OpsgenieAlertOperator`

Change parameter name from ``visibleTo`` to ``visible_to`` in OpsgenieAlertOperator for pylint compatible

#### `airflow.providers.imap.hooks.imap.ImapHook`

#### `airflow.providers.imap.sensors.imap_attachment.ImapAttachmentSensor`

ImapHook:

* The order of arguments has changed for `has_mail_attachment`,
`retrieve_mail_attachments` and `download_mail_attachments`.
* A new `mail_filter` argument has been added to each of those.


#### `airflow.providers.http.hooks.http.HttpHook`

The HTTPHook is now secured by default: `verify=True` (before: `verify=False`)
This can be overwritten by using the extra_options param as `{'verify': False}`.

#### `airflow.providers.cloudant.hooks.cloudant.CloudantHook`

* upgraded cloudant version from `>=0.5.9,<2.0` to `>=2.0`
* removed the use of the `schema` attribute in the connection
* removed `db` function since the database object can also be retrieved by calling `cloudant_session['database_name']`

For example:

```python
from airflow.providers.cloudant.hooks.cloudant import CloudantHook

with CloudantHook().get_conn() as cloudant_session:
    database = cloudant_session['database_name']
```

See the [docs](https://python-cloudant.readthedocs.io/en/latest/) for more information on how to use the new cloudant version.

#### `airflow.providers.snowflake`

When initializing a Snowflake hook or operator, the value used for `snowflake_conn_id` was always `snowflake_conn_id`, regardless of whether or not you specified a value for it. The default `snowflake_conn_id` value is now switched to `snowflake_default` for consistency and will be properly overridden when specified.

### Other changes

This release also includes changes that fall outside any of the sections above.

#### Standardised "extra" requirements

We standardised the Extras names and synchronized providers package names with the main airflow extras.

We deprecated a number of extras in 2.0.

| Deprecated extras         | New extras       |
|---------------------------|------------------|
| atlas                     | apache.atlas     |
| aws                       | amazon           |
| azure                     | microsoft.azure  |
| azure_blob_storage        | microsoft.azure  |
| azure_data_lake           | microsoft.azure  |
| azure_cosmos              | microsoft.azure  |
| azure_container_instances | microsoft.azure  |
| cassandra                 | apache.cassandra |
| druid                     | apache.druid     |
| gcp                       | google           |
| gcp_api                   | google           |
| hdfs                      | apache.hdfs      |
| hive                      | apache.hive      |
| kubernetes                | cncf.kubernetes  |
| mssql                     | microsoft.mssql  |
| pinot                     | apache.pinot     |
| webhdfs                   | apache.webhdfs   |
| winrm                     | apache.winrm     |

For example:

If you want to install integration for Microsoft Azure, then instead of `pip install apache-airflow[atlas]`
you should use `pip install apache-airflow[apache.atlas]`.


NOTE!

On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
`pip install --upgrade pip==20.2.4` or, in case you use Pip 20.3, you need to add option
`--use-deprecated legacy-resolver` to your pip install command.


If you want to install integration for Microsoft Azure, then instead of

```
pip install 'apache-airflow[azure_blob_storage,azure_data_lake,azure_cosmos,azure_container_instances]'
```

you should execute `pip install 'apache-airflow[azure]'`

If you want to install integration for Amazon Web Services, then instead of
`pip install 'apache-airflow[s3,emr]'`, you should execute `pip install 'apache-airflow[aws]'`

The deprecated extras will be removed in 3.0.

#### Simplify the response payload of endpoints /dag_stats and /task_stats

The response of endpoints `/dag_stats` and `/task_stats` help UI fetch brief statistics about DAGs and Tasks. The format was like

```json
{
    "example_http_operator": [
        {
            "state": "success",
            "count": 0,
            "dag_id": "example_http_operator",
            "color": "green"
        },
        {
            "state": "running",
            "count": 0,
            "dag_id": "example_http_operator",
            "color": "lime"
        },
        ...
],
...
}
```

The `dag_id` was repeated in the payload, which makes the response payload unnecessarily bigger.

Now the `dag_id` will not appear repeated in the payload, and the response format is like

```json
{
    "example_http_operator": [
        {
            "state": "success",
            "count": 0,
            "color": "green"
        },
        {
            "state": "running",
            "count": 0,
            "color": "lime"
        },
        ...
],
...
}
```

## Airflow 1.10.15

No breaking changes.

## Airflow 1.10.14

### `[scheduler] max_threads` config has been renamed to `[scheduler] parsing_processes`

From Airflow 1.10.14, `max_threads` config under `[scheduler]` section has been renamed to `parsing_processes`.

This is to align the name with the actual code where the Scheduler launches the number of processes defined by
`[scheduler] parsing_processes` to parse the DAG files.

### Airflow CLI changes in line with 2.0

The Airflow CLI has been organized so that related commands are grouped together as subcommands,
which means that if you use these commands in your scripts, they will now raise a DeprecationWarning and
you have to make changes to them before you upgrade to Airflow 2.0.

This section describes the changes that have been made, and what you need to do to update your script.

The ability to manipulate users from the command line has been changed. ``airflow create_user``,  ``airflow delete_user``
 and ``airflow list_users`` has been grouped to a single command `airflow users` with optional flags `create`, `list` and `delete`.

The `airflow list_dags` command is now `airflow dags list`, `airflow pause` is `airflow dags pause`, etc.

In Airflow 1.10 and 2.0 there is an `airflow config` command but there is a difference in behavior. In Airflow 1.10,
it prints all config options while in Airflow 2.0, it's a command group. `airflow config` is now `airflow config list`.
You can check other options by running the command `airflow config --help`

Compatibility with the old CLI has been maintained, but they will no longer appear in the help

You can learn about the commands by running ``airflow --help``. For example to get help about the ``celery`` group command,
you have to run the help command: ``airflow celery --help``.

| Old command                   | New command                        |     Group          |
|-------------------------------|------------------------------------|--------------------|
| ``airflow worker``            | ``airflow celery worker``          |    ``celery``      |
| ``airflow flower``            | ``airflow celery flower``          |    ``celery``      |
| ``airflow trigger_dag``       | ``airflow dags trigger``           |    ``dags``        |
| ``airflow delete_dag``        | ``airflow dags delete``            |    ``dags``        |
| ``airflow show_dag``          | ``airflow dags show``              |    ``dags``        |
| ``airflow list_dag``          | ``airflow dags list``              |    ``dags``        |
| ``airflow dag_status``        | ``airflow dags status``            |    ``dags``        |
| ``airflow backfill``          | ``airflow dags backfill``          |    ``dags``        |
| ``airflow list_dag_runs``     | ``airflow dags list-runs``         |    ``dags``        |
| ``airflow pause``             | ``airflow dags pause``             |    ``dags``        |
| ``airflow unpause``           | ``airflow dags unpause``           |    ``dags``        |
| ``airflow next_execution``    | ``airflow dags next-execution``    |    ``dags``        |
| ``airflow test``              | ``airflow tasks test``             |    ``tasks``       |
| ``airflow clear``             | ``airflow tasks clear``            |    ``tasks``       |
| ``airflow list_tasks``        | ``airflow tasks list``             |    ``tasks``       |
| ``airflow task_failed_deps``  | ``airflow tasks failed-deps``      |    ``tasks``       |
| ``airflow task_state``        | ``airflow tasks state``            |    ``tasks``       |
| ``airflow run``               | ``airflow tasks run``              |    ``tasks``       |
| ``airflow render``            | ``airflow tasks render``           |    ``tasks``       |
| ``airflow initdb``            | ``airflow db init``                |     ``db``         |
| ``airflow resetdb``           | ``airflow db reset``               |     ``db``         |
| ``airflow upgradedb``         | ``airflow db upgrade``             |     ``db``         |
| ``airflow checkdb``           | ``airflow db check``               |     ``db``         |
| ``airflow shell``             | ``airflow db shell``               |     ``db``         |
| ``airflow pool``              | ``airflow pools``                  |     ``pools``      |
| ``airflow create_user``       | ``airflow users create``           |     ``users``      |
| ``airflow delete_user``       | ``airflow users delete``           |     ``users``      |
| ``airflow list_users``        | ``airflow users list``             |     ``users``      |
| ``airflow rotate_fernet_key`` | ``airflow rotate-fernet-key``      |                    |
| ``airflow sync_perm``         | ``airflow sync-perm``              |                    |

## Airflow 1.10.13

### TimeSensor is now timezone aware

Previously `TimeSensor` always compared the `target_time` with the current time in UTC.

Now it will compare `target_time` with the current time in the timezone of the DAG,
defaulting to the `default_timezone` in the global config.

### Removed Kerberos support for HDFS hook

The HDFS hook's Kerberos support has been removed due to removed python-krbV dependency from PyPI
and generally lack of support for SSL in Python3 (Snakebite-py3 we use as dependency has no
support for SSL connection to HDFS).

SSL support still works for WebHDFS hook.

### Unify user session lifetime configuration

In previous version of Airflow user session lifetime could be configured by
`session_lifetime_days` and `force_log_out_after` options. In practise only `session_lifetime_days`
had impact on session lifetime, but it was limited to values in day.
We have removed mentioned options and introduced new `session_lifetime_minutes`
option which simplify session lifetime configuration.

Before

 ```ini
[webserver]
force_log_out_after = 0
session_lifetime_days = 30
 ```

After

 ```ini
[webserver]
session_lifetime_minutes = 43200
 ```

### Adding Operators, Hooks and Sensors via Airflow Plugins is deprecated

The ability to import Operators, Hooks and Sensors via the plugin mechanism has been deprecated and will raise warnings
in Airflow 1.10.13 and will be removed completely in Airflow 2.0.

Check https://airflow.apache.org/docs/1.10.13/howto/custom-operator.html to see how you can create and import
Custom Hooks, Operators and Sensors.

## Airflow 1.10.12

### Clearing tasks skipped by SkipMixin will skip them

Previously, when tasks skipped by SkipMixin (such as BranchPythonOperator, BaseBranchOperator and ShortCircuitOperator) are cleared, they execute. Since 1.10.12, when such skipped tasks are cleared,
they will be skipped again by the newly introduced NotPreviouslySkippedDep.

### The pod_mutation_hook function will now accept a kubernetes V1Pod object

As of airflow 1.10.12, using the `airflow.contrib.kubernetes.Pod` class in the `pod_mutation_hook` is now deprecated. Instead we recommend that users
treat the `pod` parameter as a `kubernetes.client.models.V1Pod` object. This means that users now have access to the full Kubernetes API
when modifying airflow pods

### pod_template_file option now available in the KubernetesPodOperator

Users can now offer a path to a yaml for the KubernetesPodOperator using the `pod_template_file` parameter.

## Airflow 1.10.11

### Use NULL as default value for dag.description

Now use NULL as default value for dag.description in dag table

### Restrict editing DagRun State in the old UI (Flask-admin based UI)

Before 1.10.11 it was possible to edit DagRun State in the `/admin/dagrun/` page
 to any text.

In Airflow 1.10.11+, the user can only choose the states from the list.

### Experimental API will deny all request by default.

The previous default setting was to allow all API requests without authentication, but this poses security
risks to users who miss this fact. This changes the default for new installs to deny all requests by default.

**Note**: This will not change the behavior for existing installs, please update check your airflow.cfg

If you wish to have the experimental API work, and aware of the risks of enabling this without authentication
(or if you have your own authentication layer in front of Airflow) you can get
the previous behaviour on a new install by setting this in your airflow.cfg:

```
[api]
auth_backend = airflow.api.auth.backend.default
```

### XCom Values can no longer be added or changed from the Webserver

Since XCom values can contain pickled data, we would no longer allow adding or
changing XCom values from the UI.

### Default for `run_as_user` configured has been changed to 50000 from 0

The UID to run the first process of the Worker PODs when using has been changed to `50000`
from the previous default of `0`. The previous default was an empty string but the code used `0` if it was
empty string.

**Before**:

```ini
[kubernetes]
run_as_user =
```

**After**:

```ini
[kubernetes]
run_as_user = 50000
```

This is done to avoid running the container as `root` user.

## Airflow 1.10.10

### Setting Empty string to a Airflow Variable will return an empty string

Previously when you set an Airflow Variable with an empty string (`''`), the value you used to get
back was ``None``. This will now return an empty string (`'''`)

Example:

```python
>> Variable.set('test_key', '')
>> Variable.get('test_key')
```

The above code returned `None` previously, now it will return `''`.

### Make behavior of `none_failed` trigger rule consistent with documentation

The behavior of the `none_failed` trigger rule is documented as "all parents have not failed (`failed` or
    `upstream_failed`) i.e. all parents have succeeded or been skipped." As previously implemented, the actual behavior
    would skip if all parents of a task had also skipped.

### Add new trigger rule `none_failed_or_skipped`

The fix to `none_failed` trigger rule breaks workflows that depend on the previous behavior.
    If you need the old behavior, you should change the tasks with `none_failed` trigger rule to `none_failed_or_skipped`.

### Success Callback will be called when a task in marked as success from UI

When a task is marked as success by a user from Airflow UI - `on_success_callback` will be called

## Airflow 1.10.9

No breaking changes.

## Airflow 1.10.8

### Failure callback will be called when task is marked failed

When task is marked failed by user or task fails due to system failures - on failure call back will be called as part of clean up

See [AIRFLOW-5621](https://jira.apache.org/jira/browse/AIRFLOW-5621) for details

## Airflow 1.10.7

### Changes in experimental API execution_date microseconds replacement

The default behavior was to strip the microseconds (and milliseconds, etc) off of all dag runs triggered by
by the experimental REST API.  The default behavior will change when an explicit execution_date is
passed in the request body.  It will also now be possible to have the execution_date generated, but
keep the microseconds by sending `replace_microseconds=false` in the request body.  The default
behavior can be overridden by sending `replace_microseconds=true` along with an explicit execution_date

### Infinite pool size and pool size query optimisation

Pool size can now be set to -1 to indicate infinite size (it also includes
optimisation of pool query which lead to poor task n^2 performance of task
pool queries in MySQL).

### Viewer won't have edit permissions on DAG view.

### Google Cloud Storage Hook

The `GoogleCloudStorageDownloadOperator` can either write to a supplied `filename` or
return the content of a file via xcom through `store_to_xcom_key` - both options are mutually exclusive.

## Airflow 1.10.6

### BaseOperator::render_template function signature changed

Previous versions of the `BaseOperator::render_template` function required an `attr` argument as the first
positional argument, along with `content` and `context`. This function signature was changed in 1.10.6 and
the `attr` argument is no longer required (or accepted).

In order to use this function in subclasses of the `BaseOperator`, the `attr` argument must be removed:

```python
result = self.render_template('myattr', self.myattr, context)  # Pre-1.10.6 call
...
result = self.render_template(self.myattr, context)  # Post-1.10.6 call
```

### Changes to `aws_default` Connection's default region

The region of Airflow's default connection to AWS (`aws_default`) was previously
set to `us-east-1` during installation.

The region now needs to be set manually, either in the connection screens in
Airflow, via the `~/.aws` config files, or via the `AWS_DEFAULT_REGION` environment
variable.

### Some DAG Processing metrics have been renamed

The following metrics are deprecated and won't be emitted in Airflow 2.0:

- `scheduler.dagbag.errors` and `dagbag_import_errors` -- use `dag_processing.import_errors` instead
- `dag_file_processor_timeouts` -- use `dag_processing.processor_timeouts` instead
- `collect_dags` -- use `dag_processing.total_parse_time` instead
- `dag.loading-duration.<basename>` -- use `dag_processing.last_duration.<basename>` instead
- `dag_processing.last_runtime.<basename>` -- use `dag_processing.last_duration.<basename>` instead

## Airflow 1.10.5

No breaking changes.

## Airflow 1.10.4

### Export MySQL timestamps as UTC

`MySqlToGoogleCloudStorageOperator` now exports TIMESTAMP columns as UTC
by default, rather than using the default timezone of the MySQL server.
This is the correct behavior for use with BigQuery, since BigQuery
assumes that TIMESTAMP columns without time zones are in UTC. To
preserve the previous behavior, set `ensure_utc` to `False.`

### Changes to DatastoreHook

* removed argument `version` from `get_conn` function and added it to the hook's `__init__` function instead and renamed it to `api_version`
* renamed the `partialKeys` argument of function `allocate_ids` to `partial_keys`

### Changes to GoogleCloudStorageHook

* the discovery-based api (`googleapiclient.discovery`) used in `GoogleCloudStorageHook` is now replaced by the recommended client based api (`google-cloud-storage`). To know the difference between both the libraries, read https://cloud.google.com/apis/docs/client-libraries-explained. PR: [#5054](https://github.com/apache/airflow/pull/5054)
* as a part of this replacement, the `multipart` & `num_retries` parameters for `GoogleCloudStorageHook.upload` method have been deprecated.

  The client library uses multipart upload automatically if the object/blob size is more than 8 MB - [source code](https://github.com/googleapis/google-cloud-python/blob/11c543ce7dd1d804688163bc7895cf592feb445f/storage/google/cloud/storage/blob.py#L989-L997). The client also handles retries automatically

* the `generation` parameter is deprecated in `GoogleCloudStorageHook.delete` and `GoogleCloudStorageHook.insert_object_acl`.

Updating to `google-cloud-storage >= 1.16` changes the signature of the upstream `client.get_bucket()` method from `get_bucket(bucket_name: str)` to `get_bucket(bucket_or_name: Union[str, Bucket])`. This method is not directly exposed by the airflow hook, but any code accessing the connection directly (`GoogleCloudStorageHook().get_conn().get_bucket(...)` or similar) will need to be updated.

### Changes in writing Logs to Elasticsearch

The `elasticsearch_` prefix has been removed from all config items under the `[elasticsearch]` section. For example `elasticsearch_host` is now just `host`.

### Removal of `non_pooled_task_slot_count` and `non_pooled_backfill_task_slot_count`

`non_pooled_task_slot_count` and `non_pooled_backfill_task_slot_count`
are removed in favor of a real pool, e.g. `default_pool`.

By default tasks are running in `default_pool`.
`default_pool` is initialized with 128 slots and user can change the
number of slots through UI/CLI. `default_pool` cannot be removed.

### `pool` config option in Celery section to support different Celery pool implementation

The new `pool` config option allows users to choose different pool
implementation. Default value is "prefork", while choices include "prefork" (default),
"eventlet", "gevent" or "solo". This may help users achieve better concurrency performance
in different scenarios.

For more details about Celery pool implementation, please refer to:

- https://docs.celeryproject.org/en/latest/userguide/workers.html#concurrency
- https://docs.celeryproject.org/en/latest/userguide/concurrency/eventlet.html


### Change to method signature in `BaseOperator` and `DAG` classes

The signature of the `get_task_instances` method in the `BaseOperator` and `DAG` classes has changed. The change does not change the behavior of the method in either case.

#### For `BaseOperator`

Old signature:

```python
def get_task_instances(self, session, start_date=None, end_date=None):
```

New signature:

```python
@provide_session
def get_task_instances(self, start_date=None, end_date=None, session=None):
```

#### For `DAG`

Old signature:

```python
def get_task_instances(
    self, session, start_date=None, end_date=None, state=None):
```

New signature:

```python
@provide_session
def get_task_instances(
    self, start_date=None, end_date=None, state=None, session=None):
```

In either case, it is necessary to rewrite calls to the `get_task_instances` method that currently provide the `session` positional argument. New calls to this method look like:

```python
# if you can rely on @provide_session
dag.get_task_instances()
# if you need to provide the session
dag.get_task_instances(session=your_session)
```

## Airflow 1.10.3

### New `dag_discovery_safe_mode` config option

If `dag_discovery_safe_mode` is enabled, only check files for DAGs if
they contain the strings "airflow" and "DAG". For backwards
compatibility, this option is enabled by default.

### RedisPy dependency updated to v3 series

If you are using the Redis Sensor or Hook you may have to update your code. See
[redis-py porting instructions] to check if your code might be affected (MSET,
MSETNX, ZADD, and ZINCRBY all were, but read the full doc).

[redis-py porting instructions]: https://github.com/andymccurdy/redis-py/tree/3.2.0#upgrading-from-redis-py-2x-to-30

### SLUGIFY_USES_TEXT_UNIDECODE or AIRFLOW_GPL_UNIDECODE no longer required

It is no longer required to set one of the environment variables to avoid
a GPL dependency. Airflow will now always use text-unidecode if unidecode
was not installed before.

### New `sync_parallelism` config option in `[celery]` section

The new `sync_parallelism` config option will control how many processes CeleryExecutor will use to
fetch celery task state in parallel. Default value is max(1, number of cores - 1)

### Rename of BashTaskRunner to StandardTaskRunner

BashTaskRunner has been renamed to StandardTaskRunner. It is the default task runner
so you might need to update your config.

`task_runner = StandardTaskRunner`

### Modification to config file discovery

If the `AIRFLOW_CONFIG` environment variable was not set and the
`~/airflow/airflow.cfg` file existed, airflow previously used
`~/airflow/airflow.cfg` instead of `$AIRFLOW_HOME/airflow.cfg`. Now airflow
will discover its config file using the `$AIRFLOW_CONFIG` and `$AIRFLOW_HOME`
environment variables rather than checking for the presence of a file.

### Changes in Google Cloud related operators

Most GCP-related operators have now optional `PROJECT_ID` parameter. In case you do not specify it,
the project id configured in
[GCP Connection](https://airflow.apache.org/howto/manage-connections.html#connection-type-gcp) is used.
There will be an `AirflowException` thrown in case `PROJECT_ID` parameter is not specified and the
connection used has no project id defined. This change should be  backwards compatible as earlier version
of the operators had `PROJECT_ID` mandatory.

Operators involved:

  * GCP Compute Operators
    * GceInstanceStartOperator
    * GceInstanceStopOperator
    * GceSetMachineTypeOperator
  * GCP Function Operators
    * GcfFunctionDeployOperator
  * GCP Cloud SQL Operators
    * CloudSqlInstanceCreateOperator
    * CloudSqlInstancePatchOperator
    * CloudSqlInstanceDeleteOperator
    * CloudSqlInstanceDatabaseCreateOperator
    * CloudSqlInstanceDatabasePatchOperator
    * CloudSqlInstanceDatabaseDeleteOperator

Other GCP operators are unaffected.

### Changes in Google Cloud related hooks

The change in GCP operators implies that GCP Hooks for those operators require now keyword parameters rather
than positional ones in all methods where `project_id` is used. The methods throw an explanatory exception
in case they are called using positional parameters.

Hooks involved:

  * GceHook
  * GcfHook
  * CloudSqlHook

Other GCP hooks are unaffected.

### Changed behaviour of using default value when accessing variables

It's now possible to use `None` as a default value with the `default_var` parameter when getting a variable, e.g.

```python
foo = Variable.get("foo", default_var=None)
if foo is None:
    handle_missing_foo()
```

(Note: there is already `Variable.setdefault()` which me be helpful in some cases.)

This changes the behaviour if you previously explicitly provided `None` as a default value. If your code expects a `KeyError` to be thrown, then don't pass the `default_var` argument.

### Removal of `airflow_home` config setting

There were previously two ways of specifying the Airflow "home" directory
(`~/airflow` by default): the `AIRFLOW_HOME` environment variable, and the
`airflow_home` config setting in the `[core]` section.

If they had two different values different parts of the code base would end up
with different values. The config setting has been deprecated, and you should
remove the value from the config file and set `AIRFLOW_HOME` environment
variable if you need to use a non default value for this.

(Since this setting is used to calculate what config file to load, it is not
possible to keep just the config option)

### Change of two methods signatures in `GCPTransferServiceHook`

The signature of the `create_transfer_job` method in `GCPTransferServiceHook`
class has changed. The change does not change the behavior of the method.

Old signature:

```python
def create_transfer_job(self, description, schedule, transfer_spec, project_id=None):
```

New signature:

```python
def create_transfer_job(self, body):
```

It is necessary to rewrite calls to method. The new call looks like this:

```python
body = {
  'status': 'ENABLED',
  'projectId': project_id,
  'description': description,
  'transferSpec': transfer_spec,
  'schedule': schedule,
}
gct_hook.create_transfer_job(body)
```

The change results from the unification of all hooks and adjust to
[the official recommendations](https://lists.apache.org/thread.html/e8534d82be611ae7bcb21ba371546a4278aad117d5e50361fd8f14fe@%3Cdev.airflow.apache.org%3E)
for the Google Cloud.

The signature of `wait_for_transfer_job` method in `GCPTransferServiceHook` has changed.

Old signature:

```python
def wait_for_transfer_job(self, job):
```

New signature:

```python
def wait_for_transfer_job(self, job, expected_statuses=(GcpTransferOperationStatus.SUCCESS, )):
```

The behavior of `wait_for_transfer_job` has changed:

Old behavior:

`wait_for_transfer_job` would wait for the SUCCESS status in specified jobs operations.

New behavior:

You can now specify an array of expected statuses. `wait_for_transfer_job` now waits for any of them.

The default value of `expected_statuses` is SUCCESS so that change is backwards compatible.

### Moved two classes to different modules

The class `GoogleCloudStorageToGoogleCloudStorageTransferOperator` has been moved from
`airflow.contrib.operators.gcs_to_gcs_transfer_operator` to `airflow.contrib.operators.gcp_transfer_operator`

the class `S3ToGoogleCloudStorageTransferOperator` has been moved from
`airflow.contrib.operators.s3_to_gcs_transfer_operator` to `airflow.contrib.operators.gcp_transfer_operator`

The change was made to keep all the operators related to GCS Transfer Services in one file.

The previous imports will continue to work until Airflow 2.0

### Fixed typo in --driver-class-path in SparkSubmitHook

The `driver_classapth` argument  to SparkSubmit Hook and Operator was
generating `--driver-classpath` on the spark command line, but this isn't a
valid option to spark.

The argument has been renamed to `driver_class_path`  and  the option it
generates has been fixed.

## Airflow 1.10.2

### New `dag_processor_manager_log_location` config option

The DAG parsing manager log now by default will be log into a file, where its location is
controlled by the new `dag_processor_manager_log_location` config option in core section.

### DAG level Access Control for new RBAC UI

Extend and enhance new Airflow RBAC UI to support DAG level ACL. Each dag now has two permissions(one for write, one for read) associated('can_dag_edit', 'can_dag_read').
The admin will create new role, associate the dag permission with the target dag and assign that role to users. That user can only access / view the certain dags on the UI
that he has permissions on. If a new role wants to access all the dags, the admin could associate dag permissions on an artificial view(``all_dags``) with that role.

We also provide a new cli command(``sync_perm``) to allow admin to auto sync permissions.

### Modification to `ts_nodash` macro

`ts_nodash` previously contained TimeZone information along with execution date. For Example: `20150101T000000+0000`. This is not user-friendly for file or folder names which was a popular use case for `ts_nodash`. Hence this behavior has been changed and using `ts_nodash` will no longer contain TimeZone information, restoring the pre-1.10 behavior of this macro. And a new macro `ts_nodash_with_tz` has been added which can be used to get a string with execution date and timezone info without dashes.

Examples:

  * `ts_nodash`: `20150101T000000`
  * `ts_nodash_with_tz`: `20150101T000000+0000`

### Semantics of next_ds/prev_ds changed for manually triggered runs

next_ds/prev_ds now map to execution_date instead of the next/previous schedule-aligned execution date for DAGs triggered in the UI.

### User model changes

This patch changes the `User.superuser` field from a hard-coded boolean to a `Boolean()` database column. `User.superuser` will default to `False`, which means that this privilege will have to be granted manually to any users that may require it.

For example, open a Python shell and

```python
from airflow import models, settings

session = settings.Session()
users = session.query(models.User).all()  # [admin, regular_user]

users[1].superuser  # False

admin = users[0]
admin.superuser = True
session.add(admin)
session.commit()
```

### Custom auth backends interface change

We have updated the version of flask-login we depend upon, and as a result any
custom auth backends might need a small change: `is_active`,
`is_authenticated`, and `is_anonymous` should now be properties. What this means is if
previously you had this in your user class

```python
def is_active(self):
  return self.active
```

then you need to change it like this

```python
@property
def is_active(self):
  return self.active
```

### Support autodetected schemas to GoogleCloudStorageToBigQueryOperator

GoogleCloudStorageToBigQueryOperator is now support schema auto-detection is available when you load data into BigQuery. Unfortunately, changes can be required.

If BigQuery tables are created outside of airflow and the schema is not defined in the task, multiple options are available:

define a schema_fields:

```python
gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
  ...
  schema_fields={...})
```

or define a schema_object:

```python
gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
  ...
  schema_object='path/to/schema/object')
```

or enabled autodetect of schema:

```python
gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
  ...
  autodetect=True)
```

## Airflow 1.10.1

### min_file_parsing_loop_time config option temporarily disabled

The scheduler.min_file_parsing_loop_time config option has been temporarily removed due to
some bugs.

### StatsD Metrics

The `scheduler_heartbeat` metric has been changed from a gauge to a counter. Each loop of the scheduler will increment the counter by 1. This provides a higher degree of visibility and allows for better integration with Prometheus using the [StatsD Exporter](https://github.com/prometheus/statsd_exporter). The scheduler's activity status can be determined by graphing and alerting using a rate of change of the counter. If the scheduler goes down, the rate will drop to 0.

### EMRHook now passes all of connection's extra to CreateJobFlow API

EMRHook.create_job_flow has been changed to pass all keys to the create_job_flow API, rather than
just specific known keys for greater flexibility.

However prior to this release the "emr_default" sample connection that was created had invalid
configuration, so creating EMR clusters might fail until your connection is updated. (Ec2KeyName,
Ec2SubnetId, TerminationProtection and KeepJobFlowAliveWhenNoSteps were all top-level keys when they
should be inside the "Instances" dict)

### LDAP Auth Backend now requires TLS

Connecting to an LDAP server over plain text is not supported anymore. The
certificate presented by the LDAP server must be signed by a trusted
certificate, or you must provide the `cacert` option under `[ldap]` in the
config file.

If you want to use LDAP auth backend without TLS then you will have to create a
custom-auth backend based on
https://github.com/apache/airflow/blob/1.10.0/airflow/contrib/auth/backends/ldap_auth.py

## Airflow 1.10

Installation and upgrading requires setting `SLUGIFY_USES_TEXT_UNIDECODE=yes` in your environment or
`AIRFLOW_GPL_UNIDECODE=yes`. In case of the latter a GPL runtime dependency will be installed due to a
dependency (python-nvd3 -> python-slugify -> unidecode).

### Replace DataProcHook.await calls to DataProcHook.wait

The method name was changed to be compatible with the Python 3.7 async/await keywords

### Setting UTF-8 as default mime_charset in email utils

### Add a configuration variable(default_dag_run_display_number) to control numbers of dag run for display

Add a configuration variable(default_dag_run_display_number) under webserver section to control the number of dag runs to show in UI.

### Default executor for SubDagOperator is changed to SequentialExecutor

### New Webserver UI with Role-Based Access Control

The current webserver UI uses the Flask-Admin extension. The new webserver UI uses the [Flask-AppBuilder (FAB)](https://github.com/dpgaspar/Flask-AppBuilder) extension. FAB has built-in authentication support and Role-Based Access Control (RBAC), which provides configurable roles and permissions for individual users.

To turn on this feature, in your airflow.cfg file (under [webserver]), set the configuration variable `rbac = True`, and then run `airflow` command, which will generate the `webserver_config.py` file in your $AIRFLOW_HOME.

#### Setting up Authentication

FAB has built-in authentication support for DB, OAuth, OpenID, LDAP, and REMOTE_USER. The default auth type is `AUTH_DB`.

For any other authentication type (OAuth, OpenID, LDAP, REMOTE_USER), see the [Authentication section of FAB docs](http://flask-appbuilder.readthedocs.io/en/latest/security.html#authentication-methods) for how to configure variables in webserver_config.py file.

Once you modify your config file, run `airflow db init` to generate new tables for RBAC support (these tables will have the prefix `ab_`).

#### Creating an Admin Account

Once configuration settings have been updated and new tables have been generated, create an admin account with `airflow create_user` command.

#### Using your new UI

Run `airflow webserver` to start the new UI. This will bring up a log in page, enter the recently created admin username and password.

There are five roles created for Airflow by default: Admin, User, Op, Viewer, and Public. To configure roles/permissions, go to the `Security` tab and click `List Roles` in the new UI.

#### Breaking changes

- AWS Batch Operator renamed property queue to job_queue to prevent conflict with the internal queue from CeleryExecutor - AIRFLOW-2542
- Users created and stored in the old users table will not be migrated automatically. FAB's built-in authentication support must be reconfigured.
- Airflow dag home page is now `/home` (instead of `/admin`).
- All ModelViews in Flask-AppBuilder follow a different pattern from Flask-Admin. The `/admin` part of the URL path will no longer exist. For example: `/admin/connection` becomes `/connection/list`, `/admin/connection/new` becomes `/connection/add`, `/admin/connection/edit` becomes `/connection/edit`, etc.
- Due to security concerns, the new webserver will no longer support the features in the `Data Profiling` menu of old UI, including `Ad Hoc Query`, `Charts`, and `Known Events`.
- HiveServer2Hook.get_results() always returns a list of tuples, even when a single column is queried, as per Python API 2.
- **UTC is now the default timezone**: Either reconfigure your workflows scheduling in UTC or set `default_timezone` as explained in https://airflow.apache.org/timezone.html#default-time-zone

### airflow.contrib.sensors.hdfs_sensors renamed to airflow.contrib.sensors.hdfs_sensor

We now rename airflow.contrib.sensors.hdfs_sensors to airflow.contrib.sensors.hdfs_sensor for consistency purpose.

### MySQL setting required

We now rely on more strict ANSI SQL settings for MySQL in order to have sane defaults. Make sure
to have specified `explicit_defaults_for_timestamp=1` in your my.cnf under `[mysqld]`

### Celery config

To make the config of Airflow compatible with Celery, some properties have been renamed:

```
celeryd_concurrency -> worker_concurrency
celery_result_backend -> result_backend
celery_ssl_active -> ssl_active
celery_ssl_cert -> ssl_cert
celery_ssl_key -> ssl_key
```

Resulting in the same config parameters as Celery 4, with more transparency.

### GCP Dataflow Operators

Dataflow job labeling is now supported in Dataflow{Java,Python}Operator with a default
"airflow-version" label, please upgrade your google-cloud-dataflow or apache-beam version
to 2.2.0 or greater.

### BigQuery Hooks and Operator

The `bql` parameter passed to `BigQueryOperator` and `BigQueryBaseCursor.run_query` has been deprecated and renamed to `sql` for consistency purposes. Using `bql` will still work (and raise a `DeprecationWarning`), but is no longer
supported and will be removed entirely in Airflow 2.0

### Redshift to S3 Operator

With Airflow 1.9 or lower, Unload operation always included header row. In order to include header row,
we need to turn off parallel unload. It is preferred to perform unload operation using all nodes so that it is
faster for larger tables. So, parameter called `include_header` is added and default is set to False.
Header row will be added only if this parameter is set True and also in that case parallel will be automatically turned off (`PARALLEL OFF`)

### Google cloud connection string

With Airflow 1.9 or lower, there were two connection strings for the Google Cloud operators, both `google_cloud_storage_default` and `google_cloud_default`. This can be confusing and therefore the `google_cloud_storage_default` connection id has been replaced with `google_cloud_default` to make the connection id consistent across Airflow.

### Logging Configuration

With Airflow 1.9 or lower, `FILENAME_TEMPLATE`, `PROCESSOR_FILENAME_TEMPLATE`, `LOG_ID_TEMPLATE`, `END_OF_LOG_MARK` were configured in `airflow_local_settings.py`. These have been moved into the configuration file, and hence if you were using a custom configuration file the following defaults need to be added.

```
[core]
fab_logging_level = WARN
log_filename_template = {{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{{{{ ts }}}}/{{{{ try_number }}}}.log
log_processor_filename_template = {{{{ filename }}}}.log

[elasticsearch]
elasticsearch_log_id_template = {{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}
elasticsearch_end_of_log_mark = end_of_log
```

The previous setting of `log_task_reader` is not needed in many cases now when using the default logging config with remote storages. (Previously it needed to be set to `s3.task` or similar. This is not needed with the default config anymore)

#### Change of per-task log path

With the change to Airflow core to be timezone aware the default log path for task instances will now include timezone information. This will by default mean all previous task logs won't be found. You can get the old behaviour back by setting the following config options:

```
[core]
log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ execution_date.strftime("%%Y-%%m-%%dT%%H:%%M:%%S") }}/{{ try_number }}.log
```

## Airflow 1.9

### SSH Hook updates, along with new SSH Operator & SFTP Operator

SSH Hook now uses the Paramiko library to create an ssh client connection, instead of the sub-process based ssh command execution previously (<1.9.0), so this is backward incompatible.

- update SSHHook constructor
- use SSHOperator class in place of SSHExecuteOperator which is removed now. Refer to test_ssh_operator.py for usage info.
- SFTPOperator is added to perform secure file transfer from serverA to serverB. Refer to test_sftp_operator.py for usage info.
- No updates are required if you are using ftpHook, it will continue to work as is.

### S3Hook switched to use Boto3

The airflow.hooks.S3_hook.S3Hook has been switched to use boto3 instead of the older boto (a.k.a. boto2). This results in a few backwards incompatible changes to the following classes: S3Hook:

- the constructors no longer accepts `s3_conn_id`. It is now called `aws_conn_id`.
- the default connection is now "aws_default" instead of "s3_default"
- the return type of objects returned by `get_bucket` is now boto3.s3.Bucket
- the return type of `get_key`, and `get_wildcard_key` is now an boto3.S3.Object.

If you are using any of these in your DAGs and specify a connection ID you will need to update the parameter name for the connection to "aws_conn_id": S3ToHiveTransfer, S3PrefixSensor, S3KeySensor, RedshiftToS3Transfer.

### Logging update

The logging structure of Airflow has been rewritten to make configuration easier and the logging system more transparent.

#### A quick recap about logging

A logger is the entry point into the logging system. Each logger is a named bucket to which messages can be written for processing. A logger is configured to have a log level. This log level describes the severity of the messages that the logger will handle. Python defines the following log levels: DEBUG, INFO, WARNING, ERROR or CRITICAL.

Each message that is written to the logger is a Log Record. Each log record contains a log level indicating the severity of that specific message. A log record can also contain useful metadata that describes the event that is being logged. This can include details such as a stack trace or an error code.

When a message is given to the logger, the log level of the message is compared to the log level of the logger. If the log level of the message meets or exceeds the log level of the logger itself, the message will undergo further processing. If it doesn’t, the message will be ignored.

Once a logger has determined that a message needs to be processed, it is passed to a Handler. This configuration is now more flexible and can be easily be maintained in a single file.

#### Changes in Airflow Logging

Airflow's logging mechanism has been refactored to use Python’s built-in `logging` module to perform logging of the application. By extending classes with the existing `LoggingMixin`, all the logging will go through a central logger. Also the `BaseHook` and `BaseOperator` already extend this class, so it is easily available to do logging.

The main benefit is easier configuration of the logging by setting a single centralized python file. Disclaimer; there is still some inline configuration, but this will be removed eventually. The new logging class is defined by setting the dotted classpath in your `~/airflow/airflow.cfg` file:

```
# Logging class
# Specify the class that will specify the logging configuration
# This class has to be on the python classpath
logging_config_class = my.path.default_local_settings.LOGGING_CONFIG
```

The logging configuration file needs to be on the `PYTHONPATH`, for example `$AIRFLOW_HOME/config`. This directory is loaded by default. Any directory may be added to the `PYTHONPATH`, this might be handy when the config is in another directory or a volume is mounted in case of Docker.

The config can be taken from `airflow/config_templates/airflow_local_settings.py` as a starting point. Copy the contents to `${AIRFLOW_HOME}/config/airflow_local_settings.py`,  and alter the config as is preferred.

```
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

from airflow import configuration as conf

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py. Please see AIRFLOW-1455.

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('core', 'log_format')

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'child_process_log_directory')

FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': LOG_FORMAT,
        },
        'airflow.processor': {
            'format': LOG_FORMAT,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'file.task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        'file.processor': {
            'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
            'formatter': 'airflow.processor',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        }
        # When using s3 or gcs, provide a customized LOGGING_CONFIG
        # in airflow_local_settings within your PYTHONPATH, see UPDATING.md
        # for details
        # 's3.task': {
        #     'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
        #     'formatter': 'airflow.task',
        #     'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
        #     's3_log_folder': S3_LOG_FOLDER,
        #     'filename_template': FILENAME_TEMPLATE,
        # },
        # 'gcs.task': {
        #     'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
        #     'formatter': 'airflow.task',
        #     'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
        #     'gcs_log_folder': GCS_LOG_FOLDER,
        #     'filename_template': FILENAME_TEMPLATE,
        # },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': LOG_LEVEL
        },
        'airflow': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.processor': {
            'handlers': ['file.processor'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'airflow.task': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task_runner': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
    }
}
```

To customize the logging (for example, use logging rotate), define one or more of the logging handles that [Python has to offer](https://docs.python.org/3/library/logging.handlers.html). For more details about the Python logging, please refer to the [official logging documentation](https://docs.python.org/3/library/logging.html).

Furthermore, this change also simplifies logging within the DAG itself:

```
root@ae1bc863e815:/airflow# python
Python 3.6.2 (default, Sep 13 2017, 14:26:54)
[GCC 4.9.2] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> from airflow.settings import *
>>>
>>> from datetime import datetime
>>> from airflow.models.dag import DAG
>>> from airflow.operators.dummy import DummyOperator
>>>
>>> dag = DAG('simple_dag', start_date=datetime(2017, 9, 1))
>>>
>>> task = DummyOperator(task_id='task_1', dag=dag)
>>>
>>> task.log.error('I want to say something..')
[2017-09-25 20:17:04,927] {<stdin>:1} ERROR - I want to say something..
```

#### Template path of the file_task_handler

The `file_task_handler` logger has been made more flexible. The default format can be changed, `{dag_id}/{task_id}/{execution_date}/{try_number}.log` by supplying Jinja templating in the `FILENAME_TEMPLATE` configuration variable. See the `file_task_handler` for more information.

#### I'm using S3Log or GCSLogs, what do I do!?

If you are logging to Google cloud storage, please see the [Google cloud platform documentation](https://airflow.apache.org/integration.html#gcp-google-cloud-platform) for logging instructions.

If you are using S3, the instructions should be largely the same as the Google cloud platform instructions above. You will need a custom logging config. The `REMOTE_BASE_LOG_FOLDER` configuration key in your airflow config has been removed, therefore you will need to take the following steps:

- Copy the logging configuration from [`airflow/config_templates/airflow_logging_settings.py`](https://github.com/apache/airflow/blob/master/airflow/config_templates/airflow_local_settings.py).
- Place it in a directory inside the Python import path `PYTHONPATH`. If you are using Python 2.7, ensuring that any `__init__.py` files exist so that it is importable.
- Update the config by setting the path of `REMOTE_BASE_LOG_FOLDER` explicitly in the config. The `REMOTE_BASE_LOG_FOLDER` key is not used anymore.
- Set the `logging_config_class` to the filename and dict. For example, if you place `custom_logging_config.py` on the base of your `PYTHONPATH`, you will need to set `logging_config_class = custom_logging_config.LOGGING_CONFIG` in your config as Airflow 1.8.

### New Features

#### Dask Executor

A new DaskExecutor allows Airflow tasks to be run in Dask Distributed clusters.

### Deprecated Features

These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer
supported and will be removed entirely in Airflow 2.0

- If you're using the `google_cloud_conn_id` or `dataproc_cluster` argument names explicitly in `contrib.operators.Dataproc{*}Operator`(s), be sure to rename them to `gcp_conn_id` or `cluster_name`, respectively. We've renamed these arguments for consistency. (AIRFLOW-1323)

- `post_execute()` hooks now take two arguments, `context` and `result`
  (AIRFLOW-886)

  Previously, post_execute() only took one argument, `context`.

- `contrib.hooks.gcp_dataflow_hook.DataFlowHook` starts to use `--runner=DataflowRunner` instead of `DataflowPipelineRunner`, which is removed from the package `google-cloud-dataflow-0.6.0`.

- The pickle type for XCom messages has been replaced by json to prevent RCE attacks.
  Note that JSON serialization is stricter than pickling, so if you want to e.g. pass
  raw bytes through XCom you must encode them using an encoding like base64.
  By default pickling is still enabled until Airflow 2.0. To disable it
  set enable_xcom_pickling = False in your Airflow config.

## Airflow 1.8.1

The Airflow package name was changed from `airflow` to `apache-airflow` during this release. You must uninstall
a previously installed version of Airflow before installing 1.8.1.

## Airflow 1.8

### Database

The database schema needs to be upgraded. Make sure to shutdown Airflow and make a backup of your database. To
upgrade the schema issue `airflow upgradedb`.

### Upgrade systemd unit files

Systemd unit files have been updated. If you use systemd please make sure to update these.

> Please note that the webserver does not detach properly, this will be fixed in a future version.

### Tasks not starting although dependencies are met due to stricter pool checking

Airflow 1.7.1 has issues with being able to over subscribe to a pool, ie. more slots could be used than were
available. This is fixed in Airflow 1.8.0, but due to past issue jobs may fail to start although their
dependencies are met after an upgrade. To workaround either temporarily increase the amount of slots above
the amount of queued tasks or use a new pool.

### Less forgiving scheduler on dynamic start_date

Using a dynamic start_date (e.g. `start_date = datetime.now()`) is not considered a best practice. The 1.8.0 scheduler
is less forgiving in this area. If you encounter DAGs not being scheduled you can try using a fixed start_date and
renaming your DAG. The last step is required to make sure you start with a clean slate, otherwise the old schedule can
interfere.

### New and updated scheduler options

Please read through the new scheduler options, defaults have changed since 1.7.1.

#### child_process_log_directory

In order to increase the robustness of the scheduler, DAGS are now processed in their own process. Therefore each
DAG has its own log file for the scheduler. These log files are placed in `child_process_log_directory` which defaults to
`<AIRFLOW_HOME>/scheduler/latest`. You will need to make sure these log files are removed.

> DAG logs or processor logs ignore and command line settings for log file locations.

#### run_duration

Previously the command line option `num_runs` was used to let the scheduler terminate after a certain amount of
loops. This is now time bound and defaults to `-1`, which means run continuously. See also num_runs.

#### num_runs

Previously `num_runs` was used to let the scheduler terminate after a certain amount of loops. Now num_runs specifies
the number of times to try to schedule each DAG file within `run_duration` time. Defaults to `-1`, which means try
indefinitely. This is only available on the command line.

#### min_file_process_interval

After how much time should an updated DAG be picked up from the filesystem.

#### min_file_parsing_loop_time

CURRENTLY DISABLED DUE TO A BUG
How many seconds to wait between file-parsing loops to prevent the logs from being spammed.

#### dag_dir_list_interval

The frequency with which the scheduler should relist the contents of the DAG directory. If while developing +dags, they are not being picked up, have a look at this number and decrease it when necessary.

#### catchup_by_default

By default the scheduler will fill any missing interval DAG Runs between the last execution date and the current date.
This setting changes that behavior to only execute the latest interval. This can also be specified per DAG as
`catchup = False / True`. Command line backfills will still work.

### Faulty DAGs do not show an error in the Web UI

Due to changes in the way Airflow processes DAGs the Web UI does not show an error when processing a faulty DAG. To
find processing errors go the `child_process_log_directory` which defaults to `<AIRFLOW_HOME>/scheduler/latest`.

### New DAGs are paused by default

Previously, new DAGs would be scheduled immediately. To retain the old behavior, add this to airflow.cfg:

```
[core]
dags_are_paused_at_creation = False
```

### Airflow Context variable are passed to Hive config if conf is specified

If you specify a hive conf to the run_cli command of the HiveHook, Airflow add some
convenience variables to the config. In case you run a secure Hadoop setup it might be
required to allow these variables by adjusting you hive configuration to add `airflow\.ctx\..*` to the regex
of user-editable configuration properties. See
[the Hive docs on Configuration Properties][hive.security.authorization.sqlstd] for more info.

[hive.security.authorization.sqlstd]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=82903061#ConfigurationProperties-SQLStandardBasedAuthorization.1

### Google Cloud Operator and Hook alignment

All Google Cloud Operators and Hooks are aligned and use the same client library. Now you have a single connection
type for all kinds of Google Cloud Operators.

If you experience problems connecting with your operator make sure you set the connection type "Google Cloud".

Also the old P12 key file type is not supported anymore and only the new JSON key files are supported as a service
account.

### Deprecated Features

These features are marked for deprecation. They may still work (and raise a `DeprecationWarning`), but are no longer
supported and will be removed entirely in Airflow 2.0

- Hooks and operators must be imported from their respective submodules

  `airflow.operators.PigOperator` is no longer supported; `from airflow.operators.pig_operator import PigOperator` is.
  (AIRFLOW-31, AIRFLOW-200)

- Operators no longer accept arbitrary arguments

  Previously, `Operator.__init__()` accepted any arguments (either positional `*args` or keyword `**kwargs`) without
  complaint. Now, invalid arguments will be rejected. (https://github.com/apache/airflow/pull/1285)

- The config value secure_mode will default to True which will disable some insecure endpoints/features

### Known Issues

There is a report that the default of "-1" for num_runs creates an issue where errors are reported while parsing tasks.
It was not confirmed, but a workaround was found by changing the default back to `None`.

To do this edit `cli.py`, find the following:

```
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=-1, type=int,
            help="Set the number of runs to execute before exiting"),
```

and change `default=-1` to `default=None`. If you have this issue please report it on the mailing list.

## Airflow 1.7.1.2

### Changes to Configuration

#### Email configuration change

To continue using the default smtp email backend, change the email_backend line in your config file from:

```
[email]
email_backend = airflow.utils.send_email_smtp
```

to:

```
[email]
email_backend = airflow.utils.email.send_email_smtp
```

#### S3 configuration change

To continue using S3 logging, update your config file so:

```
s3_log_folder = s3://my-airflow-log-bucket/logs
```

becomes:

```
remote_base_log_folder = s3://my-airflow-log-bucket/logs
remote_log_conn_id = <your desired s3 connection>
```
