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

# pylint: disable=too-many-lines
"""This module contains Google BigQuery operators."""
import enum
import hashlib
import json
import re
import uuid
import warnings
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, SupportsAbs, Union

import attr
from google.api_core.exceptions import Conflict
from google.cloud.bigquery import TableReference

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.operators.sql import SQLCheckOperator, SQLIntervalCheckOperator, SQLValueCheckOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.utils.decorators import apply_defaults

BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"

_DEPRECATION_MSG = (
    "The bigquery_conn_id parameter has been deprecated. You should pass the gcp_conn_id parameter."
)


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class BigQueryConsoleLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    name = 'BigQuery Console'

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_id = ti.xcom_pull(task_ids=operator.task_id, key='job_id')
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id) if job_id else ''


@attr.s(auto_attribs=True)
class BigQueryConsoleIndexableLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    index: int = attr.ib()

    @property
    def name(self) -> str:
        return f'BigQuery Console #{self.index + 1}'

    def get_link(self, operator: BaseOperator, dttm: datetime):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_ids = ti.xcom_pull(task_ids=operator.task_id, key='job_id')
        if not job_ids:
            return None
        if len(job_ids) < self.index:
            return None
        job_id = job_ids[self.index]
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id)


class _BigQueryDbHookMixin:
    def get_db_hook(self) -> BigQueryHook:
        """Get BigQuery DB Hook"""
        return BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
            labels=self.labels,
        )


class BigQueryCheckOperator(_BigQueryDbHookMixin, SQLCheckOperator):
    """
    Performs checks against BigQuery. The ``BigQueryCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCheckOperator`

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed
    :type sql: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :type labels: dict
    """

    template_fields = (
        'sql',
        'gcp_conn_id',
        'impersonation_chain',
        'labels',
    )
    template_ext = ('.sql',)
    ui_color = BigQueryUIColors.CHECK.value

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        use_legacy_sql: bool = True,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        labels: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, **kwargs)
        if bigquery_conn_id:
            warnings.warn(_DEPRECATION_MSG, DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.sql = sql
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels


class BigQueryValueCheckOperator(_BigQueryDbHookMixin, SQLValueCheckOperator):
    """
    Performs a simple value check using sql code.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryValueCheckOperator`

    :param sql: the sql to be executed
    :type sql: str
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :type labels: dict
    """

    template_fields = (
        'sql',
        'gcp_conn_id',
        'pass_value',
        'impersonation_chain',
        'labels',
    )
    template_ext = ('.sql',)
    ui_color = BigQueryUIColors.CHECK.value

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        use_legacy_sql: bool = True,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        labels: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, pass_value=pass_value, tolerance=tolerance, **kwargs)

        if bigquery_conn_id:
            warnings.warn(_DEPRECATION_MSG, DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.impersonation_chain = impersonation_chain
        self.labels = labels


class BigQueryIntervalCheckOperator(_BigQueryDbHookMixin, SQLIntervalCheckOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    This method constructs a query like so ::

        SELECT {metrics_threshold_dict_key} FROM {table}
        WHERE {date_filter_column}=<date>

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryIntervalCheckOperator`

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :type metrics_thresholds: dict
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :type use_legacy_sql: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :type labels: dict
    """

    template_fields = (
        'table',
        'gcp_conn_id',
        'sql1',
        'sql2',
        'impersonation_chain',
        'labels',
    )
    ui_color = BigQueryUIColors.CHECK.value

    @apply_defaults
    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict,
        date_filter_column: str = 'ds',
        days_back: SupportsAbs[int] = -7,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        use_legacy_sql: bool = True,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        labels: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            table=table,
            metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column,
            days_back=days_back,
            **kwargs,
        )

        if bigquery_conn_id:
            warnings.warn(_DEPRECATION_MSG, DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.labels = labels


class BigQueryGetDataOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and returns data in a python list. The number of elements in the returned list will
    be equal to the number of rows fetched. Each element in the list will again be a list
    where element would represent the columns values for that row.

    **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDataOperator`

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.

    **Example**: ::

        get_data = BigQueryGetDataOperator(
            task_id='get_data_from_bq',
            dataset_id='test_dataset',
            table_id='Transaction_partitions',
            max_results=100,
            selected_fields='DATE',
            gcp_conn_id='airflow-conn-id'
        )

    :param dataset_id: The dataset ID of the requested table. (templated)
    :type dataset_id: str
    :param table_id: The table ID of the requested table. (templated)
    :type table_id: str
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :type max_results: int
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :type selected_fields: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param location: The location used for the operation.
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'dataset_id',
        'table_id',
        'max_results',
        'selected_fields',
        'impersonation_chain',
    )
    ui_color = BigQueryUIColors.QUERY.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        max_results: int = 100,
        selected_fields: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.table_id = table_id
        self.max_results = int(max_results)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> list:
        self.log.info(
            'Fetching Data from %s.%s max results: %s', self.dataset_id, self.table_id, self.max_results
        )

        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        rows = hook.list_rows(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            max_results=self.max_results,
            selected_fields=self.selected_fields,
            location=self.location,
        )

        self.log.info('Total extracted rows: %s', len(rows))

        table_data = [row.values() for row in rows]
        return table_data


# pylint: disable=too-many-instance-attributes
class BigQueryExecuteQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database.
    This operator does not assert idempotency.

    :param sql: the sql code to be executed (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'.
    :param destination_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that, if set, will store the results
        of the query. (templated)
    :type destination_dataset_table: str
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :type write_disposition: str
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :type create_disposition: str
    :param allow_large_results: Whether to allow large results.
    :type allow_large_results: bool
    :param flatten_results: If true and query uses legacy SQL dialect, flattens
        all nested and repeated fields in the query results. ``allow_large_results``
        must be ``true`` if this is set to ``false``. For standard SQL queries, this
        flag is ignored and results are never flattened.
    :type flatten_results: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :type udf_config: list
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: bool
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :type maximum_billing_tier: int
    :param maximum_bytes_billed: Limits the bytes billed for this job.
        Queries that will have bytes billed beyond this limit will fail
        (without incurring a charge). If unspecified, this will be
        set to your project default.
    :type maximum_bytes_billed: float
    :param api_resource_configs: a dictionary that contain params
        'configuration' applied for Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
        for example, {'query': {'useQueryCache': False}}. You could use it
        if you need to provide some params that are not supported by BigQueryOperator
        like args.
    :type api_resource_configs: dict
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: Optional[Union[list, tuple, set]]
    :param query_params: a list of dictionary containing query parameter types and
        values, passed to BigQuery. The structure of dictionary should look like
        'queryParameters' in Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
        For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': 'romeoandjuliet' } }]. (templated)
    :type query_params: list
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :type priority: str
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and expiration as per API specifications.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this query be stored sorted
        by one or more columns. BigQuery supports clustering for both partitioned and
        non-partitioned tables. The order of columns given determines the sort order.
    :type cluster_fields: list[str]
    :param location: The geographic location of the job. Required except for
        US and EU. See details at
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'sql',
        'destination_dataset_table',
        'labels',
        'query_params',
        'impersonation_chain',
    )
    template_ext = ('.sql',)
    ui_color = BigQueryUIColors.QUERY.value

    @property
    def operator_extra_links(self):
        """Return operator extra links"""
        if isinstance(self.sql, str):
            return (BigQueryConsoleLink(),)
        return (BigQueryConsoleIndexableLink(i) for i, _ in enumerate(self.sql))

    # pylint: disable=too-many-arguments, too-many-locals
    @apply_defaults
    def __init__(
        self,
        *,
        sql: Union[str, Iterable],
        destination_dataset_table: Optional[str] = None,
        write_disposition: str = 'WRITE_EMPTY',
        allow_large_results: Optional[bool] = False,
        flatten_results: Optional[bool] = None,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        udf_config: Optional[list] = None,
        use_legacy_sql: bool = True,
        maximum_billing_tier: Optional[int] = None,
        maximum_bytes_billed: Optional[float] = None,
        create_disposition: str = 'CREATE_IF_NEEDED',
        schema_update_options: Optional[Union[list, tuple, set]] = None,
        query_params: Optional[list] = None,
        labels: Optional[dict] = None,
        priority: str = 'INTERACTIVE',
        time_partitioning: Optional[dict] = None,
        api_resource_configs: Optional[dict] = None,
        cluster_fields: Optional[List[str]] = None,
        location: Optional[str] = None,
        encryption_configuration: Optional[dict] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
            )
            gcp_conn_id = bigquery_conn_id

        warnings.warn(
            "This operator is deprecated. Please use `BigQueryInsertJobOperator`.",
            DeprecationWarning,
        )

        self.sql = sql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.flatten_results = flatten_results
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.maximum_bytes_billed = maximum_bytes_billed
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.labels = labels
        self.priority = priority
        self.time_partitioning = time_partitioning
        self.api_resource_configs = api_resource_configs
        self.cluster_fields = cluster_fields
        self.location = location
        self.encryption_configuration = encryption_configuration
        self.hook = None  # type: Optional[BigQueryHook]
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        if self.hook is None:
            self.log.info('Executing: %s', self.sql)
            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        if isinstance(self.sql, str):
            job_id = self.hook.run_query(
                sql=self.sql,
                destination_dataset_table=self.destination_dataset_table,
                write_disposition=self.write_disposition,
                allow_large_results=self.allow_large_results,
                flatten_results=self.flatten_results,
                udf_config=self.udf_config,
                maximum_billing_tier=self.maximum_billing_tier,
                maximum_bytes_billed=self.maximum_bytes_billed,
                create_disposition=self.create_disposition,
                query_params=self.query_params,
                labels=self.labels,
                schema_update_options=self.schema_update_options,
                priority=self.priority,
                time_partitioning=self.time_partitioning,
                api_resource_configs=self.api_resource_configs,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration,
            )
        elif isinstance(self.sql, Iterable):
            job_id = [
                self.hook.run_query(
                    sql=s,
                    destination_dataset_table=self.destination_dataset_table,
                    write_disposition=self.write_disposition,
                    allow_large_results=self.allow_large_results,
                    flatten_results=self.flatten_results,
                    udf_config=self.udf_config,
                    maximum_billing_tier=self.maximum_billing_tier,
                    maximum_bytes_billed=self.maximum_bytes_billed,
                    create_disposition=self.create_disposition,
                    query_params=self.query_params,
                    labels=self.labels,
                    schema_update_options=self.schema_update_options,
                    priority=self.priority,
                    time_partitioning=self.time_partitioning,
                    api_resource_configs=self.api_resource_configs,
                    cluster_fields=self.cluster_fields,
                    encryption_configuration=self.encryption_configuration,
                )
                for s in self.sql
            ]
        else:
            raise AirflowException(f"argument 'sql' of type {type(str)} is neither a string nor an iterable")
        context['task_instance'].xcom_push(key='job_id', value=job_id)

    def on_kill(self) -> None:
        super().on_kill()
        if self.hook is not None:
            self.log.info('Cancelling running query')
            self.hook.cancel_query()


class BigQueryCreateEmptyTableOperator(BaseOperator):
    """
    Creates a new, empty table in the specified BigQuery dataset,
    optionally with schema.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.
    You can also create a table without schema.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateEmptyTableOperator`

    :param project_id: The project to create the table into. (templated)
    :type project_id: str
    :param dataset_id: The dataset to create the table into. (templated)
    :type dataset_id: str
    :param table_id: The Name of the table to be created. (templated)
    :type table_id: str
    :param table_resource: Table resource as described in documentation:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
        If provided all other parameters are ignored.
    :type table_resource: Dict[str, Any]
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

    :type schema_fields: list
    :param gcs_schema_object: Full path to the JSON file containing
        schema (templated). For
        example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
    :type gcs_schema_object: str
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
    :type time_partitioning: dict
    :param bigquery_conn_id: [Optional] The connection ID used to connect to Google Cloud and
        interact with the Bigquery service.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: [Optional] The connection ID used to connect to Google Cloud.
        and interact with the Google Cloud Storage service.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the table, passed to BigQuery

        **Example (with schema JSON in GCS)**: ::

            CreateTable = BigQueryCreateEmptyTableOperator(
                task_id='BigQueryCreateEmptyTableOperator_task',
                dataset_id='ODS',
                table_id='Employees',
                project_id='internal-gcp-project',
                gcs_schema_object='gs://schema-bucket/employee_schema.json',
                bigquery_conn_id='airflow-conn-id',
                google_cloud_storage_conn_id='airflow-conn-id'
            )

        **Corresponding Schema file** (``employee_schema.json``): ::

            [
              {
                "mode": "NULLABLE",
                "name": "emp_name",
                "type": "STRING"
              },
              {
                "mode": "REQUIRED",
                "name": "salary",
                "type": "INTEGER"
              }
            ]

        **Example (with schema in the DAG)**: ::

            CreateTable = BigQueryCreateEmptyTableOperator(
                task_id='BigQueryCreateEmptyTableOperator_task',
                dataset_id='ODS',
                table_id='Employees',
                project_id='internal-gcp-project',
                schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                               {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}],
                bigquery_conn_id='airflow-conn-id-account',
                google_cloud_storage_conn_id='airflow-conn-id'
            )
    :type labels: dict
    :param view: [Optional] A dictionary containing definition for the view.
        If set, it will create a view instead of a table:

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
    :type view: dict
    :param materialized_view: [Optional] The materialized view definition.
    :type materialized_view: dict
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    :param location: The location used for the operation.
    :type location: str
    :param cluster_fields: [Optional] The fields used for clustering.
            BigQuery supports clustering for both partitioned and
            non-partitioned tables.

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#clustering.fields
    :type cluster_fields: list
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param exists_ok: If ``True``, ignore "already exists" errors when creating the table.
    :type exists_ok: bool
    """

    template_fields = (
        'dataset_id',
        'table_id',
        'project_id',
        'gcs_schema_object',
        'labels',
        'view',
        'materialized_view',
        'impersonation_chain',
    )
    template_fields_renderers = {"table_resource": "json", "materialized_view": "json"}
    ui_color = BigQueryUIColors.TABLE.value

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        table_resource: Optional[Dict[str, Any]] = None,
        project_id: Optional[str] = None,
        schema_fields: Optional[List] = None,
        gcs_schema_object: Optional[str] = None,
        time_partitioning: Optional[Dict] = None,
        bigquery_conn_id: str = 'google_cloud_default',
        google_cloud_storage_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        labels: Optional[Dict] = None,
        view: Optional[Dict] = None,
        materialized_view: Optional[Dict] = None,
        encryption_configuration: Optional[Dict] = None,
        location: Optional[str] = None,
        cluster_fields: Optional[List[str]] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        exists_ok: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = {} if time_partitioning is None else time_partitioning
        self.labels = labels
        self.view = view
        self.materialized_view = materialized_view
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.cluster_fields = cluster_fields
        self.table_resource = table_resource
        self.impersonation_chain = impersonation_chain
        self.exists_ok = exists_ok

    def execute(self, context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if not self.schema_fields and self.gcs_schema_object:
            gcs_bucket, gcs_object = _parse_gcs_url(self.gcs_schema_object)
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields = json.loads(gcs_hook.download(gcs_bucket, gcs_object))
        else:
            schema_fields = self.schema_fields

        try:
            self.log.info('Creating table')
            table = bq_hook.create_empty_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                schema_fields=schema_fields,
                time_partitioning=self.time_partitioning,
                cluster_fields=self.cluster_fields,
                labels=self.labels,
                view=self.view,
                materialized_view=self.materialized_view,
                encryption_configuration=self.encryption_configuration,
                table_resource=self.table_resource,
                exists_ok=self.exists_ok,
            )
            self.log.info(
                'Table %s.%s.%s created successfully', table.project, table.dataset_id, table.table_id
            )
        except Conflict:
            self.log.info('Table %s.%s already exists.', self.dataset_id, self.table_id)


# pylint: disable=too-many-instance-attributes
class BigQueryCreateExternalTableOperator(BaseOperator):
    """
    Creates a new external table in the dataset with the data from Google Cloud
    Storage.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateExternalTableOperator`

    :param bucket: The bucket to point the external table to. (templated)
    :type bucket: str
    :param source_objects: List of Google Cloud Storage URIs to point
        table to. If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :type source_objects: list
    :param destination_project_dataset_table: The dotted ``(<project>.)<dataset>.<table>``
        BigQuery table to load data into (templated). If ``<project>`` is not included,
        project will be the project defined in the connection json.
    :type destination_project_dataset_table: str
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

        Should not be set when source_format is 'DATASTORE_BACKUP'.
    :param table_resource: Table resource as described in documentation:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
        If provided all other parameters are ignored. External schema from object will be resolved.
    :type table_resource: Dict[str, Any]
    :type schema_fields: list
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
    :type schema_object: str
    :param source_format: File format of the data.
    :type source_format: str
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :type compression: str
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
    :type skip_leading_rows: int
    :param field_delimiter: The delimiter to use for the CSV.
    :type field_delimiter: str
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :type max_bad_records: int
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :type quote_character: str
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :type allow_quoted_newlines: bool
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :type allow_jagged_rows: bool
    :param bigquery_conn_id: (Optional) The connection ID used to connect to Google Cloud and
        interact with the Bigquery service.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
        and interact with the Google Cloud Storage service.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param src_fmt_configs: configure optional fields specific to the source format
    :type src_fmt_configs: dict
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :type labels: dict
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    :param location: The location used for the operation.
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'source_objects',
        'schema_object',
        'destination_project_dataset_table',
        'labels',
        'table_resource',
        'impersonation_chain',
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value

    # pylint: disable=too-many-arguments,too-many-locals
    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str,
        source_objects: List,
        destination_project_dataset_table: str,
        table_resource: Optional[Dict[str, Any]] = None,
        schema_fields: Optional[List] = None,
        schema_object: Optional[str] = None,
        source_format: str = 'CSV',
        compression: str = 'NONE',
        skip_leading_rows: int = 0,
        field_delimiter: str = ',',
        max_bad_records: int = 0,
        quote_character: Optional[str] = None,
        allow_quoted_newlines: bool = False,
        allow_jagged_rows: bool = False,
        bigquery_conn_id: str = 'google_cloud_default',
        google_cloud_storage_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        src_fmt_configs: Optional[dict] = None,
        labels: Optional[Dict] = None,
        encryption_configuration: Optional[Dict] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        # GCS config
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        # BQ config
        kwargs_passed = any(
            [
                destination_project_dataset_table,
                schema_fields,
                source_format,
                compression,
                skip_leading_rows,
                field_delimiter,
                max_bad_records,
                quote_character,
                allow_quoted_newlines,
                allow_jagged_rows,
                src_fmt_configs,
                labels,
                encryption_configuration,
            ]
        )

        if not table_resource:
            warnings.warn(
                "Passing table parameters via keywords arguments will be deprecated. "
                "Please use provide table definition using `table_resource` parameter."
                "You can still use external `schema_object`. ",
                DeprecationWarning,
                stacklevel=2,
            )

        if table_resource and kwargs_passed:
            raise ValueError("You provided both `table_resource` and exclusive keywords arguments.")

        self.table_resource = table_resource
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression
        self.skip_leading_rows = skip_leading_rows
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows

        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.src_fmt_configs = src_fmt_configs or {}
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if not self.schema_fields and self.schema_object and self.source_format != 'DATASTORE_BACKUP':
            gcs_hook = GCSHook(
                gcp_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            schema_fields = json.loads(gcs_hook.download(self.bucket, self.schema_object))
        else:
            schema_fields = self.schema_fields

        if schema_fields and self.table_resource:
            self.table_resource["externalDataConfiguration"]["schema"] = schema_fields

        if self.table_resource:
            tab_ref = TableReference.from_string(self.destination_project_dataset_table)
            bq_hook.create_empty_table(
                table_resource=self.table_resource,
                project_id=tab_ref.project,
                table_id=tab_ref.table_id,
                dataset_id=tab_ref.dataset_id,
            )
        else:
            source_uris = [f"gs://{self.bucket}/{source_object}" for source_object in self.source_objects]

            bq_hook.create_external_table(
                external_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                compression=self.compression,
                skip_leading_rows=self.skip_leading_rows,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                src_fmt_configs=self.src_fmt_configs,
                labels=self.labels,
                encryption_configuration=self.encryption_configuration,
            )


class BigQueryDeleteDatasetOperator(BaseOperator):
    """
    This operator deletes an existing dataset from your Project in Big query.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteDatasetOperator`

    :param project_id: The project id of the dataset.
    :type project_id: str
    :param dataset_id: The dataset to be deleted.
    :type dataset_id: str
    :param delete_contents: (Optional) Whether to force the deletion even if the dataset is not empty.
        Will delete all tables (if any) in the dataset if set to True.
        Will raise HttpError 400: "{dataset_id} is still in use" if set to False and dataset is not empty.
        The default value is False.
    :type delete_contents: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    **Example**: ::

        delete_temp_data = BigQueryDeleteDatasetOperator(
            dataset_id='temp-dataset',
            project_id='temp-project',
            delete_contents=True, # Force the deletion of the dataset as well as its tables (if any).
            gcp_conn_id='_my_gcp_conn_',
            task_id='Deletetemp',
            dag=dag)
    """

    template_fields = (
        'dataset_id',
        'project_id',
        'impersonation_chain',
    )
    ui_color = BigQueryUIColors.DATASET.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: Optional[str] = None,
        delete_contents: bool = False,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.delete_contents = delete_contents
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

        super().__init__(**kwargs)

    def execute(self, context) -> None:
        self.log.info('Dataset id: %s Project id: %s', self.dataset_id, self.project_id)

        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        bq_hook.delete_dataset(
            project_id=self.project_id, dataset_id=self.dataset_id, delete_contents=self.delete_contents
        )


class BigQueryCreateEmptyDatasetOperator(BaseOperator):
    """
    This operator is used to create new dataset for your Project in BigQuery.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryCreateEmptyDatasetOperator`

    :param project_id: The name of the project where we want to create the dataset.
    :type project_id: str
    :param dataset_id: The id of dataset. Don't need to provide, if datasetId in dataset_reference.
    :type dataset_id: str
    :param location: The geographic location where the dataset should reside.
    :type location: str
    :param dataset_reference: Dataset reference that could be provided with request body.
        More info:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_reference: dict
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param exists_ok: If ``True``, ignore "already exists" errors when creating the dataset.
    :type exists_ok: bool
        **Example**: ::

            create_new_dataset = BigQueryCreateEmptyDatasetOperator(
                dataset_id='new-dataset',
                project_id='my-project',
                dataset_reference={"friendlyName": "New Dataset"}
                gcp_conn_id='_my_gcp_conn_',
                task_id='newDatasetCreator',
                dag=dag)
    """

    template_fields = (
        'dataset_id',
        'project_id',
        'dataset_reference',
        'impersonation_chain',
    )
    template_fields_renderers = {"dataset_reference": "json"}
    ui_color = BigQueryUIColors.DATASET.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        project_id: Optional[str] = None,
        dataset_reference: Optional[Dict] = None,
        location: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        exists_ok: bool = False,
        **kwargs,
    ) -> None:

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.dataset_reference = dataset_reference if dataset_reference else {}
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.exists_ok = exists_ok

        super().__init__(**kwargs)

    def execute(self, context) -> None:
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            bq_hook.create_empty_dataset(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                dataset_reference=self.dataset_reference,
                location=self.location,
                exists_ok=self.exists_ok,
            )
        except Conflict:
            dataset_id = self.dataset_reference.get("datasetReference", {}).get("datasetId", self.dataset_id)
            self.log.info('Dataset %s already exists.', dataset_id)


class BigQueryGetDatasetOperator(BaseOperator):
    """
    This operator is used to return the dataset specified by dataset_id.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDatasetOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: dataset
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    """

    template_fields = (
        'dataset_id',
        'project_id',
        'impersonation_chain',
    )
    ui_color = BigQueryUIColors.DATASET.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info('Start getting dataset: %s:%s', self.project_id, self.dataset_id)

        dataset = bq_hook.get_dataset(dataset_id=self.dataset_id, project_id=self.project_id)
        return dataset.to_api_repr()


class BigQueryGetDatasetTablesOperator(BaseOperator):
    """
    This operator retrieves the list of tables in the specified dataset.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDatasetTablesOperator`

    :param dataset_id: the dataset ID of the requested dataset.
    :type dataset_id: str
    :param project_id: (Optional) the project of the requested dataset. If None,
        self.project_id will be used.
    :type project_id: str
    :param max_results: (Optional) the maximum number of tables to return.
    :type max_results: int
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'dataset_id',
        'project_id',
        'impersonation_chain',
    )
    ui_color = BigQueryUIColors.DATASET.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        project_id: Optional[str] = None,
        max_results: Optional[int] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.max_results = max_results
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        return bq_hook.get_dataset_tables(
            dataset_id=self.dataset_id,
            project_id=self.project_id,
            max_results=self.max_results,
        )


class BigQueryPatchDatasetOperator(BaseOperator):
    """
    This operator is used to patch dataset for your Project in BigQuery.
    It only replaces fields that are provided in the submitted dataset resource.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryPatchDatasetOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param dataset_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_resource: dict
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: dataset
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    """

    template_fields = (
        'dataset_id',
        'project_id',
        'impersonation_chain',
    )
    template_fields_renderers = {"dataset_resource": "json"}
    ui_color = BigQueryUIColors.DATASET.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        dataset_resource: dict,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:

        warnings.warn(
            "This operator is deprecated. Please use BigQueryUpdateDatasetOperator.",
            DeprecationWarning,
            stacklevel=3,
        )
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        return bq_hook.patch_dataset(
            dataset_id=self.dataset_id,
            dataset_resource=self.dataset_resource,
            project_id=self.project_id,
        )


class BigQueryUpdateTableOperator(BaseOperator):
    """
    This operator is used to update table for your Project in BigQuery.
    Use ``fields`` to specify which fields of table to update. If a field
    is listed in ``fields`` and is ``None`` in table, it will be deleted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpdateTableOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in table_reference.
    :param table_id: The id of table. Don't need to provide,
        if tableId in table_reference.
    :type table_id: str
    :param table_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
    :type table_resource: Dict[str, Any]
    :param fields: The fields of ``table`` to change, spelled as the Table
        properties (e.g. "friendly_name").
    :type fields: List[str]
    :param project_id: The name of the project where we want to create the table.
        Don't need to provide, if projectId in table_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: table
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
    """

    template_fields = (
        'dataset_id',
        'table_id',
        'project_id',
        'impersonation_chain',
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value

    @apply_defaults
    def __init__(
        self,
        *,
        table_resource: Dict[str, Any],
        fields: Optional[List[str]] = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.project_id = project_id
        self.fields = fields
        self.gcp_conn_id = gcp_conn_id
        self.table_resource = table_resource
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        return bq_hook.update_table(
            table_resource=self.table_resource,
            fields=self.fields,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            project_id=self.project_id,
        )


class BigQueryUpdateDatasetOperator(BaseOperator):
    """
    This operator is used to update dataset for your Project in BigQuery.
    Use ``fields`` to specify which fields of dataset to update. If a field
    is listed in ``fields`` and is ``None`` in dataset, it will be deleted.
    If no ``fields`` are provided then all fields of provided ``dataset_resource``
    will be used.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpdateDatasetOperator`

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param dataset_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_resource: Dict[str, Any]
    :param fields: The properties of dataset to change (e.g. "friendly_name").
    :type fields: Sequence[str]
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]

    :rtype: dataset
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    """

    template_fields = (
        'dataset_id',
        'project_id',
        'impersonation_chain',
    )
    template_fields_renderers = {"dataset_resource": "json"}
    ui_color = BigQueryUIColors.DATASET.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_resource: Dict[str, Any],
        fields: Optional[List[str]] = None,
        dataset_id: Optional[str] = None,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.fields = fields
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        fields = self.fields or list(self.dataset_resource.keys())

        dataset = bq_hook.update_dataset(
            dataset_resource=self.dataset_resource,
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            fields=fields,
        )
        return dataset.to_api_repr()


class BigQueryDeleteTableOperator(BaseOperator):
    """
    Deletes BigQuery tables

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryDeleteTableOperator`

    :param deletion_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
        will be deleted. (templated)
    :type deletion_dataset_table: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param ignore_if_missing: if True, then return success even if the
        requested table does not exist.
    :type ignore_if_missing: bool
    :param location: The location used for the operation.
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'deletion_dataset_table',
        'impersonation_chain',
    )
    ui_color = BigQueryUIColors.TABLE.value

    @apply_defaults
    def __init__(
        self,
        *,
        deletion_dataset_table: str,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        ignore_if_missing: bool = False,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.deletion_dataset_table = deletion_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.ignore_if_missing = ignore_if_missing
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> None:
        self.log.info('Deleting: %s', self.deletion_dataset_table)
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        hook.delete_table(table_id=self.deletion_dataset_table, not_found_ok=self.ignore_if_missing)


class BigQueryUpsertTableOperator(BaseOperator):
    """
    Upsert BigQuery table

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryUpsertTableOperator`

    :param dataset_id: A dotted
        ``(<project>.|<project>:)<dataset>`` that indicates which dataset
        will be updated. (templated)
    :type dataset_id: str
    :param table_resource: a table resource. see
        https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
    :type table_resource: dict
    :param project_id: The name of the project where we want to update the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param location: The location used for the operation.
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'dataset_id',
        'table_resource',
        'impersonation_chain',
    )
    template_fields_renderers = {"table_resource": "json"}
    ui_color = BigQueryUIColors.TABLE.value

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str,
        table_resource: dict,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.table_resource = table_resource
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context) -> None:
        self.log.info('Upserting Dataset: %s with table_resource: %s', self.dataset_id, self.table_resource)
        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        hook.run_table_upsert(
            dataset_id=self.dataset_id,
            table_resource=self.table_resource,
            project_id=self.project_id,
        )


# pylint: disable=too-many-arguments
class BigQueryInsertJobOperator(BaseOperator):
    """
    Executes a BigQuery job. Waits for the job to complete and returns job id.
    This operator work in the following way:

    - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
    - creates ``job_id`` in form of
        ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
    - submits a BigQuery job using the ``job_id``
    - if job with given id already exists then it tries to reattach to the job if its not done and its
        state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

    Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

    For job definition see here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryInsertJobOperator`


    :param configuration: The configuration parameter maps directly to BigQuery's
        configuration field in the job  object. For more details see
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    :type configuration: Dict[str, Any]
    :param job_id: The ID of the job. It will be suffixed with hash of job configuration
        unless ``force_rerun`` is True.
        The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
        dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
        be generated.
    :type job_id: str
    :param force_rerun: If True then operator will use hash of uuid as job id suffix
    :type force_rerun: bool
    :param reattach_states: Set of BigQuery job's states in case of which we should reattach
        to the job. Should be other than final states.
    :param project_id: Google Cloud Project where the job is running
    :type project_id: str
    :param location: location the job is running
    :type location: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    :type cancel_on_kill: bool
    """

    template_fields = (
        "configuration",
        "job_id",
        "impersonation_chain",
    )
    template_ext = (".json",)
    template_fields_renderers = {"configuration": "json"}
    ui_color = BigQueryUIColors.QUERY.value

    @apply_defaults
    def __init__(
        self,
        configuration: Dict[str, Any],
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        job_id: Optional[str] = None,
        force_rerun: bool = True,
        reattach_states: Optional[Set[str]] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.force_rerun = force_rerun
        self.reattach_states: Set[str] = reattach_states or set()
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: Optional[BigQueryHook] = None

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.configuration, str) and self.configuration.endswith('.json'):
            with open(self.configuration) as file:
                self.configuration = json.loads(file.read())

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        # Submit a new job
        job = hook.insert_job(
            configuration=self.configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
        )
        # Start the job and wait for it to complete and get the result.
        job.result()
        return job

    @staticmethod
    def _handle_job_error(job: BigQueryJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def _job_id(self, context):
        if self.force_rerun:
            hash_base = str(uuid.uuid4())
        else:
            hash_base = json.dumps(self.configuration, sort_keys=True)

        uniqueness_suffix = hashlib.md5(hash_base.encode()).hexdigest()

        if self.job_id:
            return f"{self.job_id}_{uniqueness_suffix}"

        exec_date = context['execution_date'].isoformat()
        job_id = f"airflow_{self.dag_id}_{self.task_id}_{exec_date}_{uniqueness_suffix}"
        return re.sub(r"[:\-+.]", "_", job_id)

    def execute(self, context: Any):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook

        job_id = self._job_id(context)

        try:
            job = self._submit_job(hook, job_id)
            self._handle_job_error(job)
        except Conflict:
            # If the job already exists retrieve it
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=job_id,
            )
            if job.state in self.reattach_states:
                # We are reattaching to a job
                job.result()
                self._handle_job_error(job)
            else:
                # Same job configuration so we need force_rerun
                raise AirflowException(
                    f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                    f"want to force rerun it consider setting `force_rerun=True`."
                    f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                )

        self.job_id = job.job_id
        return job.job_id

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(  # type: ignore[union-attr]
                job_id=self.job_id, project_id=self.project_id, location=self.location
            )
