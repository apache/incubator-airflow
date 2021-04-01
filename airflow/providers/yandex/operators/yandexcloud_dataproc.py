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

from typing import Dict, Iterable, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.yandex.hooks.yandexcloud_dataproc import DataprocHook
from airflow.utils.decorators import apply_defaults


class DataprocCreateClusterOperator(BaseOperator):
    """Creates Yandex.Cloud Data Proc cluster.

    :param folder_id: ID of the folder in which cluster should be created.
    :type folder_id: Optional[str]
    :param cluster_name: Cluster name. Must be unique inside the folder.
    :type cluster_name: Optional[str]
    :param cluster_description: Cluster description.
    :type cluster_description: str
    :param cluster_image_version: Cluster image version. Use default.
    :type cluster_image_version: str
    :param ssh_public_keys: List of SSH public keys that will be deployed to created compute instances.
    :type ssh_public_keys: Optional[Union[str, Iterable[str]]]
    :param subnet_id: ID of the subnetwork. All Data Proc cluster nodes will use one subnetwork.
    :type subnet_id: str
    :param services: List of services that will be installed to the cluster. Possible options:
        HDFS, YARN, MAPREDUCE, HIVE, TEZ, ZOOKEEPER, HBASE, SQOOP, FLUME, SPARK, SPARK, ZEPPELIN, OOZIE
    :type services: Iterable[str]
    :param s3_bucket: Yandex.Cloud S3 bucket to store cluster logs.
                      Jobs will not work if the bucket is not specified.
    :type s3_bucket: Optional[str]
    :param zone: Availability zone to create cluster in.
                 Currently there are ru-central1-a, ru-central1-b and ru-central1-c.
    :type zone: str
    :param service_account_id: Service account id for the cluster.
                               Service account can be created inside the folder.
    :type service_account_id: Optional[str]
    :param masternode_resource_preset: Resources preset (CPU+RAM configuration)
                                       for the master node of the cluster.
    :type masternode_resource_preset: str
    :param masternode_disk_size: Masternode storage size in GiB.
    :type masternode_disk_size: int
    :param masternode_disk_type: Masternode storage type. Possible options: network-ssd, network-hdd.
    :type masternode_disk_type: str
    :param datanode_resource_preset: Resources preset (CPU+RAM configuration)
                                     for the data nodes of the cluster.
    :type datanode_resource_preset: str
    :param datanode_disk_size: Datanodes storage size in GiB.
    :type datanode_disk_size: int
    :param datanode_disk_type: Datanodes storage type. Possible options: network-ssd, network-hdd.
    :type datanode_disk_type: str
    :param computenode_resource_preset: Resources preset (CPU+RAM configuration)
                                        for the compute nodes of the cluster.
    :type computenode_resource_preset: str
    :param computenode_disk_size: Computenodes storage size in GiB.
    :type computenode_disk_size: int
    :param computenode_disk_type: Computenodes storage type. Possible options: network-ssd, network-hdd.
    :type computenode_disk_type: str
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    @apply_defaults
    def __init__(
        self,
        *,
        folder_id: Optional[str] = None,
        cluster_name: Optional[str] = None,
        cluster_description: str = '',
        cluster_image_version: str = '1.1',
        ssh_public_keys: Optional[Union[str, Iterable[str]]] = None,
        subnet_id: Optional[str] = None,
        services: Iterable[str] = ('HDFS', 'YARN', 'MAPREDUCE', 'HIVE', 'SPARK'),
        s3_bucket: Optional[str] = None,
        zone: str = 'ru-central1-b',
        service_account_id: Optional[str] = None,
        masternode_resource_preset: str = 's2.small',
        masternode_disk_size: int = 15,
        masternode_disk_type: str = 'network-ssd',
        datanode_resource_preset: str = 's2.small',
        datanode_disk_size: int = 15,
        datanode_disk_type: str = 'network-ssd',
        datanode_count: int = 2,
        computenode_resource_preset: str = 's2.small',
        computenode_disk_size: int = 15,
        computenode_disk_type: str = 'network-ssd',
        computenode_count: int = 0,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.folder_id = folder_id
        self.connection_id = connection_id
        self.cluster_name = cluster_name
        self.cluster_description = cluster_description
        self.cluster_image_version = cluster_image_version
        self.ssh_public_keys = ssh_public_keys
        self.subnet_id = subnet_id
        self.services = services
        self.s3_bucket = s3_bucket
        self.zone = zone
        self.service_account_id = service_account_id
        self.masternode_resource_preset = masternode_resource_preset
        self.masternode_disk_size = masternode_disk_size
        self.masternode_disk_type = masternode_disk_type
        self.datanode_resource_preset = datanode_resource_preset
        self.datanode_disk_size = datanode_disk_size
        self.datanode_disk_type = datanode_disk_type
        self.datanode_count = datanode_count
        self.computenode_resource_preset = computenode_resource_preset
        self.computenode_disk_size = computenode_disk_size
        self.computenode_disk_type = computenode_disk_type
        self.computenode_count = computenode_count
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        self.hook = DataprocHook(
            connection_id=self.connection_id,
        )
        operation_result = self.hook.client.create_cluster(
            folder_id=self.folder_id,
            cluster_name=self.cluster_name,
            cluster_description=self.cluster_description,
            cluster_image_version=self.cluster_image_version,
            ssh_public_keys=self.ssh_public_keys,
            subnet_id=self.subnet_id,
            services=self.services,
            s3_bucket=self.s3_bucket,
            zone=self.zone,
            service_account_id=self.service_account_id,
            masternode_resource_preset=self.masternode_resource_preset,
            masternode_disk_size=self.masternode_disk_size,
            masternode_disk_type=self.masternode_disk_type,
            datanode_resource_preset=self.datanode_resource_preset,
            datanode_disk_size=self.datanode_disk_size,
            datanode_disk_type=self.datanode_disk_type,
            datanode_count=self.datanode_count,
            computenode_resource_preset=self.computenode_resource_preset,
            computenode_disk_size=self.computenode_disk_size,
            computenode_disk_type=self.computenode_disk_type,
            computenode_count=self.computenode_count,
        )
        context['task_instance'].xcom_push(key='cluster_id', value=operation_result.response.id)
        context['task_instance'].xcom_push(key='yandexcloud_connection_id', value=self.connection_id)


class DataprocDeleteClusterOperator(BaseOperator):
    """Deletes Yandex.Cloud Data Proc cluster.

    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    :param cluster_id: ID of the cluster to remove. (templated)
    :type cluster_id: Optional[str]
    """

    template_fields = ['cluster_id']

    @apply_defaults
    def __init__(
        self, *, connection_id: Optional[str] = None, cluster_id: Optional[str] = None, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.cluster_id = cluster_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.client.delete_cluster(cluster_id)


class DataprocCreateHiveJobOperator(BaseOperator):
    """Runs Hive job in Data Proc cluster.

    :param query: Hive query.
    :type query: Optional[str]
    :param query_file_uri: URI of the script that contains Hive queries. Can be placed in HDFS or S3.
    :type query_file_uri: Optional[str]
    :param properties: A mapping of property names to values, used to configure Hive.
    :type properties: Optional[Dist[str, str]]
    :param script_variables: Mapping of query variable names to values.
    :type script_variables: Optional[Dist[str, str]]
    :param continue_on_failure: Whether to continue executing queries if a query fails.
    :type continue_on_failure: bool
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    template_fields = ['cluster_id']

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        query: Optional[str] = None,
        query_file_uri: Optional[str] = None,
        script_variables: Optional[Dict[str, str]] = None,
        continue_on_failure: bool = False,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Hive job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.query_file_uri = query_file_uri
        self.script_variables = script_variables
        self.continue_on_failure = continue_on_failure
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.client.create_hive_job(
            query=self.query,
            query_file_uri=self.query_file_uri,
            script_variables=self.script_variables,
            continue_on_failure=self.continue_on_failure,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocCreateMapReduceJobOperator(BaseOperator):
    """Runs Mapreduce job in Data Proc cluster.

    :param main_jar_file_uri: URI of jar file with job.
                              Can be placed in HDFS or S3. Can be specified instead of main_class.
    :type main_jar_file_uri: Optional[str]
    :param main_class: Name of the main class of the job. Can be specified instead of main_jar_file_uri.
    :type main_class: Optional[str]
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :type file_uris: Optional[Iterable[str]]
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :type archive_uris: Optional[Iterable[str]]
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :type jar_file_uris: Optional[Iterable[str]]
    :param properties: Properties for the job.
    :type properties: Optional[Dist[str, str]]
    :param args: Arguments to be passed to the job.
    :type args: Optional[Iterable[str]]
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    template_fields = ['cluster_id']

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        main_class: Optional[str] = None,
        main_jar_file_uri: Optional[str] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Mapreduce job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.client.create_mapreduce_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocCreateSparkJobOperator(BaseOperator):
    """Runs Spark job in Data Proc cluster.

    :param main_jar_file_uri: URI of jar file with job. Can be placed in HDFS or S3.
    :type main_jar_file_uri: Optional[str]
    :param main_class: Name of the main class of the job.
    :type main_class: Optional[str]
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :type file_uris: Optional[Iterable[str]]
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :type archive_uris: Optional[Iterable[str]]
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :type jar_file_uris: Optional[Iterable[str]]
    :param properties: Properties for the job.
    :type properties: Optional[Dist[str, str]]
    :param args: Arguments to be passed to the job.
    :type args: Optional[Iterable[str]]
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    template_fields = ['cluster_id']

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        main_class: Optional[str] = None,
        main_jar_file_uri: Optional[str] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Spark job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_class = main_class
        self.main_jar_file_uri = main_jar_file_uri
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.client.create_spark_job(
            main_class=self.main_class,
            main_jar_file_uri=self.main_jar_file_uri,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )


class DataprocCreatePysparkJobOperator(BaseOperator):
    """Runs Pyspark job in Data Proc cluster.

    :param main_python_file_uri: URI of python file with job. Can be placed in HDFS or S3.
    :type main_python_file_uri: Optional[str]
    :param python_file_uris: URIs of python files used in the job. Can be placed in HDFS or S3.
    :type python_file_uris: Optional[Iterable[str]]
    :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
    :type file_uris: Optional[Iterable[str]]
    :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
    :type archive_uris: Optional[Iterable[str]]
    :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
    :type jar_file_uris: Optional[Iterable[str]]
    :param properties: Properties for the job.
    :type properties: Optional[Dist[str, str]]
    :param args: Arguments to be passed to the job.
    :type args: Optional[Iterable[str]]
    :param name: Name of the job. Used for labeling.
    :type name: str
    :param cluster_id: ID of the cluster to run job in.
                       Will try to take the ID from Dataproc Hook object if ot specified. (templated)
    :type cluster_id: Optional[str]
    :param connection_id: ID of the Yandex.Cloud Airflow connection.
    :type connection_id: Optional[str]
    """

    template_fields = ['cluster_id']

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        main_python_file_uri: Optional[str] = None,
        python_file_uris: Optional[Iterable[str]] = None,
        jar_file_uris: Optional[Iterable[str]] = None,
        archive_uris: Optional[Iterable[str]] = None,
        file_uris: Optional[Iterable[str]] = None,
        args: Optional[Iterable[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        name: str = 'Pyspark job',
        cluster_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.main_python_file_uri = main_python_file_uri
        self.python_file_uris = python_file_uris
        self.jar_file_uris = jar_file_uris
        self.archive_uris = archive_uris
        self.file_uris = file_uris
        self.args = args
        self.properties = properties
        self.name = name
        self.cluster_id = cluster_id
        self.connection_id = connection_id
        self.hook: Optional[DataprocHook] = None

    def execute(self, context) -> None:
        cluster_id = self.cluster_id or context['task_instance'].xcom_pull(key='cluster_id')
        connection_id = self.connection_id or context['task_instance'].xcom_pull(
            key='yandexcloud_connection_id'
        )
        self.hook = DataprocHook(
            connection_id=connection_id,
        )
        self.hook.client.create_pyspark_job(
            main_python_file_uri=self.main_python_file_uri,
            python_file_uris=self.python_file_uris,
            jar_file_uris=self.jar_file_uris,
            archive_uris=self.archive_uris,
            file_uris=self.file_uris,
            args=self.args,
            properties=self.properties,
            name=self.name,
            cluster_id=cluster_id,
        )
