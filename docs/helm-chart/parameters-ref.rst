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

Parameters reference
====================

The following tables lists the configurable parameters of the Airflow chart and their default values.

.. list-table::
   :widths: 15 10 30
   :header-rows: 1

   * - Parameter
     - Description
     - Default
   * - ``fullnameOverride``
     - Provide a name to substitute for the full names of resources
     - ``~``
   * - ``nameOverride``
     - Override the name of the chart
     - ``~``
   * - ``uid``
     - UID to run airflow pods under
     - ``1``
   * - ``gid``
     - GID to run airflow pods under
     - ``1``
   * - ``nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``labels``
     - Common labels to add to all objects defined in this chart
     - ``1``
   * - ``privateRegistry.enabled``
     - Enable usage of a private registry for Airflow base image
     - ``1``
   * - ``privateRegistry.repository``
     - Repository where base image lives (eg: quay.io)
     - ``1``
   * - ``ingress.enabled``
     - Enable Kubernetes Ingress support
     - ``1``
   * - ``ingress.web.*``
     - Configs for the Ingress of the web Service
     - Please refer to ``values.yaml``
   * - ``ingress.flower.*``
     - Configs for the Ingress of the flower Service
     - Please refer to ``values.yaml``
   * - ``networkPolicies.enabled``
     - Enable Network Policies to restrict traffic
     - ``1``
   * - ``airflowHome``
     - Location of airflow home directory
     - ``1``
   * - ``rbac.create``
     - Deploy pods with Kubernetes RBAC enabled
     - ``true``
   * - ``airflowPodAnnotations``
     - Extra annotations to apply to all Airflow pods.
     - ``{}``
   * - ``airflowConfigAnnotations``
     - Extra annotations to apply to the main Airflow configmap.
     - ``{}``
   * - ``executor``
     - Airflow executor (eg SequentialExecutor, LocalExecutor, CeleryExecutor, KubernetesExecutor)
     - ``1``
   * - ``allowPodLaunching``
     - Allow airflow pods to talk to Kubernetes API to launch more pods
     - ``1``
   * - ``defaultAirflowRepository``
     - Fallback docker repository to pull airflow image from
     - ``apache/airflow``
   * - ``defaultAirflowTag``
     - Fallback docker image tag to deploy
     - ``2.0.2``
   * - ``airflowVersion``
     - Airflow version (Used to make some decisions based on Airflow Version being deployed)
     - ``2.0.2``
   * - ``images.airflow.repository``
     - Docker repository to pull image from. Update this to deploy a custom image
     - ``~``
   * - ``images.airflow.tag``
     - Docker image tag to pull image from. Update this to deploy a new custom image tag
     - ``~``
   * - ``images.airflow.pullPolicy``
     - PullPolicy for airflow image
     - ``IfNotPresent``
   * - ``images.flower.repository``
     - Docker repository to pull image from. Update this to deploy a custom image
     - ``~``
   * - ``images.flower.tag``
     - Docker image tag to pull image from. Update this to deploy a new custom image tag
     - ``~``
   * - ``images.flower.pullPolicy``
     - PullPolicy for flower image
     - ``IfNotPresent``
   * - ``images.statsd.repository``
     - Docker repository to pull image from. Update this to deploy a custom image
     - ``apache/airflow``
   * - ``images.statsd.tag``
     - Docker image tag to pull image from. Update this to deploy a new custom image tag
     - ``airflow-statsd-exporter-2021.04.28-v0.17.0``
   * - ``images.statsd.pullPolicy``
     - PullPolicy for statsd-exporter image
     - ``IfNotPresent``
   * - ``images.redis.repository``
     - Docker repository to pull image from. Update this to deploy a custom image
     - ``redis``
   * - ``images.redis.tag``
     - Docker image tag to pull image from. Update this to deploy a new custom image tag
     - ``6-buster``
   * - ``images.redis.pullPolicy``
     - PullPolicy for redis image
     - ``IfNotPresent``
   * - ``images.pgbouncer.repository``
     - Docker repository to pull image from. Update this to deploy a custom image
     - ``apache/airflow``
   * - ``images.pgbouncer.tag``
     - Docker image tag to pull image from. Update this to deploy a new custom image tag
     - ``airflow-pgbouncer-2021.04.28-1.14.0``
   * - ``images.pgbouncer.pullPolicy``
     - PullPolicy for PgBouncer image
     - ``IfNotPresent``
   * - ``images.pgbouncerExporter.repository``
     - Docker repository to pull image from. Update this to deploy a custom image
     - ``apache/airflow``
   * - ``images.pgbouncerExporter.tag``
     - Docker image tag to pull image from. Update this to deploy a new custom image tag
     - ``airflow-pgbouncer-exporter-2021.04.28-0.5.0``
   * - ``images.pgbouncerExporter.pullPolicy``
     - PullPolicy for ``pgbouncer-exporter`` image
     - ``IfNotPresent``
   * - ``env``
     - Environment variables key/values to mount into Airflow pods (deprecated, prefer using ``extraEnv``)
     - ``1``
   * - ``secret``
     - Secret name/key pairs to mount into Airflow pods
     - ``1``
   * - ``extraEnv``
     - Extra env 'items' that will be added to the definition of airflow containers
     - ``1``
   * - ``extraEnvFrom``
     - Extra envFrom 'items' that will be added to the definition of airflow containers
     - ``1``
   * - ``extraSecrets``
     - Extra Secrets that will be managed by the chart
     - ``1``
   * - ``extraConfigMaps``
     - Extra ConfigMaps that will be managed by the chart
     - ``1``
   * - ``data.metadataSecretName``
     - Secret name to mount Airflow connection string from
     - ``1``
   * - ``data.resultBackendSecretName``
     - Secret name to mount Celery result backend connection string from
     - ``1``
   * - ``data.brokerUrlSecretName``
     - Secret name to mount redis connection url string from
     - ``1``
   * - ``data.metadataConection``
     - Field separated connection data (alternative to secret name)
     - ``1``
   * - ``data.resultBackendConnection``
     - Field separated connection data (alternative to secret name)
     - ``1``
   * - ``data.brokerUrl``
     - String containing the redis broker url (if you are using an "external" redis)
     - ``1``
   * - ``fernetKey``
     - String representing an Airflow Fernet key
     - ``1``
   * - ``fernetKeySecretName``
     - Secret name for Airflow Fernet key
     - ``1``
   * - ``kerberos.enabled``
     - Enable kerberos support for workers
     - ``1``
   * - ``kerberos.ccacheMountPath``
     - Location of the ccache volume
     - ``1``
   * - ``kerberos.ccacheFileName``
     - Name of the ccache file
     - ``1``
   * - ``kerberos.configPath``
     - Path for the Kerberos config file
     - ``1``
   * - ``kerberos.keytabPath``
     - Path for the Kerberos keytab file
     - ``1``
   * - ``kerberos.principal``
     - Name of the Kerberos principal
     - ``1``
   * - ``kerberos.reinitFrequency``
     - Frequency of reinitialization of the Kerberos token
     - ``1``
   * - ``kerberos.config``
     - Content of the configuration file for kerberos (might be templated using Helm templates)
     - ``1``
   * - ``workers.replicas``
     - Replica count for Celery workers (if applicable)
     - ``1``
   * - ``workers.keda.enabled``
     - Enable KEDA autoscaling features
     - ``1``
   * - ``workers.keda.pollingInverval``
     - How often KEDA should poll the backend database for metrics in seconds
     - ``1``
   * - ``workers.keda.cooldownPeriod``
     - How often KEDA should wait before scaling down in seconds
     - ``1``
   * - ``workers.keda.maxReplicaCount``
     - Maximum number of Celery workers KEDA can scale to
     - ``1``
   * - ``workers.kerberosSidecar.enabled``
     - Enable Kerberos sidecar for the worker
     - ``1``
   * - ``workers.kerberosSidecar.resources.limits.cpu``
     - CPU Limit of Kerberos sidecar for the worker
     - ``1``
   * - ``workers.kerberosSidecar.resources.limits.memory``
     - Memory Limit of Kerberos sidecar for the worker
     - ``1``
   * - ``workers.kerberosSidecar.resources.requests.cpu``
     - CPU Request of Kerberos sidecar for the worker
     - ``1``
   * - ``workers.kerberosSidecar.resources.requests.memory``
     - Memory Request of Kerberos sidecar for the worker
     - ``1``
   * - ``workers.persistence.enabled``
     - Enable log persistence in workers via StatefulSet
     - ``1``
   * - ``workers.persistence.size``
     - Size of worker volumes if enabled
     - ``1``
   * - ``workers.persistence.storageClassName``
     - Storage class worker volumes should use if enabled
     - ``1``
   * - ``workers.resources.limits.cpu``
     - CPU Limit of workers
     - ``1``
   * - ``workers.resources.limits.memory``
     - Memory Limit of workers
     - ``1``
   * - ``workers.resources.requests.cpu``
     - CPU Request of workers
     - ``1``
   * - ``workers.resources.requests.memory``
     - Memory Request of workers
     - ``1``
   * - ``workers.terminationGracePeriodSeconds``
     - How long Kubernetes should wait for Celery workers to gracefully drain before force killing
     - ``1``
   * - ``workers.safeToEvict``
     - Allow Kubernetes to evict worker pods if needed (node downscaling)
     - ``1``
   * - ``workers.serviceAccount.create``
     - Create ServiceAccount for workers
     - ``true``
   * - ``workers.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``workers.serviceAccount.annotations``
     - Annotations to add to worker kubernetes service account
     - ``{}``
   * - ``workers.extraVolumes``
     - Mount additional volumes into worker
     - ``1``
   * - ``workers.extraVolumeMounts``
     - Mount additional volumes into worker
     - ``1``
   * - ``workers.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``workers.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``workers.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``workers.hostAliases``
     - HostAliases to use in Celery workers
     - ``[]``
   * - ``workers.updateStrategy``
     - The strategy used to replace old Pods by new ones persistence is enabled.
     - ``~``
   * - ``workers.strategy``
     - The strategy used to replace old Pods by new ones when persistence is not enabled.
     - ``{"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}``
   * - ``scheduler.podDisruptionBudget.enabled``
     - Enable PDB on Airflow scheduler
     - ``1``
   * - ``scheduler.podDisruptionBudget.config.maxUnavailable``
     - MaxUnavailable pods for scheduler
     - ``1``
   * - ``scheduler.replicas``
     - # of parallel schedulers (Airflow 2.0 using Mysql 8+ or Postgres only)
     - ``1``
   * - ``scheduler.resources.limits.cpu``
     - CPU Limit of scheduler
     - ``1``
   * - ``scheduler.resources.limits.memory``
     - Memory Limit of scheduler
     - ``1``
   * - ``scheduler.resources.requests.cpu``
     - CPU Request of scheduler
     - ``1``
   * - ``scheduler.resources.requests.memory``
     - Memory Request of scheduler
     - ``1``
   * - ``scheduler.airflowLocalSettings``
     - Custom Airflow local settings python file
     - ``1``
   * - ``scheduler.safeToEvict``
     - Allow Kubernetes to evict scheduler pods if needed (node downscaling)
     - ``1``
   * - ``scheduler.serviceAccount.create``
     - Create ServiceAccount for scheduler
     - ``true``
   * - ``scheduler.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``scheduler.serviceAccount.annotations``
     - Annotations to add to scheduler kubernetes service account
     - ``{}``
   * - ``scheduler.extraVolumes``
     - Mount additional volumes into scheduler
     - ``1``
   * - ``scheduler.extraVolumeMounts``
     - Mount additional volumes into scheduler
     - ``1``
   * - ``scheduler.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``scheduler.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``scheduler.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``webserver.livenessProbe.initialDelaySeconds``
     - Webserver LivenessProbe initial delay
     - ``1``
   * - ``webserver.livenessProbe.timeoutSeconds``
     - Webserver LivenessProbe timeout seconds
     - ``1``
   * - ``webserver.livenessProbe.failureThreshold``
     - Webserver LivenessProbe failure threshold
     - ``1``
   * - ``webserver.livenessProbe.periodSeconds``
     - Webserver LivenessProbe period seconds
     - ``1``
   * - ``webserver.readinessProbe.initialDelaySeconds``
     - Webserver ReadinessProbe initial delay
     - ``1``
   * - ``webserver.readinessProbe.timeoutSeconds``
     - Webserver ReadinessProbe timeout seconds
     - ``1``
   * - ``webserver.readinessProbe.failureThreshold``
     - Webserver ReadinessProbe failure threshold
     - ``1``
   * - ``webserver.readinessProbe.periodSeconds``
     - Webserver ReadinessProbe period seconds
     - ``1``
   * - ``webserver.replicas``
     - How many Airflow webserver replicas should run
     - ``1``
   * - ``webserver.resources.limits.cpu``
     - CPU Limit of webserver
     - ``1``
   * - ``webserver.resources.limits.memory``
     - Memory Limit of webserver
     - ``1``
   * - ``webserver.resources.requests.cpu``
     - CPU Request of webserver
     - ``1``
   * - ``webserver.resources.requests.memory``
     - Memory Request of webserver
     - ``1``
   * - ``webserver.service.annotations``
     - Annotations to be added to the webserver service
     - ``1``
   * - ``webserver.defaultUser``
     - Optional default airflow user information
     - ``1``
   * - ``webserver.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``webserver.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``webserver.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``webserver.serviceAccount.create``
     - Create ServiceAccount for webserver
     - ``true``
   * - ``webserver.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``webserver.serviceAccount.annotations``
     - Annotations to add to webserver kubernetes service account
     - ``{}``
   * - ``flower.enabled``
     - Enable flower
     - ``1``
   * - ``flower.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``flower.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``flower.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``flower.serviceAccount.create``
     - Create ServiceAccount for flower
     - ``true``
   * - ``flower.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``flower.serviceAccount.annotations``
     - Annotations to add to flower kubernetes service account
     - ``{}``
   * - ``statsd.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``statsd.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``statsd.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``statsd.extraMappings``
     - Additional mappings for statsd exporter
     - ``1``
   * - ``statsd.serviceAccount.create``
     - Create ServiceAccount for statsd
     - ``true``
   * - ``statsd.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``statsd.serviceAccount.annotations``
     - Annotations to add to statsd kubernetes service account
     - ``{}``
   * - ``pgbouncer.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``pgbouncer.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``pgbouncer.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``pgbouncer.configSecretName``
     - Name of existing PgBouncer config secret
     - ``~``
   * - ``pgbouncer.serviceAccount.create``
     - Create ServiceAccount for PgBouncer
     - ``true``
   * - ``pgbouncer.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``pgbouncer.serviceAccount.annotations``
     - Annotations to add to PgBouncer kubernetes service account
     - ``{}``
   * - ``redis.enabled``
     - Enable the redis provisioned by the chart
     - ``1``
   * - ``redis.terminationGracePeriodSeconds``
     - Grace period for tasks to finish after SIGTERM is sent from Kubernetes.
     - ``1``
   * - ``redis.persistence.enabled``
     - Enable persistent volumes.
     - ``1``
   * - ``redis.persistence.size``
     - Volume size for redis StatefulSet.
     - ``1Gi``
   * - ``redis.persistence.storageClassName``
     - If using a custom storage class, pass name ref to all StatefulSets here.
     - ``1``
   * - ``redis.resources.limits.cpu``
     - CPU Limit of redis
     - ``1``
   * - ``redis.resources.limits.memory``
     - Memory Limit of redis
     - ``1``
   * - ``redis.resources.requests.cpu``
     - CPU Request of redis
     - ``1``
   * - ``redis.resources.requests.memory``
     - Memory Request of redis
     - ``1``
   * - ``redis.passwordSecretName``
     - Redis password secret.
     - ``1``
   * - ``redis.password``
     - If password is set, create secret with it, else generate a new one on install.
     - ``1``
   * - ``redis.safeToEvict``
     - This setting tells Kubernetes that its ok to evict when it wants to scale a node down.
     - ``1``
   * - ``redis.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``redis.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``redis.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``redis.serviceAccount.create``
     - Create ServiceAccount for redis
     - ``true``
   * - ``redis.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``redis.serviceAccount.annotations``
     - Annotations to add to redis kubernetes service account
     - ``{}``
   * - ``cleanup.nodeSelector``
     - Node labels for pod assignment
     - ``1``
   * - ``cleanup.affinity``
     - Affinity labels for pod assignment
     - ``1``
   * - ``cleanup.tolerations``
     - Toleration labels for pod assignment
     - ``1``
   * - ``cleanup.serviceAccount.create``
     - Create ServiceAccount for cleanup pods
     - ``true``
   * - ``cleanup.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``cleanup.serviceAccount.annotations``
     - Annotations to add to cleanup cronjob kubernetes service account
     - ``{}``
   * - ``createUserJob.serviceAccount.create``
     - Create ServiceAccount for create user job
     - ``true``
   * - ``createUserJob.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``createUserJob.serviceAccount.annotations``
     - Annotations to add to ``createUserJob`` kubernetes service account
     - ``{}``
   * - ``migrateDatabaseJob.serviceAccount.create``
     - Create ServiceAccount for migrate database job
     - ``true``
   * - ``migrateDatabaseJob.serviceAccount.name``
     - Name of ServiceAccount. If not set and create is true, a name is generated using the release name.
     - ``~``
   * - ``migrateDatabaseJob.serviceAccount.annotations``
     - Annotations to add to ``migrateDatabaseJob`` kubernetes service account
     - ``{}``
   * - ``dags.persistence.*``
     - Dag persistence configuration
     - Please refer to ``values.yaml``
   * - ``dags.gitSync.*``
     - Git sync configuration
     - Please refer to ``values.yaml``
   * - ``logs.persistence.*``
     - Log persistence configuration
     - Please refer to ``values.yaml``
   * - ``multiNamespaceMode``
     - Whether the KubernetesExecutor can launch pods in multiple namespaces
     - ``1``




Specify each parameter using the ``--set key=value[,key=value]`` argument to ``helm install``. For example,

.. code-block:: bash

  helm install --name my-release \
    --set executor=CeleryExecutor \
    --set enablePodLaunching=false .
