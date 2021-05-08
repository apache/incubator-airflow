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

###########################
KubernetesPodOperator Guide
###########################

.. contents:: :local:



KubernetesPodOperator with YAML file / JSON Spec
================================================

Below is an example of a basic pod_template_file for a KubernetesPodOperator pod. Please note that the first container is named base, as this is required for Airflow to later run the pod

.. code-block:: yaml

  apiVersion: v1
  kind: Pod
  metadata:
    labels:
      app: myapp
    name: myapp-pod
    namespace: default
  spec:
    containers:
      - name: base
        image: alpine
        env:
          - name: TIME_OUT
            value: '15'
          - name: ENV_TYPE
            value: 'test'
        envFrom:
          - secretRef:
              name: db-credentials
        volumeMounts:
          - name: myapp-volume
            mountPath: /root/myapp
        resources:
          limits:
            cpu: 2
            memory: "200Mi"
          requests:
            cpu: 1
            memory: "100Mi"
        command: ['sh', '-c', 'echo "myapp-pod created from YAML pod template."']
    volumes:
      - name: myapp-volume
        persistentVolumeClaim:
          claimName: myapp-pvc-rw


Once you have created a pod_template_file, you can use this file as a basis for your KPO pod, while using the other supplied arguments as "overrides". In this example,, we are able to take this template and modify the namespace and name of the pod.

.. code-block:: python

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_yaml_config", start_date=days_ago(1),
           schedule_interval='@once', tags=["example"]) as dag:

      pod_template_path = '/abs/path/to/file/k8s_pod_template.yaml'
      task1 = KubernetesPodOperator(task_id='k8s_pod_yaml_config_task',
                                    pod_template_file=pod_template_path,
                                    namespace='default',
                                    name='airflow_pod_yaml_config',
                                    startup_timeout_seconds=60,)


- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_yaml_config
  AIRFLOW_CTX_TASK_ID=k8s_pod_yaml_config_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-05T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-05T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: myapp-pod had an event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: myapp-pod
  {pod_launcher.py:176} INFO - Event: myapp-pod had an event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id myapp-pod Succeeded

  {pod_launcher.py:136} INFO - myapp-pod created from YAML pod template.

  {taskinstance.py:1136} INFO - Marking task as SUCCESS. dag_id=example_k8s_yaml_config, task_id=
  k8s_pod_yaml_config_task, execution_date=20201205T000000, start_date=20201206T130803, end_date=20201206T130818
  {taskinstance.py:1195} INFO - 0 downstream tasks scheduled from follow-on schedule check
  {dagrun.py:447} INFO - Marking run <DagRun example_k8s_yaml_config @ 2020-12-05 00:00:00+00:00:
  backfill__2020-12-05T00:00:00+00:00, externally triggered: False> successful
  {backfill_job.py:377} INFO - [backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 1 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 0
  {backfill_job.py:830} INFO - Backfill done. Exiting.


If you need to examine a pod that is not running correctly, run the command ``kubectl describe pod <my pod name> to get all relevant details about the pod from the k8s cluster.

.. code-block:: bash

  $ kubectl describe pod myapp-pod

  Name:         myapp-pod
  Namespace:    default
  Priority:     0
  Node:         minikube/192.168.49.2
  Start Time:   Sun, 06 Dec 2020 18:38:08 +0530
  Labels:       app=myapp
  Annotations:  <none>
  Status:       Succeeded
  IP:           172.17.0.7
  IPs:
    IP:  172.17.0.7
  Containers:
    base:
      Container ID:  docker://41a9d68a3f7d8c74c356f6c46d1fe09924d463e2ac0c7161c06d256374478546
      Image:         alpine
      Image ID:      docker-pullable://alpine@sha256:c0e9560cda118f9ec63ddefb4a173a2b2a0347082d7dff7dc14272e7841a5b5a
      Port:          <none>
      Host Port:     <none>
      Command:
        sh
        -c
        echo "myapp-pod created from YAML pod template."
      State:          Terminated
        Reason:       Completed
        Exit Code:    0
        Started:      Sun, 06 Dec 2020 18:38:15 +0530
        Finished:     Sun, 06 Dec 2020 18:38:15 +0530
      Ready:          False
      Restart Count:  0
      Limits:
        cpu:     2
        memory:  200Mi
      Requests:
        cpu:     1
        memory:  100Mi
      Environment Variables from:
        db-credentials  Secret  Optional: false
      Environment:
        TIME_OUT:  15
        ENV_TYPE:  test
      Mounts:
        /root/myapp from myapp-volume (rw)
        /var/run/secrets/kubernetes.io/serviceaccount from default-token-ltgdm (ro)
  Conditions:
    Type              Status
    Initialized       True
    Ready             False
    ContainersReady   False
    PodScheduled      True
  Volumes:
    myapp-volume:
      Type:       PersistentVolumeClaim (a reference to a PersistentVolumeClaim in the same namespace)
      ClaimName:  myapp-pvc-rw
      ReadOnly:   false
    default-token-ltgdm:
      Type:        Secret (a volume populated by a Secret)
      SecretName:  default-token-ltgdm
      Optional:    false
  QoS Class:       Burstable
  Node-Selectors:  <none>
  Tolerations:     node.kubernetes.io/not-ready:NoExecute op=Exists for 300s
                   node.kubernetes.io/unreachable:NoExecute op=Exists for 300s
  Events:
    Type    Reason     Age    From               Message
    ----    ------     ----   ----               -------
    Normal  Scheduled  4m53s  default-scheduler  Successfully assigned default/myapp-pod to minikube
    Normal  Pulling    4m53s  kubelet            Pulling image "alpine"
    Normal  Pulled     4m47s  kubelet            Successfully pulled image "alpine" in 5.837110465s
    Normal  Created    4m47s  kubelet            Created container base
    Normal  Started    4m47s  kubelet            Started container base



.. _howto/operator:KubernetesPodOperator:

Kubernetes Pod Operator "Hello World!"
======================================


How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` uses the
Kubernetes API to launch a pod in a Kubernetes cluster. By supplying an
image URL and a command with optional arguments, the operator uses the Kube Python Client to generate a Kubernetes API
request that dynamically launches those individual pods.
Users can specify a kubeconfig file using the ``config_file`` parameter, otherwise the operator will default
to ``~/.kube/config``.

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` enables task-level
resource configuration and is optimal for custom Python
dependencies that are not available through the public PyPI repository. It also allows users to supply a template
YAML file using the ``pod_template_file`` parameter.
Ultimately, it allows Airflow to act a job orchestrator - no matter the language those jobs are written in.


.. code-block:: python

  with DAG(dag_id="example_k8s_operator", start_date=days_ago(1),
           schedule_interval='@once', tags=["example"]) as dag:
      task1 = KubernetesPodOperator(task_id='k8s_pod_operator_task',
                                    name='airflow_pod_operator',
                                    namespace='default',
                                    image='alpine',
                                    cmds=["sh", "-c",
                                          'echo "Hello World from pod [$HOSTNAME]"'],
                                    startup_timeout_seconds=60,
                                    )

- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_operator
  AIRFLOW_CTX_TASK_ID=k8s_pod_operator_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-03T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-03T00:00:00+00:00

  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c had an event
  of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c had an event of type
  Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c Succeeded

  {pod_launcher.py:136} INFO - Hello World from pod [airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c]

  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c had an event of
  type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c Succeeded
  {taskinstance.py:1136} INFO - Marking task as SUCCESS. dag_id=example_k8s_operator, task_id=k8s_pod_operator_task,
  execution_date=20201203T000000, start_date=20201204T140331, end_date=20201204T140345
  ................................................................................................................

Another option for getting the pod launched by Airflow is to take advantage of the labels that Airflow injects into every KubernetesPodOperator pod. Using the line below, you can quickly get the name of the pod launched by Airflow.

``kubectl get pods -l dag_id=<your dag id>,task_id=<your task id>``

.. code-block:: bash

  $ kubectl get pods -l dag_id=example_k8s_operator,task_id=k8s_pod_operator_task

    NAME                                                    READY   STATUS      RESTARTS   AGE
    airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c   0/1     Completed   0          14m

  $ kubectl describe pod airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c

    Name:         airflow-pod-operator-aed97ecd64854367ad7d0ff39f37859c
    Namespace:    default
    Priority:     0
    Node:         minikube/192.168.49.2
    Start Time:   Fri, 04 Dec 2020 19:33:36 +0530
    Labels:       airflow_version=2.0.0b2
                  dag_id=example_k8s_operator
                  execution_date=2020-12-03T0000000000-767fcb862
                  kubernetes_pod_operator=True
                  task_id=k8s_pod_operator_task
                  try_number=1
    Annotations:  <none>
    Status:       Succeeded
    IP:           172.17.0.7
    IPs:
      IP:  172.17.0.7
    Containers:
      base:
        Container ID:  docker://56c91324dc925b0bad0d60474e35d8c7eb7fad7d8410ca123b657f1416207504
        Image:         alpine
        Image ID:      docker-pullable://alpine@sha256:c0e9560cda118f9ec63ddefb4a173a2b2a0347082d7dff7dc14272e7841a5b5a
        Port:          <none>
        Host Port:     <none>
        Command:
          sh
          -c
          echo "Hello World from pod [$HOSTNAME]"
        State:          Terminated
          Reason:       Completed
          Exit Code:    0
          Started:      Fri, 04 Dec 2020 19:33:43 +0530
          Finished:     Fri, 04 Dec 2020 19:33:43 +0530
        Ready:          False
        Restart Count:  0
        Environment:    <none>
        Mounts:
          /var/run/secrets/kubernetes.io/serviceaccount from default-token-ltgdm (ro)
    Conditions:
      Type              Status
      Initialized       True
      Ready             False
      ContainersReady   False
      PodScheduled      True
    Volumes:
      default-token-ltgdm:
        Type:        Secret (a volume populated by a Secret)
        SecretName:  default-token-ltgdm
        Optional:    false
    QoS Class:       BestEffort
    Node-Selectors:  <none>
    Tolerations:     node.kubernetes.io/not-ready:NoExecute op=Exists for 300s
                     node.kubernetes.io/unreachable:NoExecute op=Exists for 300s
    Events:
      Type    Reason     Age   From               Message
      ----    ------     ----  ----               -------
      Normal  Scheduled  15m   default-scheduler  Successfully assigned default/airflow-pod-operator
                                                  -aed97ecd64854367ad7d0ff39f37859c to minikube
      Normal  Pulling    15m   kubelet            Pulling image "alpine"
      Normal  Pulled     15m   kubelet            Successfully pulled image "alpine" in 4.214686688s
      Normal  Created    15m   kubelet            Created container base
      Normal  Started    15m   kubelet            Started container base


Difference between ``KubernetesPodOperator`` and Kubernetes object spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` can be considered
a substitute for a Kubernetes object spec definition that is able
to be run in the Airflow scheduler in the DAG context. If using the operator, there is no need to create the
equivalent YAML/JSON object spec for the Pod you would like to run.
The YAML file can still be provided with the ``pod_template_file`` or even the Pod Spec constructed in Python via
the ``full_pod_spec`` parameter which requires a Kubernetes ``V1Pod``.


Defining Environment Variables for Pod
======================================


- Creating Task using KubernetesPodOperator with given environment variables.

.. code-block:: python

  from kubernetes.client import V1EnvVar

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_operator_env", start_date=days_ago(1), schedule_interval='@once',
           tags=["example"]) as dag:

      env_vars = [V1EnvVar(name='TIME_OUT', value='5'), V1EnvVar(name='ENV_TYPE', value='test')]

      task1 = KubernetesPodOperator(task_id='k8s_pod_operator_env_task',
                                    name='airflow_pod_operator_env',
                                    namespace='default',
                                    env_vars=env_vars,
                                    image='alpine',
                                    cmds=["sh", "-c",
                                          'echo "Reading environment variables TIME_OUT : $TIME_OUT   ENV_TYPE :'
                                          ' $ENV_TYPE"'],
                                    startup_timeout_seconds=60,
                                    )


- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_operator_env
  AIRFLOW_CTX_TASK_ID=k8s_pod_operator_env_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-03T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-03T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 had an
  event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-operator-env-3824c08cb2f04af7928103a027189668
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 had an event
  of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 Succeeded

  {pod_launcher.py:136} INFO - Reading environment variables TIME_OUT : 5   ENV_TYPE : test

  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 had an
  event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 Succeeded
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 had an event
  of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-env-3824c08cb2f04af7928103a027189668 Succeeded
  ...................................................................................................................



Accessing ConfigMap
===========================


- YAML file for creating ConfigMap in Kubernetes

.. code-block:: yaml

  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: myapp-config
  data:
    TIME_OUT: "15"
    ENV_TYPE: "test"



- Creating ConfigMap using ``kubectl`` command

.. code-block:: bash

  $ kubectl apply -f k8s_configmap.yaml
    configmap/myapp-config created

  $ kubectl describe configmaps myapp-config
    Name:         myapp-config
    Namespace:    default
    Labels:       <none>
    Annotations:  <none>

    Data
    ====
    TIME_OUT:
    ----
    15
    ENV_TYPE:
    ----
    test
    Events:  <none>



- Accessing variables from ConfigMap inside the Pod

.. code-block:: python

  from kubernetes.client import V1ConfigMapEnvSource, V1EnvFromSource

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_config_map", start_date=days_ago(1),
          schedule_interval='@once', tags=["example"]) as dag:

      config_map = [V1EnvFromSource(config_map_ref=V1ConfigMapEnvSource(name='myapp-config')), ]

      task1 = KubernetesPodOperator(task_id='k8s_pod_operator_config_map_task',
                                    name='airflow_pod_operator_config_map',
                                    namespace='default',
                                    image='alpine',
                                    env_from=config_map,
                                    cmds=["sh", "-c",
                                          'echo "Reading environment variables TIME_OUT : $TIME_OUT   ENV_TYPE :'
                                          ' $ENV_TYPE"'],
                                    startup_timeout_seconds=60,
                                    )


- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_config_map
  AIRFLOW_CTX_TASK_ID=k8s_pod_operator_config_map_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-04T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-04T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-config-map-d472d9495b0741bc84e34c11d09c66fe had
  an event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-operator-config-map-d472d9495b0741bc84e34c11d09c66fe
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-config-map-d472d9495b0741bc84e34c11d09c66fe had an
  event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-config-map-d472d9495b0741bc84e34c11d09c66fe Succeeded

  {pod_launcher.py:136} INFO - Reading environment variables TIME_OUT : 15   ENV_TYPE : test

  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-config-map-d472d9495b0741bc84e34c11d09c66fe had an
  event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-config-map-d472d9495b0741bc84e34c11d09c66fe Succeeded



Accessing Secrets
=================


- Creating Secrets

.. code-block:: bash

  $ echo -n 'root' > DB_USER
  $ echo -n 'ent3r$ce@d00r' > DB_PWD

  $ kubectl create secret generic db-credentials --from-file=DB_USER  --from-file=DB_PWD

  $ kubectl describe secrets db-credentials

    Name:         db-credentials
    Namespace:    default
    Labels:       <none>
    Annotations:  <none>

    Type:  Opaque

    Data
    ====
    DB_PWD:   13 bytes
    DB_USER:  4 bytes


- Accessing secret inside pod as environment variable

.. code-block:: python

  from kubernetes.client import V1EnvFromSource, V1SecretEnvSource

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_secret", start_date=days_ago(1), schedule_interval='@once', tags=["example"]) as dag:

      db_credentials = [V1EnvFromSource(secret_ref=V1SecretEnvSource(name='db-credentials')), ]

      task1 = KubernetesPodOperator(task_id='k8s_pod_operator_secret_task',
                                    name='airflow_pod_operator_secret',
                                    namespace='default',
                                    image='alpine',
                                    env_from=db_credentials,
                                    cmds=["sh", "-c",
                                          'echo "Reading environment variables DB_USER: : $DB_USER:   DB_PWD :'
                                          ' $DB_PWD"'],
                                    startup_timeout_seconds=60,
                                    )


- After executing / debugging example and checking the logs

.. code-block:: bash

   {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_secret
  AIRFLOW_CTX_TASK_ID=k8s_pod_operator_secret_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-04T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-04T00:00:00+00:00

  {pod_launcher.py:136} INFO - Reading environment variables DB_USER: : root:   DB_PWD : ent3r$ce@d00r

  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-secret-3f7d6b3e5dcf4673aa1f584e26f1d012 had an
  event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-secret-3f7d6b3e5dcf4673aa1f584e26f1d012 Succeeded



Mounting Persistent  Volume to Pod
==================================

- Creating PersistentVolume


.. code-block:: yaml

  apiVersion: v1
  kind: PersistentVolume
  metadata:
    name: myapp-pv
  spec:
    capacity:
      storage: 20Mi
    accessModes:
      - ReadWriteMany
    persistentVolumeReclaimPolicy: Retain
    hostPath:
      path: /tmp/myapp


.. code-block:: bash

  $ kubectl apply -f myapp_pv.yaml


- Creating PersistentVolumeClaim

.. code-block:: yaml

  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: myapp-pvc-rw
  spec:
    resources:
      requests:
        storage: 20Mi
    accessModes:
    - ReadWriteMany
    storageClassName: ""


.. code-block:: bash

  $ kubectl apply -f myapp_pvc.yaml


- Writing and Reading file from Persistent Volume using KubernetesPodOperator

.. code-block:: python

  from kubernetes.client import V1VolumeMount, V1Volume, V1PersistentVolumeClaimVolumeSource

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_volume", start_date=days_ago(1),
           schedule_interval='@once', tags=["example"]) as dag:
      myapp_volume = V1Volume(
          name='myapp-volume',
          persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name='myapp-pvc-rw'))

      myapp_volume_mount = V1VolumeMount(mount_path='/root/myapp', name='myapp-volume')

      task1 = KubernetesPodOperator(task_id='k8s_volume_read_task',
                                    name='airflow_pod_volume_read',
                                    namespace='default',
                                    image='alpine',
                                    volumes=[myapp_volume, ],
                                    volume_mounts=[myapp_volume_mount, ],
                                    cmds=["sh", "-c",
                                          'date > /root/myapp/date.txt',
                                          ],
                                    startup_timeout_seconds=60,
                                    )

      task2 = KubernetesPodOperator(task_id='k8s_volume_write_task',
                                    name='airflow_pod_volume_write',
                                    namespace='default',
                                    image='alpine',
                                    volumes=[myapp_volume, ],
                                    volume_mounts=[myapp_volume_mount, ],
                                    cmds=["sh", "-c",
                                          'echo "Reading date from date.txt : "$(cat /root/myapp/date.txt)',
                                          ],
                                    startup_timeout_seconds=60,
                                    )

      task1 >> task2



- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_volume
  AIRFLOW_CTX_TASK_ID=k8s_volume_read_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-04T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-04T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-volume-read-7055ebbfe703448ba6e8ba35487265e3 had an
  event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-volume-read-7055ebbfe703448ba6e8ba35487265e3
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-volume-read-7055ebbfe703448ba6e8ba35487265e3 Succeeded
  {pod_launcher.py:176} INFO - Event: airflow-pod-volume-read-7055ebbfe703448ba6e8ba35487265e3 had an
  event of type Succeeded
  {backfill_job.py:377} INFO - [backfill progress] | finished run 0 of 1 | tasks waiting: 1 | succeeded: 1 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 1
  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_volume
  AIRFLOW_CTX_TASK_ID=k8s_volume_write_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-04T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-04T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-volume-write-23495d6738994e1d96765dfef49f345c had an
  event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-volume-write-23495d6738994e1d96765dfef49f345c
  {pod_launcher.py:176} INFO - Event: airflow-pod-volume-write-23495d6738994e1d96765dfef49f345c had an
  event of type Running

  {pod_launcher.py:136} INFO - Reading date from date.txt : Sat Dec 5 13:58:35 UTC 2020

  {pod_launcher.py:176} INFO - Event: airflow-pod-volume-write-23495d6738994e1d96765dfef49f345c had an event
  of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-volume-write-23495d6738994e1d96765dfef49f345c Succeeded
  {backfill_job.py:377} INFO - [backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 2 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 0
  {backfill_job.py:830} INFO - Backfill done. Exiting.


Mounting Secrets as Volume
==========================

- Example Dag demonstrating use of ``Secret`` class which internally configures ``Volume`` and ``VolumeMount`` for
  given secret.

.. code-block:: python

  from kubernetes.client import V1Volume, V1SecretVolumeSource, V1VolumeMount

  from airflow import DAG
  from airflow.kubernetes.secret import Secret
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_secret_volume", start_date=days_ago(1), schedule_interval='@once', tags=["example"]) as dag:

      secret = Secret('volume', '/etc/my-secret', 'db-credentials')
      # Is Equal to below two lines
      # secret_volume = V1Volume(name='my-secret-vol', secret=V1SecretVolumeSource(secret_name='db-credentials'))
      # secret_volume_mount = V1VolumeMount(mount_path='/etc/my-secret', name='my-secret-vol', read_only=True)

      task1 = KubernetesPodOperator(task_id='k8s_pod_operator_secret_volume_task',
                                    name='airflow_pod_operator_secret_volume',
                                    namespace='default',
                                    secrets=[secret, ],
                                    # secrets is equal to below two lines
                                    # volumes=[secret_volume, ],
                                    # volume_mounts=[secret_volume_mount, ],
                                    image='alpine',
                                    cmds=["sh", "-c",
                                          'echo "Secret Directory Content "$(ls -l /etc/my-secret)'],
                                    in_cluster=False,
                                    startup_timeout_seconds=60,
                                    )



- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_secret_volume
  AIRFLOW_CTX_TASK_ID=k8s_pod_operator_secret_volume_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-06T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-06T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-secret-volume-c03db098442b45f2aeb58e2dbca8e78f had
  an event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-operator-secret-volume-
  c03db098442b45f2aeb58e2dbca8e78f
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-secret-volume-c03db098442b45f2aeb58e2dbca8e78f
  had an event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-secret-volume-c03db098442b45f2aeb58e2dbca8e78f
  Succeeded

  {pod_launcher.py:136} INFO - Secret Directory Content
  total 0
  lrwxrwxrwx 1 root root 13 Dec 7 15:17 DB_PWD -> ..data/DB_PWD
  lrwxrwxrwx 1 root root 14 Dec 7 15:17 DB_USER -> ..data/DB_USER

  {taskinstance.py:1195} INFO - 0 downstream tasks scheduled from follow-on schedule check
  {dagrun.py:444} INFO - Marking run <DagRun example_k8s_secret_volume
  @ 2020-12-06 00:00:00+00:00: backfill__2020-12-06T00:00:00+00:00, externally triggered: False> successful
  {backfill_job.py:377} INFO - [backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 1 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 0
  {backfill_job.py:830} INFO - Backfill done. Exiting.



Defining Pod resource Requirements and Limits
=============================================


This example defines ResourceRequirements for Pod, which allow users to modify CPU, memory, and GPU requests and limits.


.. code-block:: python

  from kubernetes.client import V1ResourceRequirements

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_limit_resource", start_date=days_ago(1),
           schedule_interval='@once', tags=["example"]) as dag:

      resource_req = V1ResourceRequirements(
                              requests={
                                  "cpu": 1,
                                  'memory': '100Mi'
                              },
                              limits={
                                  "cpu": 2,
                                  'memory': '200Mi',
                              }
      )

      task1 = KubernetesPodOperator(task_id='k8s_pod_limit_resource_task',
                                    name='airflow_pod_limit_resource',
                                    namespace='default',
                                    image='alpine',
                                    resources=resource_req,
                                    cmds=["sh", "-c",
                                          'echo "Hello World from pod [$HOSTNAME]"'],
                                    startup_timeout_seconds=60,
                                    )


- After executing / debugging example and checking the logs

.. code-block:: bash

  INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_limit_resource
  AIRFLOW_CTX_TASK_ID=k8s_pod_limit_resource_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-05T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-05T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6 had an
  event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6
  {pod_launcher.py:176} INFO - Event: airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6 had an
  event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6 Succeeded

  {pod_launcher.py:136} INFO - Hello World from pod [airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6]

  {taskinstance.py:1136} INFO - Marking task as SUCCESS. dag_id=example_k8s_limit_resource,task_id=
  k8s_pod_limit_resource_task, execution_date=20201205T000000, start_date=20201206T035456, end_date=20201206T035517
  {backfill_job.py:377} INFO - [backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 1 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 0
  {backfill_job.py:830} INFO - Backfill done. Exiting.


- Describing Pod configuration

.. code-block:: bash

  $ kubectl describe pod airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6

    Name:         airflow-pod-limit-resource-ac4c107dd10549c89a2015f976e729d6
    Namespace:    default
    Priority:     0
    Node:         minikube/192.168.49.2
    Start Time:   Sun, 06 Dec 2020 09:25:02 +0530
    Labels:       airflow_version=2.0.0b2
                  dag_id=example_k8s_limit_resource
                  execution_date=2020-12-05T0000000000-c71846343
                  kubernetes_pod_operator=True
                  task_id=k8s_pod_limit_resource_task
                  try_number=1
    Annotations:  <none>
    Status:       Succeeded
    IP:           172.17.0.7
    IPs:
      IP:  172.17.0.7
    Containers:
      base:
        Container ID:  docker://819ae8713aefb51b4f9bbdd7567adf706ebcee402418e1c4ef358c0bab90bc8b
        Image:         alpine
        Image ID:      docker-pullable://alpine@sha256:c0e9560cda118f9ec63ddefb4a173a2b2a0347082d7dff7dc14272e7841a5b5a
        Port:          <none>
        Host Port:     <none>
        Command:
          sh
          -c
          echo "Hello World from pod [$HOSTNAME]"
        State:          Terminated
          Reason:       Completed
          Exit Code:    0
          Started:      Sun, 06 Dec 2020 09:25:14 +0530
          Finished:     Sun, 06 Dec 2020 09:25:14 +0530
        Ready:          False
        Restart Count:  0

        Limits:
          cpu:     2
          memory:  200Mi
        Requests:
          cpu:        1
          memory:     100Mi

        Environment:  <none>
        Mounts:
          /var/run/secrets/kubernetes.io/serviceaccount from default-token-ltgdm (ro)
    ..................................................................................




Using XCOM with KubernetesPodOperator
=====================================

How does XCom work?
^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` handles
XCom values differently than other operators. In order to pass a XCom value
from your Pod you must specify the ``do_xcom_push`` as ``True``. This will create a sidecar container that runs
alongside the Pod. The Pod must write the XCom value into this location at the ``/airflow/xcom/return.json`` path.

See the following example on how this occurs:


- Example Dag : KubernetesPodOperator task writes contents to be returned to ``/airflow/xcom/return.json`` and reading
  values returned using ``xcom_pull(key, task_ids)``.


.. exampleinclude:: /../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes_xcom.py
    :language: python
    :start-after: [START kubernetes_xcom]
    :end-before: [END kubernetes_xcom]


- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_operator_xcom
  AIRFLOW_CTX_TASK_ID=k8s_pod_operator_xcom_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-05T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-05T00:00:00+00:00
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-xcom-73e57ff9a14d489292eb98d84ca9d25c had
  an event of type Pending
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-operator-xcom-73e57ff9a14d489292eb98d84ca9d25c
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-xcom-73e57ff9a14d489292eb98d84ca9d25c had
  an event of type Running

  {pod_launcher.py:269} INFO - Running command... cat /airflow/xcom/return.json**

  {pod_launcher.py:269} INFO - Running command... kill -s SIGINT 1

  {pod_launcher.py:152} INFO - {"date": "Sun Dec 6 04:23:31 UTC 2020", "release": "5.4.0-56-generic"}

  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-xcom-73e57ff9a14d489292eb98d84ca9d25c
  had an event of type Running
  {taskinstance.py:1136} INFO - Marking task as SUCCESS. dag_id=example_k8s_operator_xcom,
  task_id=k8s_pod_operator_xcom_task, execution_date=20201205T000000, start_date=20201206T042318, end_date=20201206T042409
  {taskinstance.py:1195} INFO - 0 downstream tasks scheduled from follow-on schedule check
  {backfill_job.py:377} INFO - [backfill progress] | finished run 0 of 1 | tasks waiting: 1 | succeeded: 1 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 1
  {base_executor.py:79} INFO - Adding to queue: ['<TaskInstance: example_k8s_operator_xcom.python_operator_xcom
  2020-12-05 00:00:00+00:00 [queued]>']
  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_operator_xcom
  AIRFLOW_CTX_TASK_ID=python_operator_xcom
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-05T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-05T00:00:00+00:00

  Value received from k8s_pod_operator  date : Sun Dec 6 04:23:31 UTC 2020   release : 5.4.0-56-generic

  {taskinstance.py:1136} INFO - Marking task as SUCCESS. dag_id=example_k8s_operator_xcom, task_id=
  python_operator_xcom, execution_date=20201205T000000, start_date=20201206T042318, end_date=20201206T042410
  {backfill_job.py:377} INFO - [backfill progress] | finished run 1 of 1 | tasks waiting: 0 | succeeded: 2 |
  running: 0 | failed: 0 | skipped: 0 | deadlocked: 0 | not ready: 0
  {backfill_job.py:830} INFO - Backfill done. Exiting.








Accessing private docker images
================================

How to use private images (container registry)?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` will
look for images hosted publicly on Dockerhub.
To pull images from a private registry (such as ECR, GCR, Quay, or others), you must create a
Kubernetes Secret that represents the credentials for accessing images from the private registry that is ultimately
specified in the ``image_pull_secrets`` parameter.

- Login to docker and creating secret  ``regcred``

.. code-block:: bash

  $ docker login

  $ cat ~/.docker/config.json

  $ kubectl create secret generic regcred \
    --from-file=.dockerconfigjson=<abs/path/to/.docker/config.json> \
    --type=kubernetes.io/dockerconfigjson



- Example Dag for Pulling private image from docker

.. code-block:: python

  from kubernetes.client import V1LocalObjectReference

  from airflow import DAG
  from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
  from airflow.utils.dates import days_ago

  with DAG(dag_id="example_k8s_private_img", start_date=days_ago(1),
           schedule_interval='@once', tags=["example"]) as dag:
      task1 = KubernetesPodOperator(task_id='k8s_private_img_task',
                                    name='airflow_pod_operator_private_img',
                                    namespace='default',
                                    image='docker_id/my-app',
                                    image_pull_secrets=[V1LocalObjectReference('regcred')],
                                    image_pull_policy='Never',
                                    cmds=["sh", "-c",
                                          'echo "Hello World from pod [$HOSTNAME]"'],
                                    in_cluster=False,
                                    startup_timeout_seconds=60,
                                    )


- After executing / debugging example and checking the logs

.. code-block:: bash

  {taskinstance.py:1230} INFO - Exporting the following env vars:
  AIRFLOW_CTX_DAG_OWNER=airflow
  AIRFLOW_CTX_DAG_ID=example_k8s_private_img
  AIRFLOW_CTX_TASK_ID=k8s_private_img_task
  AIRFLOW_CTX_EXECUTION_DATE=2020-12-05T00:00:00+00:00
  AIRFLOW_CTX_DAG_RUN_ID=backfill__2020-12-05T00:00:00+00:00
  {pod_launcher.py:113} WARNING - Pod not yet started: airflow-pod-operator-private-img-8339c8fa8477451dad44e91fcf6f0b03
  {pod_launcher.py:176} INFO - Event: airflow-pod-operator-private-img-8339c8fa8477451dad44e91fcf6f0b03 had
  an event of type Succeeded
  {pod_launcher.py:289} INFO - Event with job id airflow-pod-operator-private-img-8339c8fa8477451dad44e91fcf6f0b03
  Succeeded

  {pod_launcher.py:136} INFO - Hello World from pod [airflow-pod-operator-private-img-8339c8fa8477451dad44e91fcf6f0b]

  {taskinstance.py:1136} INFO - Marking task as SUCCESS. dag_id=example_k8s_private_img, task_id=k8s_private_img_task,
  execution_date=20201205T000000, start_date=20201206T141046, end_date=20201206T141101
  {taskinstance.py:1195} INFO - 0 downstream tasks scheduled from follow-on schedule check
  {dagrun.py:447} INFO - Marking run <DagRun example_k8s_private_img @ 2020-12-05 00:00:00+00:00:
  backfill__2020-12-05T00:00:00+00:00, externally triggered: False> successful
