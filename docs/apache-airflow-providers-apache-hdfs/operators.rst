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



Apache Hadoop HDFS Operators
============================


`Apache Hadoop HDFS <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__ is a distributed file system
designed to run on commodity hardware. It has many similarities with existing distributed file systems.
However, the differences from other distributed file systems are significant.
HDFS is highly fault-tolerant and is designed to be deployed on low-cost hardware.
HDFS provides high throughput access to application data and is suitable for applications that have
large data sets. HDFS relaxes a few POSIX requirements to enable streaming access to file
system data. HDFS is now an Apache Hadoop sub project.

.. contents::
  :depth: 1
  :local:

Prerequisite
------------

To use operators, you must configure a :doc:`HDFS Connection <connections>`.

.. _howto/operator:HdfsFolderSensor:

HdfsFolderSensor
----------------
Waits for a non-empty directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hdfs.sensors.hdfs.HdfsFolderSensor` operator is used to
check for a non-empty directory in HDFS.

Use the ``filepath`` parameter to poke until the provided file is found.

.. _howto/operator:HdfsRegexSensor:

HdfsRegexSensor
---------------
Waits for matching files by matching on regex
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hdfs.sensors.hdfs.HdfsRegexSensor` operator is used to check for
matching files by matching on regex in HDFS.

Use the ``filepath`` parameter to mention the keyspace and table for the record. Use dot notation to target a
specific keyspace.


.. _howto/operator:HdfsSensor:

Waits for a file or folder to land in HDFS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hdfs.sensors.hdfs.HdfsSensor` operator is used to check for a file or folder to land in HDFS.

Use the ``filepath`` parameter to poke until the provided file is found.

Reference
^^^^^^^^^

For further information, look at `HDFS Architecture Guide  <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`_.
