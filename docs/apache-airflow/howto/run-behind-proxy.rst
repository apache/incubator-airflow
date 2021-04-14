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



Running Airflow behind a reverse proxy
======================================

Airflow can be set up behind a reverse proxy, with the ability to set its endpoint with great
flexibility.

For example, you can configure your reverse proxy to get:

::

    https://lab.mycompany.com/myorg/airflow/

To do so, you need to set the following setting in your ``airflow.cfg``::

    base_url = http://my_host/myorg/airflow

Additionally if you use Celery Executor, you can get Flower in ``/myorg/flower`` with::

    flower_url_prefix = /myorg/flower

Your reverse proxy (ex: nginx) should be configured as follow:

- pass the url and http header as it for the Airflow webserver, without any rewrite, for example::

      server {
        listen 80;
        server_name lab.mycompany.com;

        location /myorg/airflow/ {
            proxy_pass http://localhost:8080;
            proxy_set_header Host $http_host;
            proxy_redirect off;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
      }

- rewrite the url for the flower endpoint::

      server {
          listen 80;
          server_name lab.mycompany.com;

          location /myorg/flower/ {
              rewrite ^/myorg/flower/(.*)$ /$1 break;  # remove prefix from http header
              proxy_pass http://localhost:5555;
              proxy_set_header Host $http_host;
              proxy_redirect off;
              proxy_http_version 1.1;
              proxy_set_header Upgrade $http_upgrade;
              proxy_set_header Connection "upgrade";
          }
      }

To ensure that Airflow generates URLs with the correct scheme when
running behind a TLS-terminating proxy, you should configure the proxy
to set the ``X-Forwarded-Proto`` header, and enable the ``ProxyFix``
middleware in your ``airflow.cfg``::

    [webserver]
    enable_proxy_fix = True

If you need to configure the individual parameters to the ``ProxyFix`` middleware,
you can set them individually in your ``airflow.cfg``::

    [webserver]
    proxy_fix_x_for = 1
    proxy_fix_x_host = 3

.. note::
    You should only enable the ``ProxyFix`` middleware when running
    Airflow behind a trusted proxy (AWS ELB, nginx, etc.).
