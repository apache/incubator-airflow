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

Email Configuration
===================

You can configure the email that is being sent in your ``airflow.cfg``
by setting a ``subject_template`` and/or a ``html_content_template``
in the ``[email]`` section.

.. code-block:: ini

  [email]
  email_backend = airflow.utils.email.send_email_smtp
  subject_template = /path/to/my_subject_template_file
  html_content_template = /path/to/my_html_content_template_file

To configure SMTP credentials, create a connection called ``smtp_default``, or
choose a custom connection name and set it in ``email_conn_id``.

If you want to check which email backend is currently set, you can use ``airflow config get-value email email_backend`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value email email_backend
    airflow.utils.email.send_email_smtp

To access the task's information you use `Jinja Templating <http://jinja.pocoo.org/docs/dev/>`_  in your template files.

For example a ``html_content_template`` file could look like this:

.. code-block::

  Try {{try_number}} out of {{max_tries + 1}}<br>
  Exception:<br>{{exception_html}}<br>
  Log: <a href="{{ti.log_url}}">Link</a><br>
  Host: {{ti.hostname}}<br>
  Log file: {{ti.log_filepath}}<br>
  Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>

.. note::
    For more information on setting the configuration, see :doc:`set-config`

.. _email-configuration-sendgrid:

Send email using SendGrid
-------------------------

Airflow can be configured to send e-mail using `SendGrid <https://sendgrid.com/>`__.

Follow the steps below to enable it:

1. Include ``sendgrid`` subpackage as part of your Airflow installation, e.g.,

  .. code-block:: ini

     pip install 'apache-airflow[sendgrid]'

2. Update ``email_backend`` property in ``[email]`` section in ``airflow.cfg``, i.e.

   .. code-block:: ini

      [email]
      email_backend = airflow.providers.sendgrid.utils.emailer.send_email
      email_conn_id = sendgrid_default

3. Create a connection called ``sendgrid_default``, or choose a custom connection
   name and set it in ``email_conn_id``.

.. _email-configuration-ses:

Send email using AWS SES
------------------------

Airflow can be configured to send e-mail using `AWS SES <https://aws.amazon.com/ses/>`__.

Follow the steps below to enable it:

1. Include ``amazon`` subpackage as part of your Airflow installation:

  .. code-block:: ini

     pip install 'apache-airflow[amazon]'

2. Update ``email_backend`` property in ``[email]`` section in ``airflow.cfg``:

   .. code-block:: ini

      [email]
      email_backend = airflow.providers.amazon.aws.utils.emailer.send_email
      email_conn_id = aws_default

3. Create a connection called ``aws_default``, or choose a custom connection
   name and set it in ``email_conn_id``.
