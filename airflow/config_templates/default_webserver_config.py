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
"""Default configuration for the Airflow webserver"""
import os

from flask_appbuilder.security.manager import AUTH_DB

# from flask_appbuilder.security.manager import AUTH_LDAP
# from flask_appbuilder.security.manager import AUTH_OAUTH
# from flask_appbuilder.security.manager import AUTH_OID
# from flask_appbuilder.security.manager import AUTH_REMOTE_USER


basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# ----------------------------------------------------
# AUTHENTICATION CONFIG
# ----------------------------------------------------
# For details on how to set up each of the following authentication, see
# http://flask-appbuilder.readthedocs.io/en/latest/security.html# authentication-methods
# for details.

# The authentication type
# AUTH_OID : Is for OpenID
# AUTH_DB : Is for database
# AUTH_LDAP : Is for LDAP
# AUTH_REMOTE_USER : Is for using REMOTE_USER from web server
# AUTH_OAUTH : Is for OAuth
AUTH_TYPE = AUTH_DB

# Uncomment to setup Full admin role name
# AUTH_ROLE_ADMIN = 'Admin'

# Uncomment to setup Public role name, no authentication needed
# AUTH_ROLE_PUBLIC = 'Public'

# Will allow user self registration
# AUTH_USER_REGISTRATION = True

# The recaptcha it's automatically enabled for user self registration is active and the keys are necessary
# RECAPTCHA_PRIVATE_KEY = PRIVATE_KEY
# RECAPTCHA_PUBLIC_KEY = PUBLIC_KEY

# Config for Flask-Mail necessary for user self registration
# MAIL_SERVER = 'smtp.gmail.com'
# MAIL_USE_TLS = True
# MAIL_USERNAME = 'yourappemail@gmail.com'
# MAIL_PASSWORD = 'passwordformail'
# MAIL_DEFAULT_SENDER = 'sender@gmail.com'

# The default user self registration role
# AUTH_USER_REGISTRATION_ROLE = "Public"

# When using OAuth Auth, uncomment to setup provider(s) info
# Google OAuth example:
# OAUTH_PROVIDERS = [{
#   'name':'google',
#     'token_key':'access_token',
#     'icon':'fa-google',
#         'remote_app': {
#             'api_base_url':'https://www.googleapis.com/oauth2/v2/',
#             'client_kwargs':{
#                 'scope': 'email profile'
#             },
#             'access_token_url':'https://accounts.google.com/o/oauth2/token',
#             'authorize_url':'https://accounts.google.com/o/oauth2/auth',
#             'request_token_url': None,
#             'client_id': GOOGLE_KEY,
#             'client_secret': GOOGLE_SECRET_KEY,
#         }
# }]

# When using LDAP Auth, setup the ldap server
# AUTH_LDAP_SERVER = "ldap://ldapserver.new"

# When using OpenID Auth, uncomment to setup OpenID providers.
# example for OpenID authentication
# OPENID_PROVIDERS = [
#    { 'name': 'Yahoo', 'url': 'https://me.yahoo.com' },
#    { 'name': 'AOL', 'url': 'http://openid.aol.com/<username>' },
#    { 'name': 'Flickr', 'url': 'http://www.flickr.com/<username>' },
#    { 'name': 'MyOpenID', 'url': 'https://www.myopenid.com' }]
