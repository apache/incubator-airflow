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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Overview](#overview)
  - [Adding a Committer or PMC Member](#adding-a-committer-or-pmc-member)
  - [Airflow Improvement Proposals (AIPs)](#airflow-improvement-proposals-aips)
  - [Support for Airflow 1.10.x releases](#support-for-airflow-110x-releases)
  - [Support for Backport Providers](#support-for-backport-providers)
  - [Release Guidelines](#release-guidelines)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Overview

This document describes all the guidelines that have been agreed upon by the committers and PMC
members on the mailing list.

## Adding a Committer or PMC Member

[COMMITTERS.rst](../COMMITTERS.rst) contains the guidelines for adding a new Committer and promoting an existing
Committer to a PMC member.

## Airflow Improvement Proposals (AIPs)

When voting on AIPs, both PMC members and committers have a binding vote.
([Link](https://lists.apache.org/thread.html/ra22cb7799e62e451fc285dee29f9df1eb17c000535ca2911c322c797%40%3Cdev.airflow.apache.org%3E))

## Support for Airflow 1.10.x releases

The Airflow 1.10.x series will be supported for six months (June 17, 2021) from Airflow 2.0.0
release date (Dec 17, 2020). Specifically, only 'critical fixes' defined as fixes to bugs
that take down Production systems, will be backported to 1.10.x until June 17, 2021.

## Support for Backport Providers

Backport providers within 1.10.x, are not released any more, as of (March 17, 2021).

## Release Guidelines

### Apache Airflow (core)

- Follow Semantic Versioning ([SEMVER](https://semver.org/))
- Changing the version of dependency should not count as breaking change
