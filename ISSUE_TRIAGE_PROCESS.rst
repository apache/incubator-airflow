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




Issue reporting and resolution process
======================================

This document explains the issue tracking and triage process within Apache
Airflow including labels, milestones, and priorities as well as the process
of resolving issues.

An unusual element of the Apache Airflow project is that you can open a PR
to fix an issue or make an enhancement, without needing to open a PR first.
This is intended to make it as easy as possible to contribute to
the project.

Labels
''''''

Since Apache Airflow uses GitHub Issues as the issue tracking system, the
use of labels is extensive. Though issue labels tend to change over time
based on components within the project, the majority of the ones listed
below should stand the test of time.

The intention with the use of labels with the Apache Airflow project is
that they should ideally be non-temporal in nature and primarily used
to indicate the following elements:

**Kind**

The “kind” labels indicate “what kind of issue it is”. The most
commonly used “kind” labels are: bug, feature, documentation, or task.

Therefore, when reporting an issue, the label of ``kind:bug`` is to
indicate a problem with the functionality, whereas the label of
``kind:feature`` is a desire to extend the functionality.

There has been discussion within the project about whether to separate
the desire for “new features” from “enhancements to existing features”,
but in practice most “feature requests” are actually enhancement requests,
so we decided to combine them both into ``kind:feature``.

The ``kind:task`` is used to categorize issues which are
identified elements of work to be done, primarily as part of a larger
change to be done as part of an AIP or something which needs to be cleaned
up in the project.

Issues of ``kind:documentation`` are for changes which need to be
made to the documentation within the project.


**Area**

The “area” set of labels should indicate the component of the code
referenced by the issue. At a high level, the biggest areas of the project
are: Airflow Core and Airflow Providers, which are referenced by ``area:core``
and ``area:providers``. This is especially important since these are now
being released and versioned independently.

There are more detailed areas of the Core Airflow project such as Scheduler, Webserver,
API, UI, Logging, and Kubernetes, which are all conceptually under the
“Airflow Core” area of the project.

Similarly within Airflow Providers, the larger providers such as Apache, AWS, Azure,
and Google who have many hooks and operators within them, have labels directly
associated with them such as ``provider/Apache``, ``provider/AWS``,
``provider/Azure``, and ``provider/Google``.
These make it easier for developers working on a single provider to
track issues for that provider.

Most issues need a combination of "kind" and "area" labels to be actionable.
For example:

* Feature request for an additional API would have ``kind:feature`` and ``area:API``
* Bug report on the User Interface would have ``kind:bug`` and ``area:UI``
* Documentation request on the Kubernetes Executor, would have ``kind:documentation`` and ``area:kubernetes``


Response to issues
''''''''''''''''''

Once an issue has been created on the Airflow project, someone from the
Airflow team or the Airflow community typically responds to this issue.
This response can have multiple elements.

**Priority**

After significant discussion about the different priority schemes currently
being used across various projects, we decided to use a priority scheme based
on the Kubernetes project, since the team felt it was easier for people to
understand.

Therefore, the priority labels used are:

* ``priority:critical``: Showstopper bug that should be resolved immediately and a patch issued as soon as possible. Typically, this is because it affects most users and would take down production systems.
* ``priority:high``: A high priority bug that affects many users and should be resolved quickly, but can wait for the next scheduled patch release.
* ``priority:medium``: A bug that should be fixed before the next release, but would not block a release if found during the release process.
* ``priority:low``: A bug with a simple workaround or a nuisance that does not stop mainstream functionality.


It's important to use priority labels effectively so we can triage incoming issues
appropriately and make sure that when we release a new version of Airflow,
we can ship a release confident that there are no “production blocker” issues in it.

This applies to both Core Airflow as well as the Airflow Providers. With the separation
of the Providers release from Core Airflow, a ``priority:critical`` bug in a single
provider could trigger an unplanned patch release of the Airflow Providers.


**Milestones**

The key temporal element in the issue triage process is the concept of milestones.
This is critical for release management purposes and will be used represent upcoming
release targets.

Issues currently being resolved will get assigned to one of the upcoming releases.
For example a feature request may be targeted for the next feature release milestone
such as ``2.x``, where a bug may be targeted for the next patch release milestone
such as ``2.x.y``.

In the interest of being precise, when an issue is tagged with a milestone, it
represents that it will be considered for that release, not that it is committed to
a release. Once a PR is created to fix that issue and when that PR is tagged with a
milestone, it implies that the PR is intended to released in that milestone.

Please note that Airflow Core and Airflow Providers are now released and
versioned separately. The use of milestones as described above is directed towards
Airflow Core releases.


**Transient Labels**

Sometimes, there is more information needed to either understand the issue or
to be able to reproduce the issue. Typically, this may require a response to the
issue creator asking for more information, with the issue then being tagged with
the label ``pending-response``.
Also, during this stage, additional labels may be added to the issue to help
classification and triage, such as ``reported_version`` and ``area``.

Occasionally an issue may require a larger discussion among the Airflow PMC or
the developer mailing list. This issue may then be tagged with the
``needs:discussion`` label.

Some issues may need a detailed review by one of the core committers of the project
and this could be tagged with a ``needs:triage`` label.


**Good First Issue**

Issues which are relatively straight forward to solve, will be tagged with
the ``GoodFirstIssue`` label.

The intention here is to galvanize contributions from new and inexperienced
contributors who are looking to contribute to the project. This has been successful
in other open source projects and early signs are that this has been helpful in the
Airflow project as well.

Ideally, these issues only require one or two files to be changed. The intention
here is that incremental changes to existing files are a lot easier for a new
contributor as compared to adding something completely new.

Another possibility here is to add “how to fix” in the comments of such issues, so
that new contributors have a running start when then pick up these issues.


**Timeliness**

For the sake of quick responses, the general “soft" rule within the Airflow project
is that if there is no assignee, anyone can take an issue to solve.

However, this depends on timely resolution of the issue by the assignee. The
expectation is as follows:

* If there is no activity on the issue for 2 weeks, the assignee will be reminded about the issue and asked if they are still working on it.
* If there is no activity even after 1 more week, the issue will be unassigned, so that someone else can pick it up and work on it.


There is a similar process when additional information is requested from the
issue creator. After the pending-response label has been assigned, if there is no
further information for a period of 1 month, the issue will be automatically closed.



**Invalidity**

At times issues are marked as invalid and later closed because of one of the
following situations:

* The issue is a duplicate of an already reported issue. In such cases, the latter issue is marked as ``duplicate``.
* Despite attempts to reproduce the issue to resolve it, the issue cannot be reproduced by the Airflow team based on the given information. In such cases, the issue is marked as ``Can’t Reproduce``.
* In some cases, the original creator realizes that the issue was incorrectly reported and then marks it as ``invalid``. Also, a committer could mark it as ``invalid`` if the issue being reported is for an unsupported operation or environment.
* In some cases, the issue may be legitimate, but may not be addressed in the short to medium term based on current project priorities or because this will be irrelevant because of an upcoming change. The committer could mark this as ``wontfix`` to set expectations that it won't be directly addressed in the near term.
