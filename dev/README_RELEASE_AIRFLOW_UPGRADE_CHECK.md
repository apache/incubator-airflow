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
**Table of contents**

- [Prepare the Apache Airflow Upgrade Check Package RC](#prepare-the-apache-airflow-upgrade-check-package-rc)
  - [Pre-requisites](#pre-requisites)
  - [Build RC artifacts](#build-rc-artifacts)
  - [Prepare PyPI convenience "snapshot" packages](#prepare-pypi-convenience-snapshot-packages)
  - [Prepare Vote email on the Apache Airflow release candidate](#prepare-vote-email-on-the-apache-airflow-release-candidate)
- [Verify the release candidate by PMCs](#verify-the-release-candidate-by-pmcs)
  - [SVN check](#svn-check)
  - [Licence check](#licence-check)
  - [Signature check](#signature-check)
  - [SHA512 sum check](#sha512-sum-check)
- [Verify release candidates by Contributors](#verify-release-candidates-by-contributors)
- [Publish the final release](#publish-the-final-release)
  - [Summarize the voting for the release](#summarize-the-voting-for-the-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Prepare PyPI "release" packages](#prepare-pypi-release-packages)
  - [Notify developers of release](#notify-developers-of-release)
  - [Update Announcements page](#update-announcements-page)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md). This document
details the steps for releasing apache-airflow-upgrade-check package.

# Prepare the Apache Airflow Upgrade Check Package RC

## Pre-requisites

- Install `setuptools-build-subpackage` v0.1.0 by running:

  ```shell script
  pip install https://github.com/ashb/setuptools-build-subpackage/archive/master.zip
  ```

## Build RC artifacts

The Release Candidate artifacts we vote upon should be the exact ones we vote against,
without any modification than renaming – i.e. the contents of the files must be
the same between voted release candidate and final release.
Because of this the version in the built artifacts that will become the
official Apache releases must not include the rcN suffix.

- Set environment variables

    ```shell script
    # Set Version
    export VERSION=1.3.0rc1


    # Set AIRFLOW_REPO_ROOT to the path of your git repo
    export AIRFLOW_REPO_ROOT=$(pwd)


    # Example after cloning
    git clone https://github.com/apache/airflow.git airflow
    cd airflow
    export AIRFLOW_REPO_ROOT=$(pwd)
    ```

- Checkout `v1-10-stable` branch:

    ```shell script
    git checkout v1-10-stable
    ```

- Set your version to 1.3.0 in `airflow/upgrade/version.py` (without the RC tag)
- Commit the version change.

- Tag your release

    ```shell script
    git tag -s upgrade-check/${VERSION}
    ```

- Clean the checkout: the sdist step below will

    ```shell script
    git clean -fxd
    ```

- Tarball the repo

    ```shell script
    git archive upgrade-check/${VERSION} --prefix=apache-airflow-upgrade-check-${VERSION%rc?}/ \
        -o apache-airflow-upgrade-check-${VERSION}-source.tar.gz \
        airflow/upgrade NOTICE LICENSE licenses/ .rat-excludes
    ```

- Generate sdist

    NOTE: Make sure your checkout is clean at this stage - any untracked or changed files will otherwise be included
     in the file produced.

    ```shell script
    python -m setuptools_build_subpackage --subpackage-folder airflow/upgrade \
        sdist --license-template ./LICENSE bdist_wheel
    ```

- Rename the sdist

    ```shell script
    mv dist/apache-airflow-upgrade-check-${VERSION%rc?}.tar.gz apache-airflow-upgrade-check-${VERSION}-bin.tar.gz
    mv dist/apache_airflow_upgrade_check-${VERSION%rc?}-py2.py3-none-any.whl apache_airflow_upgrade_check-${VERSION}-py2.py3-none-any.whl
    ```

- Generate SHA512/ASC (If you have not generated a key yet, generate it by following instructions on
  http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

    ```shell script
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-upgrade-check-${VERSION}-source.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-upgrade-check-${VERSION}-bin.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache_airflow_upgrade_check-${VERSION}-py2.py3-none-any.whl
    ```

- Push the artifacts to ASF dev dist repo

  ```shell script
  # First clone the repo
  svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

  # Create new folder for the release
  cd airflow-dev/upgrade-check
  svn mkdir ${VERSION}

  # Move the artifacts to svn folder & commit
  mv ${AIRFLOW_REPO_ROOT}/apache{-,_}airflow*-${VERSION}* ${VERSION}/
  cd ${VERSION}
  svn add *
  svn commit -m "Add artifacts for Upgrade Check ${VERSION}"
  ```

## Prepare PyPI convenience "snapshot" packages

At this point we have the artefact that we vote on, but as a convenience to developers we also want to
publish "snapshots" of the RC builds to pypi for installing via pip. Also those packages
are used to build the production docker image in DockerHub, so we need to upload the packages
before we push the tag to GitHub. Pushing the tag to GitHub automatically triggers image building in
DockerHub.

To do this we need to

- Build the package:

    ```shell script
    cd ${AIRFLOW_REPO_ROOT}
    python -m setuptools_build_subpackage --subpackage-folder airflow/upgrade \
        egg_info --tag-build "$(sed -e "s/^[0-9.]*//" <<<"$VERSION")" sdist \
        --license-template ./LICENSE bdist_wheel
    ```

- Verify the artifacts that would be uploaded:

    ```shell script
    twine check dist/*
    ```

- Upload the package to PyPi's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the test package looks good by downloading it and installing it into a virtual environment.
  The package download link is available at: https://test.pypi.org/project/apache-airflow-upgrade-check/#history

- Upload the package to PyPi's production environment:

  ```shell script
  twine upload -r pypi dist/*
  ```

- Again, confirm that the package is available here: https://pypi.org/project/apache-airflow-upgrade-check

It is important to stress that this snapshot should not be named "release", and it
is not supposed to be used by and advertised to the end-users who do not read the devlist.

- Push Tag for the release candidate

    ```shell script
    git push origin upgrade-check/${VERSION}
    ```

## Prepare Vote email on the Apache Airflow release candidate

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:

```
[VOTE] Release apache-airflow-upgrade-check 1.3.0 from 1.3.0rc1
```

Body:

```
Hey all,

This calls for the release of a new dist: apache-airflow-upgrade-check,
version 1.3.0. This represents the contents of the airflow/upgrade/
tree (plus a few supporting files) as a separate dist.

This code is based on the v1-10-stable branch, the git tag is https://github.com/apache/airflow/releases/tag/upgrade-check%2F1.3.0rc1

This email is calling a vote on the release, which will last until Friday 12th February 23:40 UTC

Consider this my (binding) +1.

The files can be downloaded from https://dist.apache.org/repos/dist/dev/airflow/upgrade-check/1.3.0rc1/

- apache-airflow-upgrade-check-1.3.0rc1-source.tar.gz is a source
release containing the files that made up the binary and wheel
releases.
- apache-airflow-upgrade-check-1.3.0rc1-bin.tar.gz is the binary
Python "sdist" release.
- apache_airflow_upgrade_check-1.3.0rc1-py2.py3-none-any.whl is the
binary Python pre-compiled wheel file.

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

This dist is also available on PyPI
https://pypi.org/project/apache-airflow-upgrade-check/

Changelog: https://github.com/apache/airflow/tree/upgrade-check/1.3.0rc1/airflow/upgrade#changelog

Only votes from PMC members are binding, but members of the community
to test the release and vote with "(non-binding)".

The test procedure for PMCs and Contributors who would like to test
this RC are described in
<https://github.com/apache/airflow/blob/master/dev/README_RELEASE_AIRFLOW.md#verify-release-candidates-by-contributors>,
but again, this time it is a little bit different.

To actually use this command you will need apache-airflow 1.10.15

Please note that the version number inside the archives exclude the
`rcX` string, so it's now simply 1.3.0. This will allow us to rename
the artifact without modifying the artifact checksums when we actually
release.

Thanks,
<your name>
```


# Verify the release candidate by PMCs

The PMCs should verify the releases in order to make sure the release is following the
[Apache Legal Release Policy](http://www.apache.org/legal/release-policy.html).

At least 3 (+1) votes should be recorded in accordance to
[Votes on Package Releases](https://www.apache.org/foundation/voting.html#ReleaseVotes)

The legal checks include:

* checking if the packages are present in the right dist folder on svn
* verifying if all the sources have correct licences
* verifying if release manager signed the releases with the right key
* verifying if all the checksums are valid for the release

## SVN check

The files should be present in the sub-folder of
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/)

The following files should be present (9 files):

* -bin-tar.gz + .asc + .sha512
* -source.tar.gz + .asc + .sha512
* -.whl + .asc + .sha512

As a PMC you should be able to clone the SVN repository:

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow
```

Or update it if you already checked it out:

```shell script
svn update .
```

## Licence check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the binary (`-bin.tar.gz`) to a folder
* Enter the folder and run the check (point to the place where you extracted the .jar)

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

where `.rat-excludes` is the file in the root of Airflow source code.

## Signature check

Make sure you have the key of person signed imported in your GPG. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

```shell script
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```shell script
gpg --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```shell script
gpg --keyserver keys.gnupg.net --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

Once you have the keys, the signatures can be verified by running this:

```shell script
for i in *.asc
do
   echo "Checking $i"; gpg --verify $i
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of the certificates used by release managers are self signed, that's why you get this
warning. By importing the server in the previous step and importing it via ID from
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) page, you know that
this is a valid Key already.

```
Checking apache-airflow-upgrade-check-1.3.0rc1-bin.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-upgrade-check-1.3.0rc1-bin.tar.gz'
gpg: Signature made Tue  9 Mar 23:22:24 2021 GMT
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [ultimate]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [ultimate]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Checking apache-airflow-upgrade-check-1.3.0rc1-source.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-upgrade-check-1.3.0rc1-source.tar.gz'
gpg: Signature made Tue  9 Mar 23:22:21 2021 GMT
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [ultimate]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [ultimate]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Checking apache_airflow_upgrade_check-1.3.0rc1-py2.py3-none-any.whl.asc
gpg: assuming signed data in 'apache_airflow_upgrade_check-1.3.0rc1-py2.py3-none-any.whl'
gpg: Signature made Tue  9 Mar 23:22:27 2021 GMT
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [ultimate]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [ultimate]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
```

## SHA512 sum check

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow-upgrade-check-1.3.0rc1-bin.tar.gz.sha512
Checking apache_airflow_upgrade_check-1.3.0rc1-py2.py3-none-any.whl.sha512
Checking apache-airflow-upgrade-check-1.3.0rc1-source.tar.gz.sha512
```

# Verify release candidates by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow Upgrade Check via simply (<VERSION> is 1.3.0
for example, and <X> is release candidate number 1,2,3,....).

```shell script
pip install apache-airflow-upgrade-check==<VERSION>rc<X>
```

Once this is installed, please run the upgrade check script.

```shell script
airflow upgrade_check
```

You should see an output like the following:

```shell script
========================================= STATUS =========================================

Check for latest versions of apache-airflow and checker...........................SUCCESS
Remove airflow.AirflowMacroPlugin class...........................................SUCCESS
Ensure users are not using custom metaclasses in custom operators.................SUCCESS
Chain between DAG and operator not allowed........................................SUCCESS
Connection.conn_type is not nullable..............................................SUCCESS
Custom Executors now require full path............................................SUCCESS
Check versions of PostgreSQL, MySQL, and SQLite to ease upgrade to Airflow 2.0....SUCCESS
Hooks that run DB functions must inherit from DBApiHook...........................SUCCESS
Fernet is enabled by default......................................................SUCCESS
GCP service account key deprecation...............................................SUCCESS
Unify hostname_callable option in core section....................................SUCCESS
Changes in import paths of hooks, operators, sensors and others...................SUCCESS
Legacy UI is deprecated by default................................................SUCCESS
Logging configuration has been moved to new section...............................SUCCESS
Removal of Mesos Executor.........................................................SUCCESS
No additional argument allowed in BaseOperator....................................SUCCESS
Users must set a kubernetes.pod_template_file value...............................SKIPPED
SendGrid email uses old airflow.contrib module....................................SUCCESS
Check Spark JDBC Operator default connection name.................................SUCCESS
Changes in import path of remote task handlers....................................SUCCESS
Connection.conn_id is not unique..................................................SUCCESS
Use CustomSQLAInterface instead of SQLAInterface for custom data models...........SUCCESS
Found 1 problem.

==========================================================================================

Users must set a kubernetes.pod_template_file value
---------------------------------------------------
Skipped because this rule applies only to environment using KubernetesExecutor.
```

You can then perform any other verifications to check that the it works as you expected.

# Publish the final release

## Summarize the voting for the release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Release apache-airflow-upgrade-check 1.3.0 from 1.3.0rc1
```

Message:

```
Hello Airflow Community,

The vote to release Apache Airflow Upgrade Check 1.3.0 based on 1.3.0rc1 is now closed.

The vote PASSED with 3 binding "+1", 3 non-binding "+1" and 0 "-1" votes:

"+1" binding votes received:
- Kaxil Naik
- Jarek Potiuk
- Xiaodong Deng

"+1" non-Binding votes:
- Dennis Akpenyi
- Ephraim Anierobi
- Vikram Koka

Vote thread:
https://lists.apache.org/thread.html/r76ec33d9a446e0e08d9d036c0e1550338ce47a5bab98ffb19be52980%40%3Cdev.airflow.apache.org%3E

I'll continue with the release process, and the release announcement will follow shortly.

Regards,
<your name>
```

## Publish release to SVN

You need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/upgrade-check/
(The migration should include renaming the files so that they no longer have the RC number in their filenames.)

The best way of doing this is to svn cp between the two repos (this avoids having to upload
the binaries again, and gives a clearer history in the svn commit logs):

```shell script
# First clone the repo
export RC=1.3.0rc1
export VERSION=${RC/rc?/}
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Create new folder for the release
cd airflow-release/upgrade-check
svn mkdir ${VERSION}
cd ${VERSION}

# Move the artifacts to svn folder & commit
for f in ../../../airflow-dev/upgrade-check/$RC/*; do svn cp $f ${$(basename $f)/rc?/}; done
svn commit -m "Release Airflow Upgrade Check ${VERSION} from ${RC}"

# Remove old release
# http://www.apache.org/legal/release-policy.html#when-to-archive
cd ..
export PREVIOUS_VERSION=1.2.0
svn rm ${PREVIOUS_VERSION}
svn commit -m "Remove old Upgrade Check release: ${PREVIOUS_VERSION}"
```

Verify that the packages appear in [airflow upgrade check](https://dist.apache.org/repos/dist/release/airflow/upgrade-check/)

## Prepare PyPI "release" packages

At this point we release an official package:

- Checkout the tag:

    ```shell script
    git checkout upgrade-check/${RC}
    ```

- Build the package:

    ```shell script
    cd ${AIRFLOW_REPO_ROOT}
    git clean -fxd
    python -m setuptools_build_subpackage --subpackage-folder airflow/upgrade \
        sdist --license-template ./LICENSE bdist_wheel
    ```

- Verify the artifacts that would be uploaded:

    ```shell script
    twine check dist/*
    ```

- Upload the package to PyPi's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the package looks good by downloading it and installing it into a virtual environment.
    The package download link is available at: https://test.pypi.org/project/apache-airflow-upgrade-check/#files

- Upload the package to PyPi's production environment:

    ```shell script
    twine upload -r pypi dist/*
    ```

- Again, confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow-upgrade-check

- Push Tag for the final version

    ```shell script
    git tag -s "upgrade-check/${VERSION}"
    git push origin "upgrade-check/${VERSION}"
    ```

## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:

```shell script
cat <<EOF
[ANNOUNCE] Apache Airflow Upgrade Check 1.3.0 released
EOF
```

Body:

```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that apache-airflow-upgrade-check ${VERSION} has just been released.

This powers the `airflow upgrade_check` to make upgrading to 2.0 easier.

The source release, as well as the binary "sdist" release, are available
at:

- https://dist.apache.org/repos/dist/release/airflow/upgrade-check/${VERSION}/
- https://downloads.apache.org/airflow/upgrade-check/${VERSION}/
- https://archive.apache.org/dist/airflow/upgrade-check/${VERSION}/ (eventually)


We also made this version available on PyPI for convenience (`pip install apache-airflow-upgrade-check`):

https://pypi.org/project/apache-airflow-upgrade-check/${VERSION}/

Find the CHANGELOG here for more details:

https://github.com/apache/airflow/tree/upgrade-check/${VERSION}/airflow/upgrade#changelog

Cheers,
<your name>
EOF
```

## Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)
