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

- [Prepare the Apache Airflow Package RC](#prepare-the-apache-airflow-package-rc)
  - [Build RC artifacts](#build-rc-artifacts)
  - [Prepare PyPI convenience "snapshot" packages](#prepare-pypi-convenience-snapshot-packages)
  - [\[Optional\] - Manually prepare production Docker Image](#%5Coptional%5C---manually-prepare-production-docker-image)
  - [Prepare Vote email on the Apache Airflow release candidate](#prepare-vote-email-on-the-apache-airflow-release-candidate)
- [Verify the release candidate by PMCs](#verify-the-release-candidate-by-pmcs)
  - [SVN check](#svn-check)
  - [Licence check](#licence-check)
  - [Signature check](#signature-check)
  - [SHA512 sum check](#sha512-sum-check)
- [Verify release candidates by Contributors](#verify-release-candidates-by-contributors)
- [Publish the final Apache Airflow release](#publish-the-final-apache-airflow-release)
  - [Summarize the voting for the Apache Airflow release](#summarize-the-voting-for-the-apache-airflow-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Prepare PyPI "release" packages](#prepare-pypi-release-packages)
  - [Update CHANGELOG.md](#update-changelogmd)
  - [\[Optional\] - Manually prepare production Docker Image](#%5Coptional%5C---manually-prepare-production-docker-image-1)
  - [Publish documentation](#publish-documentation)
  - [Notify developers of release](#notify-developers-of-release)
  - [Update Announcements page](#update-announcements-page)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md).

# Prepare the Apache Airflow Package RC

## Build RC artifacts

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any modification than renaming – i.e. the contents of the files must be the same between voted release candidate and final release. Because of this the version in the built artifacts that will become the official Apache releases must not include the rcN suffix.

- Set environment variables

    ```shell script
    # Set Version
    export VERSION=1.10.2rc3


    # Set AIRFLOW_REPO_ROOT to the path of your git repo
    export AIRFLOW_REPO_ROOT=$(pwd)


    # Example after cloning
    git clone https://github.com/apache/airflow.git airflow
    cd airflow
    export AIRFLOW_REPO_ROOT=$(pwd)
    ```

- Set your version to 1.10.2 in `setup.py` (without the RC tag)
- Commit the version change.

- Tag your release

    ```shell script
    git tag -s ${VERSION}
    ```

- Clean the checkout: the sdist step below will

    ```shell script
    git clean -fxd
    ```

- Tarball the repo

    ```shell script
    git archive --format=tar.gz ${VERSION} --prefix=apache-airflow-${VERSION}/ -o apache-airflow-${VERSION}-source.tar.gz
    ```


- Generate sdist

    NOTE: Make sure your checkout is clean at this stage - any untracked or changed files will otherwise be included
     in the file produced.

    ```shell script
    python setup.py compile_assets sdist bdist_wheel
    ```

- Rename the sdist

    **Airflow 2+**:

    ```shell script
    mv dist/apache-airflow-${VERSION%rc?}.tar.gz apache-airflow-${VERSION}-bin.tar.gz
    mv dist/apache_airflow-${VERSION%rc?}-py3-none-any.whl apache_airflow-${VERSION}-py3-none-any.whl
    ```

    **Airflow 1.10.x**:

    ```shell script
    mv dist/apache-airflow-${VERSION%rc?}.tar.gz apache-airflow-${VERSION}-bin.tar.gz
    mv dist/apache_airflow-${VERSION%rc?}-py2.py3-none-any.whl apache_airflow-${VERSION}-py2.py3-none-any.whl
    ```

- Generate SHA512/ASC (If you have not generated a key yet, generate it by following instructions on http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

    **Airflow 2+**:

    ```shell script
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-${VERSION}-source.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-${VERSION}-bin.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache_airflow-${VERSION}-py3-none-any.whl
    ```

    **Airflow 1.10.x**:

    ```shell script
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-${VERSION}-source.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-${VERSION}-bin.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache_airflow-${VERSION}-py2.py3-none-any.whl
    ```

- Tag & Push latest constraints files. This pushes constraints with rc suffix (this is expected)!

    **Airflow 2+**:

    ```shell script
    git checkout constraints-2-0
    git tag -s "constraints-${VERSION}"
    git push origin "constraints-${VERSION}"
    ```

    **Airflow 1.10.x**:

    ```shell script
    git checkout constraints-1-10
    git tag -s "constraints-${VERSION}"
    git push origin "constraints-${VERSION}"
    ```

- Push the artifacts to ASF dev dist repo

```
# First clone the repo
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# Create new folder for the release
cd airflow-dev
svn mkdir ${VERSION}

# Move the artifacts to svn folder & commit
mv ${AIRFLOW_REPO_ROOT}/apache{-,_}airflow-${VERSION}* ${VERSION}/
cd ${VERSION}
svn add *
svn commit -m "Add artifacts for Airflow ${VERSION}"
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
    python setup.py compile_assets egg_info --tag-build "$(sed -e "s/^[0-9.]*//" <<<"$VERSION")" sdist bdist_wheel
    ```

- Verify the artifacts that would be uploaded:

    ```shell script
    twine check dist/*
    ```

- Upload the package to PyPi's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the test package looks good by downloading it and installing it into a virtual environment. The package download link is available at:
https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPi's production environment:
`twine upload -r pypi dist/*`

- Again, confirm that the package is available here:
https://pypi.python.org/pypi/apache-airflow

It is important to stress that this snapshot should not be named "release", and it
is not supposed to be used by and advertised to the end-users who do not read the devlist.

- Push Tag for the release candidate

    This step should only be done now and not before, because it triggers an automated build of
    the production docker image, using the packages that are currently released in PyPI
    (both airflow and latest provider packages).

    ```shell script
    git push origin ${VERSION}
    ```

## \[Optional\] - Manually prepare production Docker Image

Production Docker images should be automatically built in 2-3 hours after the release tag has been
pushed. If this did not happen - please login to DockerHub and check the status of builds:
[Build Timeline](https://hub.docker.com/repository/docker/apache/airflow/timeline)

In case you need, you can also build and push the images manually:

Airflow 2+:

```shell script
export DOCKER_REPO=docker.io/apache/airflow
for python_version in "3.6" "3.7" "3.8"
(
  export DOCKER_TAG=${VERSION}-python${python_version}
  ./scripts/ci/images/ci_build_dockerhub.sh
)
```

This will wipe Breeze cache and docker-context-files in order to make sure the build is "clean".

Airflow 1.10:

```shell script
for python_version in "2.7" "3.5" "3.6" "3.7" "3.8"
do
    ./breeze build-image --production-image --python ${python_version} \
        --image-tag apache/airflow:${VERSION}-python${python_version} --build-cache-local
    docker push apache/airflow:${VERSION}-python${python_version}
done
docker tag apache/airflow:${VERSION}-python3.6 apache/airflow:${VERSION}
docker push apache/airflow:${VERSION}
```


## Prepare Vote email on the Apache Airflow release candidate

- Use the dev/airflow-jira script to generate a list of Airflow JIRAs that were closed in the release.

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:

```
[VOTE] Airflow 1.10.2rc3
```

Body:

```
Hey all,

I have cut Airflow 1.10.2 RC3. This email is calling a vote on the release,
which will last for 72 hours. Consider this my (binding) +1.

Airflow 1.10.2 RC3 is available at:
https://dist.apache.org/repos/dist/dev/airflow/1.10.2rc3/

*apache-airflow-1.10.2rc3-source.tar.gz* is a source release that comes
with INSTALL instructions.
*apache-airflow-1.10.2rc3-bin.tar.gz* is the binary Python "sdist" release.

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Only votes from PMC members are binding, but the release manager should encourage members of the community
to test the release and vote with "(non-binding)".

The test procedure for PMCs and Contributors who would like to test this RC are described in
https://github.com/apache/airflow/blob/master/dev/README.md#vote-and-verify-the-apache-airflow-release-candidate

Please note that the version number excludes the `rcX` string, so it's now
simply 1.10.2. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.


Changes since 1.10.2rc2:
*Bugs*:
[AIRFLOW-3732] Fix issue when trying to edit connection in RBAC UI
[AIRFLOW-2866] Fix missing CSRF token head when using RBAC UI (#3804)
...


*Improvements*:
[AIRFLOW-3302] Small CSS fixes (#4140)
[Airflow-2766] Respect shared datetime across tabs
...


*New features*:
[AIRFLOW-2874] Enables FAB's theme support (#3719)
[AIRFLOW-3336] Add new TriggerRule for 0 upstream failures (#4182)
...


*Doc-only Change*:
[AIRFLOW-XXX] Fix BashOperator Docstring (#4052)
[AIRFLOW-3018] Fix Minor issues in Documentation
...

Cheers,
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

Optionally you can use `check.files.py` script to verify that all expected files are
present in SVN. This script may help also with verifying installation of the packages.

```shell script
python check_files.py -v {VERSION} -t airflow -p {PATH_TO_SVN}
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
Checking apache-airflow-1.10.12rc4-bin.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-1.10.12rc4-bin.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:28 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache_airflow-1.10.12rc4-py2.py3-none-any.whl.asc
gpg: assuming signed data in 'apache_airflow-1.10.12rc4-py2.py3-none-any.whl'
gpg: Signature made sob, 22 sie 2020, 20:28:31 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache-airflow-1.10.12rc4-source.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-1.10.12rc4-source.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:25 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
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
Checking apache-airflow-1.10.12rc4-bin.tar.gz.sha512
Checking apache_airflow-1.10.12rc4-py2.py3-none-any.whl.sha512
Checking apache-airflow-1.10.12rc4-source.tar.gz.sha512
```

# Verify release candidates by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow via simply (<VERSION> is 1.10.12 for example, and <X> is
release candidate number 1,2,3,....).

```shell script
pip install apache-airflow==<VERSION>rc<X>
```

Optionally it can be followed with constraints

```shell script
pip install apache-airflow==<VERSION>rc<X> \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-<VERSION>/constraints-3.6.txt"`
```

Note that the constraints contain python version that you are installing it with.

You can use any of the installation methods you prefer (you can even install it via the binary wheel
downloaded from the SVN).

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Running the following command will use tmux inside breeze, create `admin` user and run Webserver & Scheduler:

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> --python 3.7 --backend postgres
```

For 1.10 releases you can also use `--no-rbac-ui` flag disable RBAC UI of Airflow:

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> --python 3.7 --backend postgres --no-rbac-ui
```

Once you install and run Airflow, you should perform any verification you see as necessary to check
that the Airflow works as you expected.

# Publish the final Apache Airflow release

## Summarize the voting for the Apache Airflow release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Airflow 1.10.2rc3
```

Message:

```
Hello,

Apache Airflow 1.10.2 (based on RC3) has been accepted.

4 “+1” binding votes received:
- Kaxil Naik  (binding)
- Bolke de Bruin (binding)
- Ash Berlin-Taylor (binding)
- Tao Feng (binding)


4 "+1" non-binding votes received:

- Deng Xiaodong (non-binding)
- Stefan Seelmann (non-binding)
- Joshua Patchus (non-binding)
- Felix Uellendall (non-binding)

Vote thread:
https://lists.apache.org/thread.html/736404ca3d2b2143b296d0910630b9bd0f8b56a0c54e3a05f4c8b5fe@%3Cdev.airflow.apache.org%3E

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
<your name>
```


## Publish release to SVN

You need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/
(The migration should include renaming the files so that they no longer have the RC number in their filenames.)

The best way of doing this is to svn cp between the two repos (this avoids having to upload the binaries again, and gives a clearer history in the svn commit logs):

```shell script
# First clone the repo
export RC=1.10.4rc5
export VERSION=${RC/rc?/}
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Create new folder for the release
cd airflow-release
svn mkdir ${VERSION}
cd ${VERSION}

# Move the artifacts to svn folder & commit
for f in ../../airflow-dev/$RC/*; do svn cp $f ${$(basename $f)/rc?/}; done
svn commit -m "Release Airflow ${VERSION} from ${RC}"

# Remove old release
# http://www.apache.org/legal/release-policy.html#when-to-archive
cd ..
export PREVIOUS_VERSION=1.10.1
svn rm ${PREVIOUS_VERSION}
svn commit -m "Remove old release: ${PREVIOUS_VERSION}"
```

Verify that the packages appear in [airflow](https://dist.apache.org/repos/dist/release/airflow/)

## Prepare PyPI "release" packages

At this point we release an official package:

- Build the package:

    ```shell script
    python setup.py compile_assets sdist bdist_wheel
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
    The package download link is available at: https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPi's production environment:

    ```shell script
    twine upload -r pypi dist/*
    ```

- Again, confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow

## Update CHANGELOG.md

- Get a diff between the last version and the current version:

    ```shell script
    git log 1.8.0..1.9.0 --pretty=oneline
    ```

- Update CHANGELOG.md with the details, and commit it.

- Re-Tag & Push the constraints files with the final release version.

    ```shell script
    git checkout constraints-${RC}
    git tag -s "constraints-${VERSION}"
    git push origin "constraints-${VERSION}"
    ```

- Push Tag for the final version

    This step should only be done now and not before, because it triggers an automated build of
    the production docker image, using the packages that are currently released in PyPI
    (both airflow and latest provider packages).

    ```shell script
    git push origin ${VERSION}
    ```

## \[Optional\] - Manually prepare production Docker Image

Production Docker images should be automatically built in 2-3 hours after the release tag has been
pushed. If this did not happen - please login to DockerHub and check the status of builds:
[Build Timeline](https://hub.docker.com/repository/docker/apache/airflow/timeline)

In case you need, you can also build and push the images manually:

Airflow 2+:

```shell script
export DOCKER_REPO=docker.io/apache/airflow
for python_version in "3.6" "3.7" "3.8"
(
  export DOCKER_TAG=${VERSION}-python${python_version}
  ./scripts/ci/images/ci_build_dockerhub.sh
)
```

This will wipe Breeze cache and docker-context-files in order to make sure the build is "clean".


Airflow 1.10:

```shell script
for python_version in "2.7" "3.5" "3.6" "3.7" "3.8"
do
    ./breeze build-image --production-image --python ${python_version} \
        --image-tag apache/airflow:${VERSION}-python${python_version} --build-cache-local
    docker push apache/airflow:${VERSION}-python${python_version}
done
docker tag apache/airflow:${VERSION}-python3.6 apache/airflow:${VERSION}
docker push apache/airflow:${VERSION}
```

## Publish documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation for the released versions is published in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site), but the documentation source code and build tools are available in the `apache/airflow` repository, so you have to coordinate between the two repositories to be able to build the documentation.

Documentation for providers can be found in the ``/docs/apache-airflow`` directory.

- First, copy the airflow-site repository and set the environment variable ``AIRFLOW_SITE_DIRECTORY``.

    ```shell script
    git clone https://github.com/apache/airflow-site.git airflow-site
    cd airflow-site
    export AIRFLOW_SITE_DIRECTORY="$(pwd)"
    ```

- Then you can go to the directory and build the necessary documentation packages

    ```shell script
    cd "${AIRFLOW_REPO_ROOT}"
    ./breeze build-docs -- --package-filter apache-airflow --for-production
    ```

- Now you can preview the documentation.

    ```shell script
    ./docs/start_doc_server.sh
    ```

- Copy the documentation to the ``airflow-site`` repository, create commit and push changes.

    ```shell script
    ./docs/publish_docs.py --package apache-airflow
    cd "${AIRFLOW_SITE_DIRECTORY}"
    git commit -m "Add documentation for Apache Airflow ${VERSION}"
    git push
    ```

## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:

```shell script
cat <<EOF
Airflow ${VERSION} is released
EOF
```

Body:

```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that Airflow ${VERSION} was just released.

The source release, as well as the binary "sdist" release, are available
here:

https://dist.apache.org/repos/dist/release/airflow/${VERSION}/

We also made this version available on PyPi for convenience (`pip install apache-airflow`):

https://pypi.python.org/pypi/apache-airflow

The documentation is available on:
https://airflow.apache.org/
https://airflow.apache.org/docs/apache-airflow/${VERSION}/

Find the CHANGELOG here for more details:

https://airflow.apache.org/changelog.html#airflow-1-10-2-2019-01-19

Cheers,
<your name>
EOF
```

## Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)
