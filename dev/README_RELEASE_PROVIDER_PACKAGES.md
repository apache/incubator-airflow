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

- [Provider packages](#provider-packages)
- [Decide when to release](#decide-when-to-release)
- [Provider packages versioning](#provider-packages-versioning)
- [Prepare Regular Provider packages (RC)](#prepare-regular-provider-packages-rc)
  - [Generate release notes](#generate-release-notes)
  - [Build regular provider packages for SVN apache upload](#build-regular-provider-packages-for-svn-apache-upload)
  - [Build and sign the source and convenience packages](#build-and-sign-the-source-and-convenience-packages)
  - [Commit the source packages to Apache SVN repo](#commit-the-source-packages-to-apache-svn-repo)
  - [Publish the Regular convenience package to PyPI](#publish-the-regular-convenience-package-to-pypi)
  - [Add tags in git](#add-tags-in-git)
  - [Prepare documentation](#prepare-documentation)
  - [Prepare voting email for Providers release candidate](#prepare-voting-email-for-providers-release-candidate)
  - [Verify the release by PMC members](#verify-the-release-by-pmc-members)
  - [Verify by Contributors](#verify-by-contributors)
- [Publish release](#publish-release)
  - [Summarize the voting for the Apache Airflow release](#summarize-the-voting-for-the-apache-airflow-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish the Regular convenience package to PyPI](#publish-the-regular-convenience-package-to-pypi-1)
  - [Publish documentation prepared before](#publish-documentation-prepared-before)
  - [Add tags in git](#add-tags-in-git-1)
  - [Notify developers of release](#notify-developers-of-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

------------------------------------------------------------------------------------------------------------

# Provider packages

The prerequisites to release Apache Airflow are described in [README.md](README.md).

You can read more about the command line tools used to generate the packages in the
[Provider packages](PROVIDER_PACKAGE_DETAILS.md).

# Decide when to release

You can release provider packages separately from the main Airflow on an ad-hoc basis, whenever we find that
a given provider needs to be released - due to new features or due to bug fixes.
You can release each provider package separately, but due to voting and release overhead we try to group
releases of provider packages together.

# Provider packages versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the provider packages. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)

# Prepare Regular Provider packages (RC)

## Generate release notes

Prepare release notes for all the packages you plan to release. When the provider package version
has not been updated since the latest version, the release notes are not generated. Release notes
are only generated, when the latest version of the package does not yet have a corresponding TAG.
The tags for providers is of the form ``providers-<PROVIDER_ID>/<VERSION>`` for example
``providers-amazon/1.0.0``. During releasing, the RC1/RC2 tags are created (for example
``providers-amazon/1.0.0rc1``).

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)


```shell script
./breeze prepare-provider-documentation [packages]
```

This command will not only prepare documentation but will also allow the release manager to review
changes implemented in all providers, and determine which of the providers should be released. For each
provider details will be printed on what changes were implemented since the last release including
links to particular commits. This should help to determine which version of provider should be released:

* increased patch-level for bugfix-only change
* increased minor version if new features are added
* increased major version if breaking changes are added

It also allows the release manager to update CHANGELOG.rst where high-level overview of the changes should be
documented for the providers released.

You can iterate and re-generate the same readme content as many times as you want.
The generated files should be added and committed to the repository.


## Build regular provider packages for SVN apache upload

Those packages might get promoted  to "final" packages by just renaming the files, so internally they
should keep the final version number without the rc suffix, even if they are rc1/rc2/... candidates.

They also need to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.

## Build and sign the source and convenience packages

* Cleanup dist folder:

```shell script
export AIRFLOW_REPO_ROOT=$(pwd)
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*
```


* Release candidate packages:

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn rc1 --package-format both
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn rc1 --package-format both PACKAGE PACKAGE ....
```

* Sign all your packages

```shell script
pushd dist
../dev/sign.sh *
popd
```

## Commit the source packages to Apache SVN repo

* Push the artifacts to ASF dev dist repo

```shell script
# First clone the repo if you do not have it
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# update the repo in case you have it already
cd airflow-dev
svn update

# Create a new folder for the release.
cd providers

# Move the artifacts to svn folder
mv ${AIRFLOW_REPO_ROOT}/dist/* .

# Add and commit
svn add *
svn commit -m "Add artifacts for Airflow Providers $(date "+%Y-%m-%d%n")"

cd ${AIRFLOW_REPO_ROOT}
```

Verify that the files are available at
[providers](https://dist.apache.org/repos/dist/dev/airflow/providers/)

## Publish the Regular convenience package to PyPI

In case of pre-release versions you build the same packages for both PyPI and SVN so you can simply use
packages generated in the previous step, and you can skip the "prepare" step below.

In order to publish release candidate to PyPI you just need to build and release packages.
The packages should however contain the rcN suffix in the version file name but not internally in the package,
so you need to use `--version-suffix-for-pypi` switch to prepare those packages.
Note that these are different packages than the ones used for SVN upload
though they should be generated from the same sources.

* Generate the packages with the right RC version (specify the version suffix with PyPI switch). Note that
this will clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*

./breeze prepare-provider-packages --version-suffix-for-pypi rc1 --package-format both
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi rc1 --package-format both \
    PACKAGE PACKAGE ....
```

* Verify the artifacts that would be uploaded:

```shell script
twine check ${AIRFLOW_REPO_ROOT}/dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest ${AIRFLOW_REPO_ROOT}/dist/*
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi ${AIRFLOW_REPO_ROOT}/dist/*
```

* Again, confirm that the packages are available under the links printed.


## Add tags in git

Assume that your remote for apache repository is called `apache` you should now
set tags for the providers in the repo.

```shell script
./dev/provider_packages/tag_providers.sh
```

## Prepare documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation  for the released versions is published in a separate repository -
[`apache/airflow-site`](https://github.com/apache/airflow-site), but the documentation source code
and build tools are available in the `apache/airflow` repository, so you have to coordinate between
the two repositories to be able to build the documentation.

Documentation for providers can be found in the `/docs/apache-airflow-providers` directory
and the `/docs/apache-airflow-providers-*/` directory. The first directory contains the package contents
lists and should be updated every time a new version of provider packages is released.

- First, copy the airflow-site repository and set the environment variable ``AIRFLOW_SITE_DIRECTORY``.

```shell script
git clone https://github.com/apache/airflow-site.git airflow-site
cd airflow-site
export AIRFLOW_SITE_DIRECTORY="$(pwd)"
```

- Then you can go to the directory and build the necessary documentation packages

```shell script
cd "${AIRFLOW_REPO_ROOT}"
./breeze build-docs -- \
  --for-production \
  --package-filter apache-airflow-providers \
  --package-filter 'apache-airflow-providers-*'
```

for all providers, or if you have just few providers:

```shell script
cd "${AIRFLOW_REPO_ROOT}"
./breeze build-docs -- \
  --for-production \
  --package-filter apache-airflow-providers \
  --package-filter 'apache-airflow-providers-PACKAGE1' \
  --package-filter 'apache-airflow-providers-PACKAGE2' \
  ...
```

If you have providers as list of provider ids beacuse you just released them you can build them with

```shell script
./dev/provider_packages/build_provider_documentation.sh amazon apache.beam google ....
```

- Now you can preview the documentation.

```shell script
./docs/start_doc_server.sh
```

- Copy the documentation to the ``airflow-site`` repository

**NOTE** In order to run the publish documentation you need to activate virtualenv where you installed
apache-airflow with doc extra:

* `pip install apache-airflow[doc]`

All providers:

```shell script
./docs/publish_docs.py \
    --package-filter apache-airflow-providers \
    --package-filter 'apache-airflow-providers-*'

cd "${AIRFLOW_SITE_DIRECTORY}"
```

If you have providers as list of provider ids because you just released them you can build them with

```shell script
./dev/provider_packages/publish_provider_documentation.sh amazon apache.beam google ....
```


- If you publish a new package, you must add it to
  [the docs index](https://github.com/apache/airflow-site/blob/master/landing-pages/site/content/en/docs/_index.md):

- Create the commit and push changes.

```shell script
branch="add-documentation-$(date "+%Y-%m-%d%n")"
git checkout -b "${branch}"
git add .
git commit -m "Add documentation for packages - $(date "+%Y-%m-%d%n")"
git push --set-upstream origin "${branch}"
```

## Prepare voting email for Providers release candidate

Make sure the packages are in https://dist.apache.org/repos/dist/dev/airflow/providers/

Send out a vote to the dev@airflow.apache.org mailing list. Here you can prepare text of the
email.

subject:


```shell script
cat <<EOF
[VOTE] Airflow Providers - release prepared $(date "+%Y-%m-%d%n")
EOF
```

```shell script
cat <<EOF
Hey all,

I have just cut the new wave Airflow Providers packages. This email is calling a vote on the release,
which will last for 72 hours - which means that it will end on $(date -d '+3 days').

Consider this my (binding) +1.

Airflow Providers are available at:
https://dist.apache.org/repos/dist/dev/airflow/providers/

*apache-airflow-providers-<PROVIDER>-*-bin.tar.gz* are the binary
 Python "sdist" release - they are also official "sources" for the provider packages.

*apache_airflow_providers_<PROVIDER>-*.whl are the binary
 Python "wheel" release.

The test procedure for PMC members who would like to test the RC candidates are described in
https://github.com/apache/airflow/blob/master/dev/README_RELEASE_PROVIDER_PACKAGES.md#verify-the-release-by-pmc-members

and for Contributors:

https://github.com/apache/airflow/blob/master/dev/README_RELEASE_PROVIDER_PACKAGES.md#verify-by-contributors


Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason


Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

Please note that the version number excludes the 'rcX' string.
This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.


Each of the packages contains a link to the detailed changelog. The changelogs are moved to the official airflow documentation:
https://github.com/apache/airflow-site/<TODO COPY LINK TO BRANCH>

<PASTE ANY HIGH-LEVEL DESCRIPTION OF THE CHANGES HERE!>


Note the links to documentation from PyPI packages are not working until we merge
the changes to airflow site after releasing the packages officially.

<PASTE TWINE UPLOAD LINKS HERE. SORT THEM BEFORE!>

Cheers,
<TODO: Your Name>

EOF
```

Due to the nature of packages, not all packages have to be released as convenience
packages in the final release. During the voting process
the voting PMCs might decide to exclude certain packages from the release if some critical
problems have been found in some packages.

Please modify the message above accordingly to clearly exclude those packages.

## Verify the release by PMC members

### SVN check

The files should be present in
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/providers/)

The following files should be present (9 files):

* -source.tar.gz + .asc + .sha512 (one set of files)
* -bin-tar.gz + .asc + .sha512 (one set of files per provider)
* -.whl + .asc + .sha512 (one set of files per provider)

As a PMC you should be able to clone the SVN repository:

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow/
```

Or update it if you already checked it out:

```shell script
svn update .
```

Optionally you can use `check.files.py` script to verify that all expected files are
present in SVN. This script may help also with verifying installation of the packages.

```shell script
python check_files.py -v {VERSION} -t providers -p {PATH_TO_SVN}
```

### Licences check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the binary (`-bin.tar.gz`) to a folder
* Enter the folder and run the check (point to the place where you extracted the .jar)

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

where `.rat-excludes` is the file in the root of Airflow source code.

### Signature check

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

### SHA512 check

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow-providers-google-1.0.0rc1-bin.tar.gz.sha512
Checking apache_airflow-providers-google-1.0.0rc1-py3-none-any.whl.sha512
```

## Verify by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version.

You can use any of the installation methods you prefer (you can even install it via the binary wheels
downloaded from the SVN).

### Installing in your local virtualenv

You have to make sure you have Airflow 2* installed in your PIP virtualenv
(the version you want to install providers with).

```shell script
pip install apache-airflow-providers-<provider>==<VERSION>rc<X>
```

### Installing with Breeze

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Here is a typical scenario.

First copy all the provider packages .whl files to the `dist` folder.

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> \
    --python 3.7 --backend postgres --install-packages-from-dist
```

### Building your own docker image

If you prefer to build your own image, you can also use the official image and PyPI packages to test
provider packages. This is especially helpful when you want to test integrations, but you need to install
additional tools. Below is an example Dockerfile, which installs providers for Google/

```dockerfile
FROM apache/airflow:2.0.0

RUN pip install --upgrade --user apache-airflow-providers-google==2.0.0.rc1

USER ${AIRFLOW_UID}
```

To build an image build and run a shell, run:

```shell script
docker build . -t my-airflow
docker run  -ti \
    --rm \
    -v "$PWD/data:/opt/airflow/" \
    -v "$PWD/keys/:/keys/" \
    -p 8080:8080 \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=True \
    my-airflow bash
```

### Additional Verification

Once you install and run Airflow, you can perform any verification you see as necessary to check
that the Airflow works as you expected.


# Publish release

## Summarize the voting for the Apache Airflow release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Airflow  Providers - release of DATE OF RELEASE
```

Message:

```
Hello,

Apache Airflow Providers (based on RC1) have been accepted.

3 “+1” binding votes received:
- Jarek Potiuk  (binding)
- Kaxil Naik (binding)
- Tomasz Urbaszek (binding)


Vote thread:
https://lists.apache.org/thread.html/736404ca3d2b2143b296d0910630b9bd0f8b56a0c54e3a05f4c8b5fe@%3Cdev.airflow.apache.org%3E

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
<your name>
```



## Publish release to SVN

The best way of doing this is to svn cp  between the two repos (this avoids having to upload the binaries
again, and gives a clearer history in the svn commit logs.

We also need to archive older releases before copying the new ones
[Release policy](http://www.apache.org/legal/release-policy.html#when-to-archive)

```shell script
# Set AIRFLOW_REPO_ROOT to the path of your git repo
export AIRFLOW_REPO_ROOT=$(pwd)

# Go to the directory where you have checked out the dev svn release
# And go to the sub-folder with RC candidates
cd "<ROOT_OF_YOUR_DEV_REPO>/providers/"
export SOURCE_DIR=$(pwd)

# Go the folder where you have checked out the release repo
# Clone it if it's not done yet
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Update to latest version
svn update

# Create providers folder if it does not exist
# All latest releases are kept in this one folder without version sub-folder
mkdir -pv providers
cd providers

# Move the artifacts to svn folder & remove the rc postfix
for file in ${SOURCE_DIR}/*
do
 base_file=$(basename ${file})
 svn mv "${file}" "${base_file//rc[0-9][\.-]/.}"
done


# If some packages have been excluded, remove them now
# Check the packages
ls *<provider>*
# Remove them
svn rm *<provider>*

# Check which old packages will be removed (you need python 3.6+)
python ${AIRFLOW_REPO_ROOT}/dev/provider_packages/remove_old_releases.py \
    --directory .

# Remove those packages
python ${AIRFLOW_REPO_ROOT}/dev/provider_packages/remove_old_releases.py \
    --directory . --execute


# Commit to SVN
svn commit -m "Release Airflow Providers on $(date)"
```

Verify that the packages appear in
[providers](https://dist.apache.org/repos/dist/release/airflow/providers)


## Publish the Regular convenience package to PyPI

* Checkout the RC Version for the RC Version released (there is a batch of providers - one of them is enough):

    ```shell script
    git checkout providers-<PROVIDER_NAME>/<VERSION_RC>
    ```

* Generate the packages with final version. Note that
  this will clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*
./breeze prepare-provider-packages --package-format both
```

if you ony build few packages, run:

```shell script
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*
./breeze prepare-provider-packages --package-format both PACKAGE PACKAGE ....
```

* Verify the artifacts that would be uploaded:

```shell script
twine check ${AIRFLOW_REPO_ROOT}/dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest ${AIRFLOW_REPO_ROOT}/dist/*
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
  Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi ${AIRFLOW_REPO_ROOT}/dist/*
```

* Again, confirm that the packages are available under the links printed.

## Publish documentation prepared before

Merge the PR that you prepared before with the documentation. If you removed some of the providers
from the release - remove the versions from the prepared documentation and update stable.txt with the
previous version for those providers before merging the PR.


## Add tags in git

Assume that your remote for apache repository is called `apache` you should now
set tags for the providers in the repo.

```shell script
./dev/provider_packages/tag_providers.sh
```


## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:

```shell script
cat <<EOF
Airflow Providers released on $(date) are ready
EOF
```

Body:

```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that new versions of Airflow Providers packages were just released.

The source release, as well as the binary releases, are available here:

https://dist.apache.org/repos/dist/release/airflow/providers/

We also made those versions available on PyPi for convenience ('pip install apache-airflow-providers-*'):

https://pypi.org/search/?q=apache-airflow-providers

The documentation is available at http://airflow.apache.org/docs/ and linked from the PyPI packages:

<PASTE TWINE UPLOAD LINKS HERE. SORT THEM BEFORE!>

Cheers,
<your name>
EOF
```
