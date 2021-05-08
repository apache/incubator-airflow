#!/usr/bin/python3
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
#
#
# Since we are renaming `master` to `main` this script helps with
# semi-automated syncing of `master` to `main` images in all the
# Docker registries we are using:
#
# * DockerHub - we keep `apache/airflow` image with distinct tags
#   that determine type of the image, because in DockerHub we only
#   have access to `apache/airflow` image
#
# * GitHub Docker Registries: (depends on the type of registry) we have
#   more flexibility:
#   * In the old GitHub docker registry - docker.pkg.github.com -
#     (current but already deprecated) we can use
#     "apache/airflow/IMAGE:tag" i
#   * in the new package registry (ghcr.io) - we can submitg anything
#     under apache/airflow-* but then we link it to the
#     project via docker image label.
#
# The script helps to keep all the registries in-sync - copies
# `master` to `main` so that we can run it to test the rename and
#  re-run it just before we switch the branches.

import subprocess
from typing import List

PYTHON_VERSIONS = ["3.6", "3.7", "3.8"]

DOCKERHUB_PREFIX = "apache/airflow"

DOCKERHUB_IMAGES = [
    "{prefix}:python{python_version}-{branch}",
    "{prefix}:{branch}-python{python_version}-ci",
    "{prefix}:{branch}-python{python_version}-ci-manifest",
    "{prefix}:{branch}-python{python_version}",
    "{prefix}:{branch}-python{python_version}-build",
]

GITHUB_DOCKER_REGISTRY_PREFIX = "docker.pkg.github.com/apache/airflow"

GITHUB_REGISTRY_IMAGES = [
    "{prefix}/{branch}-python{python_version}-ci-v2:latest",
    "{prefix}/{branch}-python{python_version}-v2:latest",
    "{prefix}/{branch}-python{python_version}-build-v2:latest",
]


GHCR_IO_PREFIX = "ghcr.io/apache/airflow"

GHCR_IO_IMAGES = [
    "{prefix}-{branch}-python{python_version}-ci-v2:latest",
    "{prefix}-{branch}-python{python_version}-v2:latest",
    "{prefix}-{branch}-python{python_version}-build-v2:latest",
]


SOURCE_BRANCH = "master"
TARGET_BRANCH = "main"


# noinspection StrFormat
def pull_push_all_images(prefix: str, images: List[str]):
    for python_version in PYTHON_VERSIONS:
        for image in images:
            source_image = image.format(prefix=prefix, branch=SOURCE_BRANCH, python_version=python_version)
            target_image = image.format(prefix=prefix, branch=TARGET_BRANCH, python_version=python_version)
            print(f"Copying image: {source_image} -> {target_image}")
            subprocess.run(["docker", "pull", source_image], check=True)
            subprocess.run(["docker", "tag", source_image, target_image], check=True)
            subprocess.run(["docker", "push", target_image], check=True)


pull_push_all_images(DOCKERHUB_PREFIX, DOCKERHUB_IMAGES)
pull_push_all_images(GITHUB_DOCKER_REGISTRY_PREFIX, GITHUB_REGISTRY_IMAGES)
pull_push_all_images(GHCR_IO_PREFIX, GHCR_IO_IMAGES)
