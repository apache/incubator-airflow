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

This folder is part of the Docker context.

Most of other folders in Airflow are not part of the context in order to make the context smaller.

The Production [Dockerfile](../Dockerfile) and the CI one [Dockerfile.ci](../Dockerfile.ci) copies
the [docker-context-files](.) folder to the image context - in case of production image it copies it to
the build segment, co content of the folder is available in the `/docker-context-file` folder inside
the build image. You can store constraint files and wheel
packages there that you want to install as PYPI packages and refer to those packages using
`--constraint-location` flag for constraints or by using `--install-from-docker-context-files` flag.

By default, the content of this folder is .gitignored so that any binaries and files you put here are only
used for local builds and not committed to the repository.
