
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


Package apache-airflow-providers-cncf-kubernetes
------------------------------------------------------

`Kubernetes <https://kubernetes.io/>`__


This is detailed commit list of changes for versions provider package: ``cncf.kubernetes``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.0.2
.....

Latest change: 2021-02-23

================================================================================================  ===========  ============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ============================================================================
`809b4f9b1 <https://github.com/apache/airflow/commit/809b4f9b18c7040682e17879248d714f2664273d>`_  2021-02-23   ``Unique pod name (#14186)``
`649335c04 <https://github.com/apache/airflow/commit/649335c043a9312ef272fa77f2bb830d52cde056>`_  2021-02-07   ``Template k8s.V1EnvVar without adding custom attributes to dict. (#14123)``
`d4c4db8a1 <https://github.com/apache/airflow/commit/d4c4db8a1833d07b1c03e4c062acea49c79bf5d6>`_  2021-02-05   ``Allow users of the KPO to template environment variables (#14083)``
`10343ec29 <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
================================================================================================  ===========  ============================================================================

1.0.1
.....

Latest change: 2021-02-04

================================================================================================  ===========  ==========================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ==========================================================================================
`88bdcfa0d <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98 <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040 <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`1b9e3d1c2 <https://github.com/apache/airflow/commit/1b9e3d1c285cb381f2f964c0c923711cd5e1e3d0>`_  2021-01-22   ``Revert "Fix error with quick-failing tasks in KubernetesPodOperator (#13621)" (#13835)``
`94d3ed61d <https://github.com/apache/airflow/commit/94d3ed61d60b134d649a4e9785b2d9c2a88cff05>`_  2021-01-21   ``Fix error with quick-failing tasks in KubernetesPodOperator (#13621)``
`3fd5ef355 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f91 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`7a560ab6d <https://github.com/apache/airflow/commit/7a560ab6de7243e736b66599842b241ae60d1cda>`_  2020-12-24   ``Pass image_pull_policy in KubernetesPodOperator correctly (#13289)``
`6cf76d7ac <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
================================================================================================  ===========  ==========================================================================================

1.0.0
.....

Latest change: 2020-12-09

================================================================================================  ===========  ================================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ================================================================================================
`32971a1a2 <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa08 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f2478 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aa <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`2037303ee <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`de3b1e687 <https://github.com/apache/airflow/commit/de3b1e687b26c524c6909b7b4dfbb60d25019751>`_  2020-11-28   ``Move connection guides to provider documentation packages (#12653)``
`c02a3f59e <https://github.com/apache/airflow/commit/c02a3f59e45d3cdd0e4c1c3bda2c62b951bcbea3>`_  2020-11-23   ``Spark-on-k8s sensor logs - properly pass defined namespace to pod log call (#11199)``
`c34ef853c <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`9e089ab89 <https://github.com/apache/airflow/commit/9e089ab89567b0a52b232f22ed3e708a05137924>`_  2020-11-19   ``Fix Kube tests (#12479)``
`d32fe78c0 <https://github.com/apache/airflow/commit/d32fe78c0d9d14f016df70a462dc3972f28abe9d>`_  2020-11-18   ``Update readmes for cncf.kube provider fixes (#12457)``
`d84a52dc8 <https://github.com/apache/airflow/commit/d84a52dc8fc597d89c5bb4941df67f5f35b70a29>`_  2020-11-18   ``Fix broken example_kubernetes DAG (#12455)``
`7c8b71d20 <https://github.com/apache/airflow/commit/7c8b71d2012d56888f21b24c4844a6838dc3e4b1>`_  2020-11-18   ``Fix backwards compatibility further (#12451)``
`008035450 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f12 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`763b40d22 <https://github.com/apache/airflow/commit/763b40d223e5e5512494a97f8335e16960e6adc3>`_  2020-11-18   ``Raise correct Warning in kubernetes/backcompat/volume_mount.py (#12432)``
`bc4bb3058 <https://github.com/apache/airflow/commit/bc4bb30588607b10b069ab63ddf2ba7b7ee673ed>`_  2020-11-18   ``Fix docstrings for Kubernetes Backcompat module (#12422)``
`cab86d80d <https://github.com/apache/airflow/commit/cab86d80d48227849906319917126f6d558b2e00>`_  2020-11-17   ``Make K8sPodOperator backwards compatible (#12384)``
`ae7cb4a1e <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333c <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`221f809c1 <https://github.com/apache/airflow/commit/221f809c1b4e4b78d5a437d012aa7daffd8410a4>`_  2020-11-14   ``Fix full_pod_spec for k8spodoperator (#12354)``
`7825e8f59 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78 <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`3f59e75cd <https://github.com/apache/airflow/commit/3f59e75cdf4a95829ac60b151135e03267e63a12>`_  2020-11-09   ``KubernetesPodOperator: use randomized name to get the failure status (#12171)``
`b2a28d159 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`7825be50d <https://github.com/apache/airflow/commit/7825be50d80d04da0db8fcee55df5e1339864c88>`_  2020-11-05   ``Randomize pod name (#12117)``
`91a64db50 <https://github.com/apache/airflow/commit/91a64db505e50712cd53928b4f2b84aece3cc1c0>`_  2020-11-04   ``Format all files (without excepions) by black (#12091)``
`4e8f9cc8d <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b0 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84e <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`53e606210 <https://github.com/apache/airflow/commit/53e6062105be0ae1761a354e2055eb0779d12e73>`_  2020-10-21   ``Enforce strict rules for yamllint (#11709)``
`349b0811c <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`eee4e30f2 <https://github.com/apache/airflow/commit/eee4e30f2caf02e16088ff5d1af1ea380a73e982>`_  2020-10-15   ``Add better debug logging to K8sexec and K8sPodOp (#11502)``
`16e712971 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`8640fb6c1 <https://github.com/apache/airflow/commit/8640fb6c100a2c6aa231798559ba194331576975>`_  2020-10-09   ``fix tests (#11368)``
`298052fce <https://github.com/apache/airflow/commit/298052fcee9d30b1f60b8dc1c9006398cd16645e>`_  2020-10-10   ``[airflow/providers/cncf/kubernetes] correct hook methods name (#11008)``
`49aad025b <https://github.com/apache/airflow/commit/49aad025b53211a5815b10aa35f7d7b489cb5316>`_  2020-10-09   ``Users can specify sub-secrets and paths k8spodop (#11369)``
`b93b6c5be <https://github.com/apache/airflow/commit/b93b6c5be3ab60960f650d0d4ee6c91271ac7909>`_  2020-10-05   ``Allow labels in KubernetesPodOperator to be templated (#10796)``
`0a0e1af80 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4 <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c38 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`a888198c2 <https://github.com/apache/airflow/commit/a888198c27bcdbc4538c02360c308ffcaca182fa>`_  2020-09-27   ``Allow overrides for pod_template_file (#11162)``
`0161b5ea2 <https://github.com/apache/airflow/commit/0161b5ea2b805d62a0317e5cab6f797b92c8abf1>`_  2020-09-26   ``Increasing type coverage for multiple provider (#11159)``
`e3f96ce7a <https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57>`_  2020-09-24   ``Fix incorrect Usage of Optional[bool] (#11138)``
`f3e87c503 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`b61225a88 <https://github.com/apache/airflow/commit/b61225a8850b20be17842c2428b91d873584c4da>`_  2020-09-21   ``Add D204 pydocstyle check (#11031)``
`cba51d49e <https://github.com/apache/airflow/commit/cba51d49eea6a0563044191c8111978836d697ef>`_  2020-09-17   ``Simplify the K8sExecutor and K8sPodOperator (#10393)``
`1294e15d4 <https://github.com/apache/airflow/commit/1294e15d44c08498e7f1022fdd6f0bc5e50e533f>`_  2020-09-16   ``KubernetesPodOperator template fix (#10963)``
`5d6d5a2f7 <https://github.com/apache/airflow/commit/5d6d5a2f7d330c83297e1dc35728a0ba803aa866>`_  2020-09-14   ``Allow to specify path to kubeconfig in KubernetesHook (#10453)``
`7edfac957 <https://github.com/apache/airflow/commit/7edfac957bc17c9abcdcfe8d524772bd2783ac5a>`_  2020-09-09   ``Add connection caching to KubernetesHook (#10447)``
`9549274d1 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`90c150568 <https://github.com/apache/airflow/commit/90c1505686b063332dba87c0c948a8b29d8fd1d4>`_  2020-09-04   ``Make grace_period_seconds option on K8sPodOperator (#10727)``
`338b412c0 <https://github.com/apache/airflow/commit/338b412c04abc3fef8126f9724b448d1a9fd0bbc>`_  2020-09-02   ``Add on_kill support for the KubernetesPodOperator (#10666)``
`596bc1337 <https://github.com/apache/airflow/commit/596bc1337988f9377571295ddb748ef8703c19c0>`_  2020-08-31   ``Adds 'cncf.kubernetes' package back to backport provider packages. (#10659)``
`1e5aa4465 <https://github.com/apache/airflow/commit/1e5aa4465c5ef8f05745bda64da62fe542f2fe28>`_  2020-08-26   ``Spark-on-K8S sensor - add driver logs (#10023)``
`fdd9b6f65 <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`2f2d8dbfa <https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148>`_  2020-08-25   ``Remove all "noinspection" comments native to IntelliJ (#10525)``
`7c206a82a <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`8cd2be9e1 <https://github.com/apache/airflow/commit/8cd2be9e161635480581a0dc723b69ed24166f8d>`_  2020-08-11   ``Fix KubernetesPodOperator reattachment (#10230)``
`cdec30125 <https://github.com/apache/airflow/commit/cdec3012542b45d23a05f62d69110944ba542e2a>`_  2020-08-07   ``Add correct signature to all operators and sensors (#10205)``
`24c8e4c2d <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`aeea71274 <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`f1fd3e2c4 <https://github.com/apache/airflow/commit/f1fd3e2c453ddce3e87ce63787598fea0707ffcf>`_  2020-07-31   ``Fix typo on reattach property of kubernetespodoperator (#10056)``
`03c435174 <https://github.com/apache/airflow/commit/03c43517445019081c55b4ac5fad3b0debdee336>`_  2020-07-31   ``Allow 'image' in 'KubernetesPodOperator' to be templated (#10068)``
`88c160306 <https://github.com/apache/airflow/commit/88c1603060fd484d4145bc253c0dc0e6797e13dd>`_  2020-07-31   ``Improve docstring note about GKEStartPodOperator on KubernetesPodOperator (#10049)``
`7d24b088c <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`33f0cd265 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`c2db0dfeb <https://github.com/apache/airflow/commit/c2db0dfeb13ee679bf4d7b57874f0fcb39c0f0ed>`_  2020-07-22   ``More strict rules in mypy (#9705) (#9906)``
`719ae2bf6 <https://github.com/apache/airflow/commit/719ae2bf6227894c3e926f717eb4dc669549d615>`_  2020-07-22   ``Dump Pod as YAML in logs for KubernetesPodOperator (#9895)``
`840799d55 <https://github.com/apache/airflow/commit/840799d5597f0d005e1deec154f6c95bad6dce61>`_  2020-07-20   ``Improve KubernetesPodOperator guide (#9079)``
`44d4ae809 <https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3>`_  2020-07-06   ``Upgrade to latest pre-commit checks (#9686)``
`8bd15ef63 <https://github.com/apache/airflow/commit/8bd15ef634cca40f3cf6ca3442262f3e05144512>`_  2020-07-01   ``Switches to Helm Chart for Kubernetes tests (#9468)``
`40bf8f28f <https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee>`_  2020-06-18   ``Detect automatically the lack of reference to the guide in the operator descriptions (#9290)``
`1d36b0303 <https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea>`_  2020-05-23   ``Fix references in docs (#8984)``
`e742ef7c7 <https://github.com/apache/airflow/commit/e742ef7c704c18bf69b7a7235adb7f75e742f902>`_  2020-05-23   ``Fix typo in test_project_structure (#8978)``
`375d1ca22 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8a <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`8985df0bf <https://github.com/apache/airflow/commit/8985df0bfcb5f2b2cd69a21b9814021f9f8ce953>`_  2020-05-16   ``Monitor pods by labels instead of names (#6377)``
`f3521fb0e <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4c <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`f82ad452b <https://github.com/apache/airflow/commit/f82ad452b0f4ebd1428bc9669641a632dc87bb8c>`_  2020-05-15   ``Fix KubernetesPodOperator pod name length validation (#8829)``
`1ccafc617 <https://github.com/apache/airflow/commit/1ccafc617c4cb9622e3460ad7c190f3ee67c3b32>`_  2020-04-02   ``Add spark_kubernetes system test (#7875)``
`cd546b664 <https://github.com/apache/airflow/commit/cd546b664fa35a2bf85acd77af578c909a327d92>`_  2020-03-23   ``Add missing call to Super class in 'cncf' & 'docker' providers (#7825)``
`6c39a3bf9 <https://github.com/apache/airflow/commit/6c39a3bf97414ba2438669894db65c36ccbeb61a>`_  2020-03-10   ``[AIRFLOW-6542] Add spark-on-k8s operator/hook/sensor (#7163)``
`42eef3821 <https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e>`_  2020-03-07   ``[AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)``
`3320e432a <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`0ec277412 <https://github.com/apache/airflow/commit/0ec2774120d43fa667a371b384e6006e1d1c7821>`_  2020-02-24   ``[AIRFLOW-5629] Implement Kubernetes priorityClassName in KubernetesPodOperator (#7395)``
`9cbd7de6d <https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b>`_  2020-02-18   ``[AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)``
`967930c0c <https://github.com/apache/airflow/commit/967930c0cb6e2293f2a49e5c9add5aa1917f3527>`_  2020-02-11   ``[AIRFLOW-5413] Allow K8S worker pod to be configured from JSON/YAML file (#6230)``
`96f834389 <https://github.com/apache/airflow/commit/96f834389e03884025534fabd862155061f53fd0>`_  2020-02-03   ``[AIRFLOW-6678] Pull event logs from Kubernetes (#7292)``
`97a429f9d <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`cf141506a <https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721>`_  2020-02-02   ``[AIRFLOW-6708] Set unique logger names (#7330)``
`373c6aa4a <https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b>`_  2020-01-30   ``[AIRFLOW-6682] Move GCP classes to providers package (#7295)``
`83c037873 <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`059eda05f <https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf>`_  2020-01-21   ``[AIRFLOW-6610] Move software classes to providers package (#7231)``
================================================================================================  ===========  ================================================================================================
