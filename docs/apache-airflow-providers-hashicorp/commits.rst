
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


Package apache-airflow-providers-hashicorp
------------------------------------------------------

Hashicorp including `Hashicorp Vault <https://www.vaultproject.io/>`__


This is detailed commit list of changes for versions provider package: ``hashicorp``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.0.2
.....

Latest change: 2021-04-06

================================================================================================  ===========  =============================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =============================================================================
`042be2e4e <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`b74f796c8 <https://github.com/apache/airflow/commit/b74f796c833283c820c81b20df1ba9f9cfa485c3>`_  2021-04-06   ``Fix deprecated warning hvac auth (#15216)``
`68e4c4dcb <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`e7bb17aeb <https://github.com/apache/airflow/commit/e7bb17aeb83b2218620c5320241b0c9f902d74ff>`_  2021-03-06   ``Use built-in 'cached_property' on Python 3.8 where possible (#14606)``
================================================================================================  ===========  =============================================================================

1.0.1
.....

Latest change: 2021-02-04

================================================================================================  ===========  ========================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ========================================================
`88bdcfa0d <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98 <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`3fd5ef355 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`295d66f91 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
================================================================================================  ===========  ========================================================

1.0.0
.....

Latest change: 2020-12-09

================================================================================================  ===========  ==================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ==================================================================================
`32971a1a2 <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`b40dffa08 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f2478 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`36a9b0f48 <https://github.com/apache/airflow/commit/36a9b0f48baf4a8ef8fc02a450a279948a8c0f02>`_  2020-11-20   ``Fix the default value for VaultBackend's config_path (#12518)``
`c34ef853c <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`008035450 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f12 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333c <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f59 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`85a18e13d <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78 <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d159 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`4e8f9cc8d <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`dd2442b1e <https://github.com/apache/airflow/commit/dd2442b1e66d4725e7193e0cab0548a4d8c71fbd>`_  2020-11-02   ``Vault with optional Variables or Connections (#11736)``
`5a439e84e <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e712971 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af80 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4 <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c38 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`9549274d1 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`3867f7662 <https://github.com/apache/airflow/commit/3867f7662559761864ec4e7be26b776c64c2f199>`_  2020-08-28   ``Update Google Cloud branding (#10615)``
`fdd9b6f65 <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`3696c34c2 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`2f2d8dbfa <https://github.com/apache/airflow/commit/2f2d8dbfafefb4be3dd80f22f31c649c8498f148>`_  2020-08-25   ``Remove all "noinspection" comments native to IntelliJ (#10525)``
`ee7ca128a <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`2f31b3060 <https://github.com/apache/airflow/commit/2f31b3060ed8274d5d1b1db7349ce607640b9199>`_  2020-07-08   ``Get Airflow configs with sensitive data from Secret Backends (#9645)``
`44d4ae809 <https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3>`_  2020-07-06   ``Upgrade to latest pre-commit checks (#9686)``
`a99aaeb49 <https://github.com/apache/airflow/commit/a99aaeb49672e913d5ff79606237f6f3614fc8f5>`_  2020-07-03   ``Allow setting Hashicorp Vault token from File (#9644)``
`d0e7db402 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a080 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`df693e0e3 <https://github.com/apache/airflow/commit/df693e0e3138f6601c4776cd529d8cb7bcde2f90>`_  2020-06-19   ``Add more authentication options for HashiCorp Vault classes (#8974)``
`c7e5bce57 <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`d47e070a7 <https://github.com/apache/airflow/commit/d47e070a79b574cca043ca9c06f91d47eecb3040>`_  2020-06-17   ``Add HashiCorp Vault Hook (split-out from Vault secret backend) (#9333)``
`f6bd817a3 <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`0b0e4f7a4 <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`375d1ca22 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8a <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4c <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`d8cb0b5dd <https://github.com/apache/airflow/commit/d8cb0b5ddb02d194742e374d9ac90dd8231f6e80>`_  2020-05-04   ``Support k8s auth method in Vault Secrets provider (#8640)``
`87969a350 <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`c1c88abfe <https://github.com/apache/airflow/commit/c1c88abfede7a36c3b1d1b511fbc6c03af46d363>`_  2020-03-28   ``Get Airflow Variables from Hashicorp Vault (#7944)``
`eb4af4f94 <https://github.com/apache/airflow/commit/eb4af4f944c77e67e167bbb6b0a2aaf075a95b50>`_  2020-03-28   ``Make BaseSecretsBackend.build_path generic (#7948)``
`686d7d50b <https://github.com/apache/airflow/commit/686d7d50bd21622724d6818021355bc6885fd3de>`_  2020-03-25   ``Standardize SecretBackend class names (#7846)``
`eef87b995 <https://github.com/apache/airflow/commit/eef87b9953347a65421f315a07dbef37ded9df66>`_  2020-03-23   ``[AIRFLOW-7105] Unify Secrets Backend method interfaces (#7830)``
`cdf1809fc <https://github.com/apache/airflow/commit/cdf1809fce0e59c8379a799f1738d8d813abbf51>`_  2020-03-23   ``[AIRFLOW-7104] Add Secret backend for GCP Secrets Manager (#7795)``
`a44beaf5b <https://github.com/apache/airflow/commit/a44beaf5bddae2a8de0429af45be5ff78a7d4d4e>`_  2020-03-19   ``[AIRFLOW-7076] Add support for HashiCorp Vault as Secrets Backend (#7741)``
================================================================================================  ===========  ==================================================================================
