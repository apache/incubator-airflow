
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


Package apache-airflow-providers-apache-hive
------------------------------------------------------

`Apache Hive <https://hive.apache.org/>`__


This is detailed commit list of changes for versions provider package: ``apache.hive``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.0.2
.....

Latest change: 2021-02-05

================================================================================================  ===========  =======================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =======================================================================
`10343ec29 <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
================================================================================================  ===========  =======================================================================

1.0.1
.....

Latest change: 2021-02-04

================================================================================================  ===========  ===========================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ===========================================================================
`88bdcfa0d <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98 <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`a9ac2b040 <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`5f81fc73c <https://github.com/apache/airflow/commit/5f81fc73c8ea3fc1c3b08080f439fa123926f250>`_  2021-01-03   ``Fix: Remove password if in LDAP or CUSTOM mode HiveServer2Hook (#11767)``
`4f494d4d9 <https://github.com/apache/airflow/commit/4f494d4d92d15f088c54019fbb8a594024bfcf2c>`_  2021-01-03   ``Fix few typos (#13450)``
`295d66f91 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`6cf76d7ac <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c8 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
================================================================================================  ===========  ===========================================================================

1.0.0
.....

Latest change: 2020-12-09

================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2 <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`a075b6df9 <https://github.com/apache/airflow/commit/a075b6df99a4f5e21d198f7be56b577432e6f9db>`_  2020-12-09   ``Rename remaining Sensors to match AIP-21 (#12927)``
`b40dffa08 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f2478 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`2037303ee <https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143>`_  2020-11-29   ``Adds support for Connection/Hook discovery from providers (#12466)``
`c34ef853c <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`008035450 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`ae7cb4a1e <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333c <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`7825e8f59 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`250436d96 <https://github.com/apache/airflow/commit/250436d962c8c950d38c1eb5e54a998891648cc9>`_  2020-11-10   ``Fix spelling in Python files (#12230)``
`502ba309e <https://github.com/apache/airflow/commit/502ba309ea470943f0e99c634269e3d2d13ce6ca>`_  2020-11-10   ``Enable Markdownlint rule - MD022/blanks-around-headings (#12225)``
`85a18e13d <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78 <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d159 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`41bf172c1 <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b0 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5a439e84e <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`872b1566a <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`349b0811c <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`16e712971 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`0a0e1af80 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4 <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c38 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`e3f96ce7a <https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57>`_  2020-09-24   ``Fix incorrect Usage of Optional[bool] (#11138)``
`f3e87c503 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`9549274d1 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`ac943c9e1 <https://github.com/apache/airflow/commit/ac943c9e18f75259d531dbda8c51e650f57faa4c>`_  2020-09-08   ``[AIRFLOW-3964][AIP-17] Consolidate and de-dup sensor tasks using Smart Sensor (#5499)``
`fdd9b6f65 <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`d76026545 <https://github.com/apache/airflow/commit/d7602654526fdd2876466371404784bd17cfe0d2>`_  2020-08-25   ``PyDocStyle: No whitespaces allowed surrounding docstring text (#10533)``
`3696c34c2 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`ee7ca128a <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`27339a5a0 <https://github.com/apache/airflow/commit/27339a5a0f9e382dbc7d32a128f0831a48ef9a12>`_  2020-08-22   ``Remove mentions of Airflow Gitter (#10460)``
`7c206a82a <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`8f8db8959 <https://github.com/apache/airflow/commit/8f8db8959e526be54d700845d36ee9f315bae2ea>`_  2020-08-12   ``DbApiHook: Support kwargs in get_pandas_df (#9730)``
`b43f90abf <https://github.com/apache/airflow/commit/b43f90abf4c7219d5d59cccb0514256bd3f2fdc7>`_  2020-08-09   ``Fix various typos in the repo (#10263)``
`3b3287d7a <https://github.com/apache/airflow/commit/3b3287d7acc76430f12b758d52cec61c7f74e726>`_  2020-08-05   ``Enforce keyword only arguments on apache operators (#10170)``
`7d24b088c <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`33f0cd265 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`c2db0dfeb <https://github.com/apache/airflow/commit/c2db0dfeb13ee679bf4d7b57874f0fcb39c0f0ed>`_  2020-07-22   ``More strict rules in mypy (#9705) (#9906)``
`5013fda8f <https://github.com/apache/airflow/commit/5013fda8f072e633c114fb39fb59a22f60200b40>`_  2020-07-20   ``Add drop_partition functionality for HiveMetastoreHook (#9472)``
`4d74ac211 <https://github.com/apache/airflow/commit/4d74ac2111862186598daf92cbf2c525617061c2>`_  2020-07-19   ``Increase typing for Apache and http provider package (#9729)``
`44d4ae809 <https://github.com/apache/airflow/commit/44d4ae809c1e3784ff95b6a5e95113c3412e56b3>`_  2020-07-06   ``Upgrade to latest pre-commit checks (#9686)``
`e13a14c87 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
`d0e7db402 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a080 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`c7e5bce57 <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`f6bd817a3 <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`c78e2a5fe <https://github.com/apache/airflow/commit/c78e2a5feae15e84b05430cfc5935f0e289fb6b4>`_  2020-06-16   ``Make hive macros py3 compatible (#8598)``
`6350fd6eb <https://github.com/apache/airflow/commit/6350fd6ebb9958982cb3fa1d466168fc31708035>`_  2020-06-08   ``Don't use the term "whitelist" - language matters (#9174)``
`10796cb7c <https://github.com/apache/airflow/commit/10796cb7ce52c8ac2f68024e531fdda779547bdf>`_  2020-06-03   ``Remove Hive/Hadoop/Java dependency from unit tests (#9029)``
`0b0e4f7a4 <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`cdb3f2545 <https://github.com/apache/airflow/commit/cdb3f25456e49d0199cd7ccd680626dac01c9be6>`_  2020-05-26   ``All classes in backport providers are now importable in Airflow 1.10 (#8991)``
`375d1ca22 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8a <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`92585ca4c <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`93ea05880 <https://github.com/apache/airflow/commit/93ea05880283a56e3d42ab07db7453977a3de8ec>`_  2020-04-21   ``[AIRFLOW-7059] pass hive_conf to get_pandas_df in HiveServer2Hook (#8380)``
`87969a350 <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`cb0bf4a14 <https://github.com/apache/airflow/commit/cb0bf4a142656ee40b43a01660b6f6b08a9840fa>`_  2020-03-30   ``Remove sql like function in base_hook (#7901)``
`4bde99f13 <https://github.com/apache/airflow/commit/4bde99f1323d72f6c84c1548079d5e98fc0a2a9a>`_  2020-03-23   ``Make airflow/providers pylint compatible (#7802)``
`7e6372a68 <https://github.com/apache/airflow/commit/7e6372a681a2a543f4710b083219aeb53b074388>`_  2020-03-23   ``Add call to Super call in apache providers (#7820)``
`3320e432a <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`4d03e33c1 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`f3ad5cf61 <https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a>`_  2020-02-03   ``[AIRFLOW-4681] Make sensors module pylint compatible (#7309)``
`97a429f9d <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`83c037873 <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`057f3ae3a <https://github.com/apache/airflow/commit/057f3ae3a4afedf6d462ecf58b01dd6304d3e135>`_  2020-01-29   ``[AIRFLOW-6670][depends on AIRFLOW-6669] Move contrib operators to providers package (#7286)``
`059eda05f <https://github.com/apache/airflow/commit/059eda05f82fefce4410f44f761f945a27d83daf>`_  2020-01-21   ``[AIRFLOW-6610] Move software classes to providers package (#7231)``
`0481b9a95 <https://github.com/apache/airflow/commit/0481b9a95786a62de4776a735ae80e746583ef2b>`_  2020-01-12   ``[AIRFLOW-6539][AIP-21] Move Apache classes to providers.apache package (#7142)``
================================================================================================  ===========  ======================================================================================================================================================================
