
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


Package apache-airflow-providers-amazon
------------------------------------------------------

Amazon integration (including `Amazon Web Services (AWS) <https://aws.amazon.com/>`__).


This is detailed commit list of changes for versions provider package: ``amazon``.
For high-level changelog, see :doc:`package information including changelog <index>`.



1.2.0
.....

Latest change: 2021-02-27

================================================================================================  ===========  ==========================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ==========================================================================
`13854c32a <https://github.com/apache/airflow/commit/13854c32a38787af6d8a52ab2465cb6185c0b74c>`_  2021-02-27   ``Adding support to put extra arguments for Glue Job. (#14027)``
`0d6cae417 <https://github.com/apache/airflow/commit/0d6cae4172ff185ec4c0fc483bf556ce3252b7b0>`_  2021-02-24   ``Avoid using threads in S3 remote logging uplod (#14414)``
`ca35bd7f7 <https://github.com/apache/airflow/commit/ca35bd7f7f6bc2fb4f2afd7762114ce262c61941>`_  2021-02-21   ``By default PIP will install all packages in .local folder (#14125)``
`1b1472630 <https://github.com/apache/airflow/commit/1b147263076d48772d417c5154f2db86fc6a6877>`_  2021-02-11   ``Allow AWS Operator RedshiftToS3Transfer To Run a Custom Query (#14177)``
`9034f277e <https://github.com/apache/airflow/commit/9034f277ef935df98b63963c824ba71e0dcd92c7>`_  2021-02-10   ``Document configuration for email backend credentials. (#14006)``
`8c5594b02 <https://github.com/apache/airflow/commit/8c5594b02ffbfc631ebc2366dbde6d8c4e56d550>`_  2021-02-08   ``includes the STS token if STS credentials are used (#11227)``
`cddbf9c11 <https://github.com/apache/airflow/commit/cddbf9c11d092422e6695d7a5a5c859fdf140753>`_  2021-02-06   ``Use MongoDB color for MongoToS3Operator (#14103)``
`10343ec29 <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
================================================================================================  ===========  ==========================================================================

1.1.0
.....

Latest change: 2021-02-04

================================================================================================  ===========  =========================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  =========================================================================================
`88bdcfa0d <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98 <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`01049ddce <https://github.com/apache/airflow/commit/01049ddce210f475d6eae9b1cb306f750a1d6dd8>`_  2021-01-31   ``Add aws ses email backend for use with EmailOperator. (#13986)``
`ecfdc60bb <https://github.com/apache/airflow/commit/ecfdc60bb607fe0d13fa7e315476c607813abab6>`_  2021-01-29   ``Add bucket_name to template fileds in S3 operators (#13973)``
`d0ab7f6d3 <https://github.com/apache/airflow/commit/d0ab7f6d3a2976167f9c4fb309c502a4f866f983>`_  2021-01-25   ``Add ExasolToS3Operator (#13847)``
`6d55f329f <https://github.com/apache/airflow/commit/6d55f329f93c5cd1e94973194c0cd7caa65309e1>`_  2021-01-25   ``AWS Glue Crawler Integration (#13072)``
`f473ca713 <https://github.com/apache/airflow/commit/f473ca7130f844bc59477674e641b42b80698bb7>`_  2021-01-24   ``Replace 'google_cloud_storage_conn_id' by 'gcp_conn_id' when using 'GCSHook' (#13851)``
`a9ac2b040 <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef355 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`29730d720 <https://github.com/apache/airflow/commit/29730d720066a4c16d524e905de8cdf07e8cd129>`_  2021-01-20   ``Add acl_policy to S3CopyObjectOperator (#13773)``
`c065d3218 <https://github.com/apache/airflow/commit/c065d32189bfee80ab938d96ad74f6492e9c9b24>`_  2021-01-19   ``AllowDiskUse parameter and docs in MongotoS3Operator (#12033)``
`ab5fe56ac <https://github.com/apache/airflow/commit/ab5fe56ac4bda0d3fcdcbf58ed2632255b7ac713>`_  2021-01-16   ``Fix bug in GCSToS3Operator (#13718)``
`04d278f93 <https://github.com/apache/airflow/commit/04d278f93ffafb40fb6e95b41ecfa5f5cba5ef98>`_  2021-01-13   ``Add S3ToFTPOperator (#11747)``
`8d42d9ed6 <https://github.com/apache/airflow/commit/8d42d9ed69b03b372c6bc01309ef22e01b8db55f>`_  2021-01-11   ``add xcom push for ECSOperator (#12096)``
`308f1d066 <https://github.com/apache/airflow/commit/308f1d06668ad427fd2483077d8e60f55ee617e6>`_  2021-01-07   ``[AIRFLOW-3723] Add Gzip capability to mongo_to_S3 operator (#13187)``
`f69405fb0 <https://github.com/apache/airflow/commit/f69405fb0b7c236968c730e1ad31a60eea2338c4>`_  2021-01-07   ``Fix S3KeysUnchangedSensor so that template_fields work (#13490)``
`4e479e1e1 <https://github.com/apache/airflow/commit/4e479e1e1b8eea71df48f5cc08a7dd15929ba177>`_  2021-01-06   ``Add S3KeySizeSensor (#13049)``
`f7a1334ab <https://github.com/apache/airflow/commit/f7a1334abe4417409498daad52c97d3f0eb95137>`_  2021-01-02   ``Add 'mongo_collection' to template_fields in MongoToS3Operator (#13361)``
`bd74eb0ca <https://github.com/apache/airflow/commit/bd74eb0ca0bb5f81cd98e2c151257a404d4a55a5>`_  2020-12-31   ``Allow Tags on AWS Batch Job Submission (#13396)``
`295d66f91 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`625576a3a <https://github.com/apache/airflow/commit/625576a3af470cddad250735b74ba11e4880de0a>`_  2020-12-18   ``Fix spelling (#13135)``
`6cf76d7ac <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c8 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
================================================================================================  ===========  =========================================================================================

1.0.0
.....

Latest change: 2020-12-09

================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                            Committed    Subject
================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2 <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`d5589673a <https://github.com/apache/airflow/commit/d5589673a95aaced0b851ea0a4061a010a924a82>`_  2020-12-08   ``Move dummy_operator.py to dummy.py (#11178) (#11293)``
`b40dffa08 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f2478 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aa <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`02d94349b <https://github.com/apache/airflow/commit/02d94349be3d201ce9d37d7358573c937fd010df>`_  2020-11-29   ``Don't use time.time() or timezone.utcnow() for duration calculations (#12353)``
`de3b1e687 <https://github.com/apache/airflow/commit/de3b1e687b26c524c6909b7b4dfbb60d25019751>`_  2020-11-28   ``Move connection guides to provider documentation packages (#12653)``
`663259d4b <https://github.com/apache/airflow/commit/663259d4b541ab10ce55fec4d2460e23917062c2>`_  2020-11-25   ``Fix AWS DataSync tests failing (#11020)``
`3fa51f94d <https://github.com/apache/airflow/commit/3fa51f94d7a17f170ddc31908d36c91f4456a20b>`_  2020-11-24   ``Add check for duplicates in provider.yaml files (#12578)``
`ed09915a0 <https://github.com/apache/airflow/commit/ed09915a02b9b99e60689e647452addaab1688fc>`_  2020-11-23   ``[AIRFLOW-5115] Bugfix for S3KeySensor failing to accept template_fields (#12389)``
`370e7d07d <https://github.com/apache/airflow/commit/370e7d07d1ed1a53b73fe878425fdcd4c71a7ed1>`_  2020-11-21   ``Fix Python Docstring parameters (#12513)``
`c34ef853c <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`008035450 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f12 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333c <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`c94b1241a <https://github.com/apache/airflow/commit/c94b1241a144294f5f1c5f461d5e3b92e4a8fc38>`_  2020-11-13   ``Add extra error handling to S3 remote logging (#9908)``
`7825e8f59 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`250436d96 <https://github.com/apache/airflow/commit/250436d962c8c950d38c1eb5e54a998891648cc9>`_  2020-11-10   ``Fix spelling in Python files (#12230)``
`85a18e13d <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78 <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d159 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`fcb6b00ef <https://github.com/apache/airflow/commit/fcb6b00efef80c81272a30cfc618202a29e0c6a9>`_  2020-11-08   ``Add authentication to AWS with Google credentials (#12079)``
`fb6bddba0 <https://github.com/apache/airflow/commit/fb6bddba0c9e3e7ef2610b4fb3f73622e48d7ea0>`_  2020-11-07   ``In AWS Secrets backend, a lookup is optional (#12143)``
`cf9437d79 <https://github.com/apache/airflow/commit/cf9437d79f9658d1309e4bfe847fe63d52ec7b99>`_  2020-11-06   ``Simplify string expressions (#12123)``
`41bf172c1 <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b0 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5e77a6154 <https://github.com/apache/airflow/commit/5e77a61543d26e5466d885d639247aa5189c011d>`_  2020-11-02   ``Docstring fix for S3DeleteBucketOperator (#12049)``
`822285134 <https://github.com/apache/airflow/commit/8222851348aa81424c9bdcea994e25e0d6692709>`_  2020-10-29   ``Add Template Fields to RedshiftToS3Operator & S3ToRedshiftOperator (#11844)``
`db121f726 <https://github.com/apache/airflow/commit/db121f726b3c7a37aca1ea05eb4714f884456005>`_  2020-10-28   ``Add truncate table (before copy) option to S3ToRedshiftOperator (#9246)``
`5a439e84e <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`8afdb6ac6 <https://github.com/apache/airflow/commit/8afdb6ac6a7997cb14806bc2734c81c00ed8da97>`_  2020-10-26   ``Fix spellings (#11825)``
`872b1566a <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`6ce855af1 <https://github.com/apache/airflow/commit/6ce855af118daeaa4c249669079ab9d9aad23945>`_  2020-10-24   ``Fix spelling (#11821)``
`3934ef224 <https://github.com/apache/airflow/commit/3934ef22494db6d9613c229aaa82ea6a366b7c2f>`_  2020-10-24   ``Remove redundant builtins imports (#11809)``
`4c8e033c0 <https://github.com/apache/airflow/commit/4c8e033c0ee7d28963d504a9216205155f20f58f>`_  2020-10-24   ``Fix spelling and grammar (#11814)``
`483068745 <https://github.com/apache/airflow/commit/48306874538eea7cfd42358d5ebb59705204bfc4>`_  2020-10-24   ``Use Python 3 style super classes (#11806)``
`0df60b773 <https://github.com/apache/airflow/commit/0df60b773671ecf8d4e5f582ac2be200cf2a2edd>`_  2020-10-23   ``Add reattach flag to ECSOperator (#10643)``
`b9d677cdd <https://github.com/apache/airflow/commit/b9d677cdd660e0be8278a64658e73359276a9682>`_  2020-10-22   ``Add type hints to  aws provider (#11531)``
`349b0811c <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`674368f66 <https://github.com/apache/airflow/commit/674368f66cf61b2a105f326f23868ac3aee08807>`_  2020-10-19   ``Fixes MySQLToS3 float to int conversion (#10437)``
`0823d46a7 <https://github.com/apache/airflow/commit/0823d46a7f267f2e45195a175021825367938add>`_  2020-10-16   ``Add type annotations for AWS operators and hooks (#11434)``
`16e712971 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`d38a0a781 <https://github.com/apache/airflow/commit/d38a0a781e123c8c50313efdb23f767d6678afe0>`_  2020-10-12   ``added type hints for aws cloud formation (#11470)``
`d305876be <https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731>`_  2020-10-12   ``Remove redundant None provided as default to dict.get() (#11448)``
`c3e340584 <https://github.com/apache/airflow/commit/c3e340584bf1892c4f73aa9e7495b5823dab0c40>`_  2020-10-11   ``Change prefix of AwsDynamoDB hook module (#11209)``
`42a23d16f <https://github.com/apache/airflow/commit/42a23d16fe9b2f165b0805fb767ecbb825c93657>`_  2020-10-11   ``Update MySQLToS3Operator's s3_bucket to template_fields (#10778)``
`422b61a9d <https://github.com/apache/airflow/commit/422b61a9dd95ab9d00b239daa14d87d7cae5ae73>`_  2020-10-09   ``Adding ElastiCache Hook for creating, describing and deleting replication groups (#8701)``
`dd98b2149 <https://github.com/apache/airflow/commit/dd98b21494ff6036242b63268140abe1294b3657>`_  2020-10-06   ``Add acl_policy parameter to GCSToS3Operator (#10804) (#10829)``
`32b3cfbcf <https://github.com/apache/airflow/commit/32b3cfbcf0209cb062dd641c1232ab25d02d4d6d>`_  2020-10-06   ``Strict type check for all hooks in amazon (#11250)``
`6d573e8ab <https://github.com/apache/airflow/commit/6d573e8abbf87e3c7281347e03d428a6e5baccd4>`_  2020-10-03   ``Add s3 key to template fields for s3/redshift transfer operators (#10890)``
`0a0e1af80 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4 <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c38 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`00ffedb8c <https://github.com/apache/airflow/commit/00ffedb8c402eb5638782628eb706a5f28215eac>`_  2020-09-30   ``Add amazon glacier to GCS transfer operator (#10947)``
`e3f96ce7a <https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57>`_  2020-09-24   ``Fix incorrect Usage of Optional[bool] (#11138)``
`f3e87c503 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`b61225a88 <https://github.com/apache/airflow/commit/b61225a8850b20be17842c2428b91d873584c4da>`_  2020-09-21   ``Add D204 pydocstyle check (#11031)``
`2410f592a <https://github.com/apache/airflow/commit/2410f592a4ab160b377f1a9e5de3b7262b9851cc>`_  2020-09-19   ``Get Airflow configs with sensitive data from AWS Systems Manager (#11023)``
`2bf7b7cac <https://github.com/apache/airflow/commit/2bf7b7cac7858f5a6a495f1a9eb4780ec84f95b4>`_  2020-09-19   ``Add typing to amazon provider EMR (#10910)``
`9edfcb7ac <https://github.com/apache/airflow/commit/9edfcb7ac46917836ec956264da8876e58d92392>`_  2020-09-19   ``Support extra_args in S3Hook and GCSToS3Operator (#11001)``
`4e1f3a69d <https://github.com/apache/airflow/commit/4e1f3a69db8614c302e4916332555034053b935c>`_  2020-09-14   ``[AIRFLOW-10645] Add AWS Secrets Manager Hook (#10655)``
`e9add7916 <https://github.com/apache/airflow/commit/e9add79160e3a16bb348e30f4e83386a371dbc1e>`_  2020-09-14   ``Fix Failing static tests on Master (#10927)``
`383a118d2 <https://github.com/apache/airflow/commit/383a118d2df618e46d81c520cd2c4a31d81b33dd>`_  2020-09-14   ``Add more type annotations to AWS hooks (#10671)``
`9549274d1 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`2934220dc <https://github.com/apache/airflow/commit/2934220dc98e295764f7791d33e121629ed2fbbb>`_  2020-09-08   ``Always return a list from S3Hook list methods (#10774)``
`f40ac9b15 <https://github.com/apache/airflow/commit/f40ac9b151124dbcd87197d6ae38c85191d41f38>`_  2020-09-01   ``Add placement_strategy option (#9444)``
`e4878e677 <https://github.com/apache/airflow/commit/e4878e6775bbe5cb2a1d786e57e009271b78bba0>`_  2020-08-31   ``fix type hints for s3 hook read_key method (#10653)``
`2ca615cff <https://github.com/apache/airflow/commit/2ca615cffefe97dfa38e1b7f60d9ed33c6628992>`_  2020-08-29   ``Update Google Cloud branding (#10642)``
`8969b7185 <https://github.com/apache/airflow/commit/8969b7185ebc3c90168ce9a2fb97dfbc74d2bed9>`_  2020-08-28   ``Removed bad characters from AWS operator (#10590)``
`8349061f9 <https://github.com/apache/airflow/commit/8349061f9cb01a92c87edd349cc844c4053851e8>`_  2020-08-26   ``Improve Docstring for AWS Athena Hook/Operator (#10580)``
`fdd9b6f65 <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`3696c34c2 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`3734876d9 <https://github.com/apache/airflow/commit/3734876d9898067ee933b84af522d53df6160d7f>`_  2020-08-24   ``Implement impersonation in google operators (#10052)``
`ee7ca128a <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`c6358045f <https://github.com/apache/airflow/commit/c6358045f9d61af63c96833cb6682d6f382a6408>`_  2020-08-22   ``Fixes S3ToRedshift COPY query (#10436)``
`7c206a82a <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`27d08b76a <https://github.com/apache/airflow/commit/27d08b76a2d171d716a1599157a8a60a121dbec6>`_  2020-08-21   ``Amazon SES Hook (#10391)``
`dea345b05 <https://github.com/apache/airflow/commit/dea345b05c2cd226e70f97a3934d7456aa1cc754>`_  2020-08-17   ``Fix AwsGlueJobSensor to stop running after the Glue job finished (#9022)``
`f6734b3b8 <https://github.com/apache/airflow/commit/f6734b3b850d33d3712763f93c114e80f5af9ffb>`_  2020-08-12   ``Enable Sphinx spellcheck for doc generation (#10280)``
`82f744b87 <https://github.com/apache/airflow/commit/82f744b871bb2c5e9a2d628e1c45ae16c1244240>`_  2020-08-11   ``Add type annotations to AwsGlueJobHook, RedshiftHook modules (#10286)``
`19bc97d0c <https://github.com/apache/airflow/commit/19bc97d0ce436a6ec9d8e9a5adcd48c0a769d01f>`_  2020-08-10   ``Revert "Add Amazon SES hook (#10004)" (#10276)``
`f06fe616e <https://github.com/apache/airflow/commit/f06fe616e66256bdc53710de505c2c6b1bd21528>`_  2020-08-10   ``Add Amazon SES hook (#10004)``
`0c77ea8a3 <https://github.com/apache/airflow/commit/0c77ea8a3c417805f66d10f0c757ca218bf8dee0>`_  2020-08-06   ``Add type annotations to S3 hook module (#10164)``
`24c8e4c2d <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`9667314b2 <https://github.com/apache/airflow/commit/9667314b2fb879edc451793a8350123507e1cfd6>`_  2020-08-05   ``Add correct signatures for operators in amazon provider package (#10167)``
`000287753 <https://github.com/apache/airflow/commit/000287753b478f29e6c25442ac253e3a6c8e8c87>`_  2020-08-03   ``Improve Typing coverage of amazon/aws/athena (#10025)``
`53ada6e79 <https://github.com/apache/airflow/commit/53ada6e7911f411e80ebb00be9f07a7cc0788d01>`_  2020-08-03   ``Add S3KeysUnchangedSensor (#9817)``
`aeea71274 <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`2b8dea64e <https://github.com/apache/airflow/commit/2b8dea64e9e8716fba8c38a1b439f7835bbd2918>`_  2020-08-01   ``Fix typo in Athena sensor retries (#10079)``
`1508c43ec <https://github.com/apache/airflow/commit/1508c43ec9594e801b415dd82472fa017791b759>`_  2020-07-29   ``Adding new SageMaker operator for ProcessingJobs (#9594)``
`7d24b088c <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`8b10a4b35 <https://github.com/apache/airflow/commit/8b10a4b35e45d536a6475bfe1491ee75fad50186>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (#9982)``
`33f0cd265 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`e7c87fe45 <https://github.com/apache/airflow/commit/e7c87fe453c6a70ed087c7ffbccaacbf0d2831b9>`_  2020-07-20   ``Refactor AwsBaseHook._get_credentials (#9878)``
`2577f9334 <https://github.com/apache/airflow/commit/2577f9334a5cb71cccd97e62b0ae2d097cb99e1a>`_  2020-07-16   ``Fix S3FileTransformOperator to support S3 Select transformation only (#8936)``
`52b6efe1e <https://github.com/apache/airflow/commit/52b6efe1ecaae74b9c2497f565e116305d575a76>`_  2020-07-15   ``Add option to delete by prefix to S3DeleteObjectsOperator (#9350)``
`553bb7af7 <https://github.com/apache/airflow/commit/553bb7af7cb7a50f7141b5b89297713cee6d19f6>`_  2020-07-13   ``Keep functions signatures in decorators (#9786)``
`2f31b3060 <https://github.com/apache/airflow/commit/2f31b3060ed8274d5d1b1db7349ce607640b9199>`_  2020-07-08   ``Get Airflow configs with sensitive data from Secret Backends (#9645)``
`07b81029e <https://github.com/apache/airflow/commit/07b81029ebc2a296fb54181f2cec11fcc7704d9d>`_  2020-07-08   ``Allow AWSAthenaHook to get more than 1000/first page of results (#6075)``
`564192c16 <https://github.com/apache/airflow/commit/564192c1625a552456cebb3751978c08eebdb2a1>`_  2020-07-08   ``Add AWS StepFunctions integrations to the aws provider (#8749)``
`ecce1ace7 <https://github.com/apache/airflow/commit/ecce1ace7a277c948c61d7d4cbfc8632cc216559>`_  2020-07-08   ``[AIRFLOW-XXXX] Remove unnecessary docstring in AWSAthenaOperator``
`a79e2d4c4 <https://github.com/apache/airflow/commit/a79e2d4c4aa105f3fac5ae6a28e29af9cd572407>`_  2020-07-06   ``Move provider's log task handlers to the provider package (#9604)``
`ee20086b8 <https://github.com/apache/airflow/commit/ee20086b8c499fa40dcaac71652f21b466e7f80f>`_  2020-07-02   ``Move S3TaskHandler to the AWS provider package (#9602)``
`40add26d4 <https://github.com/apache/airflow/commit/40add26d459c2511a6d9d305ae7300f0d6104211>`_  2020-06-29   ``Remove almost all references to airflow.contrib (#9559)``
`c858babdd <https://github.com/apache/airflow/commit/c858babddf8b18b417993b5bfefec1c5635510da>`_  2020-06-26   ``Remove kwargs from Super calls in AWS Secrets Backends (#9523)``
`87fdbd070 <https://github.com/apache/airflow/commit/87fdbd0708d942af98d35604fe5962962e25d246>`_  2020-06-25   ``Use literal syntax instead of function calls to create data structure (#9516)``
`c7a454aa3 <https://github.com/apache/airflow/commit/c7a454aa32bf33133d042e8438ac259b32144b21>`_  2020-06-22   ``Add AWS ECS system test (#8888)``
`df8efd04f <https://github.com/apache/airflow/commit/df8efd04f394afc4b5affb677bc78d8b7bd5275a>`_  2020-06-21   ``Enable & Fix "Docstring Content Issues" PyDocStyle Check (#9460)``
`e13a14c87 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
`d0e7db402 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a080 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`992a18c84 <https://github.com/apache/airflow/commit/992a18c84a355d13e821c703e7364f12233c37dc>`_  2020-06-19   ``Move MySqlToS3Operator to transfers (#9400)``
`a60f589aa <https://github.com/apache/airflow/commit/a60f589aa251cc3df6bec5b306ad4a7f736f539f>`_  2020-06-19   ``Add MySqlToS3Operator (#9054)``
`c7e5bce57 <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`40bf8f28f <https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee>`_  2020-06-18   ``Detect automatically the lack of reference to the guide in the operator descriptions (#9290)``
`f6bd817a3 <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`58a8ec0e4 <https://github.com/apache/airflow/commit/58a8ec0e46f624ee0369dd156dd8fb4f81884a21>`_  2020-06-16   ``AWSBatchOperator <> ClientHook relation changed to composition (#9306)``
`a80cd25e8 <https://github.com/apache/airflow/commit/a80cd25e8eb7f8b5d89af26cdcd62a5bbe44d65c>`_  2020-06-15   ``Close/Flush byte stream in s3 hook load_string and load_bytes (#9211)``
`ffb857403 <https://github.com/apache/airflow/commit/ffb85740373f7adb70d28ec7d5a8886380170e5e>`_  2020-06-14   ``Decrypt secrets from SystemsManagerParameterStoreBackend (#9214)``
`a69b031f2 <https://github.com/apache/airflow/commit/a69b031f20c5a1cd032f9873394374f661811e8f>`_  2020-06-10   ``Add S3ToRedshift example dag and system test (#8877)``
`17adcea83 <https://github.com/apache/airflow/commit/17adcea835cb7b0cf2d8da0ac7dda5549cfa3e45>`_  2020-06-02   ``Fix handling of subprocess error handling in s3_file_transform and gcs (#9106)``
`357e11e0c <https://github.com/apache/airflow/commit/357e11e0cfb4c02833018e073bc4f5e5b52fae4f>`_  2020-05-29   ``Add Delete/Create S3 bucket operators (#8895)``
`1ed171bfb <https://github.com/apache/airflow/commit/1ed171bfb265ded8674058bdc425640d25f1f4fc>`_  2020-05-28   ``Add script_args for S3FileTransformOperator (#9019)``
`0b0e4f7a4 <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`1d36b0303 <https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea>`_  2020-05-23   ``Fix references in docs (#8984)``
`f946f96da <https://github.com/apache/airflow/commit/f946f96da45d8e6101805450d8cab7ccb2774ad0>`_  2020-05-23   ``Old json boto compat removed from dynamodb_to_s3 operator (#8987)``
`375d1ca22 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8a <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`f4edd90a9 <https://github.com/apache/airflow/commit/f4edd90a94b8f91bbefbbbfba367372399559596>`_  2020-05-16   ``Speed up TestAwsLambdaHook by not actually running a function (#8882)``
`92585ca4c <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`85bbab27d <https://github.com/apache/airflow/commit/85bbab27dbb4f55f6f322b894fe3d54797076c15>`_  2020-05-15   ``Add EMR operators howto docs (#8863)``
`e61b9bb9b <https://github.com/apache/airflow/commit/e61b9bb9bbe6d8a0621310f3583483b9135c6770>`_  2020-05-13   ``Add AWS EMR System tests (#8618)``
`ed3f5131a <https://github.com/apache/airflow/commit/ed3f5131a27e2ef0422f2495a4532630a6204f82>`_  2020-05-13   ``Correctly pass sleep time from AWSAthenaOperator down to the hook. (#8845)``
`7236862a1 <https://github.com/apache/airflow/commit/7236862a1f5361b5e99c03dd63dae9b966efcd24>`_  2020-05-12   ``[AIRFLOW-2310] Enable AWS Glue Job Integration (#6007)``
`d590e5e76 <https://github.com/apache/airflow/commit/d590e5e7679322bebb1472fa8c7ec6d183e4154a>`_  2020-05-11   ``Add option to propagate tags in ECSOperator (#8811)``
`0c3db84c3 <https://github.com/apache/airflow/commit/0c3db84c3ce5107f53ed5ecc48edfdfe1b97feff>`_  2020-05-11   ``[AIRFLOW-7068] Create EC2 Hook, Operator and Sensor (#7731)``
`cbebed2b4 <https://github.com/apache/airflow/commit/cbebed2b4d0bd1e0984c331c0270e83bf8df8540>`_  2020-05-10   ``Allow passing backend_kwargs to AWS SSM client (#8802)``
`c7788a689 <https://github.com/apache/airflow/commit/c7788a6894cb79c22153434dd9b977393b8236be>`_  2020-05-10   ``Add imap_attachment_to_s3 example dag and system test (#8669)``
`ff5b70149 <https://github.com/apache/airflow/commit/ff5b70149bf51012156378c8fc8b072c7c280d9d>`_  2020-05-07   ``Add google_api_to_s3_transfer example dags and system tests (#8581)``
`4421f011e <https://github.com/apache/airflow/commit/4421f011eeec2d1022a39933e27f530fb9f9c1b1>`_  2020-05-01   ``Improve template capabilities of EMR job and step operators (#8572)``
`379a884d6 <https://github.com/apache/airflow/commit/379a884d645a4d73db1c81e3450adc82571989ea>`_  2020-04-28   ``fix: aws hook should work without conn id (#8534)``
`74bc316c5 <https://github.com/apache/airflow/commit/74bc316c56192f14677e9406d3878887a836062b>`_  2020-04-27   ``[AIRFLOW-4438] Add Gzip compression to S3_hook (#8571)``
`7ea66a1a9 <https://github.com/apache/airflow/commit/7ea66a1a9594704869e82513d3a06fe35b6109b2>`_  2020-04-26   ``Add example DAG for ECSOperator (#8452)``
`b6434dedf <https://github.com/apache/airflow/commit/b6434dedf974085e5f8891446fa63104836c8fdf>`_  2020-04-24   ``[AIRFLOW-7111] Add generate_presigned_url method to S3Hook (#8441)``
`becedd5af <https://github.com/apache/airflow/commit/becedd5af8df01a0210e0a3fa78e619785f39908>`_  2020-04-19   ``Remove unrelated EC2 references in ECSOperator (#8451)``
`ab1290cb0 <https://github.com/apache/airflow/commit/ab1290cb0c5856fa85c8596bfdf780fcdfd99c31>`_  2020-04-13   ``Make launch_type parameter optional (#8248)``
`87969a350 <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`b46d6c060 <https://github.com/apache/airflow/commit/b46d6c060280da59193a28cf67e791eb825cb51c>`_  2020-04-08   ``Add support for AWS Secrets Manager as Secrets Backend (#8186)``
`68d1714f2 <https://github.com/apache/airflow/commit/68d1714f296989b7aad1a04b75dc033e76afb747>`_  2020-04-04   ``[AIRFLOW-6822] AWS hooks should cache boto3 client (#7541)``
`8a0240257 <https://github.com/apache/airflow/commit/8a02402576f83869d5134b4bddef5d73c15a8320>`_  2020-03-31   ``Rename CloudBaseHook to GoogleBaseHook and move it to google.common (#8011)``
`7239d9a82 <https://github.com/apache/airflow/commit/7239d9a82dbb3b9bdf27b531daa70338af9dd796>`_  2020-03-28   ``Get Airflow Variables from AWS Systems Manager Parameter Store (#7945)``
`eb4af4f94 <https://github.com/apache/airflow/commit/eb4af4f944c77e67e167bbb6b0a2aaf075a95b50>`_  2020-03-28   ``Make BaseSecretsBackend.build_path generic (#7948)``
`438da7241 <https://github.com/apache/airflow/commit/438da7241eb537e3ef5ae711629446155bf738a3>`_  2020-03-28   ``[AIRFLOW-5825] SageMakerEndpointOperator is not idempotent (#7891)``
`686d7d50b <https://github.com/apache/airflow/commit/686d7d50bd21622724d6818021355bc6885fd3de>`_  2020-03-25   ``Standardize SecretBackend class names (#7846)``
`eef87b995 <https://github.com/apache/airflow/commit/eef87b9953347a65421f315a07dbef37ded9df66>`_  2020-03-23   ``[AIRFLOW-7105] Unify Secrets Backend method interfaces (#7830)``
`5648dfbc3 <https://github.com/apache/airflow/commit/5648dfbc300337b10567ef4e07045ea29d33ec06>`_  2020-03-23   ``Add missing call to Super class in 'amazon', 'cloudant & 'databricks' providers (#7827)``
`a36002412 <https://github.com/apache/airflow/commit/a36002412334c445e4eab41fdbb85ef31b6fd384>`_  2020-03-19   ``[AIRFLOW-5705] Make AwsSsmSecretsBackend consistent with VaultBackend (#7753)``
`2a54512d7 <https://github.com/apache/airflow/commit/2a54512d785ba603ba71381dc3dfa049e9f74063>`_  2020-03-17   ``[AIRFLOW-5705] Fix bugs in AWS SSM Secrets Backend (#7745)``
`a8b5fc74d <https://github.com/apache/airflow/commit/a8b5fc74d07e50c91bb64cb66ca1a450aa5ce6e1>`_  2020-03-16   ``[AIRFLOW-4175] S3Hook load_file should support ACL policy paramete (#7733)``
`e31e9ddd2 <https://github.com/apache/airflow/commit/e31e9ddd2332e5d92422baf668acee441646ad68>`_  2020-03-14   ``[AIRFLOW-5705] Add secrets backend and support for AWS SSM (#6376)``
`3bb60afc7 <https://github.com/apache/airflow/commit/3bb60afc7b8319996385d681faac342afe2b3bd2>`_  2020-03-13   ``[AIRFLOW-6975] Base AWSHook AssumeRoleWithSAML (#7619)``
`c0c5f11ad <https://github.com/apache/airflow/commit/c0c5f11ad11a5a38e0553c1a36aa75eb83efae51>`_  2020-03-12   ``[AIRFLOW-6884] Make SageMakerTrainingOperator idempotent (#7598)``
`b7cdda1c6 <https://github.com/apache/airflow/commit/b7cdda1c64595bc7f85519337029de259e573fce>`_  2020-03-10   ``[AIRFLOW-4438] Add Gzip compression to S3_hook (#7680)``
`42eef3821 <https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e>`_  2020-03-07   ``[AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)``
`9a94ab246 <https://github.com/apache/airflow/commit/9a94ab246db8c09aa83bb6a6d245b1ca9563bcd9>`_  2020-03-01   ``[AIRFLOW-6962] Fix compeleted to completed (#7600)``
`1b38f6d9b <https://github.com/apache/airflow/commit/1b38f6d9b6710bd5e25fc16883599f1842ab7cb9>`_  2020-02-29   ``[AIRFLOW-5908] Add download_file to S3 Hook (#6577)``
`3ea3e1a2b <https://github.com/apache/airflow/commit/3ea3e1a2b580b7ed10efe668de0cc37b03673500>`_  2020-02-26   ``[AIRFLOW-6824] EMRAddStepsOperator problem with multi-step XCom (#7443)``
`6eaa7e3b1 <https://github.com/apache/airflow/commit/6eaa7e3b1845644d5ec65a00a997f4029bec9628>`_  2020-02-25   ``[AIRFLOW-5924] Automatically unify bucket name and key in S3Hook (#6574)``
`3320e432a <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`7d0e7122d <https://github.com/apache/airflow/commit/7d0e7122dd14576d834c6f66fe919a72b100b7f8>`_  2020-02-24   ``[AIRFLOW-6830] Add Subject/MessageAttributes to SNS hook and operator (#7451)``
`4d03e33c1 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`47a922b86 <https://github.com/apache/airflow/commit/47a922b86426968bfa07cc7892d2eeeca761d884>`_  2020-02-21   ``[AIRFLOW-6854] Fix missing typing_extensions on python 3.8 (#7474)``
`9cbd7de6d <https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b>`_  2020-02-18   ``[AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)``
`58c3542ed <https://github.com/apache/airflow/commit/58c3542ed25061320ce61dbe0adf451a44c738dd>`_  2020-02-12   ``[AIRFLOW-5231] Fix S3Hook.delete_objects method (#7375)``
`b7aa778b3 <https://github.com/apache/airflow/commit/b7aa778b38df2f116a1c20031e72fea8b97315bf>`_  2020-02-10   ``[AIRFLOW-6767] Correct name for default Athena workgroup (#7394)``
`9282185e6 <https://github.com/apache/airflow/commit/9282185e6624e64bb7f17447f81c1b2d1bb4d56d>`_  2020-02-09   ``[AIRFLOW-6761] Fix WorkGroup param in AWSAthenaHook (#7386)``
`94fccca97 <https://github.com/apache/airflow/commit/94fccca97030ee59d89f302a98137b17e7b01a33>`_  2020-02-04   ``[AIRFLOW-XXXX] Add pre-commit check for utf-8 file encoding (#7347)``
`f3ad5cf61 <https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a>`_  2020-02-03   ``[AIRFLOW-4681] Make sensors module pylint compatible (#7309)``
`88e40c714 <https://github.com/apache/airflow/commit/88e40c714d2853aa8966796945b2907c263fed08>`_  2020-02-03   ``[AIRFLOW-6716] Fix AWS Datasync Example DAG (#7339)``
`a311d3d82 <https://github.com/apache/airflow/commit/a311d3d82e0c2e32bcb56e29f33c95ed0a2a2ddc>`_  2020-02-03   ``[AIRFLOW-6718] Fix more occurrences of utils.dates.days_ago (#7341)``
`cb766b05b <https://github.com/apache/airflow/commit/cb766b05b17b80fd54a5ce6ac3ee35a631115000>`_  2020-02-03   ``[AIRFLOW-XXXX] Fix Static Checks on CI (#7342)``
`97a429f9d <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`7527eddc5 <https://github.com/apache/airflow/commit/7527eddc5e9729aa7e732209a07d57985f6c73e4>`_  2020-02-02   ``[AIRFLOW-4364] Make all code in airflow/providers/amazon pylint compatible (#7336)``
`cf141506a <https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721>`_  2020-02-02   ``[AIRFLOW-6708] Set unique logger names (#7330)``
`63aa3db88 <https://github.com/apache/airflow/commit/63aa3db88f8824efe79622301efd9f8ba75b991c>`_  2020-02-02   ``[AIRFLOW-6258] Add CloudFormation operators to AWS providers (#6824)``
`af4157fde <https://github.com/apache/airflow/commit/af4157fdeffc0c18492b518708c0db44815067ab>`_  2020-02-02   ``[AIRFLOW-6672] AWS DataSync - better logging of error message (#7288)``
`373c6aa4a <https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b>`_  2020-01-30   ``[AIRFLOW-6682] Move GCP classes to providers package (#7295)``
`83c037873 <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`1988a97e8 <https://github.com/apache/airflow/commit/1988a97e8f687e28a5a39b29677fb514e097753c>`_  2020-01-28   ``[AIRFLOW-6659] Move AWS Transfer operators to providers package (#7274)``
`ab10443e9 <https://github.com/apache/airflow/commit/ab10443e965269efe9c1efaf5fa33bcdbe609f13>`_  2020-01-28   ``[AIRFLOW-6424] Added a operator to modify EMR cluster (#7213)``
`40246132a <https://github.com/apache/airflow/commit/40246132a7ef3b07fe3173c6e7646ed6b53aad6e>`_  2020-01-28   ``[AIRFLOW-6654] AWS DataSync - bugfix when creating locations (#7270)``
`82c0e5aff <https://github.com/apache/airflow/commit/82c0e5aff6004f636b98e207c3caec40b403fbbe>`_  2020-01-28   ``[AIRFLOW-6655] Move AWS classes to providers (#7271)``
`599e4791c <https://github.com/apache/airflow/commit/599e4791c91cff411b1bf1c45555db5094c2b420>`_  2020-01-18   ``[AIRFLOW-6541] Use EmrJobFlowSensor for other states (#7146)``
`c319e81ca <https://github.com/apache/airflow/commit/c319e81cae1de31ad1373903252d8608ffce1fba>`_  2020-01-17   ``[AIRFLOW-6572] Move AWS classes to providers.amazon.aws package (#7178)``
`941a07057 <https://github.com/apache/airflow/commit/941a070578bc7d9410715b89658548167352cc4d>`_  2020-01-15   ``[AIRFLOW-6570] Add dag tag for all example dag (#7176)``
`78d8fe694 <https://github.com/apache/airflow/commit/78d8fe6944b689b9b0af99255286e34e06eedec3>`_  2020-01-08   ``[AIRFLOW-6245] Add custom waiters for AWS batch jobs (#6811)``
`e0b022725 <https://github.com/apache/airflow/commit/e0b022725749181bd4e30933e4a0ffefb993eede>`_  2019-12-28   ``[AIRFLOW-6319] Add support for AWS Athena workgroups (#6871)``
`57da45685 <https://github.com/apache/airflow/commit/57da45685457520d51a0967e2aeb5e5ff162dfa7>`_  2019-12-24   ``[AIRFLOW-6333] Bump Pylint to 2.4.4 & fix/disable new checks (#6888)``
`cf647c27e <https://github.com/apache/airflow/commit/cf647c27e0f35bbd1183bfcf87a106cbdb69d3fa>`_  2019-12-18   ``[AIRFLOW-6038] AWS DataSync reworked (#6773)``
`7502cad28 <https://github.com/apache/airflow/commit/7502cad2844139d57e4276d971c0706a361d9dbe>`_  2019-12-17   ``[AIRFLOW-6206] Move and rename AWS batch operator [AIP-21] (#6764)``
`c4c635df6 <https://github.com/apache/airflow/commit/c4c635df6906f56e01724573923e19763bb0da62>`_  2019-12-17   ``[AIRFLOW-6083] Adding ability to pass custom configuration to lambda client. (#6678)``
`4fb498f87 <https://github.com/apache/airflow/commit/4fb498f87ef89acc30f2576ebc5090ab0653159e>`_  2019-12-09   ``[AIRFLOW-6072] aws_hook: Outbound http proxy setting and other enhancements (#6686)``
`a1e2f8635 <https://github.com/apache/airflow/commit/a1e2f863526973b17892ec31caf09eded95c1cd2>`_  2019-11-20   ``[AIRFLOW-6021] Replace list literal with list constructor (#6617)``
`baae14084 <https://github.com/apache/airflow/commit/baae140847cdf9d84e905fb6d1f119d6950eecf9>`_  2019-11-19   ``[AIRFLOW-5781] AIP-21 Migrate AWS Kinesis to /providers/amazon/aws (#6588)``
`504cfbac1 <https://github.com/apache/airflow/commit/504cfbac1a4ec2e2fd169523ed357808f63881bb>`_  2019-11-18   ``[AIRFLOW-5783] AIP-21 Move aws redshift into providers structure (#6539)``
`992f0e3ac <https://github.com/apache/airflow/commit/992f0e3acf11163294508858515a5f79116e3ad8>`_  2019-11-12   ``AIRFLOW-5824: AWS DataSync Hook and Operators added (#6512)``
`c015eb2f6 <https://github.com/apache/airflow/commit/c015eb2f6496b9721afda9e85d5d4af3bbe0696b>`_  2019-11-10   ``[AIRFLOW-5786] Migrate AWS SNS to /providers/amazon/aws (#6502)``
`3d76fb4bf <https://github.com/apache/airflow/commit/3d76fb4bf25e5b7d3d30e0d64867b5999b77f0b0>`_  2019-11-09   ``[AIRFLOW-5782] Migrate AWS Lambda to /providers/amazon/aws [AIP-21] (#6518)``
================================================================================================  ===========  ======================================================================================================================================================================
