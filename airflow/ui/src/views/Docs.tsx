/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React from 'react';
import {
  Box,
  Button,
  Flex,
  Heading,
  Icon,
  Link,
  List,
  ListItem,
  Text,
  useColorModeValue,
} from '@chakra-ui/react';
import { FaGithub } from 'react-icons/fa';
import { FiExternalLink, FiGlobe } from 'react-icons/fi';

import AppContainer from 'components/AppContainer';

const Docs: React.FC = () => {
  // TEMP: This static list needs to be provided w/ a (currently non-existent) API endpoint
  const providers = [
    { path: 'amazon', name: 'Amazon' },
    { path: 'apache-beam', name: 'Apache Beam' },
    { path: 'apache-cassandra', name: 'Apache Cassandra' },
    { path: 'apache-druid', name: 'Apache Druid' },
    { path: 'apache-hdfs', name: 'Apache HDFS' },
    { path: 'apache-hive', name: 'Apache Hive' },
    { path: 'apache-kylin', name: 'Apache Kylin' },
    { path: 'apache-livy', name: 'Apache Livy' },
    { path: 'apache-pig', name: 'Apache Pig' },
    { path: 'apache-pinot', name: 'Apache Pinot' },
    { path: 'apache-spark', name: 'Apache Spark' },
    { path: 'apache-sqoop', name: 'Apache Sqoop' },
    { path: 'celery', name: 'Celery' },
    { path: 'cloudant', name: 'IBM Cloudant' },
    { path: 'cncf-kubernetes', name: 'Kubernetes' },
    { path: 'databricks', name: 'Databricks' },
    { path: 'datadog', name: 'Datadog' },
    { path: 'dingding', name: 'Dingding' },
    { path: 'discord', name: 'Discord' },
    { path: 'docker', name: 'Docker' },
    { path: 'elasticsearch', name: 'Elasticsearch' },
    { path: 'exasol', name: 'Exasol' },
    { path: 'facebook', name: 'Facebook' },
    { path: 'ftp', name: 'File Transfer Protocol (FTP)' },
    { path: 'google', name: 'Google' },
    { path: 'grpc', name: 'gRPC' },
    { path: 'hashicorp', name: 'Hashicorp' },
    { path: 'http', name: 'Hypertext Transfer Protocol (HTTP)' },
    { path: 'imap', name: 'Internet Message Access Protocol (IMAP)' },
    { path: 'jdbc', name: 'Java Database Connectivity (JDBC)' },
    { path: 'jenkins', name: 'Jenkins' },
    { path: 'jira', name: 'Jira' },
    { path: 'microsoft-azure', name: 'Microsoft Azure' },
    { path: 'microsoft-mssql', name: 'Microsoft SQL Server (MSSQL)' },
    { path: 'microsoft-winrm', name: 'Windows Remote Management (WinRM)' },
    { path: 'mongo', name: 'MongoDB' },
    { path: 'mysql', name: 'MySQL' },
    { path: 'neo4j', name: 'Neo4J' },
    { path: 'odbc', name: 'ODBC' },
    { path: 'openfaas', name: 'OpenFaaS' },
    { path: 'opsgenie', name: 'Opsgenie' },
    { path: 'oracle', name: 'Oracle' },
    { path: 'pagerduty', name: 'Pagerduty' },
    { path: 'papermill', name: 'Papermill' },
    { path: 'plexus', name: 'Plexus' },
    { path: 'postgres', name: 'PostgreSQL' },
    { path: 'presto', name: 'Presto' },
    { path: 'qubole', name: 'Qubole' },
    { path: 'redis', name: 'Redis' },
    { path: 'salesforce', name: 'Salesforce' },
    { path: 'samba', name: 'Samba' },
    { path: 'segment', name: 'Segment' },
    { path: 'sendgrid', name: 'Sendgrid' },
    { path: 'sftp', name: 'SFTP' },
    { path: 'singularity', name: 'Singularity' },
    { path: 'slack', name: 'Slack' },
    { path: 'snowflake', name: 'Snowflake' },
    { path: 'sqlite', name: 'SQLite' },
    { path: 'ssh', name: 'SSH' },
    { path: 'telegram', name: 'Telegram' },
    { path: 'vertica', name: 'Vertica' },
    { path: 'yandex', name: 'Yandex' },
    { path: 'zendesk', name: 'Zendesk' },
  ];

  const webURL = process.env.WEBSERVER_URL;

  return (
    <AppContainer>
      <Box mx="auto" my={8} maxWidth="900px">
        <Flex mt={8}>
          <Box flex="1">
            <Heading as="h1">Documentation</Heading>
            <Text mt={4}>
              Apache Airflow Core, which includes webserver, scheduler, CLI and other components
              that are needed for minimal Airflow&nbsp;installation.
            </Text>
            <Button
              as="a"
              href="https://airflow.apache.org/docs/apache-airflow/stable/index.html"
              variant="solid"
              rightIcon={<FiExternalLink />}
              mt={4}
              target="_blank"
              rel="noopener noreferrer"
            >
              Apache Airflow Docs
            </Button>
          </Box>
          <Box ml={8} p={4} bg={useColorModeValue('gray.100', 'gray.700')} borderRadius="md">
            <Heading as="h3" size="sm">Links</Heading>
            <List mt={4} spacing={2}>
              <ListItem>
                <Link href="https://airflow.apache.org/" isExternal color="teal.500">
                  <Icon as={FiGlobe} mr={1} />
                  Apache Airflow Website
                </Link>
              </ListItem>
              <ListItem>
                <Link href="https://github.com/apache/airflow" isExternal color="teal.500">
                  <Icon as={FaGithub} mr={1} />
                  apache/airflow on GitHub
                </Link>
              </ListItem>
            </List>
          </Box>
        </Flex>
        <Box mt={10}>
          <Heading as="h3" size="lg">REST API Reference</Heading>
          <Flex mt={4}>
            <Button
              as="a"
              href={`${webURL}/api/v1/ui/`}
              target="_blank"
              rel="noopener noreferrer"
              variant="outline"
              rightIcon={<FiExternalLink />}
              mr={2}
            >
              Swagger
            </Button>
            <Button
              as="a"
              href={`${webURL}/redoc`}
              target="_blank"
              rel="noopener noreferrer"
              variant="outline"
              rightIcon={<FiExternalLink />}
            >
              Redoc
            </Button>
          </Flex>
        </Box>
        <Box mt={10}>
          <Heading as="h3" size="lg">Providers Packages</Heading>
          <Text mt={4}>
            Providers packages include integrations with third party integrations.
            They are updated independently of the Apache Airflow&nbsp;core.
          </Text>

          <List spacing={2} mt={4} style={{ columns: 3 }}>
            {providers.map((p) => (
              <ListItem key={p.path}>
                <Link
                  href={`https://airflow.apache.org/docs/apache-airflow-providers-${p.path}/stable/index.html`}
                  isExternal
                  color="teal.500"
                >
                  {p.name}
                </Link>
              </ListItem>
            ))}
          </List>
        </Box>
      </Box>
    </AppContainer>
  );
};

export default Docs;
