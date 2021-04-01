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
import { Link } from 'react-router-dom';
import useReactRouter from 'use-react-router';
import {
  Box,
  Flex,
  Heading,
  useColorModeValue,
} from '@chakra-ui/react';

import AppContainer from 'components/AppContainer';

import { useDagRuns, useTaskInstances } from 'api';
import { defaultDagRuns, defaultTaskInstances } from 'api/defaults';

import type {
  Dag as DagType, DagRun as DagRunType, TaskInstance as TaskInstanceType,
} from 'interfaces';

interface RouterProps {
  match: { params: { dagId: DagType['dagId'], dagRunId: DagRunType['dagRunId'] } }
}

const PipelineContainer: React.FC = ({ children }) => {
  const { match: { params: { dagId, dagRunId } } }: RouterProps = useReactRouter();

  const { data: { dagRuns } = defaultDagRuns } = useDagRuns(dagId);
  const { data: { taskInstances } = defaultTaskInstances } = useTaskInstances(dagId, dagRunId);

  const linkColor = useColorModeValue('gray.400', 'gray.500');
  const dividerColor = useColorModeValue('gray.100', 'gray.700');

  return (
    <AppContainer
      breadcrumb={(
        <Heading as="h1" size="md">
          <Box
            as="span"
            color={linkColor}
            _hover={{ color: 'teal.500' }}
          >
            <Link to="/pipelines" color="currentColor">Pipelines</Link>
            /
          </Box>
          {dagId}
        </Heading>
      )}
    >
      <Flex height="100%">
        <Box flex="1" borderRightWidth="2px" borderColor={dividerColor}>
          <Heading mb={2}>Runs</Heading>
          {dagRuns.map((dagRun: DagRunType) => (
            <Box key={dagRun.dagRunId}>
              <Link to={`/pipelines/${dagId}/${dagRun.dagRunId}`}>{dagRun.dagRunId}</Link>
            </Box>
          ))}
          {dagRunId && (
            <>
              <Heading mb={2} size="md" mt={8}>Task Instances:</Heading>
              {taskInstances.map((ti: TaskInstanceType) => (
                <Box key={ti.taskId}>
                  <Link to={`/pipelines/${dagId}/${dagRunId}/${ti.taskId}`}>{ti.taskId}</Link>
                </Box>
              ))}
            </>
          )}
        </Box>
        <Box width="50%" pl={4} ml="2px">
          {children}
        </Box>
      </Flex>
    </AppContainer>
  );
};

export default PipelineContainer;
