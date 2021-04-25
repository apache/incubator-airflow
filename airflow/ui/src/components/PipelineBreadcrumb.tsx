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
import {
  Box, Flex, Heading, useColorModeValue,
} from '@chakra-ui/react';

import type {
  Dag as DagType,
  DagRun as DagRunType,
  Task as TaskType,
} from 'interfaces';

interface Props {
  dagId: DagType['dagId'];
  dagRunId?: DagRunType['dagRunId'];
  taskId?: TaskType['taskId'];
}

const PipelineBreadcrumb: React.FC<Props> = ({ dagId, dagRunId, taskId }) => {
  const dividerColor = useColorModeValue('gray.100', 'gray.700');

  return (
    <Flex py={2}>
      <Box>
        <Heading as="h5" size="xs" color="gray.500">PIPELINE</Heading>
        <Heading as="h4" size="sm">
          {!dagRunId && dagId}
          {dagRunId && (
            <Box
              as={Link}
              to={`/pipelines/${dagId}`}
              color="currentColor"
              _hover={{ color: 'teal.500' }}
            >
              {dagId}
            </Box>
          )}
        </Heading>
      </Box>
      {dagRunId && (
        <>
          <Box color={dividerColor} px={2} fontSize="2em" lineHeight="1em">/</Box>
          <Box>
            <Heading as="h5" size="xs" color="gray.500">RUN</Heading>
            <Heading as="h4" size="sm">
              {!taskId && dagRunId}
              {taskId && (
                <Box
                  as={Link}
                  to={`/pipelines/${dagId}/${dagRunId}`}
                  color="currentColor"
                  _hover={{ color: 'teal.500' }}
                >
                  {dagRunId}
                </Box>
              )}
            </Heading>
          </Box>
        </>
      )}
      {taskId && (
        <>
          <Box color={dividerColor} px={2} fontSize="2em" lineHeight="1em">/</Box>
          <Box>
            <Heading as="h5" size="xs" color="gray.500">TASK INSTANCE</Heading>
            <Heading as="h4" size="sm">{taskId}</Heading>
          </Box>
        </>
      )}
    </Flex>
  );
};

export default PipelineBreadcrumb;
