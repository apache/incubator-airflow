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
import { Link as RouterLink } from 'react-router-dom';
import {
  Flex,
  Link,
  Tr,
  Td,
  Tag,
  Tooltip,
  useColorModeValue,
  Switch,
  useDisclosure,
  IconButton,
} from '@chakra-ui/react';

import TriggerRunModal from 'components/TriggerRunModal';
import compareObjectProps from 'utils/memo';
import type { Dag, DagTag } from 'interfaces';
import { useSaveDag } from 'api';
import { MdPlayArrow } from 'react-icons/md';

interface Props {
  dag: Dag;
}

const Row: React.FC<Props> = ({ dag }) => {
  const { isOpen, onToggle, onClose } = useDisclosure();
  const mutation = useSaveDag(dag.dagId);
  const togglePaused = () => mutation.mutate({ isPaused: !dag.isPaused });

  const oddColor = useColorModeValue('gray.50', 'gray.900');
  const hoverColor = useColorModeValue('gray.100', 'gray.700');

  return (
    <Tr
      _odd={{ backgroundColor: oddColor }}
      _hover={{ backgroundColor: hoverColor }}
    >
      <Td onClick={(e) => e.stopPropagation()} paddingRight="0" width="58px">
        <Tooltip
          label={dag.isPaused ? 'Activate DAG' : 'Pause DAG'}
          aria-label={dag.isPaused ? 'Activate DAG' : 'Pause DAG'}
          hasArrow
        >
          {/* span helps tooltip find its position */}
          <span>
            <Switch
              role="switch"
              isChecked={!dag.isPaused}
              onChange={togglePaused}
            />
          </span>
        </Tooltip>
      </Td>
      <Td>
        <Flex alignItems="center">
          <Link
            as={RouterLink}
            to={`/pipelines/${dag.dagId}`}
            fontWeight="bold"
          >
            {dag.dagId}
          </Link>
          {dag.tags.map((tag: DagTag) => (
            <Tag
              size="sm"
              mt="1"
              ml="1"
              mb="1"
              key={tag.name}
            >
              {tag.name}
            </Tag>
          ))}
        </Flex>
      </Td>
      <Td textAlign="right">
        <Tooltip
          label="Trigger DAG"
          aria-label="Trigger DAG"
          hasArrow
        >
          <IconButton
            size="sm"
            aria-label="Trigger Dag"
            icon={<MdPlayArrow />}
            onClick={onToggle}
          />
        </Tooltip>
        <TriggerRunModal dagId={dag.dagId} isOpen={isOpen} onClose={onClose} />
      </Td>
    </Tr>
  );
};

export default React.memo(Row, compareObjectProps);
