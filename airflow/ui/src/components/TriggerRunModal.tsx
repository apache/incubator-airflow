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

import React, { ChangeEvent, useState } from 'react';
import {
  Button,
  FormControl,
  FormLabel,
  Modal,
  ModalHeader,
  ModalFooter,
  ModalCloseButton,
  ModalOverlay,
  ModalContent,
  ModalBody,
  Textarea,
} from '@chakra-ui/react';

import type { Dag } from 'interfaces';
import { useTriggerRun } from 'api';

interface Props {
  dagId: Dag['dagId'];
  isOpen: boolean;
  onClose: () => void;
}

const TriggerRunModal: React.FC<Props> = ({ dagId, isOpen, onClose }) => {
  const mutation = useTriggerRun(dagId);
  const [config, setConfig] = useState('{}');

  const onTrigger = () => {
    mutation.mutate({
      conf: JSON.parse(config),
      executionDate: new Date(),
    });
    onClose();
  };

  return (
    <Modal size="lg" isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>
          Trigger Run:
          {' '}
          {dagId}
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <FormControl>
            <FormLabel htmlFor="configuration">Configuration JSON (Optional)</FormLabel>
            <Textarea name="configuration" value={config} onChange={(e: ChangeEvent<HTMLTextAreaElement>) => setConfig(e.target.value)} />
          </FormControl>
        </ModalBody>
        <ModalFooter>
          <Button variant="ghost" onClick={onClose}>Cancel</Button>
          <Button ml={2} onClick={onTrigger}>
            Trigger
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default TriggerRunModal;
