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
  Flex,
  useColorModeValue,
} from '@chakra-ui/react';

import { useVersion } from 'api';
import { defaultVersion } from 'api/defaults';
import AppHeader from './AppHeader';
import AppNav from './AppNav';

interface Props {
  breadcrumb?: React.ReactNode;
}

const AppContainer: React.FC<Props> = ({ children, breadcrumb }) => {
  const { data: { version, gitVersion } = defaultVersion } = useVersion();
  const bodyBg = useColorModeValue('white', 'gray.800');
  const overlayBg = useColorModeValue('gray.100', 'gray.700');

  return (
    <Flex width="100vw" height="100vh" alignItems="stretch">
      <AppNav bodyBg={bodyBg} overlayBg={overlayBg} />
      <Box flex="1" alignItems="stretch">
        <AppHeader bodyBg={bodyBg} overlayBg={overlayBg} breadcrumb={breadcrumb} />
        <Flex direction="column" height="100vh" pt="56px" overflowY="scroll">
          <Box
            as="main"
            role="main"
            flex="1"
            px="4"
          >
            {children}
          </Box>
          <Box
            as="footer"
            role="contentinfo"
            p="4"
            color={useColorModeValue('gray.600', 'gray.300')}
            bg={overlayBg}
          >
            Apache Airflow
            {' '}
            <a
              href={`https://pypi.python.org/pypi/apache-airflow/${version}`}
              target="_blank"
              rel="noreferrer"
            >
              {`v${version}`}
            </a>
            {gitVersion && (
              <>
                <br />
                Git Version:
                {' '}
                {gitVersion}
              </>
            )}
          </Box>
        </Flex>
      </Box>
    </Flex>
  );
};

export default AppContainer;
