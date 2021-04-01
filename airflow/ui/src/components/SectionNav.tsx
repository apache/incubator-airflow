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
  useColorModeValue,
} from '@chakra-ui/react';

import SectionNavBtn from 'components/SectionNavBtn';

interface Props {
  currentView: string;
  navItems: {
    label: string;
    path: string;
  }[]
}

const SectionNav: React.FC<Props> = ({ currentView, navItems }) => {
  const bg = useColorModeValue('gray.100', 'gray.700');
  return (
    <Box
      pt={2}
      mx={-4}
      px={4}
      pb={2}
      bg={bg}
    >
      <Box
        display="flex"
        alignItems="center"
        justifyContent="space-between"
      >
        <Box as="nav">
          {navItems.map((item) => (
            <SectionNavBtn key={item.label} item={item} currentView={currentView} />
          ))}
        </Box>
      </Box>
    </Box>
  );
};

export default SectionNav;
