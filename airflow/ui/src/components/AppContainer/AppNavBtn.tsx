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
import { Link, useLocation } from 'react-router-dom';
import {
  Box,
  Icon,
  Tooltip,
} from '@chakra-ui/react';

import type { IconType } from 'react-icons/lib';

interface Props {
  navItem: {
    label: string;
    icon: IconType | typeof Icon;
    path?: string;
    activePath?: string;
    href?: string;
  };
}

const AppNavBtn: React.FC<Props> = ({ navItem }) => {
  const location = useLocation();
  const {
    label, icon, path, href, activePath,
  } = navItem;
  const isHome = activePath === '/';
  const isActive = activePath && ((isHome && location.pathname === '/') || (!isHome && location.pathname.includes(activePath)));

  return (
    <Tooltip
      key={label}
      label={label}
      aria-label={label}
      placement="right"
      hasArrow
    >
      <Box
        as={Link}
        to={path || ''}
        href={href}
        target={href && '_blank'}
        aria-label={label}
        display="flex"
        width="56px"
        height="56px"
        alignItems="center"
        justifyContent="center"
        borderRightWidth="3px"
        borderLeftWidth="3px"
        borderColor="transparent"
        borderLeftColor={isActive ? 'blue.500' : 'transparent'}
        color={isActive ? 'blue.500' : 'gray.500'}
        _hover={{
          color: 'blue.500',
        }}
      >
        <Icon
          as={icon}
          width="1.4em"
          height="1.4em"
          color="currentcolor"
        />
      </Box>
    </Tooltip>
  );
};

export default AppNavBtn;
