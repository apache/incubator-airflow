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

import React, { useState, FormEvent } from 'react';
import {
  Box,
  Button,
  FormLabel,
  FormControl,
  Icon,
  Input,
  InputGroup,
  InputLeftElement,
  Spinner,
  Alert,
  AlertIcon,
} from '@chakra-ui/react';
import { FiLock, FiUser } from 'react-icons/fi';

import AppContainer from 'components/AppContainer';

import { useAuthContext } from 'providers/auth/context';

const Login: React.FC = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const { login, error, loading } = useAuthContext();

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    login(username, password);
  };

  return (
    <AppContainer>
      <Box display="flex" alignItems="center" justifyContent="center" height="80vh">
        <Box as="form" width="100%" maxWidth="400px" mx="auto" onSubmit={onSubmit}>
          <FormControl>
            <FormLabel htmlFor="username">Username</FormLabel>
            <InputGroup>
              <InputLeftElement>
                <Icon as={FiUser} color="gray.300" />
              </InputLeftElement>
              <Input
                autoFocus
                autoCapitalize="none"
                name="username"
                placeholder="Username"
                data-testid="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                isRequired
              />
            </InputGroup>
          </FormControl>
          <FormControl mt={4}>
            <FormLabel htmlFor="password">Password</FormLabel>
            <InputGroup>
              <InputLeftElement>
                <Icon as={FiLock} color="gray.300" />
              </InputLeftElement>
              <Input
                type="password"
                name="password"
                placeholder="Password"
                data-testid="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                isRequired
              />
            </InputGroup>
          </FormControl>
          <Button
            width="100%"
            mt={4}
            type="submit"
            disabled={!username || !password}
            data-testid="submit"
          >
            {loading ? <Spinner size="md" speed="0.85s" /> : 'Log in'}
          </Button>
          {error && (
            <Alert status="error" my="4" key={error.message}>
              <AlertIcon />
              {error.message}
            </Alert>
          )}
        </Box>
      </Box>
    </AppContainer>
  );
};

export default Login;
