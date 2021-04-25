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
import '@testing-library/jest-dom';
import { render, waitFor, fireEvent } from '@testing-library/react';
import nock from 'nock';
import axios from 'axios';
import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

import Pipelines from 'views/Pipelines';
import {
  defaultHeaders, QueryWrapper, RouterWrapper, url,
} from './utils';

dayjs.extend(utc);
dayjs.extend(timezone);
axios.defaults.adapter = require('axios/lib/adapters/http');

const sampleDag = {
  dagId: 'dagId1',
  description: 'string',
  fileToken: 'string',
  fileloc: 'string',
  isPaused: false,
  isSubdag: true,
  rootDagId: 'string',
  owners: [
    'string',
  ],
  tags: [
    {
      name: 'string',
    },
  ],
};

describe('Test Pipelines Table', () => {
  beforeAll(() => {
    dayjs.tz.setDefault('UTC');
  });

  beforeEach(() => {
    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .persist()
      .get('/version')
      .reply(200, { version: '', gitVersion: '' });
  });

  afterAll(() => {
    nock.cleanAll();
  });

  test('Show a loading indicator before data loads', async () => {
    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .get('/dags')
      .reply(200, {
        dags: [sampleDag],
        totalEntries: 1,
      });

    const { getByText } = render(
      <QueryWrapper><Pipelines /></QueryWrapper>,
      {
        wrapper: RouterWrapper,
      },
    );
    expect(getByText('Loading…')).toBeInTheDocument();
    await waitFor(() => expect(getByText(sampleDag.dagId)).toBeInTheDocument());
  });

  test('Show Empty State text if there are no dags', async () => {
    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .get('/dags')
      .reply(404, {
        dags: [],
        totalEntries: 0,
      });

    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .persist()
      .intercept(`/dags/${sampleDag.dagId}`, 'PATCH')
      .reply(200, { ...sampleDag, ...{ isPaused: !sampleDag.isPaused } });

    const { getByText } = render(
      <QueryWrapper><Pipelines /></QueryWrapper>,
      {
        wrapper: RouterWrapper,
      },
    );

    await waitFor(() => expect(getByText('No Pipelines found.')).toBeInTheDocument());
  });

  test('Toggle a pipeline on/off', async () => {
    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .get('/dags')
      .reply(200, {
        dags: [sampleDag],
        totalEntries: 1,
      });

    nock(url)
      .defaultReplyHeaders(defaultHeaders)
      .persist()
      .intercept(`/dags/${sampleDag.dagId}`, 'PATCH')
      .reply(200, { ...sampleDag, ...{ isPaused: !sampleDag.isPaused } });

    const { getByText, getByRole } = render(
      <QueryWrapper><Pipelines /></QueryWrapper>,
      {
        wrapper: RouterWrapper,
      },
    );

    await waitFor(() => expect(getByText(sampleDag.dagId)).toBeInTheDocument());
    const toggle = getByRole('switch');
    const input = toggle.querySelector('input') as HTMLInputElement;
    expect(input.checked).toBeTruthy();
    fireEvent.click(toggle);
    // 'Dag Updated' is the toast confirming the change happened
    await waitFor(() => expect(getByText('Pipeline Updated')).toBeInTheDocument());
    await waitFor(() => expect(input.checked).toBeFalsy());
  });
});
