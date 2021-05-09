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
import { render, fireEvent } from '@testing-library/react';

import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

import TimezoneDropdown from 'components/AppContainer/TimezoneDropdown';
import DateProvider, { HOURS_24 } from 'providers/DateProvider';
import { ChakraWrapper } from './utils';

dayjs.extend(utc);
dayjs.extend(timezone);

describe('test timezone dropdown', () => {
  test('Can search for a new timezone and the date changes', () => {
    const { getByText } = render(
      <DateProvider>
        <TimezoneDropdown />
      </DateProvider>,
      { wrapper: ChakraWrapper },
    );

    const initialTime = dayjs().tz('UTC').format(HOURS_24);

    expect(getByText(initialTime)).toBeInTheDocument();
    const button = getByText(initialTime);
    fireEvent.click(button);
    const focusedElement = document.activeElement;
    if (focusedElement) {
      fireEvent.change(focusedElement, { target: { value: 'Anch' } });
    }
    const option = getByText('-08:00 America/Anchorage');
    expect(option).toBeInTheDocument();
    fireEvent.click(option);

    expect(getByText(dayjs().tz('America/Anchorage').format(HOURS_24))).toBeInTheDocument();
  });
});
