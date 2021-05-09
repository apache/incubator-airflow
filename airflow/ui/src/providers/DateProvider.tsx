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

import React, {
  createContext, useContext, useState, ReactNode, ReactElement, useEffect,
} from 'react';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import dayjsTz from 'dayjs/plugin/timezone';

dayjs.extend(utc);
dayjs.extend(dayjsTz);

export const HOURS_24 = 'HH:mm Z';
export const HOURS_12 = 'h:mmA Z';

interface DateContextData {
  timezone: string;
  setTimezone: (value: string) => void;
  dateFormat: string;
  toggle24Hour: () => void;
}

export const DateContext = createContext<DateContextData>({
  timezone: 'UTC',
  setTimezone: () => {},
  dateFormat: HOURS_24,
  toggle24Hour: () => {},
});

export const useDateContext = () => useContext(DateContext);

type Props = {
  children: ReactNode;
};

const DateProvider = ({ children }: Props): ReactElement => {
  // TODO: add in default_timezone when GET /ui-metadata is available
  // guess timezone on browser or default to utc and don't guess when testing
  const isTest = process.env.NODE_ENV === 'test';
  const [timezone, setTimezone] = useState(isTest ? 'UTC' : dayjs.tz.guess());
  const [dateFormat, setDateFormat] = useState(HOURS_24);

  const toggle24Hour = () => {
    setDateFormat(dateFormat === HOURS_24 ? HOURS_12 : HOURS_24);
  };

  useEffect(() => {
    dayjs.tz.setDefault(timezone);
  }, [timezone]);

  return (
    <DateContext.Provider
      value={{
        timezone,
        setTimezone,
        dateFormat,
        toggle24Hour,
      }}
    >
      {children}
    </DateContext.Provider>
  );
};

export default DateProvider;
