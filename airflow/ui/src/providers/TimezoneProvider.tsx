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

interface TimezoneContextData {
  timezone: string;
  setTimezone: (value: string) => void;
}

export const TimezoneContext = createContext<TimezoneContextData>({
  timezone: 'UTC',
  setTimezone: () => {},
});

export const useTimezoneContext = () => useContext(TimezoneContext);

type Props = {
  children: ReactNode;
};

const TimezoneProvider = ({ children }: Props): ReactElement => {
  // TODO: add in default_timezone when GET /ui-metadata is available
  // guess timezone on browser or default to utc
  const [timezone, setTimezone] = useState(dayjs.tz.guess() || 'UTC');

  useEffect(() => {
    dayjs.tz.setDefault(timezone);
  }, [timezone]);

  return (
    <TimezoneContext.Provider
      value={{
        timezone,
        setTimezone,
      }}
    >
      {children}
    </TimezoneContext.Provider>
  );
};

export default TimezoneProvider;
