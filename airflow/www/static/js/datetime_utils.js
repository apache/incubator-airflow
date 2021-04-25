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

/* global moment, $, document */
export const defaultFormat = 'YYYY-MM-DD, HH:mm:ss';
export const defaultFormatWithTZ = 'YYYY-MM-DD, HH:mm:ss z';
export const defaultTZFormat = 'z (Z)';
export const dateTimeAttrFormat = 'YYYY-MM-DDThh:mm:ssTZD';

export function formatTimezone(what) {
  if (what instanceof moment) {
    return what.isUTC() ? 'UTC' : what.format(defaultTZFormat);
  }

  if (what === 'UTC') {
    return what;
  }

  return moment().tz(what).format(defaultTZFormat);
}

export function isoDateToTimeEl(datetime, options) {
  const dateTimeObj = moment(datetime);

  const addTitle = $.extend({ title: true }, options).title;

  const el = document.createElement('time');
  el.setAttribute('datetime', dateTimeObj.format());
  if (addTitle) {
    el.setAttribute('title', dateTimeObj.isUTC() ? '' : `UTC: ${dateTimeObj.clone().utc().format()}`);
  }
  el.innerText = dateTimeObj.format(defaultFormat);
  return el;
}

export const formatDateTime = (datetime) => moment(datetime).format(defaultFormatWithTZ);

export const convertAndFormatUTC = (datetime, tz) => {
  let dateTimeObj = moment.utc(datetime);
  if (tz) dateTimeObj = dateTimeObj.tz(tz);
  return dateTimeObj.format(defaultFormatWithTZ);
};

export const secondsToString = (seconds) => {
  let numdays    = Math.floor((seconds % 31536000) / 86400);
  let numhours   = Math.floor(((seconds % 31536000) % 86400) / 3600);
  let numminutes = Math.floor((((seconds % 31536000) % 86400) % 3600) / 60);
  let numseconds = Math.floor((((seconds % 31536000) % 86400) % 3600) % 60);
  return (numdays > 0    ? numdays    + (numdays    === 1 ? ' day '    : ' days ')    : '') +
         (numhours > 0   ? numhours   + (numhours   === 1 ? ' hour '   : ' hours ')   : '') +
         (numminutes > 0 ? numminutes + (numminutes === 1 ? ' minute ' : ' minutes ') : '') +
         (numseconds > 0 ? numseconds + (numseconds === 1 ? ' second'  : ' seconds')  : '');
};

export function updateAllDateTimes() {
  // Called after `moment.tz.setDefault` has changed the default TZ to display.

  $('time[data-datetime-convert!="false"]').each((_, el) => {
    const $el = $(el);
    const dt = moment($el.attr('datetime'));
    $el.text(dt.format(defaultFormat));
    if ($el.attr('title') !== undefined) {
      // If displayed date is not UTC, have the UTC date in a title attribute
      $el.attr('title', dt.isUTC() ? '' : `UTC: ${dt.clone().utc().format()}`);
    }
  });

  // Update any date-time inputs.
  //
  // Since we have set the default timezone for moment, it will automatically
  // convert it to the new target for us
  $('.datetime input').each((_, el) => {
    el.value = moment(el.value).format();
  });
}

export function setDisplayedTimezone(tz) {
  moment.tz.setDefault(tz);
  updateAllDateTimes();
}
