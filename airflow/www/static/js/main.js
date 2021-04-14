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
/* global $, moment, Airflow, window, localStorage, document */

import {
  dateTimeAttrFormat,
  formatTimezone,
  isoDateToTimeEl,
  setDisplayedTimezone,
} from './datetime_utils';

window.isoDateToTimeEl = isoDateToTimeEl;

// We pull moment in via a webpack entrypoint rather than import so that we don't put it in more than a single .js file. This "exports" it to be globally available.
window.moment = Airflow.moment;

function displayTime() {
  const now = moment();
  $('#clock')
    .attr('datetime', now.format(dateTimeAttrFormat))
    .html(`${now.format('HH:mm')} <strong>${formatTimezone(now)}</strong>`);
}

function changDisplayedTimezone(tz) {
  localStorage.setItem('selected-timezone', tz);
  setDisplayedTimezone(tz);
  displayTime();
  $('body').trigger({
    type: 'airflow.timezone-change',
    timezone: tz,
  });
}

var el = document.createElement('span');

export function escapeHtml(text) {
  el.textContent = text;
  return el.innerHTML;
}

window.escapeHtml = escapeHtml;

export function convertSecsToHumanReadable(seconds) {
  var oriSeconds = seconds;
  var floatingPart = oriSeconds- Math.floor(oriSeconds);

  seconds = Math.floor(seconds);

  var secondsPerHour = 60 * 60;
  var secondsPerMinute = 60;

  var hours = Math.floor(seconds / secondsPerHour);
  seconds = seconds - hours * secondsPerHour;

  var minutes = Math.floor(seconds / secondsPerMinute);
  seconds = seconds - minutes * secondsPerMinute;

  var readableFormat = '';
  if (hours > 0) {
    readableFormat += hours + 'Hours ';
  }
  if (minutes > 0) {
    readableFormat += minutes + 'Min ';
  }
  if (seconds + floatingPart > 0) {
    if (Math.floor(oriSeconds) === oriSeconds) {
      readableFormat += seconds + 'Sec';
    } else {
      seconds += floatingPart;
      readableFormat += seconds.toFixed(3) + 'Sec';
    }
  }
  return readableFormat;
}
window.convertSecsToHumanReadable = convertSecsToHumanReadable;

function postAsForm(url, parameters) {
  var form = $('<form></form>');

  form.attr('method', 'POST');
  form.attr('action', url);

  $.each(parameters || {}, function(key, value) {
    var field = $('<input></input>');

    field.attr('type', 'hidden');
    field.attr('name', key);
    field.attr('value', value);

    form.append(field);
  });

  var field = $('<input></input>');

  field.attr('type', 'hidden');
  field.attr('name', 'csrf_token');
  field.attr('value', csrfToken);

  form.append(field);

  // The form needs to be a part of the document in order for us to be able
  // to submit it.
  $(document.body).append(form);
  form.submit();
}

window.postAsForm = postAsForm;

function initializeUITimezone() {
  const local = moment.tz.guess();

  const selectedTz = localStorage.getItem('selected-timezone');
  const manualTz = localStorage.getItem('chosen-timezone');

  function setManualTimezone(tz) {
    localStorage.setItem('chosen-timezone', tz);
    if (tz == local && tz == Airflow.serverTimezone) {
      $('#timezone-manual').hide();
      return;
    }

    $('#timezone-manual a').data('timezone', tz).text(formatTimezone(tz));
    $('#timezone-manual').show();
  }

  if (manualTz) {
    setManualTimezone(manualTz);
  }

  changDisplayedTimezone(selectedTz || Airflow.defaultUITimezone);

  if (Airflow.serverTimezone !== 'UTC') {
    $('#timezone-server a').html(`${formatTimezone(Airflow.serverTimezone)} <span class="label label-primary">Server</span>`);
    $('#timezone-server').show();
  }

  if (Airflow.serverTimezone !== local) {
    $('#timezone-local a')
      .attr('data-timezone', local)
      .html(`${formatTimezone(local)} <span class="label label-info">Local</span>`);
  } else {
    $('#timezone-local').hide();
  }

  $('a[data-timezone]').click((evt) => {
    changDisplayedTimezone($(evt.target).data('timezone'));
  });

  $('#timezone-other').typeahead({
    source: $(moment.tz.names().map((tzName) => {
      const category = tzName.split('/', 1)[0];
      return { category, name: tzName.replace('_', ' '), tzName };
    })),
    showHintOnFocus: 'all',
    showCategoryHeader: true,
    items: 'all',
    afterSelect(data) {
      // Clear it for next time we open the pop-up
      this.$element.val('');

      setManualTimezone(data.tzName);
      changDisplayedTimezone(data.tzName);

      // We need to delay the close event to not be in the form handler,
      // otherwise bootstrap ignores it, thinking it's caused by interaction on
      // the <form>
      setTimeout(() => {
        document.activeElement.blur();
        // Bug in typeahed, it thinks it's still shown!
        this.shown = false;
        this.focused = false;
      }, 1);
    },
  });
}

$(document).ready(() => {
  initializeUITimezone();

  $('#clock')
    .attr('data-original-title', hostName)
    .attr('data-placement', 'bottom')
    .parent()
    .show();

  displayTime();
  setInterval(displayTime, 1000);

  $.ajaxSetup({
    beforeSend: function(xhr, settings) {
      if (!/^(GET|HEAD|OPTIONS|TRACE)$/i.test(settings.type) && !this.crossDomain) {
        xhr.setRequestHeader("X-CSRFToken", csrfToken);
      }
    }
  });

  $.fn.datetimepicker.defaults.format = 'YYYY-MM-DD HH:mm:ssZ';
  $.fn.datetimepicker.defaults.sideBySide = true;
  $('.datetimepicker').datetimepicker();

  // Fix up filter fields from FAB adds to the page. This event is fired after
  // the FAB registered one which adds the new control
  $('#filter_form a.filter').click(() => {
    $('.datetimepicker').datetimepicker();
  });

  // Global Tooltip selector
  $('.js-tooltip').tooltip();
});
