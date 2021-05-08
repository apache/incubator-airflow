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

/* global calendarData, statesColors, document, window, $, d3, moment */
import getMetaValue from './meta_value';

const dagId = getMetaValue('dag_id');
const treeUrl = getMetaValue('tree_url');

function getTreeViewURL(d) {
  return `${treeUrl
  }?dag_id=${encodeURIComponent(dagId)
  }&base_date=${encodeURIComponent(d.toISOString())}`;
}

// date helpers
function formatDay(d) {
  return ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][d];
}

function toMoment(y, m, d) {
  return moment.utc([y, m, d]);
}

function weekOfMonth(y, m, d) {
  const monthOffset = toMoment(y, m, 1).day();
  const dayOfMonth = toMoment(y, m, d).date();
  return Math.floor((dayOfMonth + monthOffset - 1) / 7);
}

function weekOfYear(y, m) {
  const yearOffset = toMoment(y, 0, 1).day();
  const dayOfYear = toMoment(y, m, 1).dayOfYear();
  return Math.floor((dayOfYear + yearOffset - 1) / 7);
}

function daysInMonth(y, m) {
  const lastDay = toMoment(y, m, 1).add(1, 'month').subtract(1, 'day');
  return lastDay.date();
}

function weeksInMonth(y, m) {
  const firstDay = toMoment(y, m, 1);
  const monthOffset = firstDay.day();
  return Math.floor((daysInMonth(y, m) + monthOffset) / 7) + 1;
}

const dateFormat = 'YYYY-MM-DD';

document.addEventListener('DOMContentLoaded', () => {
  $('span.status_square').tooltip({ html: true });

  // JSON.parse is faster for large payloads than an object literal
  const rootData = JSON.parse(calendarData);

  const dayTip = d3.tip()
    .attr('class', 'tooltip d3-tip')
    .html((toolTipHtml) => toolTipHtml);

  // draw the calendar
  function draw() {
    // display constants
    const leftRightMargin = 32;
    const titleHeight = 24;
    const yearLabelWidth = 34;
    const dayLabelWidth = 14;
    const dayLabelPadding = 4;
    const yearPadding = 20;
    const cellSize = 16;
    const yearHeight = cellSize * 7 + 2;
    const maxWeeksInYear = 53;
    const legendHeight = 30;
    const legendSwatchesPadding = 4;
    const legendSwtchesTextWidth = 44;

    // group dag run stats by year -> month -> day -> state
    let dagStates = d3
      .nest()
      .key((dr) => moment.utc(dr.date, dateFormat).year())
      .key((dr) => moment.utc(dr.date, dateFormat).month())
      .key((dr) => moment.utc(dr.date, dateFormat).date())
      .key((dr) => dr.state)
      .map(rootData.dag_states);

    // Make sure we have one year displayed for each year between the start and end dates.
    // This also ensures we do not have show an empty calendar view when no dag runs exist.
    const startYear = moment.utc(rootData.start_date, dateFormat).year();
    const endYear = moment.utc(rootData.end_date, dateFormat).year();
    for (let y = startYear; y <= endYear; y += 1) {
      dagStates[y] = dagStates[y] || {};
    }

    dagStates = d3
      .entries(dagStates)
      .map((keyVal) => ({
        year: keyVal.key,
        dagStates: keyVal.value,
      }))
      .sort((data) => data.year);

    // root SVG element
    const fullWidth = (
      leftRightMargin * 2 + yearLabelWidth + dayLabelWidth
      + maxWeeksInYear * cellSize
    );
    const yearsHeight = (yearHeight + yearPadding) * dagStates.length + yearPadding;
    const fullHeight = titleHeight + legendHeight + yearsHeight;

    const svg = d3
      .select('#calendar-svg')
      .attr('width', fullWidth)
      .attr('height', fullHeight)
      .call(dayTip);

    // Add the title
    svg
      .append('g')
      .append('text')
      .attr('x', fullWidth / 2)
      .attr('y', 20)
      .attr('text-anchor', 'middle')
      .attr('class', 'title')
      .text('DAG states');

    // Add the legend
    const legend = svg
      .append('g')
      .attr('transform', `translate(0, ${titleHeight + legendHeight / 2})`);

    let legendXOffset = fullWidth - leftRightMargin;

    function drawLegend(rightState, leftState, numSwatches = 1, swatchesWidth = cellSize) {
      const startColor = statesColors[leftState || rightState];
      const endColor = statesColors[rightState];

      legendXOffset -= legendSwtchesTextWidth;
      legend
        .append('text')
        .attr('x', legendXOffset)
        .attr('y', cellSize / 2)
        .attr('text-anchor', 'start')
        .attr('class', 'status-label')
        .attr('alignment-baseline', 'middle')
        .text(rightState);
      legendXOffset -= legendSwatchesPadding;

      legendXOffset -= swatchesWidth;
      legend
        .append('g')
        .attr('transform', `translate(${legendXOffset}, 0)`)
        .selectAll('g')
        .data(d3.range(numSwatches))
        .enter()
        .append('rect')
        .attr('x', (v) => v * (swatchesWidth / numSwatches))
        .attr('width', swatchesWidth / numSwatches)
        .attr('height', cellSize)
        .attr('class', 'day')
        .attr('fill', (v) => d3.interpolateHsl(startColor, endColor)(v / numSwatches));
      legendXOffset -= legendSwatchesPadding;

      if (leftState !== undefined) {
        legend
          .append('text')
          .attr('x', legendXOffset)
          .attr('y', cellSize / 2)
          .attr('text-anchor', 'end')
          .attr('class', 'status-label')
          .attr('alignment-baseline', 'middle')
          .text(leftState);
        legendXOffset -= legendSwtchesTextWidth;
      }
    }

    drawLegend('no_status');
    drawLegend('running');
    drawLegend('failed', 'success', 10, 100);

    // Add the years groups, each holding one year of data.
    const years = svg
      .append('g')
      .attr('transform', `translate(${leftRightMargin}, ${titleHeight + legendHeight})`);

    const year = years
      .selectAll('g')
      .data(dagStates)
      .enter()
      .append('g')
      .attr('transform', (d, i) => `translate(0, ${yearPadding + (yearHeight + yearPadding) * i})`);

    year
      .append('text')
      .attr('x', -yearHeight * 0.5)
      .attr('transform', 'rotate(270)')
      .attr('text-anchor', 'middle')
      .attr('class', 'year-label')
      .text((d) => d.year);

    // write day names
    year
      .append('g')
      .attr('transform', `translate(${yearLabelWidth}, ${dayLabelPadding})`)
      .attr('text-anchor', 'end')
      .selectAll('g')
      .data(d3.range(7))
      .enter()
      .append('text')
      .attr('y', (i) => (i + 0.5) * cellSize)
      .attr('class', 'day-label')
      .text(formatDay);

    // create months groups to old the individual day cells & month outline for each month.
    const months = year
      .append('g')
      .attr('transform', `translate(${yearLabelWidth + dayLabelWidth}, 0)`);

    const month = months
      .append('g')
      .selectAll('g')
      .data((data) => d3
        .range(12)
        .map((i) => ({
          year: data.year,
          month: i,
          dagStates: data.dagStates[i] || {},
        })))
      .enter()
      .append('g')
      .attr('transform', (data) => `translate(${weekOfYear(data.year, data.month) * cellSize}, 0)`);

    const tipHtml = (data) => {
      const stateCounts = d3.entries(data.dagStates).map((kv) => `${kv.value[0].count} ${kv.key}`);
      const date = toMoment(data.year, data.month, data.day);
      const daySr = formatDay(date.day());
      const dateStr = date.format(dateFormat);
      return `<strong>${daySr} ${dateStr}</strong><br>${stateCounts.join('<br>')}`;
    };

    // Create the day cells
    month
      .selectAll('g')
      .data((data) => d3
        .range(daysInMonth(data.year, data.month))
        .map((i) => {
          const day = i + 1;
          const dagRunsByState = data.dagStates[day] || {};
          return {
            year: data.year,
            month: data.month,
            day,
            dagStates: dagRunsByState,
          };
        }))
      .enter()
      .append('rect')
      .attr('x', (data) => weekOfMonth(data.year, data.month, data.day) * cellSize)
      .attr('y', (data) => toMoment(data.year, data.month, data.day).day() * cellSize)
      .attr('width', cellSize)
      .attr('height', cellSize)
      .attr('class', 'day')
      .attr('fill', (data) => {
        const runningCount = (data.dagStates.running || [{ count: 0 }])[0].count;
        if (runningCount > 0) return statesColors.running;

        const successCount = (data.dagStates.success || [{ count: 0 }])[0].count;
        const failedCount = (data.dagStates.failed || [{ count: 0 }])[0].count;
        if (successCount + failedCount === 0) return statesColors.no_status;

        let ratioFailures;
        if (failedCount === 0) ratioFailures = 0;
        else {
          // We use a minimum color interpolation floor, so that days with low failures ratios
          // don't appear almost as green as days with not failure at all.
          const floor = 0.5;
          ratioFailures = floor + (failedCount / (failedCount + successCount)) * (1 - floor);
        }
        return d3.interpolateHsl(statesColors.success, statesColors.failed)(ratioFailures);
      })
      .on('click', (data) => {
        window.location.href = getTreeViewURL(
          // add 1 day and subtract 1 ms to not show any run from the next day.
          toMoment(data.year, data.month, data.day).add(1, 'day').subtract(1, 'ms'),
        );
      })
      .on('mouseover', function showTip(data) {
        const tt = tipHtml(data);
        dayTip.direction('n');
        dayTip.show(tt, this);
      })
      .on('mouseout', function hideTip(data) {
        dayTip.hide(data, this);
      });

    // add outline (path) around month
    month
      .selectAll('g')
      .data((data) => [data])
      .enter()
      .append('path')
      .attr('class', 'month')
      .style('fill', 'none')
      .attr('d', (data) => {
        const firstDayOffset = toMoment(data.year, data.month, 1).day();
        const lastDayOffset = toMoment(data.year, data.month, 1).add(1, 'month').day();
        const weeks = weeksInMonth(data.year, data.month);
        return d3.svg.line()([
          [0, firstDayOffset * cellSize],
          [cellSize, firstDayOffset * cellSize],
          [cellSize, 0],
          [weeks * cellSize, 0],
          [weeks * cellSize, lastDayOffset * cellSize],
          [(weeks - 1) * cellSize, lastDayOffset * cellSize],
          [(weeks - 1) * cellSize, 7 * cellSize],
          [0, 7 * cellSize],
          [0, firstDayOffset * cellSize],
        ]);
      });
  }

  function update() {
    $('#loading').remove();
    draw();
  }

  update();
});
