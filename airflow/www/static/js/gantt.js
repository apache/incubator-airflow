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
/**
 * @author Dimitry Kudrayvtsev
 * @version 2.1
 * @modifiedby Maxime Beauchemin
 */

// Taken from
// https://github.com/benjaminoakes/moment-strftime/blob/1886cabc4b07d13e3046ae075d357e7aad92ea93/lib/moment-strftime.js
// but I couldn't work out how to make webpack not include moment again.
// TODO: revisit our webpack config
//
// -- Begin moment-strftime
// Copyright (c) 2012 Benjamin Oakes, MIT Licensed

/* global d3, document, moment, call_modal, data $ */

import tiTooltip from './task_instances';

const replacements = {
  a: 'ddd',
  A: 'dddd',
  b: 'MMM',
  B: 'MMMM',
  c: 'lll',
  d: 'DD',
  '-d': 'D',
  e: 'D',
  F: 'YYYY-MM-DD',
  H: 'HH',
  '-H': 'H',
  I: 'hh',
  '-I': 'h',
  j: 'DDDD',
  '-j': 'DDD',
  k: 'H',
  l: 'h',
  m: 'MM',
  '-m': 'M',
  M: 'mm',
  '-M': 'm',
  p: 'A',
  P: 'a',
  S: 'ss',
  '-S': 's',
  u: 'E',
  w: 'd',
  W: 'WW',
  x: 'll',
  X: 'LTS',
  y: 'YY',
  Y: 'YYYY',
  z: 'ZZ',
  Z: 'z',
  f: 'SSS',
  '%': '%',
};

moment.fn.strftime = function (format) {
  // Break up format string based on strftime tokens
  const tokens = format.split(/(%\-?.)/);
  const momentFormat = tokens.map((token) => {
    // Replace strftime tokens with moment formats
    if (token[0] === '%' && !!Object.getOwnPropertyDescriptor(replacements, token.substr(1))) {
      return replacements[token.substr(1)];
    }
    // Escape non-token strings to avoid accidental formatting
    return token.length > 0 ? `[${token}]` : token;
  }).join('');

  return this.format(momentFormat);
};
// -- End moment-strftime

d3.gantt = () => {
  const FIT_TIME_DOMAIN_MODE = 'fit';
  const tip = d3.tip()
    .attr('class', 'tooltip d3-tip')
    .offset([-10, 0])
    .html((d) => tiTooltip(d, { includeTryNumber: true }));

  let margin = {
    top: 20,
    right: 40,
    bottom: 20,
    left: 150,
  };
  const yAxisLeftOffset = 220;
  let selector = 'body';
  let timeDomainStart = d3.time.day.offset(new Date(), -3);
  let timeDomainEnd = d3.time.hour.offset(new Date(), +3);
  let timeDomainMode = FIT_TIME_DOMAIN_MODE;// fixed or fit
  let taskTypes = [];
  let height = document.body.clientHeight - margin.top - margin.bottom - 5;
  let width = $('.gantt').width() - margin.right - margin.left - 5;

  let tickFormat = '%H:%M';

  const keyFunction = (d) => d.start_date + d.task_id + d.end_date;

  let x = d3
    .time
    .scale()
    .domain([timeDomainStart, timeDomainEnd])
    .range([0, (width - yAxisLeftOffset)])
    .clamp(true);

  let y = d3
    .scale
    .ordinal()
    .domain(taskTypes)
    .rangeRoundBands([0, height - margin.top - margin.bottom], 0.1);

  const rectTransform = (d) => `translate(${x(d.start_date.valueOf()) + yAxisLeftOffset},${y(d.task_id)})`;

  // We can't use d3.time.format as that uses local time, so instead we use
  // moment as that handles our "global" timezone.
  const tickFormatter = (d) => moment(d).strftime(tickFormat);

  let xAxis = d3.svg.axis().scale(x).orient('bottom').tickFormat(tickFormatter)
    .tickSubdivide(true)
    .tickSize(8)
    .tickPadding(8);

  let yAxis = d3.svg.axis().scale(y).orient('left').tickSize(0);

  const initTimeDomain = (tasks) => {
    if (timeDomainMode === FIT_TIME_DOMAIN_MODE) {
      if (tasks === undefined || tasks.length < 1) {
        timeDomainStart = d3.time.day.offset(new Date(), -3);
        timeDomainEnd = d3.time.hour.offset(new Date(), +3);
        return;
      }

      tasks.forEach((a) => {
        if (!(a.start_date instanceof moment)) {
          a.start_date = moment(a.start_date);
        }
        if (!(a.end_date instanceof moment)) {
          a.end_date = moment(a.end_date);
        }
      });
      timeDomainEnd = moment.max(tasks.map((a) => a.end_date)).valueOf();
      timeDomainStart = moment.min(tasks.map((a) => a.start_date)).valueOf();
    }
  };

  const initAxis = () => {
    x = d3
      .time
      .scale()
      .domain([timeDomainStart, timeDomainEnd])
      .range([0, width - yAxisLeftOffset])
      .clamp(true);
    y = d3
      .scale
      .ordinal()
      .domain(taskTypes)
      .rangeRoundBands([0, height - margin.top - margin.bottom], 0.1);
    xAxis = d3.svg.axis().scale(x).orient('bottom').tickFormat(tickFormatter)
      .tickSubdivide(true)
      .tickSize(8)
      .tickPadding(8);

    yAxis = d3.svg.axis().scale(y).orient('left').tickSize(0);
  };

  function gantt(tasks) {
    initTimeDomain(tasks);
    initAxis();

    const svg = d3.select(selector)
      .append('svg')
      .attr('class', 'chart')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('class', 'gantt-chart')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .attr('transform', `translate(${margin.left}, ${margin.top})`);

    svg.selectAll('.chart')
      .data(tasks, keyFunction).enter()
      .append('rect')
      .on('mouseover', tip.show)
      .on('mouseout', tip.hide)
      .on('click', (d) => {
        call_modal(d.task_id, d.execution_date, d.extraLinks);
      })
      .attr('class', (d) => d.state || 'null')
      .attr('y', 0)
      .attr('transform', rectTransform)
      .attr('height', () => y.rangeBand())
      .attr('width', (d) => d3.max([x(d.end_date.valueOf()) - x(d.start_date.valueOf()), 1]));

    svg.append('g')
      .attr('class', 'x axis')
      .attr('transform', `translate(${yAxisLeftOffset}, ${height - margin.top - margin.bottom})`)
      .transition()
      .call(xAxis);

    svg.append('g').attr('class', 'y axis').transition().attr('transform', `translate(${yAxisLeftOffset}, 0)`)
      .call(yAxis);
    svg.call(tip);

    return gantt;
  }

  gantt.redraw = (tasks) => {
    initTimeDomain(tasks);
    initAxis();

    const svg = d3.select('.chart');

    const ganttChartGroup = svg.select('.gantt-chart');
    const rect = ganttChartGroup.selectAll('rect').data(tasks, keyFunction);

    rect.enter()
      .insert('rect', ':first-child')
      .attr('rx', 5)
      .attr('ry', 5)
      .attr('class', (d) => d.state || 'null')
      .transition()
      .attr('y', 0)
      .attr('transform', rectTransform)
      .attr('height', () => y.rangeBand())
      .attr('width', (d) => d3.max([x(d.end_date.valueOf()) - x(d.start_date.valueOf()), 1]));

    rect.exit().remove();

    svg.select('.x').transition().call(xAxis);
    svg.select('.y').transition().call(yAxis);

    return gantt;
  };

  gantt.margin = function (value) {
    if (!arguments.length) return margin;
    margin = value;
    return gantt;
  };

  gantt.timeDomain = function (value) {
    if (!arguments.length) return [timeDomainStart, timeDomainEnd];
    timeDomainStart = +value[0];
    timeDomainEnd = +value[1];
    return gantt;
  };

  /**
   * @param {string}
   *                vale The value can be "fit" - the domain fits the data or
   *                "fixed" - fixed domain.
   */
  gantt.timeDomainMode = function (value) {
    if (!arguments.length) return timeDomainMode;
    timeDomainMode = value;
    return gantt;
  };

  gantt.taskTypes = function (value) {
    if (!arguments.length) return taskTypes;
    taskTypes = value;
    return gantt;
  };

  gantt.width = function (value) {
    if (!arguments.length) return width;
    width = +value;
    return gantt;
  };

  gantt.height = function (value) {
    if (!arguments.length) return height;
    height = +value;
    return gantt;
  };

  gantt.tickFormat = function (value) {
    if (!arguments.length) return tickFormat;
    tickFormat = value;
    return gantt;
  };

  gantt.selector = function (value) {
    if (!arguments.length) return selector;
    selector = value;
    return gantt;
  };

  return gantt;
};

document.addEventListener('DOMContentLoaded', () => {
  const gantt = d3.gantt()
    .taskTypes(data.taskNames)
    .height(data.height)
    .selector('.gantt')
    .tickFormat('%H:%M:%S');
  gantt(data.tasks);
  $('body').on('airflow.timezone-change', () => {
    gantt.redraw(data.tasks);
  });
});
