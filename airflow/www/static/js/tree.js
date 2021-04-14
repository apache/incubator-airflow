/* eslint-disable func-names */
/* eslint-disable no-underscore-dangle */
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

/* global treeData, document, window, $, d3, moment, call_modal_dag, call_modal, */
import { escapeHtml } from './main';
import tiTooltip from './task_instances';
import getMetaValue from './meta_value';

function toDateString(ts) {
  const dt = new Date(ts * 1000);
  return dt.toISOString();
}

function isDagRun(d) {
  return d.run_id !== undefined;
}

function nodeClass(d) {
  let sclass = 'node';
  if (d.children === undefined && d._children === undefined) sclass += ' leaf';
  else {
    sclass += ' parent';
    if (d.children === undefined) sclass += ' collapsed';
    else sclass += ' expanded';
  }
  return sclass;
}

document.addEventListener('DOMContentLoaded', () => {
  $('span.status_square').tooltip({ html: true });

  // JSON.parse is faster for large payloads than an object literal
  const data = JSON.parse(treeData);
  const tree = d3.layout.tree().nodeSize([0, 25]);
  const nodes = tree.nodes(data);
  const nodeobj = {};

  const now = Date.now() / 1000;
  const devicePixelRatio = window.devicePixelRatio || 1;
  const barHeight = 20;
  const axisHeight = 50;
  const squareSize = 10;
  // calculate the white space between the tree and instances based on # of instances and tree depth
  let treeDepth = 0;
  nodes.forEach((node) => {
    if (node.depth > treeDepth) treeDepth = node.depth;
  });
  treeDepth += 1;
  const squareX = window.innerWidth - (data.instances.length * squareSize) - (treeDepth * 50);

  const squareSpacing = 2;
  const margin = {
    top: barHeight / 2 + axisHeight, right: 0, bottom: 0, left: barHeight / 2,
  };
  const width = parseInt(960 * devicePixelRatio, 10) - margin.left - margin.right;
  const barWidth = width * 0.9;

  let i = 0;
  const duration = 400;
  let root;

  function populateTaskInstanceProperties(node) {
  // populate task instance properties for display purpose
    let j;
    for (j = 0; j < node.instances.length; j += 1) {
      const dataInstance = data.instances[j];
      const row = node.instances[j];

      if (row === null) {
        node.instances[j] = {
          task_id: node.name,
          execution_date: dataInstance.execution_date,
        };
        continue;
      }

      const taskInstance = {
        state: row[0],
        try_number: row[1],
        start_ts: row[2],
        duration: row[3],
      };
      node.instances[j] = taskInstance;

      taskInstance.task_id = node.name;
      taskInstance.operator = node.operator;
      taskInstance.execution_date = dataInstance.execution_date;
      taskInstance.external_trigger = dataInstance.external_trigger;

      // compute start_date and end_date if applicable
      if (taskInstance.start_ts !== null) {
        taskInstance.start_date = toDateString(taskInstance.start_ts);
        if (taskInstance.state === 'running') {
          taskInstance.duration = now - taskInstance.start_ts;
        } else if (taskInstance.duration !== null) {
          taskInstance.end_date = toDateString(taskInstance.start_ts + taskInstance.duration);
        }
      }
    }
  }

  for (i = 0; i < nodes.length; i += 1) {
    const node = nodes[i];
    nodeobj[node.name] = node;

    if (node.name === '[DAG]') {
    // skip synthetic root node since it's doesn't contain actual task instances
      continue;
    }

    if (node.start_ts !== undefined) {
      node.start_date = toDateString(node.start_ts);
    }
    if (node.end_ts !== undefined) {
      node.end_date = toDateString(node.end_ts);
    }
    if (node.depends_on_past === undefined) {
      node.depends_on_past = false;
    }

    populateTaskInstanceProperties(node);
  }

  const diagonal = d3.svg.diagonal()
    .projection((d) => [d.y, d.x]);

  const taskTip = d3.tip()
    .attr('class', 'tooltip d3-tip')
    .html((toolTipHtml) => toolTipHtml);

  const svg = d3.select('#tree-svg')
  // .attr("width", width + margin.left + margin.right)
    .append('g')
    .attr('class', 'level')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  data.x0 = 0;
  data.y0 = 0;

  const baseNode = nodes.length === 1 ? nodes[0] : nodes[1];
  const numSquare = baseNode.instances.length;

  const xScale = d3.scale.linear()
    .range([
      squareSize / 2,
      (numSquare * squareSize) + ((numSquare - 1) * squareSpacing) - (squareSize / 2),
    ]);

  d3.select('#tree-svg')
    .insert('g')
    .attr('transform',
      `translate(${squareX + margin.left}, ${axisHeight})`)
    .attr('class', 'axis')
    .call(
      d3.svg.axis()
        .scale(xScale)
        .orient('top')
      // show a tick every 5 instances
        .ticks(Math.floor(numSquare / 5) || 1)
        .tickFormat((d) => {
          if (!numSquare || (d > 0 && numSquare < 3)) {
            // don't render ticks when there are no instances or when the ticks would overlap
            return '';
          }
          const tickIndex = d === 1 ? numSquare - 1 : Math.round(d * numSquare);
          return moment(baseNode.instances[tickIndex].execution_date).format('MMM DD, HH:mm');
        }),
    )
    .selectAll('text')
    .attr('transform', 'rotate(-30)')
    .style('text-anchor', 'start')
    .call(taskTip);

  function update(source) {
  // Compute the flattened node list. TODO use d3.layout.hierarchy.
    const nodes = tree.nodes(root);

    const height = Math.max(500, nodes.length * barHeight + margin.top + margin.bottom);
    const width = squareX
      + (numSquare * (squareSize + squareSpacing))
      + margin.left + margin.right + 50;
    d3.select('#tree-svg').transition()
      .duration(duration)
      .attr('height', height)
      .attr('width', width);

    d3.select(self.frameElement).transition()
      .duration(duration)
      .style('height', `${height}px`);

    // Compute the "layout".
    nodes.forEach((n, i) => {
      n.x = i * barHeight;
    });

    // Update the nodes…
    const node = svg.selectAll('g.node')
      .data(nodes, (d) => d.id || (d.id = ++i));

    const nodeEnter = node.enter().append('g')
      .attr('class', nodeClass)
      .attr('transform', () => `translate(${source.y0},${source.x0})`)
      .style('opacity', 1e-6);

    nodeEnter.append('circle')
      .attr('r', (barHeight / 3))
      .attr('class', 'task')
      .attr('data-toggle', 'tooltip')
      .on('mouseover', function (d) {
        let tt = '';
        if (d.operator !== undefined) {
          if (d.operator !== undefined) {
            tt += `operator: ${escapeHtml(d.operator)}<br>`;
          }

          tt += `depends_on_past: ${escapeHtml(d.depends_on_past)}<br>`;
          tt += `upstream: ${escapeHtml(d.num_dep)}<br>`;
          tt += `retries: ${escapeHtml(d.retries)}<br>`;
          tt += `owner: ${escapeHtml(d.owner)}<br>`;
          tt += `start_date: ${escapeHtml(d.start_date)}<br>`;
          tt += `end_date: ${escapeHtml(d.end_date)}<br>`;
        }
        taskTip.direction('e');
        taskTip.show(tt, this);
        d3.select(this).transition()
          .style('stroke-width', 3);
      })
      .on('mouseout', function (d) {
        taskTip.hide(d);
        d3.select(this).transition()
          .style('stroke-width', (dd) => (isDagRun(dd) ? '2' : '1'));
      })
      .attr('height', barHeight)
      .attr('width', (d) => barWidth - d.y)
      .style('fill', (d) => d.ui_color)
      .attr('task_id', (d) => d.name)
      .on('click', toggles);

    const text = nodeEnter.append('text')
      .attr('dy', 3.5)
      .attr('dx', barHeight / 2)
      .text((d) => d.name);

    nodeEnter.append('g')
      .attr('class', 'stateboxes')
      .attr('transform',
        (d) => `translate(${squareX - d.y},0)`)
      .selectAll('rect')
      .data((d) => d.instances)
      .enter()
      .append('rect')
      .on('click', (d) => {
        if (d.task_id === undefined) call_modal_dag(d);
        else if (nodeobj[d.task_id].operator === 'SubDagOperator') {
          // I'm pretty sure that true is not a valid subdag id, which is what call_modal wants
          call_modal(
            d.task_id,
            d.execution_date,
            nodeobj[d.task_id].extra_links,
            d.try_number,
            true,
          );
        } else {
          call_modal(
            d.task_id,
            d.execution_date,
            nodeobj[d.task_id].extra_links,
            d.try_number,
            undefined,
          );
        }
      })
      .attr('class', (d) => `state ${d.state}`)
      .attr('data-toggle', 'tooltip')
      .attr('rx', (d) => (isDagRun(d) ? '5' : '1'))
      .attr('ry', (d) => (isDagRun(d) ? '5' : '1'))
      .style('shape-rendering', (d) => (isDagRun(d) ? 'auto' : 'crispEdges'))
      .style('stroke-width', (d) => (isDagRun(d) ? '2' : '1'))
      .style('stroke-opacity', (d) => (d.external_trigger ? '0' : '1'))
      .on('mouseover', function (d) {
        // Calculate duration if it doesn't exist
        const tt = tiTooltip({ ...d, duration: d.duration || moment(d.end_date).diff(d.start_date, 'seconds') });
        taskTip.direction('n');
        taskTip.show(tt, this);
        d3.select(this).transition()
          .style('stroke-width', 3);
      })
      .on('mouseout', function (d) {
        taskTip.hide(d);
        d3.select(this).transition()
          .style('stroke-width', (dd) => (isDagRun(dd) ? '2' : '1'));
      })
      .attr('x', (d, j) => (j * (squareSize + squareSpacing)))
      .attr('y', -squareSize / 2)
      .attr('width', 10)
      .attr('height', 10);

    // Transition nodes to their new position.
    nodeEnter.transition()
      .duration(duration)
      .attr('transform', (d) => `translate(${d.y},${d.x})`)
      .style('opacity', 1);

    node.transition()
      .duration(duration)
      .attr('class', nodeClass)
      .attr('transform', (d) => `translate(${d.y},${d.x})`)
      .style('opacity', 1);

    // Transition exiting nodes to the parent's new position.
    node.exit().transition()
      .duration(duration)
      .attr('transform', () => `translate(${source.y},${source.x})`)
      .style('opacity', 1e-6)
      .remove();

    // Update the links…
    const link = svg.selectAll('path.link')
      .data(tree.links(nodes), (d) => d.target.id);

    // Enter any new links at the parent's previous position.
    link.enter().insert('path', 'g')
      .attr('class', 'link')
      .attr('d', () => {
        const o = { x: source.x0, y: source.y0 };
        return diagonal({ source: o, target: o });
      })
      .transition()
      .duration(duration)
      .attr('d', diagonal);

    // Transition links to their new position.
    link.transition()
      .duration(duration)
      .attr('d', diagonal);

    // Transition exiting nodes to the parent's new position.
    link.exit().transition()
      .duration(duration)
      .attr('d', () => {
        const o = { x: source.x, y: source.y };
        return diagonal({ source: o, target: o });
      })
      .remove();

    // Stash the old positions for transition.
    nodes.forEach((d) => {
      d.x0 = d.x;
      d.y0 = d.y;
    });

    $('#loading').remove();
  }

  update(root = data);

  function toggles(clicked) {
  // Collapse nodes with the same task id
    d3.selectAll(`[task_id='${clicked.name}']`).each((d) => {
      if (clicked !== d && d.children) {
        d._children = d.children;
        d.children = null;
        update(d);
      }
    });

    // Toggle clicked node
    if (clicked._children) {
      clicked.children = clicked._children;
      clicked._children = null;
    } else {
      clicked._children = clicked.children;
      clicked.children = null;
    }
    update(clicked);
  }
});
