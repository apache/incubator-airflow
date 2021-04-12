/* eslint-disable no-underscore-dangle */
/* eslint-disable no-use-before-define */
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

/*
  global d3, document, call_modal, nodes, taskInstances, tasks, edges, dagreD3, localStorage, $
*/

import getMetaValue from './meta_value';
import { escapeHtml } from './main';
import tiTooltip, { taskNoInstanceTooltip } from './task_instances';

const dagId = getMetaValue('dag_id');
const executionDate = getMetaValue('execution_date');
const arrange = getMetaValue('arrange');
const taskInstancesUrl = getMetaValue('task_instances_url');

// Build a map mapping node id to tooltip for all the TaskGroups.
function getTaskGroupTips(node) {
  const tips = new Map();
  if (node.children !== undefined) {
    tips.set(node.id, node.tooltip);
    for (const child of node.children.values()) {
      for (const [key, val] of getTaskGroupTips(child)) tips.set(key, val);
    }
  }
  return tips;
}

const taskGroupTips = getTaskGroupTips(nodes);
// This maps the actual taskId to the current graph node id that contains the task
// (because tasks may be grouped into a group node)
const mapTaskToNode = new Map();

// Below variables are being used in dag.js

const getTaskInstanceURL = `${taskInstancesUrl}?dag_id=${encodeURIComponent(dagId)}&execution_date=${
  encodeURIComponent(executionDate)}`;

const duration = 500;
const stateFocusMap = {
  success: false,
  running: false,
  failed: false,
  skipped: false,
  upstream_failed: false,
  up_for_reschedule: false,
  up_for_retry: false,
  queued: false,
  no_status: false,
};
const taskTip = d3.tip()
  .attr('class', 'tooltip d3-tip')
  .html((toolTipHtml) => toolTipHtml);

// Preparation of DagreD3 data structures
// "compound" is set to true to make use of clusters to display TaskGroup.
const g = new dagreD3.graphlib.Graph({ compound: true }).setGraph({
  nodesep: 30,
  ranksep: 15,
  rankdir: arrange,
})
  .setDefaultEdgeLabel(() => ({ lineInterpolate: 'basis' }));

const render = dagreD3.render();
const svg = d3.select('#graph-svg');
let innerSvg = d3.select('#graph-svg g');

// Remove the node with this nodeId from g.
function removeNode(nodeId) {
  if (g.hasNode(nodeId)) {
    const node = g.node(nodeId);
    if (node.children !== undefined) {
      // If the child is an expanded group node, remove children too.
      node.children.forEach((child) => {
        removeNode(child.id);
      });
    }
  }
  g.removeNode(nodeId);
}

// Collapse the children of the given group node.
function collapseGroup(nodeId, node) {
  // Remove children nodes
  node.children.forEach((child) => {
    removeNode(child.id);
  });
  // Map task that are under this node to this node's id
  for (const childId of getChildrenIds(node)) mapTaskToNode.set(childId, nodeId);

  node = g.node(nodeId);

  // Set children edges onto the group edge
  edges.forEach((edge) => {
    const sourceId = mapTaskToNode.get(edge.source_id);
    const targetId = mapTaskToNode.get(edge.target_id);
    if (sourceId !== targetId && !g.hasEdge(sourceId, targetId)) {
      g.setEdge(sourceId, targetId, {
        curve: d3.curveBasis,
        arrowheadClass: 'arrowhead',
      });
    }
  });

  draw();
  focusGroup(nodeId);

  removeExpandedGroup(nodeId, node);
}

// Update the page to show the latest DAG.
function draw() {
  innerSvg.remove();
  innerSvg = svg.append('g');
  // Run the renderer. This is what draws the final graph.
  innerSvg.call(render, g);
  innerSvg.call(taskTip);

  // When an expanded group is clicked, collapse it.
  d3.selectAll('g.cluster').on('click', (nodeId) => {
    if (d3.event.defaultPrevented) return;
    const node = g.node(nodeId);
    collapseGroup(nodeId, node);
  });
  // When a node is clicked, action depends on the node type.
  d3.selectAll('g.node').on('click', (nodeId) => {
    const node = g.node(nodeId);
    if (node.children !== undefined && Object.keys(node.children).length > 0) {
      // A group node
      if (d3.event.defaultPrevented) return;
      expandGroup(nodeId, node);
    } else if (nodeId in tasks) {
      // A task node
      const task = tasks[nodeId];
      let tryNumber;
      if (nodeId in taskInstances) tryNumber = taskInstances[nodeId].tryNumber;
      else tryNumber = 0;

      if (task.task_type === 'SubDagOperator') call_modal(nodeId, executionDate, task.extra_links, tryNumber, true);
      else call_modal(nodeId, executionDate, task.extra_links, tryNumber, undefined);
    } else {
      // join node between TaskGroup. Ignore.
    }
  });

  d3.selectAll('g.node').on('mouseover', function mousover(d) {
    d3.select(this).selectAll('rect').attr('data-highlight', 'highlight');
    highlightNodes(g.predecessors(d));
    highlightNodes(g.successors(d));
    const adjacentNodeNames = [d, ...g.predecessors(d), ...g.successors(d)];

    d3.selectAll('g.nodes g.node')
      .filter((x) => !adjacentNodeNames.includes(x))
      .attr('data-highlight', 'fade');

    d3.selectAll('g.edgePath')[0].forEach((x) => {
      const val = g.nodeEdges(d).includes(x.__data__) ? 'highlight' : 'fade';
      d3.select(x).attr('data-highlight', val);
    });
    d3.selectAll('g.edgeLabel')[0].forEach((x) => {
      if (!g.nodeEdges(d).includes(x.__data__)) {
        d3.select(x).attr('data-highlight', 'fade');
      }
    });
  });

  d3.selectAll('g.node').on('mouseout', function mouseout(d) {
    d3.select(this).selectAll('rect, circle').attr('data-highlight', null);
    unHighlightNodes(g.predecessors(d));
    unHighlightNodes(g.successors(d));
    d3.selectAll('g.node, g.edgePath, g.edgeLabel')
      .attr('data-highlight', null);
    localStorage.removeItem(focusedGroupKey(dagId));
  });
  updateNodesStates(taskInstances);
  setUpZoomSupport();
}

let zoom = null;

function setUpZoomSupport() {
  // Set up zoom support for Graph
  zoom = d3.behavior.zoom().on('zoom', () => {
    innerSvg.attr('transform', `translate(${d3.event.translate})scale(${d3.event.scale})`);
  });
  svg.call(zoom);

  // Centering the DAG on load
  // Get Dagre Graph dimensions
  const graphWidth = g.graph().width;
  const graphHeight = g.graph().height;
  // Get SVG dimensions
  const padding = 20;
  const svgBb = svg.node().getBoundingClientRect();
  const width = svgBb.width - padding * 2;
  const height = svgBb.height - padding; // we are not centering the dag vertically

  // Calculate applicable scale for zoom
  const zoomScale = Math.min(
    Math.min(width / graphWidth, height / graphHeight),
    1.5, // cap zoom level to 1.5 so nodes are not too large
  );

  zoom.translate([(width / 2) - ((graphWidth * zoomScale) / 2) + padding, padding]);
  zoom.scale(zoomScale);
  zoom.event(innerSvg);
}

function highlightNodes(nodes) {
  nodes.forEach((nodeid) => {
    const myNode = g.node(nodeid).elem;
    d3.select(myNode)
      .selectAll('rect, circle')
      .attr('data-highlight', 'highlight');
  });
}

function unHighlightNodes(nodes) {
  nodes.forEach((nodeid) => {
    const myNode = g.node(nodeid).elem;
    d3.select(myNode)
      .selectAll('rect, circle')
      .attr('data-highlight', null);
  });
}

d3.selectAll('.js-state-legend-item')
  .on('mouseover', function mouseover() {
    if (!stateIsSet()) {
      const state = $(this).data('state');
      focusState(state);
    }
  })
  .on('mouseout', () => {
    if (!stateIsSet()) {
      clearFocus();
    }
  });

d3.selectAll('.js-state-legend-item').on('click', function click() {
  const state = $(this).data('state');

  clearFocus();
  if (!stateFocusMap[state]) {
    const color = d3.select(this).style('border-color');
    focusState(state, this, color);
    setFocusMap(state);
  } else {
    setFocusMap();
    d3.selectAll('.js-state-legend-item')
      .style('background-color', null);
  }
});

// Returns true if a node's id or its children's id matches searchText
function nodeMatches(nodeId, searchText) {
  if (nodeId.indexOf(searchText) > -1) return true;

  // The node's own id does not match, it may have children that match
  const node = g.node(nodeId);
  if (node.children !== undefined) {
    const children = getChildrenIds(node);
    for (const child of children) {
      if (child.indexOf(searchText) > -1) return true;
    }
  }
  return false;
}

d3.select('#searchbox').on('keyup', () => {
  const s = document.getElementById('searchbox').value;

  if (s === '') return;

  let match = null;

  if (stateIsSet()) {
    clearFocus();
    setFocusMap();
  }

  d3.selectAll('g.nodes g.node').forEach(function highlight(d) {
    if (s === '') {
      d3.selectAll('g.edgePaths, g.edgeLabel').attr('data-highlight', null);
      d3.select(this).attr('data-highlight', null);
    } else {
      d3.selectAll('g.edgePaths, g.edgeLabel').attr('data-highlight', 'fade');
      if (nodeMatches(d, s)) {
        if (!match) match = this;
        d3.select(this).attr('data-highlight', null);
      } else {
        d3.select(this).attr('data-highlight', 'fade');
      }
    }
  });

  // This moves the matched node to the center of the graph area
  if (match) {
    const transform = d3.transform(d3.select(match).attr('transform'));

    const svgBb = svg.node().getBoundingClientRect();
    transform.translate = [
      svgBb.width / 2 - transform.translate[0],
      svgBb.height / 2 - transform.translate[1],
    ];
    transform.scale = [1, 1];

    if (zoom != null) {
      zoom.translate(transform.translate);
      zoom.scale(1);
      zoom.event(innerSvg);
    }
  }
});

function clearFocus() {
  d3.selectAll('g.node, g.edgePaths, g.edgeLabel')
    .attr('data-highlight', null);
  localStorage.removeItem(focusedGroupKey(dagId));
}

function focusState(state, node, color) {
  d3.selectAll('g.node, g.edgePaths, g.edgeLabel')
    .attr('data-highlight', 'fade');
  d3.selectAll(`g.node.${state}`)
    .attr('data-highlight', null);
  d3.selectAll(`g.node.${state} rect`)
    .attr('data-highlight', null);
  d3.select(node)
    .style('background-color', color);
}

function setFocusMap(state) {
  for (const key in stateFocusMap) {
    if ({}.hasOwnProperty.call(stateFocusMap, key)) {
      stateFocusMap[key] = false;
    }
  }
  if (state != null) {
    stateFocusMap[state] = true;
  }
}

function stateIsSet() {
  for (const key in stateFocusMap) {
    if (stateFocusMap[key]) {
      return true;
    }
  }
  return false;
}

function handleRefresh() {
  $('#loading-dots').css('display', 'inline-block');
  $.get(getTaskInstanceURL)
    .done(
      (tis) => {
        // eslint-disable-next-line no-global-assign
        taskInstances = JSON.parse(tis);
        updateNodesStates(taskInstances);
        setTimeout(() => { $('#loading-dots').hide(); }, 500);
        $('#error').hide();
      },
    ).fail((_, textStatus, err) => {
      $('#error_msg').text(`${textStatus}: ${err}`);
      $('#error').show();
      setTimeout(() => { $('#loading-dots').hide(); }, 500);
      $('#chart_section').hide(1000);
      $('#datatable_section').hide(1000);
    });
}

let refreshInterval;

function startOrStopRefresh() {
  if ($('#auto_refresh').is(':checked')) {
    refreshInterval = setInterval(() => {
      handleRefresh();
    }, 3000); // run refresh every 3 seconds
  } else {
    clearInterval(refreshInterval);
  }
}

$('#auto_refresh').change(() => {
  if ($('#auto_refresh').is(':checked')) {
    // Run an initial refesh before starting interval if manually turned on
    handleRefresh();
    localStorage.removeItem('disableAutoRefresh');
  } else {
    localStorage.setItem('disableAutoRefresh', 'true');
  }
  startOrStopRefresh();
});

function initRefresh() {
  if (localStorage.getItem('disableAutoRefresh')) {
    $('#auto_refresh').removeAttr('checked');
  }
  startOrStopRefresh();
  d3.select('#refresh_button').on('click', () => handleRefresh());
}

// Generate tooltip for a group node
function groupTooltip(nodeId, tis) {
  const numMap = new Map([['success', 0],
    ['failed', 0],
    ['upstream_failed', 0],
    ['up_for_retry', 0],
    ['running', 0],
    ['no_status', 0]]);
  for (const child of getChildrenIds(g.node(nodeId))) {
    if (child in tis) {
      const ti = tis[child];
      const stateKey = ti.state == null ? 'no_status' : ti.state;
      if (numMap.has(stateKey)) numMap.set(stateKey, numMap.get(stateKey) + 1);
    }
  }

  const tip = taskGroupTips.get(nodeId);
  let tt = `${escapeHtml(tip)}<br><br>`;
  for (const [key, val] of numMap.entries()) tt += `<strong>${escapeHtml(key)}:</strong> ${val} <br>`;

  return tt;
}

// Assigning css classes based on state to nodes
// Initiating the tooltips
function updateNodesStates(tis) {
  for (const nodeId of g.nodes()) {
    const { elem } = g.node(nodeId);
    elem.setAttribute('class', `node enter ${getNodeState(nodeId, tis)}`);
    elem.setAttribute('data-toggle', 'tooltip');

    const taskId = nodeId;
    elem.onmouseover = (evt) => {
      if (taskId in tis) {
        const tt = tiTooltip(tis[taskId]);
        taskTip.show(tt, evt.target); // taskTip is defined in graph.html
      } else if (taskGroupTips.has(taskId)) {
        const tt = groupTooltip(taskId, tis);
        taskTip.show(tt, evt.target);
      } else if (taskId in tasks) {
        const tt = taskNoInstanceTooltip(taskId, tasks[taskId]);
        taskTip.show(tt, evt.target);
      }
    };
    elem.onmouseout = taskTip.hide;
    elem.onclick = taskTip.hide;
  }
}

// Returns list of children id of the given task group
function getChildrenIds(group) {
  const children = [];
  for (const [key, val] of Object.entries(group.children)) {
    if (val.children === undefined) {
      // node
      children.push(val.id);
    } else {
      // group
      const subGroupChildren = getChildrenIds(val);
      for (const id of subGroupChildren) {
        children.push(id);
      }
    }
  }
  return children;
}

// Return list of all task group ids in the given task group including the given group itself.
function getAllGroupIds(group) {
  const children = [group.id];

  for (const [key, val] of Object.entries(group.children)) {
    if (val.children !== undefined) {
      // group
      const subGroupChildren = getAllGroupIds(val);
      for (const id of subGroupChildren) {
        children.push(id);
      }
    }
  }
  return children;
}

// Return the state for the node based on the state of its taskinstance or that of its
// children if it's a group node
function getNodeState(nodeId, tis) {
  const node = g.node(nodeId);

  if (node.children === undefined) {
    if (nodeId in tis) {
      return tis[nodeId].state || 'no_status';
    }
    return 'no_status';
  }
  const children = getChildrenIds(node);

  const childrenStates = new Set();
  children.forEach((taskId) => {
    if (taskId in tis) {
      const { state } = tis[taskId];
      childrenStates.add(state == null ? 'no_status' : state);
    }
  });

  // In this order, if any of these states appeared in childrenStates, return it as
  // the group state.
  const priority = ['failed', 'upstream_failed', 'up_for_retry', 'up_for_reschedule',
    'queued', 'scheduled', 'sensing', 'running', 'shutdown', 'removed',
    'no_status', 'success', 'skipped'];

  for (const state of priority) {
    if (childrenStates.has(state)) return state;
  }
  return 'no_status';
}

// Returns the key used to store expanded task group ids in localStorage
function expandedGroupsKey() {
  return `expandedGroups_${dagId}`;
}

// Returns the key used to store the focused task group id in localStorage
function focusedGroupKey() {
  return `focused_group_${dagId}`;
}

// Focus the graph on the expanded/collapsed node
function focusGroup(nodeId) {
  if (nodeId != null && zoom != null) {
    const { x } = g.node(nodeId);
    const { y } = g.node(nodeId);
    // This is the total canvas size.
    const { width, height } = svg.node().getBoundingClientRect();

    // This is the size of the node or the cluster (i.e. group)
    let rect = d3.selectAll('g.node').filter((n) => n === nodeId).select('rect');
    if (rect.empty()) rect = d3.selectAll('g.cluster').filter((n) => n === nodeId).select('rect');

    // Is there a better way to get nodeWidth and nodeHeight ?
    const [nodeWidth, nodeHeight] = [
      rect[0][0].attributes.width.value, rect[0][0].attributes.height.value,
    ];

    // Calculate zoom scale to fill most of the canvas with the node/cluster in focus.
    const scale = Math.min(
      Math.min(width / nodeWidth, height / nodeHeight),
      1.5, // cap zoom level to 1.5 so nodes are not too large
    ) * 0.9;

    const [deltaX, deltaY] = [width / 2 - x * scale, height / 2 - y * scale];
    zoom.translate([deltaX, deltaY]);
    zoom.scale(scale);
    zoom.event(innerSvg.transition().duration(duration));

    const children = new Set(g.children(nodeId));
    // Set data attr to highlight the focused group (via CSS).
    d3.selectAll('g.nodes g.node').forEach(function cssHighlight(d) {
      if (d === nodeId || children.has(d)) {
        d3.select(this)
          .attr('data-highlight', null);
      } else {
        d3.select(this)
          .attr('data-highlight', 'fade');
      }
    });

    localStorage.setItem(focusedGroupKey(dagId), nodeId);
  }
}

// Expands a group node
function expandGroup(nodeId, node, focus = true) {
  node.children.forEach((val) => {
    // Set children nodes
    g.setNode(val.id, val.value);
    mapTaskToNode.set(val.id, val.id);
    g.node(val.id).id = val.id;
    if (val.children !== undefined) {
      // Set children attribute so that the group can be expanded later when needed.
      const groupNode = g.node(val.id);
      groupNode.children = val.children;
      // Map task that are under this node to this node's id
      for (const childId of getChildrenIds(val)) mapTaskToNode.set(childId, val.id);
    }
    // Only call setParent if node is not the root node.
    if (nodeId != null) g.setParent(val.id, nodeId);
  });

  // Add edges
  edges.forEach((edge) => {
    const sourceId = mapTaskToNode.get(edge.source_id);
    const targetId = mapTaskToNode.get(edge.target_id);
    if (sourceId !== targetId && !g.hasEdge(sourceId, targetId)) {
      g.setEdge(sourceId, targetId, {
        curve: d3.curveBasis,
        arrowheadClass: 'arrowhead',
        label: edge.label,
      });
    }
  });

  g.edges().forEach((edge) => {
    // Remove edges that were associated with the expanded group node..
    if (nodeId === edge.v || nodeId === edge.w) {
      g.removeEdge(edge.v, edge.w);
    }
  });

  draw();

  if (focus) {
    focusGroup(nodeId);
  }

  saveExpandedGroup(nodeId);
}

function getSavedGroups() {
  let expandedGroups;
  try {
    expandedGroups = new Set(JSON.parse(localStorage.getItem(expandedGroupsKey(dagId))));
  } catch {
    expandedGroups = new Set();
  }

  return expandedGroups;
}

// Clean up invalid group_ids from saved_group_ids (e.g. due to DAG changes)
function pruneInvalidSavedGroupIds() {
  // All the groupIds in the whole DAG
  const allGroupIds = new Set(getAllGroupIds(nodes));
  let expandedGroups = getSavedGroups(dagId);
  expandedGroups = Array.from(expandedGroups).filter((groupId) => allGroupIds.has(groupId));
  localStorage.setItem(expandedGroupsKey(dagId), JSON.stringify(expandedGroups));
}

// Remember the expanded groups in local storage so that it can be used
// to restore the expanded state of task groups.
function saveExpandedGroup(nodeId) {
  // expandedGroups is a Set
  const expandedGroups = getSavedGroups(dagId);
  expandedGroups.add(nodeId);
  localStorage.setItem(expandedGroupsKey(dagId), JSON.stringify(Array.from(expandedGroups)));
}

// Remove the nodeId from the expanded state
function removeExpandedGroup(nodeId, node) {
  const expandedGroups = getSavedGroups(dagId);
  const childGroupIds = getAllGroupIds(node);
  childGroupIds.forEach((childId) => expandedGroups.delete(childId));
  localStorage.setItem(expandedGroupsKey(dagId), JSON.stringify(Array.from(expandedGroups)));
}

// Restore previously expanded task groups
function expandSavedGroups(expandedGroups, node) {
  if (node.children === undefined) return;

  node.children.forEach((childNode) => {
    if (expandedGroups.has(childNode.id)) {
      expandGroup(childNode.id, g.node(childNode.id), false);

      expandSavedGroups(expandedGroups, childNode);
    }
  });
}

pruneInvalidSavedGroupIds();
const focusNodeId = localStorage.getItem(focusedGroupKey(dagId));
const expandedGroups = getSavedGroups(dagId);

// Always expand the root node
expandGroup(null, nodes);

// Expand the node that were previously expanded
expandSavedGroups(expandedGroups, nodes);

// Restore focus (if available)
if (g.hasNode(focusNodeId)) {
  focusGroup(focusNodeId);
}

initRefresh();
