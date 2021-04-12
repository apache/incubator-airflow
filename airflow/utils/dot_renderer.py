#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Renderer DAG (tasks and dependencies) to the graphviz object."""
from typing import Dict, List, Optional

import graphviz

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskmixin import TaskMixin
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.www.views import dag_edges


def _refine_color(color: str):
    """
    Converts color in #RGB (12 bits) format to #RRGGBB (32 bits), if it possible.
    Otherwise, it returns the original value. Graphviz does not support colors in #RGB format.

    :param color: Text representation of color
    :return: Refined representation of color
    """
    if len(color) == 4 and color[0] == "#":
        color_r = color[1]
        color_g = color[2]
        color_b = color[3]
        return "#" + color_r + color_r + color_g + color_g + color_b + color_b
    return color


def _draw_task(task: BaseOperator, parent_graph: graphviz.Digraph, states_by_task_id: Dict[str, str]) -> None:
    """Draw a single task on the given parent_graph"""
    if states_by_task_id:
        state = states_by_task_id.get(task.task_id, State.NONE)
        color = State.color_fg(state)
        fill_color = State.color(state)
    else:
        color = task.ui_fgcolor
        fill_color = task.ui_color

    parent_graph.node(
        task.task_id,
        _attributes={
            "label": task.label,
            "shape": "rectangle",
            "style": "filled,rounded",
            "color": _refine_color(color),
            "fillcolor": _refine_color(fill_color),
        },
    )


def _draw_task_group(
    task_group: TaskGroup, parent_graph: graphviz.Digraph, states_by_task_id: Dict[str, str]
) -> None:
    """Draw the given task_group and its children on the given parent_graph"""
    # Draw joins
    if task_group.upstream_group_ids or task_group.upstream_task_ids:
        parent_graph.node(
            task_group.upstream_join_id,
            _attributes={
                "label": "",
                "shape": "circle",
                "style": "filled,rounded",
                "color": _refine_color(task_group.ui_fgcolor),
                "fillcolor": _refine_color(task_group.ui_color),
                "width": "0.2",
                "height": "0.2",
            },
        )

    if task_group.downstream_group_ids or task_group.downstream_task_ids:
        parent_graph.node(
            task_group.downstream_join_id,
            _attributes={
                "label": "",
                "shape": "circle",
                "style": "filled,rounded",
                "color": _refine_color(task_group.ui_fgcolor),
                "fillcolor": _refine_color(task_group.ui_color),
                "width": "0.2",
                "height": "0.2",
            },
        )

    # Draw children
    for child in sorted(task_group.children.values(), key=lambda t: t.label):
        _draw_nodes(child, parent_graph, states_by_task_id)


def _draw_nodes(node: TaskMixin, parent_graph: graphviz.Digraph, states_by_task_id: Dict[str, str]) -> None:
    """Draw the node and its children on the given parent_graph recursively."""
    if isinstance(node, BaseOperator):
        _draw_task(node, parent_graph, states_by_task_id)
    else:
        # Draw TaskGroup
        if node.is_root:
            # No need to draw background for root TaskGroup.
            _draw_task_group(node, parent_graph, states_by_task_id)
        else:
            with parent_graph.subgraph(name=f"cluster_{node.group_id}") as sub:
                sub.attr(
                    shape="rectangle",
                    style="filled",
                    color=_refine_color(node.ui_fgcolor),
                    # Partially transparent CornflowerBlue
                    fillcolor="#6495ed7f",
                    label=node.label,
                )
                _draw_task_group(node, sub, states_by_task_id)


def render_dag(dag: DAG, tis: Optional[List[TaskInstance]] = None) -> graphviz.Digraph:
    """
    Renders the DAG object to the DOT object.

    If an task instance list is passed, the nodes will be painted according to task statuses.

    :param dag: DAG that will be rendered.
    :type dag: airflow.models.dag.DAG
    :param tis: List of task instances
    :type tis: Optional[List[TaskInstance]]
    :return: Graphviz object
    :rtype: graphviz.Digraph
    """
    dot = graphviz.Digraph(
        dag.dag_id,
        graph_attr={
            "rankdir": dag.orientation if dag.orientation else "LR",
            "labelloc": "t",
            "label": dag.dag_id,
        },
    )
    states_by_task_id = None
    if tis is not None:
        states_by_task_id = {ti.task_id: ti.state for ti in tis}

    _draw_nodes(dag.task_group, dot, states_by_task_id)

    for edge in dag_edges(dag):
        # Gets an optional label for the edge; this will be None if none is specified.
        label = dag.get_edge_info(edge["source_id"], edge["target_id"]).get("label")
        # Add the edge to the graph with optional label
        # (we can just use the maybe-None label variable directly)
        dot.edge(edge["source_id"], edge["target_id"], label)

    return dot
