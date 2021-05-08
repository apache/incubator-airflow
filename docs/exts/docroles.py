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

#
#
"""Document roles"""
from functools import partial

from docutils import nodes, utils
from sphinx.ext.autodoc.importer import import_module, mock


class RoleException(Exception):
    """Exception for roles extension"""


def get_template_field(env, fullname):
    """
    Gets template fields for specific operator class.

    :param env: env config
    :param fullname: Full path to operator class.
        For example: ``airflow.providers.google.cloud.operators.vision.CloudVisionCreateProductSetOperator``
    :return: List of template field
    :rtype: list[str]
    """
    modname, classname = fullname.rsplit(".", 1)

    try:
        with mock(env.config.autodoc_mock_imports):
            mod = import_module(modname)
    except ImportError:
        raise RoleException(f"Error loading {modname} module.")

    clazz = getattr(mod, classname)
    if not clazz:
        raise RoleException(f"Error finding {classname} class in {modname} module.")

    template_fields = getattr(clazz, "template_fields")

    if not template_fields:
        raise RoleException(f"Could not find the template fields for {classname} class in {modname} module.")

    return list(template_fields)


def template_field_role(
    app,
    typ,  # pylint: disable=unused-argument
    rawtext,
    text,
    lineno,
    inliner,
    options=None,  # pylint: disable=unused-argument
    content=None,
):  # pylint: disable=unused-argument
    """
    A role that allows you to include a list of template fields in the middle of the text. This is especially
    useful when writing guides describing how to use the operator.
    The result is a list of fields where each field is shorted in the literal block.

    Sample usage::

    :template-fields:`airflow.operators.bash.BashOperator`

    For further information look at:

    * [http://docutils.sourceforge.net/docs/howto/rst-roles.html](Creating reStructuredText Interpreted
      Text Roles)
    """
    if options is None:
        options = {}
    if content is None:
        content = []
    text = utils.unescape(text)

    try:
        template_fields = get_template_field(app.env, text)
    except RoleException as e:
        msg = inliner.reporter.error(
            f"invalid class name {text} \n{e}",
            line=lineno,
        )
        prb = inliner.problematic(rawtext, rawtext, msg)
        return [prb], [msg]

    node = nodes.inline(rawtext=rawtext)
    for i, field in enumerate(template_fields):
        if i != 0:
            node += nodes.Text(", ")
        node += nodes.literal(field, "", nodes.Text(field))

    return [node], []


def setup(app):
    """Sets the extension up"""
    from docutils.parsers.rst import roles  # pylint: disable=wrong-import-order

    roles.register_local_role("template-fields", partial(template_field_role, app))

    return {"version": "builtin", "parallel_read_safe": True, "parallel_write_safe": True}
