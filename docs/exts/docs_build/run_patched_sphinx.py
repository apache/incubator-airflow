#!/usr/bin/env python
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
import logging
import logging as python_logging
import os
import sys

import autoapi
from autoapi.extension import (
    DEFAULT_FILE_PATTERNS,
    DEFAULT_IGNORE_PATTERNS,
    LANGUAGE_MAPPERS,
    LANGUAGE_REQUIREMENTS,
    ExtensionError,
    RemovedInAutoAPI2Warning,
    _normalise_autoapi_dirs,
    warnings,
)
from autoapi.mappers.python import mapper
from sphinx.cmd.build import main
from sphinx.util import logging as sphinx_logging


def run_autoapi(app):  # pylint: disable=too-many-branches
    """
    Load AutoAPI data from the filesystem.
    """
    if app.config.autoapi_type not in LANGUAGE_MAPPERS:
        raise ExtensionError(
            "Invalid autoapi_type setting, "
            "following values is allowed: {}".format(
                ", ".join(f'"{api_type}"' for api_type in sorted(LANGUAGE_MAPPERS))
            )
        )

    if not app.config.autoapi_dirs:
        raise ExtensionError("You must configure an autoapi_dirs setting")

    if app.config.autoapi_include_summaries is not None:
        warnings.warn(
            "autoapi_include_summaries has been replaced by the show-module-summary AutoAPI option\n",
            RemovedInAutoAPI2Warning,
        )
        if app.config.autoapi_include_summaries:
            app.config.autoapi_options.append("show-module-summary")

    # Make sure the paths are full
    normalised_dirs = _normalise_autoapi_dirs(app.config.autoapi_dirs, app.confdir)
    for _dir in normalised_dirs:
        if not os.path.exists(_dir):
            raise ExtensionError(
                "AutoAPI Directory `{dir}` not found. "
                "Please check your `autoapi_dirs` setting.".format(dir=_dir)
            )

    normalized_root = os.path.normpath(os.path.join(app.srcdir, app.config.autoapi_root))
    url_root = os.path.join("/", app.config.autoapi_root)

    if not all(
        import_name in sys.modules for _, import_name in LANGUAGE_REQUIREMENTS[app.config.autoapi_type]
    ):
        raise ExtensionError(
            "AutoAPI of type `{type}` requires following "
            "packages to be installed and included in extensions list: "
            "{packages}".format(
                type=app.config.autoapi_type,
                packages=", ".join(
                    '{import_name} (available as "{pkg_name}" on PyPI)'.format(
                        pkg_name=pkg_name, import_name=import_name
                    )
                    for pkg_name, import_name in LANGUAGE_REQUIREMENTS[app.config.autoapi_type]
                ),
            )
        )

    sphinx_mapper = LANGUAGE_MAPPERS[app.config.autoapi_type]
    template_dir = app.config.autoapi_template_dir
    if template_dir and not os.path.isabs(template_dir):
        if not os.path.isdir(template_dir):
            template_dir = os.path.join(app.confdir, app.config.autoapi_template_dir)
        elif app.confdir != os.getcwd():
            warnings.warn(
                "autoapi_template_dir will be expected to be "
                " relative to conf.py instead of "
                "relative to where sphinx-build is run\n",
                RemovedInAutoAPI2Warning,
            )
    sphinx_mapper_obj = sphinx_mapper(app, template_dir=template_dir, url_root=url_root)

    if app.config.autoapi_file_patterns:
        file_patterns = app.config.autoapi_file_patterns
    else:
        file_patterns = DEFAULT_FILE_PATTERNS.get(app.config.autoapi_type, [])

    if app.config.autoapi_ignore:
        ignore_patterns = app.config.autoapi_ignore
    else:
        ignore_patterns = DEFAULT_IGNORE_PATTERNS.get(app.config.autoapi_type, [])

    if ".rst" in app.config.source_suffix:
        out_suffix = ".rst"
    elif ".txt" in app.config.source_suffix:
        out_suffix = ".txt"
    else:
        # Fallback to first suffix listed
        out_suffix = app.config.source_suffix[0]

    if sphinx_mapper_obj.load(patterns=file_patterns, dirs=normalised_dirs, ignore=ignore_patterns):
        sphinx_mapper_obj.map(options=app.config.autoapi_options)

        if app.config.autoapi_generate_api_docs:
            sphinx_mapper_obj.output_rst(root=normalized_root, source_suffix=out_suffix)

        if app.config.autoapi_type == "python":
            app.env.autoapi_objects = sphinx_mapper_obj.objects
            app.env.autoapi_all_objects = sphinx_mapper_obj.all_objects


# HACK: sphinx-auto map did not correctly use the confdir attribute instead of srcdir when specifying the
# directory to contain the generated files.
# Unfortunately we have a problem updating to a newer version of this library and we have to use
# sphinx-autoapi v1.0.0, so I am monkeypatching this library to fix this one problem.
autoapi.extension.run_autoapi = run_autoapi


def _resolve_module_placeholders(modules, module_name, visit_path, resolved):
    """Resolve all placeholder children under a module.

    :param modules: A mapping of module names to their data dictionary.
        Placeholders are resolved in place.
    :type modules: dict(str, dict)
    :param module_name: The name of the module to resolve.
    :type module_name: str
    :param visit_path: An ordered set of visited module names.
    :type visited: collections.OrderedDict
    :param resolved: A set of already resolved module names.
    :type resolved: set(str)
    """
    if module_name in resolved:
        return

    visit_path[module_name] = True

    module, children = modules[module_name]
    for child in list(children.values()):
        if child["type"] != "placeholder":
            continue

        if child["original_path"] in modules:
            module["children"].remove(child)
            children.pop(child["name"])
            continue

        imported_from, original_name = child["original_path"].rsplit(".", 1)
        if imported_from in visit_path:
            msg = f"Cannot resolve cyclic import: {', '.join(visit_path)}, {imported_from}"
            mapper.LOGGER.warning(msg)
            module["children"].remove(child)
            children.pop(child["name"])
            continue

        if imported_from not in modules:
            if not imported_from.startswith("airflow.providers") or module_name.startswith(
                "airflow.providers"
            ):
                msg = f"Cannot resolve import of unknown module {imported_from} in {module_name}"
                mapper.LOGGER.warning(msg)
            module["children"].remove(child)
            children.pop(child["name"])
            continue

        _resolve_module_placeholders(modules, imported_from, visit_path, resolved)

        if original_name == "*":
            original_module, originals_map = modules[imported_from]

            # Replace the wildcard placeholder
            # with a list of named placeholders.
            new_placeholders = mapper._expand_wildcard_placeholder(original_module, originals_map, child)
            child_index = module["children"].index(child)
            module["children"][child_index : child_index + 1] = new_placeholders
            children.pop(child["name"])

            for new_placeholder in new_placeholders:
                if new_placeholder["name"] not in children:
                    children[new_placeholder["name"]] = new_placeholder
                original = originals_map[new_placeholder["name"]]
                mapper._resolve_placeholder(new_placeholder, original)
        elif original_name not in modules[imported_from][1]:
            msg = f"Cannot resolve import of {child['original_path']} in {module_name}"
            mapper.LOGGER.warning(msg)
            module["children"].remove(child)
            children.pop(child["name"])
            continue
        else:
            original = modules[imported_from][1][original_name]
            mapper._resolve_placeholder(child, original)

    del visit_path[module_name]
    resolved.add(module_name)


mapper._resolve_module_placeholders = _resolve_module_placeholders

sys.exit(main(sys.argv[1:]))
