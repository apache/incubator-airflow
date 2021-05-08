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

import os


def copy_docker_compose(app, exception):
    """Sphinx "build-finished" event handler."""
    from sphinx.builders import html as builders

    if exception or not isinstance(app.builder, builders.StandaloneHTMLBuilder):
        return

    # Replace `|version|` in the docker-compose.yaml that requires manual substitutions
    for path in app.config.html_extra_with_substitutions:
        with open(path) as file:
            with open(os.path.join(app.outdir, os.path.basename(path)), "w") as output_file:
                for line in file:
                    output_file.write(line.replace('|version|', app.config.version))

    # Replace `|version|` in the installation.html that requires manual substitutions (in links)
    for path in app.config.manual_substitutions_in_generated_html:
        with open(os.path.join(app.outdir, os.path.basename(path))) as input_file:
            content = input_file.readlines()
        with open(os.path.join(app.outdir, os.path.basename(path)), "wt") as output_file:
            for line in content:
                output_file.write(line.replace('|version|', app.config.version))


def setup(app):
    """Setup plugin"""
    app.connect("build-finished", copy_docker_compose)

    app.add_config_value("html_extra_with_substitutions", [], '[str]')
    app.add_config_value("manual_substitutions_in_generated_html", [], '[str]')

    return {
        'parallel_write_safe': True,
    }
