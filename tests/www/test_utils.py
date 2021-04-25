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

import re
import unittest
from datetime import datetime
from urllib.parse import parse_qs

from bs4 import BeautifulSoup
from parameterized import parameterized

from airflow.www import utils
from airflow.www.utils import wrapped_markdown
from tests.test_utils.config import conf_vars


class TestUtils(unittest.TestCase):
    def test_empty_variable_should_not_be_hidden(self):
        assert not utils.should_hide_value_for_key("")
        assert not utils.should_hide_value_for_key(None)

    def test_normal_variable_should_not_be_hidden(self):
        assert not utils.should_hide_value_for_key("key")

    def test_sensitive_variable_should_be_hidden(self):
        assert utils.should_hide_value_for_key("google_api_key")

    def test_sensitive_variable_should_be_hidden_ic(self):
        assert utils.should_hide_value_for_key("GOOGLE_API_KEY")

    @parameterized.expand(
        [
            ('key', 'TRELLO_KEY', True),
            ('key', 'TRELLO_API_KEY', True),
            ('key', 'GITHUB_APIKEY', True),
            ('key, token', 'TRELLO_TOKEN', True),
            ('mysecretword, mysensitivekey', 'GITHUB_mysecretword', True),
        ],
    )
    def test_sensitive_variable_fields_should_be_hidden(
        self, sensitive_variable_fields, key, expected_result
    ):
        with conf_vars({('admin', 'sensitive_variable_fields'): str(sensitive_variable_fields)}):
            assert expected_result == utils.should_hide_value_for_key(key)

    @parameterized.expand(
        [
            (None, 'TRELLO_API', False),
            ('token', 'TRELLO_KEY', False),
            ('token, mysecretword', 'TRELLO_KEY', False),
        ],
    )
    def test_normal_variable_fields_should_not_be_hidden(
        self, sensitive_variable_fields, key, expected_result
    ):
        with conf_vars({('admin', 'sensitive_variable_fields'): str(sensitive_variable_fields)}):
            assert expected_result == utils.should_hide_value_for_key(key)

    def check_generate_pages_html(self, current_page, total_pages, window=7, check_middle=False):
        extra_links = 4  # first, prev, next, last
        search = "'>\"/><img src=x onerror=alert(1)>"
        html_str = utils.generate_pages(current_page, total_pages, search=search)

        assert search not in html_str, "The raw search string shouldn't appear in the output"
        assert 'search=%27%3E%22%2F%3E%3Cimg+src%3Dx+onerror%3Dalert%281%29%3E' in html_str

        assert callable(html_str.__html__), "Should return something that is HTML-escaping aware"

        dom = BeautifulSoup(html_str, 'html.parser')
        assert dom is not None

        ulist = dom.ul
        ulist_items = ulist.find_all('li')
        assert min(window, total_pages) + extra_links == len(ulist_items)

        page_items = ulist_items[2:-2]
        mid = int(len(page_items) / 2)
        for i, item in enumerate(page_items):
            a_node = item.a
            href_link = a_node['href']
            node_text = a_node.string
            if node_text == str(current_page + 1):
                if check_middle:
                    assert mid == i
                assert 'javascript:void(0)' == href_link
                assert 'active' in item['class']
            else:
                assert re.search(r'^\?', href_link), 'Link is page-relative'
                query = parse_qs(href_link[1:])
                assert query['page'] == [str(int(node_text) - 1)]
                assert query['search'] == [search]

    def test_generate_pager_current_start(self):
        self.check_generate_pages_html(current_page=0, total_pages=6)

    def test_generate_pager_current_middle(self):
        self.check_generate_pages_html(current_page=10, total_pages=20, check_middle=True)

    def test_generate_pager_current_end(self):
        self.check_generate_pages_html(current_page=38, total_pages=39)

    def test_params_no_values(self):
        """Should return an empty string if no params are passed"""
        assert '' == utils.get_params()

    def test_params_search(self):
        assert 'search=bash_' == utils.get_params(search='bash_')

    def test_params_none_and_zero(self):
        query_str = utils.get_params(a=0, b=None, c='true')
        # The order won't be consistent, but that doesn't affect behaviour of a browser
        pairs = list(sorted(query_str.split('&')))
        assert ['a=0', 'c=true'] == pairs

    def test_params_all(self):
        query = utils.get_params(status='active', page=3, search='bash_')
        assert {'page': ['3'], 'search': ['bash_'], 'status': ['active']} == parse_qs(query)

    def test_params_escape(self):
        assert 'search=%27%3E%22%2F%3E%3Cimg+src%3Dx+onerror%3Dalert%281%29%3E' == utils.get_params(
            search="'>\"/><img src=x onerror=alert(1)>"
        )

    def test_state_token(self):
        # It's shouldn't possible to set these odd values anymore, but lets
        # ensure they are escaped!
        html = str(utils.state_token('<script>alert(1)</script>'))

        assert '&lt;script&gt;alert(1)&lt;/script&gt;' in html
        assert '<script>alert(1)</script>' not in html

    def test_task_instance_link(self):

        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(
                utils.task_instance_link(
                    {'dag_id': '<a&1>', 'task_id': '<b2>', 'execution_date': datetime.now()}
                )
            )

        assert '%3Ca%261%3E' in html
        assert '%3Cb2%3E' in html
        assert '<a&1>' not in html
        assert '<b2>' not in html

    def test_dag_link(self):
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(utils.dag_link({'dag_id': '<a&1>', 'execution_date': datetime.now()}))

        assert '%3Ca%261%3E' in html
        assert '<a&1>' not in html

    def test_dag_link_when_dag_is_none(self):
        """Test that when there is no dag_id, dag_link does not contain hyperlink"""
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(utils.dag_link({}))

        assert 'None' in html
        assert '<a href=' not in html

    def test_dag_run_link(self):
        from airflow.www.app import cached_app

        with cached_app(testing=True).test_request_context():
            html = str(
                utils.dag_run_link({'dag_id': '<a&1>', 'run_id': '<b2>', 'execution_date': datetime.now()})
            )

        assert '%3Ca%261%3E' in html
        assert '%3Cb2%3E' in html
        assert '<a&1>' not in html
        assert '<b2>' not in html


class TestAttrRenderer(unittest.TestCase):
    def setUp(self):
        self.attr_renderer = utils.get_attr_renderer()

    def test_python_callable(self):
        def example_callable(unused_self):
            print("example")

        rendered = self.attr_renderer["python_callable"](example_callable)
        assert '&quot;example&quot;' in rendered

    def test_python_callable_none(self):
        rendered = self.attr_renderer["python_callable"](None)
        assert "" == rendered

    def test_markdown(self):
        markdown = "* foo\n* bar"
        rendered = self.attr_renderer["doc_md"](markdown)
        assert "<li>foo</li>" in rendered
        assert "<li>bar</li>" in rendered

    def test_markdown_none(self):
        rendered = self.attr_renderer["python_callable"](None)
        assert "" == rendered


class TestWrappedMarkdown(unittest.TestCase):
    def test_wrapped_markdown_with_docstring_curly_braces(self):
        rendered = wrapped_markdown("{braces}", css_class="a_class")
        assert '<div class="a_class" ><p>{braces}</p></div>' == rendered

    def test_wrapped_markdown_with_some_markdown(self):
        rendered = wrapped_markdown("*italic*\n**bold**\n", css_class="a_class")
        assert (
            '''<div class="a_class" ><p><em>italic</em>
<strong>bold</strong></p></div>'''
            == rendered
        )

    def test_wrapped_markdown_with_table(self):
        rendered = wrapped_markdown(
            """| Job | Duration |
               | ----------- | ----------- |
               | ETL | 14m |"""
        )

        assert (
            '<div class="None" ><table>\n<thead>\n<tr>\n<th>Job</th>\n'
            '<th>Duration</th>\n</tr>\n</thead>\n<tbody>\n<tr>\n<td>ETL'
            '</td>\n<td>14m</td>\n</tr>\n</tbody>\n'
            '</table></div>'
        ) == rendered

    def test_wrapped_markdown_with_indented_lines(self):
        rendered = wrapped_markdown(
            """
                # header
                1st line
                2nd line
            """
        )

        assert '<div class="None" ><h1>header</h1>\n<p>1st line\n2nd line</p></div>' == rendered
