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

import argparse
import contextlib
import io
import re
from collections import Counter
from unittest import TestCase

import pytest

from airflow.cli import cli_parser

# Can not be `--snake_case` or contain uppercase letter
ILLEGAL_LONG_OPTION_PATTERN = re.compile("^--[a-z]+_[a-z]+|^--.*[A-Z].*")
# Only can be `-[a-z]` or `-[A-Z]`
LEGAL_SHORT_OPTION_PATTERN = re.compile("^-[a-zA-z]$")

cli_args = {k: v for k, v in cli_parser.__dict__.items() if k.startswith("ARG_")}


class TestCli(TestCase):
    def test_arg_option_long_only(self):
        """
        Test if the name of cli.args long option valid
        """
        optional_long = [
            arg for arg in cli_args.values() if len(arg.flags) == 1 and arg.flags[0].startswith("-")
        ]
        for arg in optional_long:
            assert ILLEGAL_LONG_OPTION_PATTERN.match(arg.flags[0]) is None, f"{arg.flags[0]} is not match"

    def test_arg_option_mix_short_long(self):
        """
        Test if the name of cli.args mix option (-s, --long) valid
        """
        optional_mix = [
            arg for arg in cli_args.values() if len(arg.flags) == 2 and arg.flags[0].startswith("-")
        ]
        for arg in optional_mix:
            assert LEGAL_SHORT_OPTION_PATTERN.match(arg.flags[0]) is not None, f"{arg.flags[0]} is not match"
            assert ILLEGAL_LONG_OPTION_PATTERN.match(arg.flags[1]) is None, f"{arg.flags[1]} is not match"

    def test_subcommand_conflict(self):
        """
        Test if each of cli.*_COMMANDS without conflict subcommand
        """
        subcommand = {
            var: cli_parser.__dict__.get(var)
            for var in cli_parser.__dict__
            if var.isupper() and var.startswith("COMMANDS")
        }
        for group_name, sub in subcommand.items():
            name = [command.name.lower() for command in sub]
            assert len(name) == len(set(name)), f"Command group {group_name} have conflict subcommand"

    def test_subcommand_arg_name_conflict(self):
        """
        Test if each of cli.*_COMMANDS.arg name without conflict
        """
        subcommand = {
            var: cli_parser.__dict__.get(var)
            for var in cli_parser.__dict__
            if var.isupper() and var.startswith("COMMANDS")
        }
        for group, command in subcommand.items():
            for com in command:
                conflict_arg = [arg for arg, count in Counter(com.args).items() if count > 1]
                assert [] == conflict_arg, (
                    f"Command group {group} function {com.name} have " f"conflict args name {conflict_arg}"
                )

    def test_subcommand_arg_flag_conflict(self):
        """
        Test if each of cli.*_COMMANDS.arg flags without conflict
        """
        subcommand = {
            key: val
            for key, val in cli_parser.__dict__.items()
            if key.isupper() and key.startswith("COMMANDS")
        }
        for group, command in subcommand.items():
            for com in command:
                position = [
                    a.flags[0] for a in com.args if (len(a.flags) == 1 and not a.flags[0].startswith("-"))
                ]
                conflict_position = [arg for arg, count in Counter(position).items() if count > 1]
                assert [] == conflict_position, (
                    f"Command group {group} function {com.name} have conflict "
                    f"position flags {conflict_position}"
                )

                long_option = [
                    a.flags[0] for a in com.args if (len(a.flags) == 1 and a.flags[0].startswith("-"))
                ] + [a.flags[1] for a in com.args if len(a.flags) == 2]
                conflict_long_option = [arg for arg, count in Counter(long_option).items() if count > 1]
                assert [] == conflict_long_option, (
                    f"Command group {group} function {com.name} have conflict "
                    f"long option flags {conflict_long_option}"
                )

                short_option = [a.flags[0] for a in com.args if len(a.flags) == 2]
                conflict_short_option = [arg for arg, count in Counter(short_option).items() if count > 1]
                assert [] == conflict_short_option, (
                    f"Command group {group} function {com.name} have conflict "
                    f"short option flags {conflict_short_option}"
                )

    def test_falsy_default_value(self):
        arg = cli_parser.Arg(("--test",), default=0, type=int)
        parser = argparse.ArgumentParser()
        arg.add_to_parser(parser)

        args = parser.parse_args(['--test', '10'])
        assert args.test == 10

        args = parser.parse_args([])
        assert args.test == 0

    def test_commands_and_command_group_sections(self):
        parser = cli_parser.get_parser()

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            with pytest.raises(SystemExit):
                parser.parse_args(['--help'])
            stdout = stdout.getvalue()
        assert "Commands" in stdout
        assert "Groups" in stdout

    def test_dag_parser_commands_and_comamnd_group_sections(self):
        parser = cli_parser.get_parser(dag_parser=True)

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            with self.assertRaises(SystemExit):
                parser.parse_args(['--help'])
            stdout = stdout.getvalue()
        self.assertIn("Commands", stdout)
        self.assertIn("Groups", stdout)

    def test_should_display_help(self):
        parser = cli_parser.get_parser()

        all_command_as_args = [
            command_as_args
            for top_command in cli_parser.airflow_commands
            for command_as_args in (
                [[top_command.name]]
                if isinstance(top_command, cli_parser.ActionCommand)
                else [[top_command.name, nested_command.name] for nested_command in top_command.subcommands]
            )
        ]
        for cmd_args in all_command_as_args:
            with pytest.raises(SystemExit):
                parser.parse_args([*cmd_args, '--help'])

    def test_dag_cli_should_display_help(self):
        parser = cli_parser.get_parser(dag_parser=True)

        all_command_as_args = [
            command_as_args
            for top_command in cli_parser.dag_cli_commands
            for command_as_args in (
                [[top_command.name]]
                if isinstance(top_command, cli_parser.ActionCommand)
                else [[top_command.name, nested_command.name] for nested_command in top_command.subcommands]
            )
        ]
        for cmd_args in all_command_as_args:
            with self.assertRaises(SystemExit):
                parser.parse_args([*cmd_args, '--help'])

    def test_positive_int(self):
        assert 1 == cli_parser.positive_int(allow_zero=True)('1')
        assert 0 == cli_parser.positive_int(allow_zero=True)('0')

        with pytest.raises(argparse.ArgumentTypeError):
            cli_parser.positive_int(allow_zero=False)('0')
            cli_parser.positive_int(allow_zero=True)('-1')
