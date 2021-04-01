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
"""Get the ISO standard day number of the week from a given day string"""
import enum


@enum.unique
class WeekDay(enum.IntEnum):
    """Python Enum containing Days of the Week"""

    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7

    @classmethod
    def get_weekday_number(cls, week_day_str):
        """
        Return the ISO Week Day Number for a Week Day

        :param week_day_str: Full Name of the Week Day. Example: "Sunday"
        :type week_day_str: str
        :return: ISO Week Day Number corresponding to the provided Weekday
        """
        sanitized_week_day_str = week_day_str.upper()

        # pylint: disable=unsupported-membership-test
        if sanitized_week_day_str not in cls.__members__:
            raise AttributeError(f'Invalid Week Day passed: "{week_day_str}"')

        return cls[sanitized_week_day_str]
