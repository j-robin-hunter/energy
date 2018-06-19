#!/usr/bin/env python
""" Main entry program for Roma Technology energy data logging/capture program.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = "Robin Hunter"
__contact__ = "rhunter@crml.com"
__copyright__ = "Copyright 2018, Roma Technology Limited"
__license__ = "GPLv3"
__status__ = "Production"
__version__ = "1.0.0"

from promise import Promise
from lib.abstract import AbstractDataLoader
from data.base.types import MeterReading


class LatestReadingDataLoader(AbstractDataLoader):
    def __init__(self, database):
        super().__init__(database)

    cache = False

    def batch_load_fn(self, keys):
        result_set = self.database.all_latest_meter_readings()
        return Promise.resolve([
            self.get_latest_meter_reading(result_set=result_set, key=key) for key in keys
        ])

    def get_latest_meter_reading(self, result_set, key):
        all_meter_readings = []
        try:
            for item in result_set.items():
                if key == "*" or key == item[0][1]['id']:
                    meter_readings = []
                    all_meter_readings.append(meter_readings)
                    points = list(item[1])
                    for point in points:
                        point['id'] = item[0][1]['id']
                        point['source'] = item[0][1]['source']
                        point['unit'] = item[0][1]['unit']
                        meter_reading = MeterReading(**point)
                        meter_readings.append(meter_reading)

        except IndexError:
            pass

        return all_meter_readings


class ReadingsBetweenDataLoader(AbstractDataLoader):
    def __init__(self, database):
        super().__init__(database)
        self.__start = 0
        self.__end = 0

    cache = False

    @property
    def start(self):
        return self.__start

    @start.setter
    def start(self, start):
        self.__start = start

    @property
    def end(self):
        return self.__end

    @end.setter
    def end(self, end):
        self.__end = end

    def batch_load_fn(self, keys):
        result_set = self.database.all_meter_readings_between(self.__start, self.__end)
        return Promise.resolve([
            self.get_meter_readings_between(result_set=result_set, key=key) for key in keys
        ])

    def get_meter_readings_between(self, result_set, key):
        all_meter_readings = []
        try:

            for item in result_set.items():
                if key == '*' or key == item[0][1]['id']:
                    meter_readings = []
                    all_meter_readings.append(meter_readings)
                    points = list(item[1])
                    for point in points:
                        point['id'] = item[0][1]['id']
                        point['source'] = item[0][1]['source']
                        point['unit'] = item[0][1]['unit']
                        meter_reading = MeterReading(**point)
                        meter_readings.append(meter_reading)

        except IndexError:
            pass

        return all_meter_readings
