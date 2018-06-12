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
from data.measurement.types import Measurement
import graphene


class LatestReadingDataLoader(AbstractDataLoader):
    def __init__(self, database):
        super().__init__(database)

    cache = False

    def batch_load_fn(self, keys):
        result_set = self.database.all_latest_measurements()
        return Promise.resolve([
            self.get_latest_measurement(result_set=result_set, key=key) for key in keys
        ])

    def get_latest_measurement(self, result_set, key):
        all_measurements = []
        try:
            if key == '*':
                for item in result_set.items():
                    points = list(item[1])
                    for point in points:
                        point['id'] = item[0][1]['id']
                        measurement = Measurement(**point)
                        all_measurements.append(measurement)
            else:
                measurement = list(result_set.get_points(tags={'id': key}))[0]
                measurement['id'] = key
                all_measurements.append(Measurement(**measurement))
        except IndexError as e:
            print(e)
            pass

        return all_measurements


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
        result_set = self.database.all_measurements_between(self.__start, self.__end)
        return Promise.resolve([
            self.get_measurements_between(result_set=result_set, key=key) for key in keys
        ])

    def get_measurements_between(self, result_set, key):
        all_measurements = []
        try:
            if key == '*':
                for item in result_set.items():
                    measurements = []
                    all_measurements.append(measurements)
                    points = list(item[1])
                    for point in points:
                        point['id'] = item[0][1]['id']
                        measurement = Measurement(id=item[0][1]['id'],
                                                  time=point['time'],
                                                  value=point['mean_value'])
                        measurements.append(measurement)
            else:
                measurements = []
                all_measurements.append(measurements)
                for measurement in list(result_set.get_points(tags={'id': key})):
                    measurements.append(Measurement(id=key,
                                                    time=measurement['time'],
                                                    value=measurement['mean_value']))
        except IndexError as e:
            print(e)
            return None

        return all_measurements
