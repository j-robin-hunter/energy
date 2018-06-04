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


class SensorReadingDataLoader(AbstractDataLoader):
    def __init__(self, database):
        super().__init__(database)

    cache = False

    def batch_load_fn(self, keys):
        measurement_result_set = self.database.summary()
        return Promise.resolve([self.get_measurement_reading(result_set=measurement_result_set, key=key) for key in keys])

    def get_measurement_reading(self, result_set, key):
        try:
            measurement = list(result_set.get_points(tags={'id': key}))[0]
        except IndexError as e:
            return None

        measurement['id'] = key
        return Measurement(**measurement)
