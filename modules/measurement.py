#!/usr/bin/env python
""" A plugin to provide a database connection using the InfluxDB time series database

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

from lib.abstract import AbstractModule
from data.database.dataloader import SensorReadingDataLoader
import logging


class Module(AbstractModule):

    RETRY_TIMEOUT = 5

    def __init__(self, module):
        super().__init__(module)

    def __del__(self):
        pass

    def process_inputs_callback(self, data):
        # Use graphql mutation to write data to storage
        result = self.module['schema'].execute(
            '''
            mutation CreateMeasurement($measurement: MeasurementInput!) {
                createMeasurement(measurement: $measurement) {
                    time
                }
            }
            ''',
            variable_values={
                "measurement": data
            },
            context_value={"database": self.module.get('database', None)}
        )
        if result.errors:
            logging.error(result.errors)
            raise RuntimeError("Error in GraphQL mutation")
