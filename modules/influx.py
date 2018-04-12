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
import influxdb
import logging
import requests
from lib.schema import Measurement


class Module(AbstractModule):

    CONNECTION_TIMEOUT = 10
    RETRY_TIMEOUT = 5

    def __init__(self, module):
        super().__init__(module)
        try:
            self.connection = influxdb.InfluxDBClient(
                host=self.get_config_value('host'),
                port=self.get_config_value('port'),
                timeout=self.CONNECTION_TIMEOUT)
            self.connection.create_database(self.get_config_value('database'))
        except Exception:
            logging.error('Unable to connect to database')
            raise RuntimeError('Unable to connect to database')

    def __del__(self):
        try:
            if self.connection is not None:
                self.connection.close()
        except Exception as e:
            logging.error('Encountered error while disposing %s: %s' % (self.getName(), str(e)))

    def process_inputs_callback(self, data):
        if data.value is not None:
            line = self._convert_data(data)

            # A ConnectionError can occur if the database becomes disconnected. If this happens the database
            # connector will do a number of retries but since the connection could be down for a
            # period due to network errors the database connection retries will be too short to be of
            # real value. As such a loop is established that will keep retrying until the connection
            # succeeds or the program gets terminated due to some other condition error
            while not self.terminate.is_set():
                try:
                    logging.debug('Writing data to database: %s' % line)
                    self.connection.write(line, {'db': self.get_config_value('database')}, 204, 'line')
                    break
                except requests.exceptions.ConnectionError:
                    logging.info('Database connection error - retrying in %d second(s)' % self.RETRY_TIMEOUT)
                    pass
                except Exception as e:
                    logging.error(str(e))
                    raise RuntimeError(str(e))
        else:
            logging.warning('Data received by module "%s" contains no value' % self.getName())

    def _convert_data(self, data):
        if isinstance(data, Measurement):
            logging.debug('Converting Measurement to InfluxDB line format')
            # Need to build formatted line accounting for missing data
            line = self.get_config_value('series')
            if data.measurement is not None:
                line = line + ',measurement=%s' % data.measurement.replace(' ', '\ ')
            if data.sensor is not None:
                line = line + ',sensor=%s' % data.sensor.replace(' ', '\ ')
            if data.unit is not None:
                line = line + ',unit=%s' % data.unit.replace(' ', '\ ')
            if data.sn is not None:
                line = line + ',sn=%s' % data.sn.replace(' ', '\ ')
            if data.model is not None:
                line = line + ',model=%s' % data.model.replace(' ', '\ ')
            if data.lat is not None:
                line = line + ',lat=%d' % data.lat
            if data.lon is not None:
                line = line + ',lon=%d' % data.lon
            # Add the data value and timestamp, which is converted to nanoseconds
            line = line + ' value=%d %d' % (data.value, data.timestamp * 1000000)
            return line
