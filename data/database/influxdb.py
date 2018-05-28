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

from lib.abstract import AbstractDatabase
import influxdb
import logging
import requests
import time
from ..base_objects import SensorReadingBase


class Database(AbstractDatabase):

    CONNECTION_TIMEOUT = 10
    RETRY_TIMEOUT = 5

    def __init__(self, config):
        super().__init__(config)
        try:
            self.connection = influxdb.InfluxDBClient(
                host=self.config['host'],
                port=self.config['port'],
                timeout=self.CONNECTION_TIMEOUT)
            self.connection.create_database(self.config['database'])
            logging.debug(f'  -- Database connected. '
                          f'database:"{self.config["database"]}", '
                          f'"host":{self.config["host"]}, '
                          f'"port":{self.config["port"]}')
        except Exception:
            logging.error('Unable to connect to database')
            raise RuntimeError('Unable to connect to database')

    def __del__(self):
        # noinspection PyBroadException
        try:
            if self.connection is not None:
                self.connection.close()
        except Exception as e:
            logging.error(f'Encountered error while disposing {self.config["type"]}: {str(e)}')

    def summary(self):
        try:
            query = f'SELECT * FROM {self.config["series"]} GROUP BY * ORDER BY DESC LIMIT 1'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise

    def write_measurement(self, measurement):
        # Since this is an HTTP request it could fail. Stay in a loop retrying until the call either
        # succeeds or the program dies due to other errors, for example queue full
        while True:
            try:
                line = self._convert_data(measurement)
                logging.debug(f'Writing data to database: {line}')
                if self.connection.write(line,
                                         {'db': self.config['database'], 'precision': 'ms'},
                                         204,
                                         'line') is False:
                    # Need to raise an error to cause GraphQL Mutation to fail
                    raise RuntimeError('InfluxDB database write failed')
                break
            except requests.exceptions.ConnectionError:
                logging.info(f'Database connection error - retrying in {self.RETRY_TIMEOUT} second(s)')
                time.sleep(self.RETRY_TIMEOUT * 1000)
                pass
            except Exception as e:
                logging.error(str(e))
                raise RuntimeError(str(e))

    def _convert_data(self, data):
        # Convert data to InfluxDB line format
        logging.debug('Converting Measurement to InfluxDB line format')

        # All entries in Point are tag values except value and timestamp.
        # All remaining entries in the passed data pertain to the actual
        # sensor so are used as additional values to reduce the number of
        # series in the InfluxDB schema
        point_vars = [v for v in dir(SensorReadingBase()) if not v.startswith('_')]
        tags = ''
        values = ''
        for key, value in data.items():
            if key != 'value' and key != 'time':
                if key in point_vars:
                    # Must escape all commas and spaces. Numeric and string
                    # tags are left unquoted, unlike values
                    if isinstance(value, str):
                        value = value.replace(' ', '\ ').replace(',', '\,')
                    tags += f',{key}={value}'
                else:
                    # Must enclose all string values in double quotes
                    if isinstance(value, str):
                        values += f',{key}="{value}"'
                    else:
                        values += f',{key}={value}'

        return f'{self.config["series"]}{tags} value={data["value"]}{values} {int(data["time"])}'
