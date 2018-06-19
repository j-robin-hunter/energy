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
from ..base_objects import MeterReadingBase


class Database(AbstractDatabase):

    CONNECTION_TIMEOUT = 20
    CONNECTION_RETIES = 5
    RETRY_TIMEOUT = 5

    def __init__(self, config):
        super().__init__(config)
        try:
            self.connection = influxdb.InfluxDBClient(
                host=self.config['host'],
                port=self.config['port'],
                timeout=self.CONNECTION_TIMEOUT,
                retries=self.CONNECTION_RETIES)

            if len(list(filter(lambda database: database['name'] == self.config['database'],
                       self.connection.get_list_database()))) == 0:
                self.init_database(self.config['database'])

            logging.debug(f'-- Database connected. '
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

    def init_database(self, name):
        try:
            # Create database
            self.connection.create_database(name)

            # Create retention policies for one day, one year and five years
            self.connection.create_retention_policy('one_day', '1d', '1', name, True)
            self.connection.create_retention_policy('one_year', '52w', '1', name, False)
            self.connection.create_retention_policy('five_year', '260w', '1', name, False)

            # Create continuous queries to down sample into one year and five year retention policies
            self.connection.query(f'create continuous query "cq_5m" on {name} begin '
                                  'select mean(value) as "value", sum(fiscal) as "fiscal"'
                                  'into "one_year"."downsampled_reading" '
                                  'from reading group by *,time(5m) end', database=name)
            self.connection.query(f'create continuous query "cq_30m" on {name} begin '
                                  'select mean(value) as "value", sum(fiscal) as "fiscal" '
                                  'into "five_year"."downsampled_reading" '
                                  'from reading group by *,time(30m) end', database=name)
        except Exception:
            raise

    def write_meter_reading(self, meter_reading):
        # Since this is an HTTP request it could fail. Stay in a loop retrying until the call either
        # succeeds or the program dies due to other errors, for example queue full
        while True:
            try:
                line = self._convert_data(meter_reading)
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
        logging.debug('Converting MeterReading to InfluxDB line format')

        # All entries in Point are tag values except value and timestamp.
        # All remaining entries in the passed data pertain to the actual
        # sensor so are used as additional values to reduce the number of
        # series in the InfluxDB schema
        point_vars = [v for v in dir(MeterReadingBase()) if not v.startswith('_')]
        tags = ''
        values = ''
        fiscal = ''
        for key, value in data.items():
            if key != 'value' and key != 'time' and key != 'fiscal':
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

        return f'reading{tags} value={data["value"]},fiscal={data.get("fiscal", 0)} {values} {int(data["time"])}'

    def all_latest_meter_readings(self):
        try:
            query = f'SELECT * FROM reading GROUP BY * ORDER BY DESC LIMIT 1'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise

    def all_meter_readings_between(self, start, end=None):
        try:
            endclause = ''
            if end is not None and end > start:
                endclause = f'AND time <= {int(end)}ms'

            query = f'SELECT * FROM one_year.downsampled_reading ' \
                    f'WHERE time >= {int(start)}ms {endclause}' \
                    f'GROUP BY * ORDER BY ASC'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise
