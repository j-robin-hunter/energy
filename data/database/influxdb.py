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
from ..base_objects import *


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

            # Create retention policies for one day, reading one year, reading five years,
            # tariff one year and tariff five years
            self.connection.create_retention_policy('one_day', '1d', '1', name, True)
            self.connection.create_retention_policy('one_year', '52w', '1', name, False)
            self.connection.create_retention_policy('five_year', '260w', '1', name, False)

            # Create continuous queries to down sample into one year and five year retention policies
            self.connection.query(f'create continuous query "reading_5m" on {name} begin '
                                  'select mean(reading) as "reading"'
                                  'into "one_year"."downsampled_reading" '
                                  'from reading group by *,time(5m) end', database=name)
            self.connection.query(f'create continuous query "reading_30m" on {name} begin '
                                  'select mean(reading) as "reading"'
                                  'into "five_year"."downsampled_reading" '
                                  'from reading group by *,time(30m) end', database=name)
            self.connection.query(f'create continuous query "tariff_5m" on {name} begin '
                                  'select sum(amount) as "amount"'
                                  'into "one_year"."downsampled_tariff" '
                                  'from tariff group by *,time(30m) fill(0) end', database=name)
            self.connection.query(f'create continuous query "tariff_30m" on {name} begin '
                                  'select sum(amount) as "amount"'
                                  'into "five_year"."downsampled_tariff" '
                                  'from tariff group by *,time(30m) fill(0) end', database=name)
        except Exception:
            raise

    def write_meter_reading(self, meter_reading):
        # Since this is an HTTP request it could fail. Stay in a loop retrying until the call either
        # succeeds or the program dies due to other errors, for example queue full
        while True:
            try:
                line = self._convert_reading_data(meter_reading)
                logging.debug(f'Writing reading data to database: {line}')
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

    @staticmethod
    def _convert_reading_data(data):
        # Convert data to InfluxDB line format
        logging.debug('Converting MeterReading to InfluxDB line format')

        point_vars = [v for v in dir(MeterReadingBase()) if not v.startswith('_')]
        tags = ''
        values = ''
        for key, value in data.items():
            if key != 'reading' and key != 'time':
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

        return f'reading{tags} reading={data["reading"]} {values} {int(data["time"])}'

    def all_meter_readings(self):
        try:
            query = f'SELECT * ' \
                    f'FROM reading ' \
                    f'GROUP BY id ' \
                    f'ORDER BY DESC ' \
                    f'LIMIT 1'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise

    def all_meter_readings_between(self, start, end=None):
        try:
            endclause = ''
            if end is not None and end > start:
                endclause = f'AND time <= {int(end)}ms'

            query = f'SELECT * ' \
                    f'FROM one_year.downsampled_reading ' \
                    f'WHERE time >= {int(start)}ms {endclause}' \
                    f'GROUP BY id ' \
                    f'ORDER BY ASC'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise

    def write_meter_tariff(self, meter_tariff):
        # Since this is an HTTP request it could fail. Stay in a loop retrying until the call either
        # succeeds or the program dies due to other errors, for example queue full
        while True:
            try:
                line = self._convert_tariff_data(meter_tariff)
                logging.debug(f'Writing tariff data to database: {line}')
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

    @staticmethod
    def _convert_tariff_data(data):
        # Convert data to InfluxDB line format
        logging.debug('Converting MeterTariff to InfluxDB line format')

        point_vars = [v for v in dir(MeterTariffBase()) if not v.startswith('_')]
        tags = ''
        values = ''
        for key, value in data.items():
            if key != 'amount' and key != 'time':
                if key in point_vars:
                    # Must escape all commas and spaces. Numeric and string
                    # tags are left unquoted, unlike values
                    if isinstance(value, str):
                        value = value.replace(' ', '\ ').replace(',', '\,')
                    if value != '':
                        tags += f',{key}={value}'
                else:
                    # Must enclose all string values in double quotes
                    if isinstance(value, str):
                        values += f',{key}="{value}"'
                    else:
                        values += f',{key}={value}'

        return f'tariff{tags} amount={data["amount"]} {values} {int(data["time"])}'

    def latest_meter_tariff_today(self):

        now = round(time.time())
        midnight = now - (now % 86400)
        try:
            query = f'SELECT SUM(amount) AS amount ' \
                    f'FROM tariff ' \
                    f'WHERE time>={int(midnight)}s ' \
                    f'GROUP BY * ' \
                    f'FILL(0) '
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise

    def all_meter_tariff_today(self):
        now = round(time.time())
        midnight = now - (now % 86400)
        try:
            query = f'SELECT SUM(amount) AS amount ' \
                    f'FROM tariff ' \
                    f'WHERE time>={int(midnight)}s ' \
                    f'GROUP BY *, time(5m) ' \
                    f'FILL(0) ' \
                    f'ORDER BY ASC'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise

    def all_meter_tariff_between(self, start, end=None):
        try:
            endclause = ''
            if end is not None and end > start:
                endclause = f'AND time <= {int(end)}ms'

            query = f'SELECT * ' \
                    f'FROM one_year.downsampled_tariff ' \
                    f'WHERE time >= {int(start)}ms {endclause} ' \
                    f'GROUP BY * ' \
                    f'ORDER BY ASC'
            return self.connection.query(query, database=self.config['database'], epoch='ms')
        except Exception as e:
            logging.error(f'Database query error: {str(e)}')
            raise