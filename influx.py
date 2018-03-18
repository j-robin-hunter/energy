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
from abstract import AbstractDatabase
import influxdb
import logging


class NoDatabaseError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class Plugin(AbstractDatabase):
    CONNECTION_TIMEOUT = 10

    def __init__(self, myname, queues, formatters, config):
        super().__init__(myname, queues, formatters, config)
        try:
            self.connection = influxdb.InfluxDBClient(
                host=self.config['host'],
                port=self.config['port'],
                timeout=self.CONNECTION_TIMEOUT)
            self.connection.create_database(self.config['database'])
        except Exception:
            logging.error('Unable to connect to database')
            raise NoDatabaseError('Unable to connect to database')

    def __del__(self):
        try:
            if self.connection is not None:
                self.connection.close()
        except Exception as e:
            logging.error('Encountered error while disposing %s: %s' % (self.myname, str(e)))

    def data_callback(self, queuename, data):
        for entry in data:
            try:
                logging.debug('Writing data to database')
                self.connection.write(entry, {'db': self.config['database']}, 204, 'line')
            except Exception:
                raise
