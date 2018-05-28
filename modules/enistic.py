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
import socket
import re
import logging


def get_number(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


class Module(AbstractModule):
    def __init__(self, module):
        super().__init__(module)
        try:
            self.modelName = None
            self.serialNumber = None

            self.meter_port = self.get_config_value('port')
            self.meters = self.get_config_value('meters')

            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('0.0.0.0', self.meter_port))
        except KeyError as e:
            logging.error('Invalid key %s in %s configuration' % (str(e), self.getName()))
            raise RuntimeError('Error in %s plugin configuration' % self.getName())

    def __del__(self):
        try:
            if self.sock is not None:
                self.sock.close()
        except Exception as e:
            logging.error('Encountered error while disposing %s: %s' % (self.getName(), str(e)))

    def process_outputs(self):
        try:
            data, addr = self.sock.recvfrom(4096)
            index = data.find(b"D1")
            if index > 0 and self.serialNumber is not None and self.modelName is not None:
                logging.debug('Enistic meter reading "%s"', data)
                tokens = data[index:len(data)].decode('utf-8').split(',')
                if len(tokens) >= 6:
                    try:
                        meter_reading = get_number(tokens[3])
                        sensor_id = list(
                                filter(lambda meter: meter['channel'] == get_number(tokens[5]), self.meters)
                            )[0].get('name')

                        logging.debug('Writing Enisitic data to measurement queue')
                        self.send_output_data(
                            sensor_id,
                            value=meter_reading / 1000,
                            sn=self.serialNumber,
                            model=self.modelName,
                            lat=52.2,
                            lon=0.3)
                    # Ignore index as it is a meter that has not been defined as having a
                    # channel/name lookup
                    except IndexError:
                        pass
            else:
                try:
                    self.serialNumber = re.search('Core:Serial=(.+?)\r', data.decode('utf-8')).group(1)
                    logging.debug('Enistic serial number record "%s"', data)
                except AttributeError:
                    try:
                        self.modelName = re.search('Status:Model=(.+?)\r', data.decode('utf-8')).group(1)
                        logging.debug('Enistic model name record "%s"', data)
                    except AttributeError:
                        pass
        except Exception as e:
            logging.error(str(e))
            raise RuntimeError(str(e))
