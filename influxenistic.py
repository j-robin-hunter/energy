#!/usr/bin/env python
""" A formatter to convert data received from an Enistic plugin to InfluxDB line format

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

from abstract import AbstractFormatter
import logging


class Formatter(AbstractFormatter):
    def __init__(self,  myname, queuename):
        super().__init__(myname, queuename)

    def __del__(self):
        pass

    def do_formatting(self, data):
        lineformat = []
        try:
            lineformat.append(
                'meter,sn=%s,model=%s,name=%s,category=%s power=%d %d' %
                (data['serial'].replace(' ', '\ '),
                 data['model'].replace(' ', '\ '),
                 data['name'].replace(' ', '\ '),
                 data['category'].replace(' ', '\ '),
                 data['power'] / 1000,
                 data['timestamp']))
        except KeyError as e:
            logging.debug('KeyError "%s" in formatter "%s"' % (str(e), self.myname))
        return lineformat
