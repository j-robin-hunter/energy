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

from data.base_objects import SensorReadingBase
import graphene


class MeasurementBase(SensorReadingBase, object):
    unit = graphene.String(description='The units, normally SI, percent or similar, associated with the measurement')
    sn = graphene.String(description='A serial number to assist in the identification of a Sensor')
    model = graphene.String(description='A model name/id to assist in the identification of a Sensor')
    lat = graphene.Float(description='The latitude to associate with a measurement')
    lon = graphene.Float(description='The longitude to associate with a measurement')


class Measurement(MeasurementBase, graphene.ObjectType):
    '''
    A Measurement provides some additional detail fields for a Sensor reading. These
    details are generally informative in nature
    '''
    pass
