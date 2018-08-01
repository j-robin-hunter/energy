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

import graphene


class MeterReadingBase(object):
    time = graphene.Float(required=True, description='Millisecond time to associate with the value')
    module = graphene.String(required=True, description='Name of the module taking the reading')
    id = graphene.String(required=True, description='A normally unique id to identify the meter that read the value')
    reading = graphene.Float(required=True, description='A single value/reading that has been taken')
    unit = graphene.String(required=True, description='The units of the value/reading')


class MeterTariffBase(object):
    time = graphene.Float(required=True, description='Millisecond time to associate with the tariff')
    id = graphene.String(required=True, description='The id to identify the meter associated with the tariff')
    name = graphene.String(required=True, description='The name of the tariff')
    amount = graphene.Float(required=True, description='The monetary amount applied since the previous reading')
    tariff = graphene.String(required=True, description='The applied tariff rate')
    tax = graphene.String(required=True, description='The tax rate applied to the tariff')
    rateid = graphene.String(required=True, description='The id used to identify a differential rate')
    type = graphene.String(required=True, description='The type of tariff associated with the reading')
    source = graphene.String(required=True, description='The source power type associated with the reading')
