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

import graphene


class Point(graphene.Interface):
    timestamp = graphene.Float(required=True)
    measurement = graphene.String()
    sensor = graphene.String()
    value = graphene.Float(required=True)
    unit = graphene.String()


class Measurement(graphene.ObjectType):
    class Meta:
        interfaces = (Point, )
    sn = graphene.String()
    model = graphene.String()
    lat = graphene.Float()
    lon = graphene.Float()


class Test(graphene.ObjectType):
    class Meta:
        interfaces = (Point, )


class Query(graphene.ObjectType):
    latest_meter = graphene.Field(Measurement)

    def resolve_latest_meter(self, info):
        print(type(Measurement))
        print('got here')
        return Measurement(
            timestamp=1234567890,
            model="1234",
            value=1.234)


schema = graphene.Schema(
    query=Query,
    types=(Measurement, ))
