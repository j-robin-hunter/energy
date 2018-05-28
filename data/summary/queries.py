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

from .types import *
import graphene


class Query(graphene.ObjectType):
    summary_now = graphene.Field(Summary)
    premises = graphene.Field(lambda: Premises)

    def resolve_summary_now(self, info):
        result_set = info.context['database'].summary()
        grid = 0
        solar = 0
        battery = 0
        wind = 0
        if str(info.context['database'].config['type']) == 'influxdb':
            grid = list(result_set.get_points(tags={'id': 'PGrid'}))[0]['value']
            solar = list(result_set.get_points(tags={'id': 'Vpv1'}))[0]['value'] * \
                    list(result_set.get_points(tags={'id': 'Ipv1'}))[0]['value']
            battery = list(result_set.get_points(tags={'id': 'Vbattery1'}))[0]['value'] * \
                list(result_set.get_points(tags={'id': 'Ibattery1'}))[0]['value']
        return Summary(grid=grid, premises=(lambda: Premises), solar=solar, battery=battery, wind=wind)

    def resolve_premises(self, info):
        premises = Premises()
        return premises
