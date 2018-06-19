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


def resolve_meter_reading(root, info, id):
    dataloader = info.context['latestloader']
    return dataloader.load(id)


def resolve_meter_readings_between(root, info, id, start, end):
    dataloader = info.context['betweenloader']
    dataloader.start = start
    dataloader.end = end
    return dataloader.load(id)


class Query(graphene.ObjectType):
    meterReading = graphene.List(graphene.List(MeterReading),
                                resolver=resolve_meter_reading,
                                id=graphene.String())
    meterReadingsBetween = graphene.List(graphene.List(MeterReading),
                                        resolver=resolve_meter_readings_between,
                                        id=graphene.String(),
                                        start=graphene.Float(),
                                        end=graphene.Float())
