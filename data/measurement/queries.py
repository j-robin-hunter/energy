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


def resolve_measurement(root, info, id):
    dataloader = info.context['dataloader']
    return dataloader.load(id)


sensor = graphene.Enum('Sensor',
                       [
                           ('WASHING_MACHINES', 'Washing Machines'),
                           ('WATER_HEATER', 'Water Heater'),
                           ('KITCHEN_ISLAND', 'Kitchen Island'),
                           ('OVENS', 'Ovens'),
                           ('UPSTAIRS_POWER', 'Upstairs Power'),
                           ('KITCHEN_POWER', 'Kitchen Power'),
                           ('OVER_GARAGE_POWER', 'Over Garage Power'),
                           ('DOWNSTAIRS_POWER', 'Downstairs Power'),
                           ('LIVING_ROOM_AND_DMX', 'Living Room & DMX'),
                           ('LIGHTING', 'Lighting'),
                           ('EVOLUTION', 'Evolution'),
                           ('VPV1', 'Vpv1'),
                           ('IPV1', 'Ipv1'),
                           ('PVTOTAL', 'PVTotal'),
                           ('VBATTERY1', 'Vbattery1'),
                           ('IBATTERY1', 'Ibattery1'),
                           ('SOC1', 'SOC1'),
                           ('SOH1', 'SOH1'),
                           ('PGRID', 'PGrid'),
                           ('ETOTAL', 'ETotal'),
                           ('EDAY', 'EDay'),
                           ('LOAD_POWER', 'LoadPower'),
                           ('E_LOAD_DAY', 'E_Load_Day'),
                           ('E_TOTAL_LOAD', 'E_Total_Load'),
                           ('PMETER', 'Pmeter')
                       ])


class Query(graphene.ObjectType):
    measurement = graphene.Field(lambda: Measurement, resolver=resolve_measurement, args=dict(id=sensor()))
