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
import data.database.globals as datawriter


class MeterReadingInput(MeterReadingBase, graphene.InputObjectType):
    pass


class CreateMeterReading(MeterReading, graphene.Mutation):
    class Arguments:
        meter_reading = MeterReadingInput()

    def mutate(self, info, meter_reading):
        for database in datawriter.databases:
            database.write_meter_reading(meter_reading)
        return CreateMeterReading(**meter_reading)


class MeterTariffInput(MeterTariffBase, graphene.InputObjectType):
    pass


class CreateMeterTariff(MeterTariff, graphene.Mutation):
    class Arguments:
        meter_tariff = MeterTariffInput()

    def mutate(self, info, meter_tariff):
        for database in datawriter.databases:
            database.write_meter_tariff(meter_tariff)
        return CreateMeterTariff(**meter_tariff)


class Mutations(graphene.ObjectType):
    create_meter_reading = CreateMeterReading.Field()
    create_meter_tariff = CreateMeterTariff.Field()
