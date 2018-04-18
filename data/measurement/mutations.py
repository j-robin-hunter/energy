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
import logging
import json


class MeasurementInput(MeasurementBase, graphene.InputObjectType):
    pass


class CreateMeasurement(Measurement, graphene.Mutation):
    class Arguments:
        measurement = MeasurementInput(required=True)

    def mutate(self, info, measurement):
        info.context['database'].write_measurement(measurement)
        return CreateMeasurement(
            timestamp=measurement.timestamp,
            category=measurement.category,
            sensor=measurement.sensor,
            value=measurement.value,
            unit=measurement.unit,
            model=measurement.model,
            lat=measurement.lat,
            lon=measurement.lon)

class Mutations(graphene.ObjectType):
    create_measurement = CreateMeasurement.Field()
