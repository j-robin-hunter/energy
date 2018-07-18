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
import os
import importlib
import inspect
import logging

current_directory = os.path.dirname(os.path.abspath(__file__))
current_module = current_directory.split(os.sep)[-1]
subdirectories = [d
                  for d in os.listdir(current_directory)
                  if os.path.isdir(os.path.join(current_directory, d)) and
                  d != '__pycache__'
                  ]


def get_queries():
    queries_base_classes = []
    for directory in subdirectories:
        try:
            module = importlib.import_module('{}.{}.queries'.format(current_module, directory))
            if module:
                classes = [x for x in inspect.getmembers(module, inspect.isclass)]
                queries = [x[1] for x in classes if 'Query' in x[0]]
                queries_base_classes += queries
        except ImportError:
            pass

    queries_base_classes = set(queries_base_classes[::-1])
    properties = {}
    for base_class in queries_base_classes:
        properties.update(base_class.__dict__['_meta'].fields)

    return type('Queries', tuple(queries_base_classes), properties)


def get_mutations():
    mutations_base_classes = []
    for directory in subdirectories:
        try:
            module = importlib.import_module('{}.{}.mutations'.format(current_module, directory))
            if module:
                classes = [x for x in inspect.getmembers(module, inspect.isclass)]
                mutations = [x[1] for x in classes if 'Mutation' in x[0]]
                mutations_base_classes += mutations
        except ImportError:
            pass

    mutations_base_classes = set(mutations_base_classes[::-1])
    properties = {}
    for base_class in mutations_base_classes:
        properties.update(base_class.__dict__['_meta'].fields)

    return type('Mutations', tuple(mutations_base_classes), properties)


def get_types():
    types_base_classes = []
    for directory in subdirectories:
        try:
            module = importlib.import_module('{}.{}.types'.format(current_module, directory))
            if module:
                classes = [x for x in inspect.getmembers(module, inspect.isclass)]
                types = [x[1] for x in classes if str(x[1]) == x[0]]
                types_base_classes += types
        except ImportError:
            pass

    types_base_classes = set(types_base_classes[::-1])

    return tuple(types_base_classes)


def schema():
    logging.debug('  -- Generating queries')
    queries = get_queries()
    logging.debug('  -- Generating mutations')
    mutations = get_mutations()
    logging.debug('  -- Generating types')
    types = get_types()

    logging.debug('  -- Generating schema')
    return graphene.Schema(query=queries, types=types, mutation=mutations)
