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

import argparse
from flask import Flask, jsonify, request
from flask_cors import CORS
from urllib.parse import urlparse
import logging
from logging.handlers import RotatingFileHandler
from urllib import request
import json
import locale
from flask_graphql import GraphQLView
import importlib

import data.graphql
from data.database.dataloaders import ReadingDataLoader
from data.database.dataloaders import ReadingsBetweenDataLoader
from data.database.dataloaders import TariffDataLoader
from data.database.dataloaders import TariffBetweenDataLoader


def read_config(path):
    parse_result = urlparse(path)

    try:
        if parse_result.scheme in ('http', 'https'):
            logging.debug('-- HTTP(s) based configuration file')
            response = request.urlopen(config)
            config_data = response.read().decode('utf-8')
        else:
            logging.debug('-- File based configuration file')
            with open(path) as configFile:
                config_data = configFile.read()

        return json.loads(config_data)

    except Exception:
        raise


def configure_logging(log_config):
    logger = logging.getLogger('')

    level = args.log
    if level is not None:
        logger.setLevel(level)

    # Configure logger
    if level is None:
        level = logging.getLevelName(log_config.get('log_level', 'INFO'))
    logger.setLevel(level)

    # If a file handler is set then remove console.
    # It will be automatically re-added if no handlers are present
    log_file = log_config.get('log_file', None)
    if log_file is not None:
        logging.debug('Writing log output to file: "{}"'.format(log_file))
    if log_file is not None:
        lsstout = logger.handlers[0]
        lhhdlr = RotatingFileHandler(log_file, maxBytes=1000000000, backupCount=5)
        logger.addHandler(lhhdlr)
        logger.removeHandler(lsstout)


app = Flask(__name__)

parser = argparse.ArgumentParser(prog="elog", description="An energy management program")
parser.add_argument("config",
                    help="Full pathname or URL to the configuration file")
parser.add_argument("--log",
                    metavar="LEVEL",
                    choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'],
                    help="Set minimum log level: DEBUG, INFO, WARN, ERROR, CRITICAL")
args = parser.parse_args()

config = read_config(args.config)
logging.info('Setting locale to {}'.format(config['site'].get('locale', '')))
locale.setlocale(locale.LC_ALL, config['site'].get('locale', ''))

configure_logging(config)

logging.info('Generating measurement and data type schema')
schema = data.graphql.schema()

logging.info('Reading databases')
database_config = config['database']
logging.info('-- Connecting to database "{}"'.format(database_config['name']))
database_module = importlib.import_module('data.database.{}'.format(database_config['type']))

database_instance = database_module.Database(database_config, None)


CORS(app, resources={
    r'/graphql/*': {'origins': '*'},
    r'/config/*': {'origins': '*'}
})


@app.route('/config/configuration')
def configuration():
    # This will always reload the configuration but it will
    # not cause either the database or schema to be reloaded
    # or re-injected. Changes to modules and/or database
    # configuration will require a restart to take effect
    return jsonify(read_config(args.config)['site'])


@app.route(config.get('shutdown', '/shutdown'))
def shutdown():
    shutdown_server()
    return 'Server shutting down...'


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True,
        context=dict(
            meterreadingloader=ReadingDataLoader(database_instance),
            meterreadingsbetweenloader=ReadingsBetweenDataLoader(database_instance),
            metertariffloader=TariffDataLoader(database_instance),
            metertariffbetweenloader=TariffBetweenDataLoader(database_instance)
        )
    )
)

if __name__ == '__main__':
    app.run(threaded=True)
