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

from urllib import request
import json
from urllib.parse import urlparse
from queue import Queue
import argparse
import logging
import importlib
from logging.handlers import RotatingFileHandler
import lib.web
import time
import threading
import data.graphql
import traceback
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)-8s:%(filename)-20s:%(funcName)-24s: %(message)s"
)

MODULES_PACKAGE = 'modules'
DATABASE_PACKAGE = 'data.database'


def read_config(config):
    parse_result = urlparse(config)

    try:
        if parse_result.scheme in ('http', 'https'):
            logging.debug('  -- HTTP(s) based configuration file')
            response = request.urlopen(config)
            config_data = response.read().decode('utf-8')
        else:
            logging.debug('  -- File based configuration file')
            with open(config) as configFile:
                config_data = configFile.read()

        return json.loads(config_data)

    except Exception:
        raise


def configure_logging(config):
    logger = logging.getLogger('')

    level = args.log
    if level is not None:
        logger.setLevel(level)

    # Configure logger
    if level is None:
        level = logging.getLevelName(config.get('log_level', 'INFO'))
    logger.setLevel(level)

    # If a file handler is set then remove console.
    # It will be automatically re-added if no handlers are present
    log_file = config.get('log_file', None)
    if log_file is not None:
        logging.debug(f'Writing log output to file: "{log_file}"')
    if log_file is not None:
        lsstout = logger.handlers[0]
        lhhdlr = RotatingFileHandler(log_file, maxBytes=1000000000, backupCount=5)
        logger.addHandler(lhhdlr)
        logger.removeHandler(lsstout)


def start_webserver(config, schema, database):
    webserver = lib.web.WebServer(config, schema, database)
    webserver.setDaemon(True)
    webserver.start()


def connect_database(config):
    database = importlib.import_module(f'.{config["type"]}', package=DATABASE_PACKAGE)
    return database.Database(config)


def main():
    modules = set()
    # noinspection PyBroadException
    try:
        config = read_config(args.config)

        # Program variables that can be set in config file
        max_queue_size = config.get('max_queue_size', 1000)

        configure_logging(config)

        logging.info('Generating measurement and data type schema')
        schema = data.graphql.schema()

        database = ''
        if config.get('database', None):
            logging.info('Connecting to database')
            database = connect_database(config['database'])

        logging.info('Starting HTTP server')
        start_webserver(config, schema, database)
        time.sleep(1)  # wait to allow server to start

        # Process all sensors adding the correct measurement class to each.
        sensors = []
        logging.info('Reading measurements')
        for measurement in config['measurement']:
            logging.debug(f'  -- Injecting schema type into measurement "{measurement["type"]}"')
            measurement_class = None
            for schema_type in schema.types:
                if measurement['type'] == str(schema_type):
                    measurement_class = schema_type
                    break
            if measurement_class is None:
                logging.error(f'No schema defined for for measurement "{measurement["type"]}"')
                raise RuntimeError('No schema definition for measurement type')

            for category in measurement['category']:
                logging.debug(f'    -- Processing sensors for category "{category["name"]}"')
                for sensor in category['sensor']:
                    logging.debug(f'      -- Collating data for sensor "{sensor["id"]}"')
                    sensor['measurement'] = measurement['type']
                    sensor['category'] = category['name']
                    sensors.append(dict(
                        type=measurement['type'],
                        sensor=sensor
                    ))

        # Process all modules. Ensure that there are no duplicates in terms of name
        logging.info('Reading modules')
        queues = {}
        for module in config['module']:
            logging.debug(f'  -- Module "{module["name"]}"')
            if {True for mod in modules if mod == module['name']} != set():
                logging.error('Duplicate module "{module["name"]}" defined in configuration file')
                raise RuntimeError(f'Duplicate "module" definition for "{module["name"]}" found in configuration file')

            if module.get('inputs', None):
                logging.debug('    -- Processing module inputs')
                for measurement in module['inputs']:
                    queues[measurement] = Queue()
                module['schema'] = schema
                module['database'] = database

            # Re-add outputs to each module using the 'extended' definition that also
            # includes the measurement class
            if module.get('outputs', None):
                logging.debug('    -- Processing module outputs')
                module['output_types'] = {sensor['type'] for sensor in sensors
                                          if sensor['sensor']['category'] in module.get('outputs', [])}
                module['outputs'] = [sensor['sensor']
                                     for sensor in sensors
                                     if sensor['sensor']['category'] in module.get('outputs', [])]
            modules.add(module['name'])

        # Start each module as a thread. A second loop is used to ensure that the data passed to the
        # module constructor is complete - for example all queue information
        for module in config['module']:
            logging.debug('Injecting queues into module')
            module['queues'] = queues
            logging.info(f'Importing and instantiating module "{module["name"]}"')
            imported = importlib.import_module(f'.{module["name"]}', package=MODULES_PACKAGE)
            instance = imported.Module(module)
            instance.setName(module['name'])
            logging.debug('  -- Starting module worker thread')
            instance.start()

        logging.info('Program started')

        # Stay in a loop monitoring queue size to ensure that no queue start to overfill.
        # If it does terminate program. While in loop verify that all module threads are still
        # running. If any have stopped then terminate the program
        while True:
            logging.info('Verifying program run status')
            time.sleep(10)

            logging.debug('  -- Check all queue sizes')
            for key in queues:
                logging.debug(f'    -- Queue "{key}" current size: {queues[key].qsize()}')
                if queues[key].qsize() >= max_queue_size:
                    logging.critical(f'Queue(s) "{key}" oversize, terminating program')
                    raise RuntimeError('Queue oversize')

            logging.debug('  -- Check all running threads')
            running_threads = set([thread.getName() for thread in threading.enumerate()])
            if modules - running_threads:
                logging.critical('Module(s) "%s" no longer running, terminating program'
                                 % ", ".join(str(e) for e in (modules - running_threads)))
                raise RuntimeError('Unexpected module termination')

    except KeyError as e:
        logging.critical(f'Missing key {str(e)} from configuration file')
    except Exception as e:
        logging.critical(f'Exception caught {type(e)}: {e}')
    finally:
        # Terminate all running plugins
        threads = [thread for thread in threading.enumerate()
                   if thread.isAlive() and thread.getName() in modules]
        for thread in threads:
            logging.info(f'Terminating {thread.getName()}')
            thread.terminate.set()
            thread.join()

        logging.info("Program terminated")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="elog", description="An energy management program")
    parser._positionals.title = "arguments"
    parser.add_argument("config",
                        help="Full pathname or URL to the configuration file")
    parser.add_argument("--log",
                        metavar="LEVEL",
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'],
                        help="Set minimum log level: DEBUG, INFO, WARN, ERROR, CRITICAL")
    args = parser.parse_args()

    main()
