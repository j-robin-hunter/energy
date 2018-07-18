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
import argparse
import logging
import importlib
from logging.handlers import RotatingFileHandler
import lib.web
import time
import threading
import data.graphql
import locale

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
            logging.debug('-- HTTP(s) based configuration file')
            response = request.urlopen(config)
            config_data = response.read().decode('utf-8')
        else:
            logging.debug('-- File based configuration file')
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
        logging.debug('Writing log output to file: "{}"'.format(log_file))
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
    database = importlib.import_module('.' + config["type"], package=DATABASE_PACKAGE)
    return database.Database(config)


def main():
    modules = set()
    try:
        config = read_config(args.config)
        locale.setlocale(locale.LC_ALL, config['configuration'].get('locale', ''))

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

        logging.info('Reading modules')
        for module in config['module']:
            logging.debug('-- Module "{}"'.format(module["name"]))
            if {True for mod in modules if mod == module['name']} != set():
                logging.error('Duplicate module "{}" defined in configuration file'.format(module["name"]))
                raise RuntimeError(
                    'Duplicate "module" definition for "{}" found in configuration file'.format(module["name"]))
            modules.add(module['name'])
            logging.info('---- Importing and instantiating module type "{}"'.format(module["type"]))
            imported = importlib.import_module('modules.{}'.format(module["type"]))
            instance = imported.Module(module,
                                       schema,
                                       database,
                                       config['configuration'].get('tariff', None))
            instance.setName(module['name'])
            logging.debug('---- Starting module worker thread')
            instance.start()
        logging.info('Program started')

        # Stay in a loop monitoring queue size to ensure that no queue start to overfill.
        # If it does terminate program. While in loop verify that all module threads are still
        # running. If any have stopped then terminate the program
        while True:
            time.sleep(10)
            logging.debug('Verifying program run status')

            logging.debug('  -- Check all running threads')
            running_threads = set([thread.getName() for thread in threading.enumerate()])
            if modules - running_threads:
                logging.critical('Module(s) "%s" no longer running, terminating program'
                                 % ", ".join(str(e) for e in (modules - running_threads)))
                raise RuntimeError('Unexpected module termination')

    except KeyError as e:
        logging.critical('Missing key {} from configuration file'.format(str(e)))
    except Exception as e:
        logging.critical('Exception caught {}: {}'.format(type(e), e))

    finally:
        # Terminate all running plugins
        threads = [thread for thread in threading.enumerate()
                   if thread.isAlive() and thread.getName() in modules]
        for thread in threads:
            logging.info('Terminating {}'.format(thread.getName()))
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
