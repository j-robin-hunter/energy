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
import time
import threading
import locale
import queue
from copy import deepcopy
import hashlib
import traceback
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)-8s:%(filename)-20s:%(funcName)-24s: %(message)s"
)

MAX_QUEUE = 1000


def read_config(path):
    parse_result = urlparse(path)

    try:
        if parse_result.scheme in ('http', 'https'):
            logging.debug('-- HTTP(s) based configuration file')
            response = request.urlopen(path)
            config_data = response.read().decode('utf-8')
        else:
            logging.debug('-- File based configuration file')
            with open(path) as configFile:
                config_data = configFile.read()

        return json.loads(config_data)

    except Exception:
        raise


def configure_logging(logging_config):
    logger = logging.getLogger('')

    level = args.log
    if level is not None:
        logger.setLevel(level)

    # Configure logger
    if level is None:
        level = logging.getLevelName(logging_config.get('log_level', 'INFO'))
    logger.setLevel(level)

    # If a file handler is set then remove console.
    # It will be automatically re-added if no handlers are present
    log_file = logging_config.get('log_file', None)
    if log_file is not None:
        logging.debug('Writing log output to file: "{}"'.format(log_file))
    if log_file is not None:
        lsstout = logger.handlers[0]
        lhhdlr = RotatingFileHandler(log_file, maxBytes=1000000000, backupCount=5)
        logger.addHandler(lhhdlr)
        logger.removeHandler(lsstout)


def main():
    modules = set()
    tariffs = {}
    database_queue = queue.Queue()
    config_change = False
    try:
        config = read_config(args.config)
        config_digest = hashlib.md5(json.dumps(config).encode('utf-8')).hexdigest()

        logging.info('Setting locale to {}'.format(config['site'].get('locale', '')))
        locale.setlocale(locale.LC_ALL, config['site'].get('locale', ''))

        configure_logging(config)

        logging.info('Reading sources')
        for power in config['site'].get('power', []):
            power_type = power['type']
            meter_module = power['meter']['module']
            meter_id = power['meter']['id']
            logging.debug('-- Source "{}, {}, {}" '.format(power_type, meter_module, meter_id))
            for tariff in power.get('tariff', []):
                tariff_copy = deepcopy(tariff)
                tariff_copy['source'] = power_type
                tariff_copy['id'] = meter_id
                tariff_copy['module'] = meter_module
                tariff_module = tariffs.get(meter_module, [])
                tariff_module.append(tariff_copy)
                tariffs[meter_module] = tariff_module

        logging.info('Reading databases')
        database_config = config['database']
        logging.info('-- Importing and instantiating database "{}"'.format(database_config['name']))
        modules.add(database_config['name'])
        database_module = importlib.import_module('data.database.{}'.format(database_config['type']))
        database_instance = database_module.Database(database_config, database_queue)
        database_instance.setName(database_config['name'])
        logging.debug('---- Starting database worker thread')
        database_instance.start()

        logging.info('Reading modules')
        for module in config.get('module', []):
            logging.debug('-- Module "{}"'.format(module["name"]))
            if {True for mod in modules if mod == module['name']} != set():
                logging.error('Duplicate module "{}" defined in configuration file'.format(module["name"]))
                raise RuntimeError(
                    'Duplicate "module" definition for "{}" found in configuration file'.format(module["name"]))
            modules.add(module['name'])
            logging.info('---- Importing and instantiating module type "{}"'.format(module["type"]))
            imported = importlib.import_module('modules.{}'.format(module["type"]))
            instance = imported.Module(module,
                                       database_queue,
                                       tariffs.get(module['name'], None))
            instance.setName(module['name'])
            logging.debug('---- Starting module worker thread')
            instance.start()

        logging.info('Program started')

        # Stay in a loop monitoring queue size to ensure that no queue start to overfill.
        # If it does terminate program. While in loop verify that all module threads are still
        # running. If any have stopped then terminate the program
        while not config_change:
            time.sleep(10)
            logging.debug('Verifying program run status')

            logging.debug('  -- Check all running threads')
            running_threads = set([thread.getName() for thread in threading.enumerate()])
            if database_queue.qsize() > MAX_QUEUE:
                logging.critical('Database queue has exceed its maximum allowed size')
                raise RuntimeError('Queue full')
            if modules - running_threads:
                logging.critical('Module(s) "%s" no longer running, terminating program'
                                 % ", ".join(str(e) for e in (modules - running_threads)))
                raise RuntimeError('Unexpected module termination')

            logging.debug('  -- Checking for config file changes')
            if config_digest != hashlib.md5(json.dumps(read_config(args.config)).encode('utf-8')).hexdigest():
                logging.info("Configuration file changed")
                config_change = True

    except KeyError as e:
        logging.critical('Missing key {} from configuration file'.format(str(e)))
    except Exception as e:
        logging.critical('Exception caught {}: {}'.format(type(e), e))
        print("Exception in user code:")
        print("-"*60)
        traceback.print_exc(file=sys.stdout)
        print("-"*60)

    finally:
        # Terminate all running plugins
        threads = [thread for thread in threading.enumerate()
                   if thread.isAlive() and thread.getName() in modules]
        for thread in threads:
            logging.info('Terminating {}'.format(thread.getName()))
            thread.terminate.set()
            thread.join()

        if config_change:
            logging.info("Program restarting")
            return 0
        else:
            logging.info("Program terminated")
            return -1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="elog", description="An energy management program")
    parser.add_argument("config",
                        help="Full pathname or URL to the configuration file")
    parser.add_argument("--log",
                        metavar="LEVEL",
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'],
                        help="Set minimum log level: DEBUG, INFO, WARN, ERROR, CRITICAL")
    args = parser.parse_args()

    ret_val = 0
    while ret_val == 0:
        ret_val = main()
        time.sleep(10)
