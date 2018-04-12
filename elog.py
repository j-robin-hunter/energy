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
from multiprocessing import Queue
import argparse
import logging
import importlib
from logging.handlers import RotatingFileHandler
import re
import lib.web
import time
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)-8s:%(filename)-20s:%(funcName)-24s: %(message)s"
)

MODULES_PACKAGE = 'modules'
LIBRARY_PACKAGE = 'lib'


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


def main():
    started_threads = set()

    try:
        logger = logging.getLogger('')

        level = args.log
        if level is not None:
            logger.setLevel(level)

        logging.info('Reading configuration <' + args.config + '>')
        config = read_config(args.config)

        # Program variables that can be set in config file
        max_queue_size = config.get('max_queue_size', 1000)

        # Configure logger
        if level is None:
            level = logging.getLevelName(config.get('log_level', 'INFO'))
        logger.setLevel(level)

        # If a file handler is set then remove console.
        # It will be automatically re-added if no handlers are present
        log_file = config.get('log_file', None)
        if log_file is not None:
            logging.debug('Writing log output to file: "%s"' % log_file)
        if log_file is not None:
            lsstout = logger.handlers[0]
            lhhdlr = RotatingFileHandler(log_file, maxBytes=1000000000, backupCount=5)
            logger.addHandler(lhhdlr)
            logger.removeHandler(lsstout)

        # Verify that there are no duplicate measurements defined in the configuration file and also
        # warn if no measurements are missing as outputs (warning) in modules or undefined across all
        # modules 'inputs' or module 'outputs' but which are not defined as measurements (error).
        # Also verify that all measurements are included in at least one module defined 'input' (warning)
        # as this will mean that data might be collected but not written to any storage or data consumer.
        # All measurements must also include at least one sensor and this sensor must not be duplicated
        # across more than one measurement

        measurements = set()
        duplicates = set()
        sensors = set()

        logging.info('Reading measurements')
        for measurement in config['measurement']:
            if measurement['name'] in measurements or measurements.add(measurement['name']):
                duplicates.add(measurement['name'])
            try:
                for sensor in measurement['sensor']:
                    if sensor['name'] in sensors or sensors.add(sensor['name']):
                        raise RuntimeError('Sensor "%s" is defined against more than one measurement that includes "%s"'
                                           % (sensor['name'], measurement['name']))
                    # Replace the sensor type with the schema object that will provide a data
                    # container for the sensor
                    logging.debug('  -- Installing data class "%s" into sensor "%s"'
                                  % (sensor['type'], sensor['name']))
                    module = importlib.import_module('.schema', package=LIBRARY_PACKAGE)
                    sensor['class'] = getattr(module, sensor.get('type', None))
                    del sensor['type']
            except KeyError:
                logging.error('Invalid sensor defined for measurement "%s"' % measurement['name'])
                raise
            except TypeError:
                raise RuntimeError('The sensor definition for measurement "%s" is incorrect' % measurement['name'])

        if duplicates:
            logging.error('Duplicate measurement "%s" defined in configuration file'
                          % ", ".join(str(e) for e in duplicates))
            raise RuntimeError('Duplicate measurement defined in configuration file')

        # Required variables
        outputs = set()
        inputs = set()
        queues = []

        logging.info('Processing modules')
        modules = []
        for module in config['module']:
            modules.append(module)
            module_name = module['name']
            logging.debug('-- Module name "%s"' % module_name)

            logging.debug('  -- Importing module package as "%s" from "%s"' % ('.' + module_name, MODULES_PACKAGE))
            try:
                module['__module'] = importlib.import_module('.' + module_name, package=MODULES_PACKAGE)
            except ModuleNotFoundError as e:
                logging.error('%s in %s in configuration file' % (str(e), module_name))
                raise RuntimeError(str(e))

            logging.debug('  -- Processing "output" and "input" measurements in module "%s"' % module_name)
            outputs_list = module.get('outputs', [])
            inputs_list = module.get('inputs', [])

            logging.debug("    -- Checking that outputs and inputs are lists")
            if not isinstance(outputs_list, list) or not isinstance(inputs_list, list):
                raise RuntimeError('Module "%s" configuration must define "outputs" and "inputs" as lists'
                                   % module_name)

            logging.debug('    -- defined outputs: %s' % outputs_list)
            logging.debug('    -- defined inputs: %s' % inputs_list)

            module_outputs = set(outputs_list)
            module_inputs = set(inputs_list)

            # Account for module inputs expressed as a '*'.
            if '*' in module_inputs:
                logging.debug('    -- Replacing input list with all measurements')
                module_inputs = measurements.copy()

            # Remove any excluded measurements from inputs list as indicted by a leading !
            for i in inputs_list:
                exclude = re.sub('^!*', '', i)
                if i != exclude:
                    logging.debug('    -- Excluding input "%s" from input list' % i)
                    if exclude in measurements:
                        module_inputs.discard(exclude)
                    else:
                        logging.error('    -- Excluded input is not a defined measurement')
                        raise RuntimeError('Excluded input "%s" from module "%s" is not a defined measurement'
                                           % (exclude, module_name))

            # Account for module outputs expressed as a '*'
            if '*' in module_outputs:
                module_outputs = measurements.copy()

            logging.debug('    -- processed outputs: %s' % module_outputs)
            logging.debug('    -- processed inputs: %s' % module_inputs)

            # Check module 'outputs' and 'inputs' that are not in 'measurements'
            logging.debug('    -- Checking for any undefined measurements')
            if module_outputs - measurements:
                logging.error('Module "%s" defined to output undefined measurement "%s"'
                              % (module_name, ", ".join(str(e) for e in (module_outputs - measurements))))
                raise RuntimeError('Module defined to output undefined measurement')
            if module_inputs - measurements:
                logging.error('Module "%s" defined to input undefined measurement "%s"'
                              % (module_name, ", ".join(str(e) for e in (module_inputs - measurements))))
                raise RuntimeError('Module defined to input undefined measurement')

            # Create a queue to process measurements defined in module 'inputs' clause and inject
            # detailed measurement data to allow module to determine how to output and input data
            # against the queue
            if module_inputs:
                logging.debug('  -- Created queue "%s" for measurements "%s"'
                              % (module_name, module_inputs))
                module['__inputs_queue'] = Queue()
                queues.append(dict(name=module_name,
                                   measurements=module_inputs,
                                   queue=module['__inputs_queue']))

            # Update the module outputs with the processed list
            module['outputs'] = module_outputs

            # Keep track of all defined outputs and inputs for error checking
            inputs.update(module_inputs)
            outputs.update(module_outputs)

        # Give all modules access to all queues so that an instantiated module
        # can determine which measurement is to be read/written to which queue
        # logging.debug('  -- Injecting queues into module')
        # module['__queues'] = queues

        # Final error checks on all module outputs and inputs before modules are started
        logging.debug('Checking for any unused "outputs" measurements')
        if measurements - outputs:
            logging.warning('Measurement(s) "%s" not defined in any module "outputs"'
                            % ", ".join(str(e) for e in (measurements - outputs)))

        logging.debug('Checking for any unused "outputs" measurements')
        if measurements - inputs:
            logging.warning('Measurement(s) "%s" not defined in any module "inputs"'
                            % ", ".join(str(e) for e in (measurements - inputs)))

        # Inject output queue sensor data and start each module as a thread
        for module in modules:
            module['__outputs'] = []
            for queue in queues:
                for output in set.intersection(queue['measurements'], module['outputs']):
                    logging.debug('  -- Collating sensor data for module "%s": %s' % (module['name'], output))
                    for measurement in config['measurement']:
                        if output == measurement['name']:
                            for sensor in measurement['sensor']:
                                sensor.update(queue=queue['queue'], measurement=measurement['name'])
                                logging.debug('    -- Injecting sensor data: %s', sensor)
                                module['__outputs'].append(sensor)

            if len(module['__outputs']) == 0:
                logging.debug('  -- Module "%s" has no defined outputs, deleting output from module data'
                              % module['name'])
                del module['__outputs']

            # Delete the outputs and inputs defined by the configuration file as the
            # queue and sensor entries now provide all of the information needed to both
            # output and input data on the correct queue in the correct format
            logging.debug('  -- Deleting configured outputs and inputs from module "%s"' % module['name'])
            if module.get('inputs', None) is not None:
                del module['inputs']
            if module.get('outputs', None) is not None:
                del module['outputs']

            logging.info('Instantiating and starting module "%s"' % module['name'])
            instance = module["__module"].Module(module)
            instance.setName(module['name'])
            logging.debug('  -- Starting module worker thread')
            instance.start()
            started_threads.add(module['name'])

        logging.info('Starting HTTP server')
        webserver = lib.web.WebServer(config['server'])
        webserver.setDaemon(True)
        webserver.start()

        logging.info('Program started')

        # Stay in a loop monitoring queue size to ensure that no queue start to overfill.
        # If it does terminate program. While in loop verify that all module threads are still
        # running. If any have stopped then terminate the program
        while True:
            logging.info('Verifying program run status')
            time.sleep(10)

            logging.debug('---- Check all queue sizes')
            oversize_queues = [queue['name'] for queue in queues if queue['queue'].qsize() >= max_queue_size]
            if oversize_queues:
                logging.critical('Queue(s) "%s" oversize, terminating program'
                                 % ", ".join(str(e) for e in oversize_queues))
                break

            logging.debug('---- Check all running threads')
            running_threads = set([thread.getName() for thread in threading.enumerate()])
            if started_threads - running_threads:
                logging.critical('Module(s) "%s" no longer running, terminating program'
                                 % ", ".join(str(e) for e in (started_threads - running_threads)))
                break

    except KeyError as e:
        logging.critical('Missing key %s from configuration file' % str(e))
    except Exception as e:
        logging.critical('Exception caught %s: %s' % (type(e), e))
    finally:
        # Terminate all running plugins
        threads = [thread for thread in threading.enumerate()
                   if thread.isAlive() and thread.getName() in started_threads]
        for thread in threads:
            logging.info('Terminating %s' % thread.getName())
            thread.terminate.set()
            thread.join()

        logging.info("Program terminated")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="elog", description="An energy management program")
    parser.positionals.title = "arguments"
    parser.add_argument("config",
                        help="Full pathname or URL to the configuration file")
    parser.add_argument("--log",
                        metavar="LEVEL",
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'],
                        help="Set minimum log level: DEBUG, INFO, WARN, ERROR, CRITICAL")
    args = parser.parse_args()

    # myname = os.path.basename(sys.argv[0])
    # pidfile = '/tmp/%s' % myname  # any name
    # daemon = Daemonize(app=myname, pid=pidfile, action=main)
    # daemon.start()

    main()
