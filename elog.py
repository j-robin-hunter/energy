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
import threading
import argparse
import logging
import importlib
import time
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)-8s:%(filename)-20s:%(funcName)-15s: %(message)s"
)


def read_config(config):
    parse_result = urlparse(config)

    try:
        if parse_result.scheme in ('http', 'https'):
            logging.debug('---- HTTP(s) based configuration file')
            response = request.urlopen(config)
            config_data = response.read()
        else:
            logging.debug('---- File based configuration file')
            with open(config) as configFile:
                config_data = configFile.read()
        return json.loads(config_data)

    except Exception:
        raise


def load_plugins(config):
    # This file will iterate through the configuration file and extract details relating to each defined plugin.
    # The types of plugins allowed are defined in the configuration file with plugins themselves being entries
    # within an array for each type of plugin defined. Every plugin has to define a name and a config. The name
    # is used to dynamically load the actual plugin while the config is passed to the plugin. Only the plugin knows
    # how to interpret the config it is passed
    config_branch = 'root'
    plugins = []
    try:
        for plugin_type in config['plugin_types']:
            config_branch = plugin_type
            logging.debug('Processing "%s" plugin types' % config_branch)
            for plugin_entry in range(len(config[plugin_type])):
                config_branch = '%s entry: %d of %d' % (plugin_type, plugin_entry + 1, len(config[plugin_type]))
                logging.debug('--- %s' % config_branch)
                plugin_name = config[plugin_type][plugin_entry]['name']

                # We now have the name of the plugin so the identifying string for any errors in the configuration
                # file can be improved to help locate any key errors etc that might occur
                config_branch = '%s %s entry' % (plugin_name, plugin_type)
                logging.debug('--- "%s" importing plugin code module' % config_branch)

                module = importlib.import_module(plugin_name)
                plugin = dict(
                    type=plugin_type,
                    name=plugin_name,
                    module=module,
                    measurements=config[plugin_type][plugin_entry]['measurements'],
                    config=config[plugin_type][plugin_entry]['config']
                )
                logging.debug('--- %s' % str(plugin))
                plugins.append(plugin)
        return plugins
    except TypeError:
        logging.error('Error in "%s" section of configuration file' % config_branch)
        raise RuntimeError('Unable to load program plugins due to configuration file error')
    except ModuleNotFoundError as e:
        logging.error('%s in %s in configuration file' % (str(e), config_branch))
        raise RuntimeError(str(e))
    except KeyError as e:
        logging.critical('Missing key %s in %s of configuration file' % (str(e), config_branch))
        raise
    except Exception:
        raise


def create_queues(config):
    try:
        queues = {}
        for measurement in config['measurements']:
            logging.debug('---- "%s"' % measurement)
            queues[measurement] = Queue()
        return queues
    except Exception:
        raise


def load_formatters(config):
    config_branch = 'root'
    formatters = []
    try:
        for formatter_entry in config.get('formatters', []):
            config_branch = 'formatters'
            formatter_name = formatter_entry['name']

            # We now have the name so update error string to aid finding config error
            config_branch = 'formatter %s' % formatter_name
            logging.debug('--- Formatter "%s"' % formatter_name)
            module = importlib.import_module(formatter_name)
            formatter = dict(
                formatter_name=formatter_name,
                formatter_module=module,
                formatter_instance=None,
                plugin_name=formatter_entry['format']['plugin_name'],
                measurement=formatter_entry['format']['measurement']
            )
            logging.debug(str(formatter))
            formatters.append(formatter)
        return formatters
    except TypeError:
        logging.error('Error in "%s" section of configuration file' % config_branch)
        raise RuntimeError('Unable to load formatters due to configuration file error')
    except ModuleNotFoundError as e:
        logging.error('%s in %s in configuration file' % (str(e), config_branch))
        raise RuntimeError(str(e))
    except KeyError as e:
        logging.critical('Missing key %s in %s of configuration file' % (str(e), config_branch))
        raise
    except Exception:
        raise


def main():
    plugin_instances = []
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

        #  If a file handler is set then remove console. It will be automnatically readded if no handlers are present
        log_file = config.get('log_file', None)
        if log_file is not None:
            logging.debug('Writing log output to file: "%s"' % log_file)
        if log_file is not None:
            lsstout = logger.handlers[0]
            lhhdlr = RotatingFileHandler(log_file, maxBytes=1000000000, backupCount=5)
            logger.addHandler(lhhdlr)
            logger.removeHandler(lsstout)

        logging.info('Creating measurement queues')
        queues = create_queues(config)
        if not queues:
            raise RuntimeError('No measurements defined in configuration file')

        logging.info('Loading formatters')
        formatters = load_formatters(config)
        if not formatters:
            logging.info('No formatters have defined in configuration file')

        logging.info('Loading plugins')
        plugins = load_plugins(config)

        logging.info('Starting plugin worker threads')
        for plugin in plugins:
            logging.debug('Processing for worker "%s"' % plugin['name'] + ' ' + plugin['type'])
            # Check that the measurement queues defined in the plugin configuration all exist as
            # defined measurement queues and add these to the list of plugin_queues
            plugin_queues = {}
            logging.debug('---- Verifying queues have been defined')
            for measurement in plugin['measurements']:
                try:
                    plugin_queues[measurement] = queues[measurement]
                    logging.debug('---- Plugin is using measurement queue "%s"' % measurement)
                except Exception:
                    raise RuntimeError(
                        'Plugin "%s" is using measurement "%s" that has not been defined in the configuration file' %
                        (plugin['name'], measurement))

            logging.debug('---- Installing plugin formatters')
            plugin_formatters = []
            for formatter in formatters:
                if formatter['plugin_name'] == plugin['name']:
                    logging.debug('---- Plugin is using formatter "%s"', formatter['formatter_name'])
                    if formatter['measurement'] not in plugin['measurements']:
                        raise RuntimeError(
                            'Formatter "%s" measurement "%s" is not configured to use any plugin "%s" measurements' %
                            (formatter['formatter_name'], formatter['measurement'], plugin['name']))
                    if formatter['formatter_instance'] is None:
                        logging.debug('------ Creating formatter instance')
                        formatter['formatter_instance'] = formatter['formatter_module'].Formatter(
                            formatter['formatter_name'],
                            formatter['measurement'])
                    plugin_formatters.append(formatter['formatter_instance'])
            logging.debug('---- Creating plugin instance')
            instance = plugin["module"].Plugin(
                plugin['name'] + ' ' + plugin['type'],
                plugin_queues,
                plugin_formatters,
                plugin['config'])
            plugin_instances.append(instance)
            instance.setName(plugin['name'])
            logging.debug('---- Starting plugin worker thread')
            instance.start()

        logging.info('Program started')

        # Stay in a loop monitoring queue size to ensure that no queue start to overfill.
        # If it does terminate program. While in loop verify that all plugin threads are still
        # running. If any have stopped then terminate the program
        max_queue_exceeded = False
        plugin_failure = False
        while not max_queue_exceeded and not plugin_failure:
            logging.debug('Verifying program run status')
            time.sleep(10)
            for queuename, queue in queues.items():
                logging.debug('Queue "%s" contains %d records' % (queuename, queue.qsize()))
                if queue.qsize() >= max_queue_size:
                    logging.critical('Queue "%s" is oversize, terminating program' % queuename)
                    max_queue_exceeded = True

            running_threads = []
            for thread in threading.enumerate():
                running_threads.append(thread.getName())
            for plugin in plugins:
                if plugin['name'] not in running_threads:
                    plugin_failure = True
                    logging.critical('Plugin "%s" thread has terminated, terminating program' % plugin['name'])
                    break

    except KeyError as e:
        logging.critical('Missing key %s from configuration file' % str(e))
    except Exception as e:
        logging.critical('Exception caught %s: %s' % (type(e), e))
    finally:
        # Terminate all running plugins
        for instance in plugin_instances:
            if instance.isAlive():
                instance.terminate.set()
                instance.join()
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
