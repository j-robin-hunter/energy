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

import threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_graphql import GraphQLView
from data.database.dataloaders import *


class WebServer(threading.Thread):
    def __init__(self, config, schema, database):
        super(WebServer, self).__init__()
        self.config = config
        self.schema = schema
        self.database = database

    def run(self):
        app = Flask(__name__)
        CORS(app, resources={
            r'/graphql/*': {'origins': '*'},
            r'/config/*': {'origins': '*'}
        })

        @app.route('/')
        def root():
            return app.send_static_file('index.html')

        @app.route('/config/configuration')
        def configuration():
            return jsonify(self.config['configuration'])

        @app.route(self.config.get('shutdown', '/shutdown'))
        def shutdown():
            shutdown_server()
            return 'Server shutting down...'

        def shutdown_server(exception=None):
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            func()

        app.add_url_rule('/graphql',
                         view_func=
                         GraphQLView.as_view('graphql',
                                             schema=self.schema,
                                             graphiql=True,
                                             context=
                                             dict(database=self.database,
                                                  meterreadingloader=ReadingDataLoader(self.database),
                                                  meterreadingsbetweenloader=ReadingsBetweenDataLoader(self.database),
                                                  metertariffloader=TariffDataLoader(self.database),
                                                  metertariffbetweenloader=TariffBetweenDataLoader(self.database)
                                                  )
                                             )
                         )

        app.run(host='0.0.0.0', port=self.config['webserver']['port'], threaded=True)
