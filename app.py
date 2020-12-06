from flask import Flask, request, jsonify
from flask_restful import Resource, Api

import numpy as np
import pandas as pd
import os
import datetime as dt
from datetime import date, timedelta

import sqlalchemy 
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()
Base.prepare(engine, reflect = True)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

app = Flask(__name__)
api = Api(app)

class Index(Resource):
    def get(self):
            routes = []
            for rule in app.url_map.iter_rules():
                routes.append('%s' % rule)
            return routes

class PrecipitationAnalysis(Resource):
    def get(self):
        #this is the first and last date
        day_one = session.query(measurement.date).order_by(measurement.date).first()
        last_day_str = session.query(measurement.date).order_by(measurement.date.desc()).first()
        last_date = dt.datetime.strptime(*last_day_str, '%Y-%m-%d')
        ein_jahr = last_date - dt.timedelta(days=365)
        ein_jahr_str = dt.datetime.strftime(ein_jahr,'%Y-%m-%d')
        ein_jahr_frag = session.query(measurement.date, measurement.prcp)\
                        .filter(measurement.date >= ein_jahr_str)\
                        .filter(measurement.prcp.isnot(None))\
                        .order_by(measurement.date).all()

        ein_jahr_df = pd.DataFrame(ein_jahr_frag, columns =['date', 'prcp'])

        ein_jahr_df.set_index('date', inplace=True)
        ein_jahr_df.groupby(['date']).sum()
        ein_jahr_dict = ein_jahr_df.to_dict()

        resp = jsonify(ein_jahr_dict)
        resp.status_code = 200
        print(resp)
        return resp


class Stations(Resource):
    def get(self):
        resp = {}

        station_query = session.query(station.station, station.name)
        station_df = pd.DataFrame(station_query, columns =['station', 'name'])
        station_df.set_index('station', inplace=True)
        resp = jsonify(station_df.to_dict())
        resp.status_code = 200
        print(resp)
        return resp

class TemperatureObservations(Resource):
    def get(self):
        resp = {}

        act_stxn_select = [measurement.station, func.count(measurement.station), station.name]

        act_stxn = session.query(*act_stxn_select).filter(station.station == measurement.station)\
                        .group_by(measurement.station).order_by(func.count(measurement.station)\
                        .desc()).all()

        most_act_stxn = act_stxn[0][0]

        act_stxn = session.query(*act_stxn_select).filter(station.station == most_act_stxn)\
                        .group_by(measurement.station).order_by(func.count(measurement.station)\
                        .desc()).all()

        day_one = session.query(measurement.date).order_by(measurement.date).first()
        last_day_str = session.query(measurement.date).order_by(measurement.date.desc()).first()
        #last_day
        #one year from last date...
        #since the date is a string, we need to use the strptime function
        last_date = dt.datetime.strptime(*last_day_str, '%Y-%m-%d')
        #last_date

        ein_jahr = last_date - dt.timedelta(days=365)
        ein_jahr_str = dt.datetime.strftime(ein_jahr,'%Y-%m-%d')
        ein_jahr_frag = session.query(measurement.date, measurement.tobs)\
                        .filter(measurement.date >= ein_jahr_str)\
                        .filter(measurement.station == most_act_stxn)\
                        .order_by(measurement.date).all()

        ein_jahr_df = pd.DataFrame(ein_jahr_frag, columns =['date', 'tobs'])

        ein_jahr_df.set_index('date', inplace=True)
        ein_jahr_dict = ein_jahr_df.to_dict()

        resp = jsonify(ein_jahr_dict)
        resp.status_code = 200
        print(resp)
        return resp

class TemperaturesDate(Resource):
    def get(self, start):
        resp = {}

        min_temp = session.query(measurement.date, func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs))\
            .filter(measurement.date == start).all()
                
        min_df = pd.DataFrame(min_temp, columns =['date', 'min', 'max', 'avg'])
        min_df.set_index('date', inplace=True)
        resp = jsonify(min_df.to_dict())
        resp.status_code = 200
        print(resp)
        return resp

class TemperaturesRange(Resource):
    def get(self, start, end):
        resp = {}

        min_temp = session.query(measurement.date, func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs))\
            .filter(measurement.date >= start)\
            .filter(measurement.date <= end).all()
                
        min_df = pd.DataFrame(min_temp, columns =['date', 'min', 'max', 'avg'])
        min_df.set_index('date', inplace=True)
        resp = jsonify(min_df.to_dict())
        resp.status_code = 200
        print(resp)
        return resp

api.add_resource(Index, '/')
api.add_resource(PrecipitationAnalysis, '/api/v1.0/precipitation')
api.add_resource(Stations, '/api/v1.0/stations')
api.add_resource(TemperatureObservations, '/api/v1.0/tobs')
api.add_resource(TemperaturesDate, '/api/v1.0/<start>')
api.add_resource(TemperaturesRange, '/api/v1.0/<start>/<end>') 

if __name__ == '__main__':
    app.run(debug=True)